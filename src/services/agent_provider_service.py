from __future__ import annotations

import importlib.metadata
import json
import logging
import tomllib
from pathlib import Path
from typing import Any

from src.agent.provider_registry import (
    PROVIDER_ORDER,
    PROVIDER_SPECS,
    ZAI_CODING_BASE_URL,
    ZAI_GENERAL_BASE_URL,
    ProviderRuntimeConfig,
    ProviderSpec,
    canonical_endpoint_fingerprint_for_config,
    default_base_url_for,
    is_zai_legacy_anthropic_base_url,
    normalize_ollama_base_url,
    normalize_provider_plain_fields,
    normalize_urlish,
    provider_spec,
    runtime_options_for_config,
)
from src.config import AppConfig, resolve_session_encryption_secret
from src.database import Database
from src.security import SessionCipher, decrypt_failure_status, log_expected_decrypt_failure
from src.services.provider_model_cache import (
    MODEL_CACHE_SETTINGS_KEY,
    ProviderModelCacheEntry,
    ProviderModelCacheMixin,
)
from src.services.provider_model_compatibility import (
    COMMUNITY_COMPATIBILITY_CATALOG_PATH,
    MODEL_COMPATIBILITY_FRESHNESS_HOURS,
    ProviderModelCompatibilityMixin,
    ProviderModelCompatibilityRecord,
)
from src.utils.json import safe_json_dumps

logger = logging.getLogger(__name__)

PROVIDER_SETTINGS_KEY = "agent_deepagents_providers_v1"
_PACKAGE_NAME = "tg-agent"
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_PYPROJECT_PATH = _PROJECT_ROOT / "pyproject.toml"
_PROVIDER_SECRET_ACTION = "restore_key_or_reenter_secret"

# Re-exported so importers keep using src.services.agent_provider_service.* (the dataclasses
# and constants moved into the mixin modules in #689). __all__ marks these intentional.
__all__ = [
    "PROVIDER_SETTINGS_KEY",
    "MODEL_CACHE_SETTINGS_KEY",
    "MODEL_COMPATIBILITY_FRESHNESS_HOURS",
    "COMMUNITY_COMPATIBILITY_CATALOG_PATH",
    "ProviderConfigService",
    "ProviderModelCacheEntry",
    "ProviderModelCompatibilityRecord",
    "ProviderModelCacheMixin",
    "ProviderModelCompatibilityMixin",
]


class ProviderConfigService(ProviderModelCacheMixin, ProviderModelCompatibilityMixin):
    def __init__(self, db: Database, config: AppConfig) -> None:
        self._db = db
        self._config = config
        secret = resolve_session_encryption_secret(config)
        self._cipher = SessionCipher(secret) if secret else None

    @property
    def writes_enabled(self) -> bool:
        return self._cipher is not None

    @property
    def provider_specs(self) -> dict[str, ProviderSpec]:
        return PROVIDER_SPECS

    async def load_provider_configs(self) -> list[ProviderRuntimeConfig]:
        raw = await self._db.get_setting(PROVIDER_SETTINGS_KEY)
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Invalid provider registry JSON in settings", exc_info=True)
            return []
        if not isinstance(parsed, list):
            return []

        configs: list[ProviderRuntimeConfig] = []
        for item in parsed:
            if not isinstance(item, dict):
                continue
            provider = str(item.get("provider", "")).strip()
            spec = provider_spec(provider)
            if spec is None:
                continue
            plain_fields = {
                field.name: str(item.get("plain_fields", {}).get(field.name, "")).strip()
                for field in spec.plain_fields
            }
            last_validation_error = str(item.get("last_validation_error", "")).strip()
            secret_fields, secret_fields_enc_preserved, secret_status = self._decrypt_secret_fields(
                item.get("secret_fields_enc", {}),
                spec,
                provider,
            )
            if secret_status != "ok":
                last_validation_error = (
                    "Saved secret values could not be decrypted. "
                    "Re-enter them with the current SESSION_ENCRYPTION_KEY."
                )
            configs.append(
                ProviderRuntimeConfig(
                    provider=provider,
                    enabled=bool(item.get("enabled", True)),
                    priority=int(item.get("priority", 0)),
                    selected_model=str(item.get("selected_model", "")).strip(),
                    plain_fields=plain_fields,
                    secret_fields=secret_fields,
                    last_validation_error=last_validation_error,
                    secret_status=secret_status,
                    secret_fields_enc_preserved=secret_fields_enc_preserved,
                )
            )
        return sorted(configs, key=self._config_sort_key)

    async def save_provider_configs(self, configs: list[ProviderRuntimeConfig]) -> None:
        if not self.writes_enabled:
            raise RuntimeError("SESSION_ENCRYPTION_KEY is required to manage deepagents providers.")
        payload = []
        for cfg in sorted(configs, key=self._config_sort_key):
            spec = PROVIDER_SPECS[cfg.provider]
            payload.append(
                {
                    "provider": cfg.provider,
                    "enabled": bool(cfg.enabled),
                    "priority": int(cfg.priority),
                    "selected_model": cfg.selected_model,
                    "plain_fields": {
                        field.name: cfg.plain_fields.get(field.name, "").strip()
                        for field in spec.plain_fields
                    },
                    "secret_fields_enc": self._encrypt_secret_fields(
                        cfg.secret_fields,
                        spec,
                        preserved=cfg.secret_fields_enc_preserved,
                    ),
                    "last_validation_error": cfg.last_validation_error,
                }
            )
        await self._db.set_setting(PROVIDER_SETTINGS_KEY, safe_json_dumps(payload, ensure_ascii=False))

    def build_provider_views(
        self,
        configs: list[ProviderRuntimeConfig],
        cache: dict[str, ProviderModelCacheEntry],
    ) -> list[dict[str, Any]]:
        views = []
        for cfg in configs:
            spec = PROVIDER_SPECS[cfg.provider]
            cache_entry = cache.get(cfg.provider, self._empty_model_cache_entry(cfg.provider))
            models = list(cache_entry.models or list(spec.static_models))
            if cfg.selected_model and cfg.selected_model not in models:
                models.insert(0, cfg.selected_model)
            model_options = [self._build_model_option(model, cfg, cache_entry) for model in models]
            selected_compatibility = self._compatibility_view(
                self.get_compatibility_record(cache_entry, cfg, fresh_only=False)
            )
            views.append(
                {
                    "provider": cfg.provider,
                    "display_name": spec.display_name,
                    "package_name": spec.package_name,
                    "enabled": cfg.enabled,
                    "priority": cfg.priority,
                    "selected_model": cfg.selected_model,
                    "plain_fields": [
                        {
                            "name": field.name,
                            "label": field.label,
                            "required": field.required,
                            "placeholder": field.placeholder,
                            "help_text": field.help_text,
                            "value": cfg.plain_fields.get(field.name, ""),
                        }
                        for field in spec.plain_fields
                    ],
                    "secret_fields": [
                        {
                            "name": field.name,
                            "label": field.label,
                            "required": field.required,
                            "placeholder": field.placeholder,
                            "help_text": field.help_text,
                            "masked": (
                                "••••••••"
                                if (
                                    cfg.secret_fields.get(field.name)
                                    or cfg.secret_fields_enc_preserved.get(field.name)
                                )
                                else ""
                            ),
                            "has_value": bool(
                                cfg.secret_fields.get(field.name)
                                or cfg.secret_fields_enc_preserved.get(field.name)
                            ),
                            "decrypt_failed": bool(cfg.secret_fields_enc_preserved.get(field.name))
                            and not cfg.secret_fields.get(field.name),
                        }
                        for field in spec.secret_fields
                    ],
                    "models": models,
                    "model_options": model_options,
                    "model_source": cache_entry.source,
                    "model_fetch_error": cache_entry.error,
                    "model_fetched_at": cache_entry.fetched_at,
                    "selected_compatibility": selected_compatibility,
                    "selected_compatibility_warning": self.compatibility_warning_for_config(
                        cfg,
                        cache_entry,
                    ),
                    "last_validation_error": cfg.last_validation_error,
                    "secret_status": cfg.secret_status,
                    "requires_secret_reentry": cfg.secret_status != "ok",
                }
            )
        return views

    def parse_provider_form(
        self,
        form: Any,
        existing_configs: list[ProviderRuntimeConfig],
    ) -> list[ProviderRuntimeConfig]:
        existing_map = {cfg.provider: cfg for cfg in existing_configs}
        configs: list[ProviderRuntimeConfig] = []
        for provider_name in PROVIDER_ORDER:
            cfg = self._parse_provider_form_item(
                form,
                existing_map,
                provider_name,
                require_present=True,
            )
            if cfg is not None:
                configs.append(cfg)
        return sorted(configs, key=self._config_sort_key)

    def parse_single_provider_form(
        self,
        form: Any,
        existing_configs: list[ProviderRuntimeConfig],
        provider_name: str,
    ) -> ProviderRuntimeConfig:
        existing_map = {cfg.provider: cfg for cfg in existing_configs}
        cfg = self._parse_provider_form_item(
            form, existing_map, provider_name, require_present=False
        )
        if cfg is None:
            raise RuntimeError(f"Unknown provider: {provider_name}")
        return cfg

    def create_empty_config(self, provider_name: str, priority: int) -> ProviderRuntimeConfig:
        spec = provider_spec(provider_name)
        if spec is None:
            raise RuntimeError(f"Unknown provider: {provider_name}")
        default_model = spec.static_models[0] if spec.static_models else ""
        return ProviderRuntimeConfig(
            provider=provider_name,
            enabled=True,
            priority=priority,
            selected_model=default_model,
            plain_fields={field.name: "" for field in spec.plain_fields},
            secret_fields={},
            secret_status="ok",
            secret_fields_enc_preserved={},
        )

    def validate_provider_config(self, cfg: ProviderRuntimeConfig) -> str:
        spec = provider_spec(cfg.provider)
        if spec is None:
            return f"Unknown provider: {cfg.provider}"
        if cfg.secret_status != "ok":
            return (
                "Saved secret values could not be decrypted. "
                "Restore SESSION_ENCRYPTION_KEY or re-enter provider secrets."
            )
        if cfg.provider == "zai":
            base_url = cfg.plain_fields.get("base_url", "").strip()
            if is_zai_legacy_anthropic_base_url(base_url):
                return (
                    "This URL is the Z.AI Anthropic-compatible proxy. Configure the "
                    "anthropic provider with this URL instead, or use the OpenAI-compatible "
                    f"endpoint {ZAI_GENERAL_BASE_URL}. Coding Plan users can explicitly set "
                    f"{ZAI_CODING_BASE_URL}."
                )
        for spec_field in spec.plain_fields:
            if spec_field.required and not cfg.plain_fields.get(spec_field.name, "").strip():
                return f"Missing required field: {spec_field.label}"
        for spec_field in spec.secret_fields:
            if spec_field.required and not cfg.secret_fields.get(spec_field.name, "").strip():
                return f"Missing required secret: {spec_field.label}"
        if not cfg.selected_model:
            return "Model is required."
        return ""

    def deepagents_runtime_options(
        self,
        cfg: ProviderRuntimeConfig,
    ) -> tuple[str, dict[str, object]]:
        # Single source of truth shared with the runtime-registry stack (#658).
        return runtime_options_for_config(cfg)

    def canonical_endpoint_fingerprint(self, cfg: ProviderRuntimeConfig) -> str | None:
        return canonical_endpoint_fingerprint_for_config(cfg)

    def default_base_url_for(self, provider_name: str) -> str:
        return default_base_url_for(provider_name)

    def normalize_ollama_base_url(self, base_url: str, api_key: str = "") -> str:
        return normalize_ollama_base_url(base_url, api_key)

    def _config_sort_key(self, cfg: ProviderRuntimeConfig) -> tuple[int, int]:
        try:
            provider_index = PROVIDER_ORDER.index(cfg.provider)
        except ValueError:
            provider_index = len(PROVIDER_ORDER)
        return (cfg.priority, provider_index)

    def _normalize_plain_fields(
        self,
        provider_name: str,
        plain_fields: dict[str, str],
        secret_fields: dict[str, str],
    ) -> dict[str, str]:
        return normalize_provider_plain_fields(provider_name, plain_fields, secret_fields)

    def normalize_provider_plain_fields(self, cfg: ProviderRuntimeConfig) -> dict[str, str]:
        return normalize_provider_plain_fields(cfg.provider, cfg.plain_fields, cfg.secret_fields)

    def _normalize_urlish(self, raw: str) -> str:
        return normalize_urlish(raw)

    def _parse_provider_form_item(
        self,
        form: Any,
        existing_map: dict[str, ProviderRuntimeConfig],
        provider_name: str,
        *,
        require_present: bool,
    ) -> ProviderRuntimeConfig | None:
        spec = provider_spec(provider_name)
        if spec is None:
            return None
        if (
            require_present
            and str(form.get(f"provider_present__{provider_name}", "")).strip() != "1"
        ):
            return None
        current = existing_map.get(provider_name)
        priority_raw = str(form.get(f"provider_priority__{provider_name}", "")).strip() or "0"
        try:
            priority = int(priority_raw)
        except ValueError:
            priority = 0
        if not require_present and current is not None:
            if f"provider_priority__{provider_name}" not in form:
                priority = current.priority
        secret_fields: dict[str, str] = {}
        secret_fields_enc_preserved: dict[str, str] = {}
        if current is not None:
            secret_fields.update(current.secret_fields)
            secret_fields_enc_preserved.update(current.secret_fields_enc_preserved)
        for spec_field in spec.secret_fields:
            submitted = str(
                form.get(f"provider_secret__{provider_name}__{spec_field.name}", "")
            ).strip()
            if submitted:
                secret_fields[spec_field.name] = submitted
                secret_fields_enc_preserved.pop(spec_field.name, None)
        secret_status = (
            "ok"
            if not secret_fields_enc_preserved
            else (current.secret_status if current is not None else "decrypt_failed")
        )
        plain_fields = {}
        for spec_field in spec.plain_fields:
            key = f"provider_field__{provider_name}__{spec_field.name}"
            if key in form:
                plain_fields[spec_field.name] = str(form.get(key, "")).strip()
            elif not require_present and current is not None:
                plain_fields[spec_field.name] = current.plain_fields.get(spec_field.name, "")
            else:
                plain_fields[spec_field.name] = ""
        enabled = str(form.get(f"provider_enabled__{provider_name}", "")).strip() == "1"
        if (
            not require_present
            and current is not None
            and f"provider_enabled__{provider_name}" not in form
        ):
            enabled = current.enabled
        selected_model = str(form.get(f"provider_model__{provider_name}", "")).strip()
        if (
            not require_present
            and current is not None
            and f"provider_model__{provider_name}" not in form
        ):
            selected_model = current.selected_model
        return ProviderRuntimeConfig(
            provider=provider_name,
            enabled=enabled,
            priority=priority,
            selected_model=selected_model,
            plain_fields=plain_fields,
            secret_fields=secret_fields,
            secret_status=secret_status,
            secret_fields_enc_preserved=secret_fields_enc_preserved,
        )

    def _encrypt_secret_fields(
        self,
        values: dict[str, str],
        spec: ProviderSpec,
        *,
        preserved: dict[str, str] | None = None,
    ) -> dict[str, str]:
        encrypted: dict[str, str] = {}
        preserved = preserved or {}
        for spec_field in spec.secret_fields:
            value = values.get(spec_field.name, "").strip()
            if value:
                assert self._cipher is not None
                encrypted[spec_field.name] = self._cipher.encrypt(value)
            elif preserved.get(spec_field.name):
                encrypted[spec_field.name] = preserved[spec_field.name]
        return encrypted

    def _decrypt_secret_fields(
        self,
        payload: dict[str, Any],
        spec: ProviderSpec,
        provider: str | None = None,
    ) -> tuple[dict[str, str], dict[str, str], str]:
        values: dict[str, str] = {}
        preserved: dict[str, str] = {}
        secret_status = "ok"
        legacy_raise = provider is None
        provider_name = provider or spec.name
        for spec_field in spec.secret_fields:
            raw = str(payload.get(spec_field.name, "")).strip() if isinstance(payload, dict) else ""
            if not raw:
                continue
            if self._cipher is None:
                if legacy_raise:
                    raise ValueError(
                        "SESSION_ENCRYPTION_KEY is not configured; "
                        "cannot decrypt saved provider secrets."
                    )
                preserved[spec_field.name] = raw
                secret_status = "missing_key"
                log_expected_decrypt_failure(
                    logger,
                    resource="agent_provider",
                    identifier=provider_name,
                    status=secret_status,
                    action=_PROVIDER_SECRET_ACTION,
                    level=logging.DEBUG,
                )
                continue
            try:
                values[spec_field.name] = self._cipher.decrypt(raw)
            except ValueError as exc:
                preserved[spec_field.name] = raw
                status = decrypt_failure_status(exc)
                secret_status = "decrypt_failed" if status == "key_mismatch" else status
                log_expected_decrypt_failure(
                    logger,
                    resource="agent_provider",
                    identifier=provider_name,
                    status=status,
                    action=_PROVIDER_SECRET_ACTION,
                    level=logging.DEBUG,
                )
        return values, preserved, secret_status

    def _app_version(self) -> str:
        try:
            with _PYPROJECT_PATH.open("rb") as fh:
                data = tomllib.load(fh)
            version = data["project"]["version"]
            if isinstance(version, str) and version:
                return version
        except Exception:
            logger.warning("Failed to read app version from %s", _PYPROJECT_PATH, exc_info=True)
        try:
            return importlib.metadata.version(_PACKAGE_NAME)
        except importlib.metadata.PackageNotFoundError:
            pass
        except Exception:
            logger.warning("Failed to read installed package version", exc_info=True)
        return "unknown"
