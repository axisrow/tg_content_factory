from __future__ import annotations

import hashlib
import importlib.metadata
import json
import logging
import tomllib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import aiohttp

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
    normalize_zai_base_url,
    provider_spec,
)
from src.config import AppConfig, resolve_session_encryption_secret
from src.database import Database
from src.security import SessionCipher, decrypt_failure_status, log_expected_decrypt_failure
from src.utils.json import safe_json_dumps

logger = logging.getLogger(__name__)

PROVIDER_SETTINGS_KEY = "agent_deepagents_providers_v1"
MODEL_CACHE_SETTINGS_KEY = "agent_deepagents_model_cache_v1"
MODEL_COMPATIBILITY_FRESHNESS_HOURS = 24
_PACKAGE_NAME = "tg-agent"
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_PYPROJECT_PATH = _PROJECT_ROOT / "pyproject.toml"
COMMUNITY_COMPATIBILITY_CATALOG_PATH = (
    _PROJECT_ROOT / "data" / "deepagents_model_compatibility_catalog.json"
)
_PROVIDER_SECRET_ACTION = "restore_key_or_reenter_secret"

@dataclass(slots=True)
class ProviderModelCompatibilityRecord:
    model: str
    status: str
    reason: str = ""
    tested_at: str = ""
    config_fingerprint: str = ""
    probe_kind: str = "auto-select"


@dataclass(slots=True)
class ProviderModelCacheEntry:
    provider: str
    models: list[str]
    source: str
    fetched_at: str = ""
    error: str = ""
    compatibility: dict[str, ProviderModelCompatibilityRecord] = field(default_factory=dict)


class AgentProviderService:
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

    async def load_model_cache(self) -> dict[str, ProviderModelCacheEntry]:
        raw = await self._db.get_setting(MODEL_CACHE_SETTINGS_KEY)
        if not raw:
            return {}
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Invalid model cache JSON in settings", exc_info=True)
            return {}
        if not isinstance(parsed, dict):
            return {}
        cache: dict[str, ProviderModelCacheEntry] = {}
        for provider, item in parsed.items():
            if not isinstance(item, dict):
                continue
            compatibility: dict[str, ProviderModelCompatibilityRecord] = {}
            compat_raw = item.get("compatibility", {})
            if isinstance(compat_raw, dict):
                for fingerprint, record in compat_raw.items():
                    if not isinstance(record, dict):
                        continue
                    fingerprint_key = str(fingerprint).strip()
                    if not fingerprint_key:
                        continue
                    compatibility[fingerprint_key] = ProviderModelCompatibilityRecord(
                        model=str(record.get("model", "")).strip(),
                        status=str(record.get("status", "unknown")).strip() or "unknown",
                        reason=str(record.get("reason", "")).strip(),
                        tested_at=str(record.get("tested_at", "")).strip(),
                        config_fingerprint=(
                            str(record.get("config_fingerprint", "")).strip() or fingerprint_key
                        ),
                        probe_kind=str(record.get("probe_kind", "auto-select")).strip()
                        or "auto-select",
                    )
            cache[provider] = ProviderModelCacheEntry(
                provider=provider,
                models=[str(model) for model in item.get("models", []) if str(model).strip()],
                source=str(item.get("source", "static")).strip() or "static",
                fetched_at=str(item.get("fetched_at", "")).strip(),
                error=str(item.get("error", "")).strip(),
                compatibility=compatibility,
            )
        return cache

    async def save_model_cache(self, cache: dict[str, ProviderModelCacheEntry]) -> None:
        payload = {
            provider: {
                "models": entry.models,
                "source": entry.source,
                "fetched_at": entry.fetched_at,
                "error": entry.error,
                "compatibility": {
                    fingerprint: {
                        "model": record.model,
                        "status": record.status,
                        "reason": record.reason,
                        "tested_at": record.tested_at,
                        "config_fingerprint": record.config_fingerprint,
                        "probe_kind": record.probe_kind,
                    }
                    for fingerprint, record in entry.compatibility.items()
                },
            }
            for provider, entry in cache.items()
        }
        await self._db.set_setting(
            MODEL_CACHE_SETTINGS_KEY, safe_json_dumps(payload, ensure_ascii=False)
        )

    async def refresh_models_for_provider(
        self, provider_name: str, cfg: ProviderRuntimeConfig | None = None
    ) -> ProviderModelCacheEntry:
        spec = provider_spec(provider_name)
        if spec is None:
            raise RuntimeError(f"Unknown provider: {provider_name}")
        if cfg is None:
            configs = await self.load_provider_configs()
            cfg = next((item for item in configs if item.provider == provider_name), None)
        cache = await self.load_model_cache()
        existing_entry = cache.get(provider_name, self._empty_model_cache_entry(provider_name))
        live_error = ""
        try:
            models = await self._fetch_live_models(spec, cfg)
            models = sorted(dict.fromkeys(models))
            if not models:
                raise RuntimeError("live model list is empty")
            entry = ProviderModelCacheEntry(
                provider=provider_name,
                models=models,
                source="live",
                fetched_at=datetime.now(UTC).isoformat(),
                compatibility=existing_entry.compatibility,
            )
        except Exception as exc:
            live_error = str(exc)
            logger.warning("Provider model refresh failed for %s: %s", provider_name, exc)
            entry = ProviderModelCacheEntry(
                provider=provider_name,
                models=list(existing_entry.models or list(spec.static_models)),
                source="static cache",
                fetched_at=datetime.now(UTC).isoformat(),
                error=live_error,
                compatibility=existing_entry.compatibility,
            )
        cache[provider_name] = entry
        await self.save_model_cache(cache)
        return entry

    async def refresh_all_models(
        self,
        configs: list[ProviderRuntimeConfig] | None = None,
    ) -> dict[str, ProviderModelCacheEntry]:
        if configs is None:
            configs = await self.load_provider_configs()
        results: dict[str, ProviderModelCacheEntry] = {}
        for cfg in configs:
            results[cfg.provider] = await self.refresh_models_for_provider(cfg.provider, cfg)
        return results

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

    def build_compatibility_payload(
        self,
        cfg: ProviderRuntimeConfig,
        cache_entry: ProviderModelCacheEntry,
    ) -> dict[str, dict[str, Any]]:
        payload: dict[str, dict[str, Any]] = {}
        models = list(cache_entry.models)
        if cfg.selected_model and cfg.selected_model not in models:
            models.insert(0, cfg.selected_model)
        for model in models:
            record = self.get_compatibility_record(
                cache_entry,
                cfg,
                model=model,
                fresh_only=False,
            )
            if record is None:
                payload[model] = {"status": "unknown", "reason": "", "tested_at": ""}
                continue
            payload[model] = {
                "status": record.status,
                "reason": record.reason,
                "tested_at": record.tested_at,
                "config_fingerprint": record.config_fingerprint,
                "probe_kind": record.probe_kind,
                "is_fresh": self.is_compatibility_record_fresh(record),
            }
        return payload

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
        spec = provider_spec(cfg.provider)
        if spec is None:
            raise RuntimeError(f"Unknown provider: {cfg.provider}")

        model_provider = spec.resolved_runtime_provider
        extra: dict[str, object] = {
            key: value for key, value in cfg.plain_fields.items() if value.strip()
        }
        if cfg.provider == "ollama":
            api_key = cfg.secret_fields.get("api_key", "").strip()
            extra["base_url"] = self.normalize_ollama_base_url(
                str(extra.get("base_url", "")),
                api_key,
            )
            if api_key:
                extra["client_kwargs"] = {"headers": {"Authorization": f"Bearer {api_key}"}}
            return model_provider, extra

        extra.update({key: value for key, value in cfg.secret_fields.items() if value.strip()})
        if cfg.provider == "zai":
            raw_base_url = cfg.plain_fields.get("base_url", "")
            if is_zai_legacy_anthropic_base_url(raw_base_url):
                raise RuntimeError(
                    "This URL is the Z.AI Anthropic-compatible proxy. Configure the "
                    "anthropic provider with this URL instead, or use the OpenAI-compatible "
                    f"endpoint {ZAI_GENERAL_BASE_URL}. Coding Plan users can explicitly set "
                    f"{ZAI_CODING_BASE_URL}."
                )
            normalized_base_url = normalize_zai_base_url(raw_base_url)
            extra["base_url"] = normalized_base_url
        return model_provider, extra

    def config_fingerprint(
        self,
        cfg: ProviderRuntimeConfig,
        *,
        model: str | None = None,
    ) -> str:
        selected_model = (model or cfg.selected_model).strip()
        spec = provider_spec(cfg.provider)
        if spec is None:
            raise RuntimeError(f"Unknown provider: {cfg.provider}")
        normalized_plain = self.normalize_provider_plain_fields(cfg)
        secret_payload = {
            field.name: cfg.secret_fields.get(field.name, "").strip()
            for field in spec.secret_fields
            if cfg.secret_fields.get(field.name, "").strip()
        }
        auth_mode = "+".join(sorted(secret_payload)) if secret_payload else "no-auth"
        secret_hash = ""
        if secret_payload:
            secret_hash = hashlib.sha256(
                safe_json_dumps(secret_payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
            ).hexdigest()[:16]
        payload = {
            "provider": cfg.provider,
            "model": selected_model,
            "plain_fields": normalized_plain,
            "auth_mode": auth_mode,
            "secret_hash": secret_hash,
        }
        return hashlib.sha256(
            safe_json_dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
        ).hexdigest()

    def get_compatibility_record(
        self,
        cache_entry: ProviderModelCacheEntry | None,
        cfg: ProviderRuntimeConfig,
        *,
        model: str | None = None,
        fresh_only: bool = False,
        max_age_hours: int = MODEL_COMPATIBILITY_FRESHNESS_HOURS,
    ) -> ProviderModelCompatibilityRecord | None:
        if cache_entry is None:
            return None
        fingerprint = self.config_fingerprint(cfg, model=model)
        record = cache_entry.compatibility.get(fingerprint)
        if record is None:
            return None
        if fresh_only and not self.is_compatibility_record_fresh(
            record, max_age_hours=max_age_hours
        ):
            return None
        return record

    def compatibility_error_for_config(
        self,
        cfg: ProviderRuntimeConfig,
        cache_entry: ProviderModelCacheEntry | None,
    ) -> str:
        record = self.get_compatibility_record(cache_entry, cfg, fresh_only=True)
        if record is None or record.status != "unsupported":
            return ""
        return record.reason or "Compatibility probe marked this model as unsupported."

    def compatibility_warning_for_config(
        self,
        cfg: ProviderRuntimeConfig,
        cache_entry: ProviderModelCacheEntry | None,
    ) -> str:
        record = self.get_compatibility_record(cache_entry, cfg, fresh_only=False)
        if record is None:
            return "Совместимость модели с deepagents ещё не проверялась."
        if record.status == "unknown":
            return record.reason or "Совместимость модели с deepagents не подтверждена."
        if not self.is_compatibility_record_fresh(record):
            return (
                "Результат проверки совместимости устарел и будет обновлён при следующей проверке."
            )
        return ""

    def is_compatibility_record_fresh(
        self,
        record: ProviderModelCompatibilityRecord,
        *,
        max_age_hours: int = MODEL_COMPATIBILITY_FRESHNESS_HOURS,
    ) -> bool:
        if not record.tested_at:
            return False
        try:
            tested_at = datetime.fromisoformat(record.tested_at)
        except ValueError:
            return False
        if tested_at.tzinfo is None:
            tested_at = tested_at.replace(tzinfo=UTC)
        return datetime.now(UTC) - tested_at <= timedelta(hours=max_age_hours)

    async def ensure_model_compatibility(
        self,
        cfg: ProviderRuntimeConfig,
        *,
        probe_runner: Callable[
            [ProviderRuntimeConfig, str], Awaitable[ProviderModelCompatibilityRecord]
        ],
        probe_kind: str = "auto-select",
        force: bool = False,
        max_age_hours: int = MODEL_COMPATIBILITY_FRESHNESS_HOURS,
    ) -> ProviderModelCompatibilityRecord:
        cache = await self.load_model_cache()
        cache_entry = cache.get(cfg.provider, self._empty_model_cache_entry(cfg.provider))
        existing = None
        if not force:
            existing = self.get_compatibility_record(
                cache_entry,
                cfg,
                fresh_only=True,
                max_age_hours=max_age_hours,
            )
        if existing is not None:
            logger.info(
                "Compatibility cache hit: provider=%s model=%s status=%s kind=%s fresh=%s",
                cfg.provider,
                existing.model or cfg.selected_model or "<empty>",
                existing.status,
                existing.probe_kind,
                self.is_compatibility_record_fresh(existing, max_age_hours=max_age_hours),
            )
            return existing

        logger.info(
            "Compatibility cache miss: provider=%s model=%s kind=%s force=%s",
            cfg.provider,
            cfg.selected_model or "<empty>",
            probe_kind,
            force,
        )
        result = await probe_runner(cfg, probe_kind)
        if not result.config_fingerprint:
            result.config_fingerprint = self.config_fingerprint(cfg)
        if not result.model:
            result.model = cfg.selected_model
        if not result.tested_at:
            result.tested_at = datetime.now(UTC).isoformat()
        if not result.probe_kind:
            result.probe_kind = probe_kind

        cache_entry.compatibility[result.config_fingerprint] = result
        if result.model and result.model not in cache_entry.models:
            cache_entry.models.insert(0, result.model)
        cache[cfg.provider] = cache_entry
        await self.save_model_cache(cache)
        return result

    async def export_compatibility_catalog(
        self,
        configs: list[ProviderRuntimeConfig],
        cache: dict[str, ProviderModelCacheEntry] | None = None,
        *,
        path: Path | None = None,
    ) -> Path:
        if cache is None:
            cache = await self.load_model_cache()
        export_path = path or COMMUNITY_COMPATIBILITY_CATALOG_PATH
        providers_payload: list[dict[str, Any]] = []

        for cfg in sorted(configs, key=self._config_sort_key):
            endpoint_fingerprint = self.canonical_endpoint_fingerprint(cfg)
            if endpoint_fingerprint is None:
                continue
            cache_entry = cache.get(cfg.provider)
            if cache_entry is None:
                continue
            models = list(cache_entry.models)
            if cfg.selected_model and cfg.selected_model not in models:
                models.insert(0, cfg.selected_model)
            model_payload: list[dict[str, Any]] = []
            for model in models:
                record = self.get_compatibility_record(
                    cache_entry,
                    cfg,
                    model=model,
                    fresh_only=False,
                )
                if record is None:
                    continue
                model_payload.append(
                    {
                        "model": model,
                        "status": record.status,
                        "reason": record.reason,
                        "tested_at": record.tested_at,
                    }
                )
            if not model_payload:
                continue
            providers_payload.append(
                {
                    "provider": cfg.provider,
                    "endpoint_fingerprint": endpoint_fingerprint,
                    "models": model_payload,
                }
            )

        payload = {
            "generated_at": datetime.now(UTC).isoformat(),
            "generated_by": f"tg-agent {self._app_version()}",
            "providers": providers_payload,
        }
        export_path.write_text(
            safe_json_dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        return export_path

    def canonical_endpoint_fingerprint(self, cfg: ProviderRuntimeConfig) -> str | None:
        return canonical_endpoint_fingerprint_for_config(cfg)

    def default_base_url_for(self, provider_name: str) -> str:
        return default_base_url_for(provider_name)

    def normalize_ollama_base_url(self, base_url: str, api_key: str = "") -> str:
        return normalize_ollama_base_url(base_url, api_key)

    async def _fetch_live_models(
        self,
        spec: ProviderSpec,
        cfg: ProviderRuntimeConfig | None,
    ) -> list[str]:
        provider = spec.name
        if provider == "zai":
            assert cfg is not None
            return await self._fetch_zai_models(
                cfg.plain_fields.get("base_url", ""),
                cfg.secret_fields.get("api_key", ""),
            )
        if spec.openai_compatible and spec.default_base_url:
            assert cfg is not None
            base_url = cfg.plain_fields.get("base_url", "").strip() or spec.default_base_url
            return await self._fetch_openai_models(base_url, cfg.secret_fields.get("api_key", ""))
        if provider == "anthropic":
            assert cfg is not None
            return await self._fetch_anthropic_models(cfg.secret_fields.get("api_key", ""))
        if provider == "google_genai":
            assert cfg is not None
            return await self._fetch_google_genai_models(cfg.secret_fields.get("api_key", ""))
        if provider == "cohere":
            assert cfg is not None
            return await self._fetch_cohere_models(cfg.secret_fields.get("api_key", ""))
        if provider == "ollama":
            assert cfg is not None
            return await self._fetch_ollama_models(
                cfg.plain_fields.get("base_url", ""),
                cfg.secret_fields.get("api_key", ""),
            )
        if provider == "huggingface":
            api_key = cfg.secret_fields.get("api_key", "") if cfg else ""
            return await self._fetch_huggingface_models(api_key)
        raise RuntimeError("live model fetch adapter is not available for this provider yet")

    async def _fetch_json(self, url: str, headers: dict[str, str] | None = None) -> Any:
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, headers=headers) as response:
                response.raise_for_status()
                return await response.json()

    async def _fetch_openai_models(self, base_url: str, api_key: str) -> list[str]:
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        payload = await self._fetch_json(base_url.rstrip("/") + "/models", headers=headers)
        return [
            str(item.get("id", "")).strip() for item in payload.get("data", []) if item.get("id")
        ]

    async def _fetch_anthropic_models(self, api_key: str) -> list[str]:
        headers = {
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        }
        payload = await self._fetch_json("https://api.anthropic.com/v1/models", headers=headers)
        return [
            str(item.get("id", "")).strip() for item in payload.get("data", []) if item.get("id")
        ]

    async def _fetch_google_genai_models(self, api_key: str) -> list[str]:
        headers = {"x-goog-api-key": api_key} if api_key else None
        payload = await self._fetch_json(
            "https://generativelanguage.googleapis.com/v1beta/models",
            headers=headers,
        )
        models = []
        for item in payload.get("models", []):
            name = str(item.get("name", "")).strip()
            if name.startswith("models/"):
                name = name.split("/", 1)[1]
            if name:
                models.append(name)
        return models

    async def _fetch_cohere_models(self, api_key: str) -> list[str]:
        headers = {"Authorization": f"Bearer {api_key}"}
        payload = await self._fetch_json("https://api.cohere.com/v1/models", headers=headers)
        return [
            str(item.get("name", "")).strip()
            for item in payload.get("models", [])
            if item.get("name")
        ]

    async def _fetch_ollama_models(self, base_url: str, api_key: str) -> list[str]:
        resolved_base_url = self.normalize_ollama_base_url(base_url, api_key)
        headers = {"Authorization": f"Bearer {api_key}"} if api_key.strip() else None
        payload = await self._fetch_json(
            resolved_base_url.rstrip("/") + "/api/tags", headers=headers
        )
        return [
            str(item.get("name", "")).strip()
            for item in payload.get("models", [])
            if item.get("name")
        ]

    async def _fetch_huggingface_models(self, api_key: str) -> list[str]:
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else None
        payload = await self._fetch_json(
            "https://huggingface.co/api/models?pipeline_tag=text-generation&limit=50",
            headers=headers,
        )
        return [str(item.get("id", "")).strip() for item in payload if item.get("id")]

    async def _fetch_zai_models(self, base_url: str, api_key: str) -> list[str]:
        if is_zai_legacy_anthropic_base_url(base_url):
            raise RuntimeError(
                "The Z.AI Anthropic-compatible proxy does not expose OpenAI-compatible "
                f"/models. Use {ZAI_GENERAL_BASE_URL} or {ZAI_CODING_BASE_URL}."
            )
        resolved_base_url = normalize_zai_base_url(base_url)
        headers = {"Authorization": f"Bearer {api_key}"}
        payload = await self._fetch_json(
            f"{resolved_base_url.rstrip('/')}/models", headers=headers
        )
        return [
            str(item.get("id", "")).strip()
            for item in payload.get("data", [])
            if item.get("id")
        ]

    def _build_model_option(
        self,
        model: str,
        cfg: ProviderRuntimeConfig,
        cache_entry: ProviderModelCacheEntry,
    ) -> dict[str, Any]:
        record = self.get_compatibility_record(
            cache_entry,
            cfg,
            model=model,
            fresh_only=False,
        )
        label = model
        status = ""
        if record is not None:
            status = record.status
            label = f"{model} [{record.status}]"
        return {
            "value": model,
            "label": label,
            "status": status,
            "reason": record.reason if record is not None else "",
            "tested_at": record.tested_at if record is not None else "",
            "is_fresh": self.is_compatibility_record_fresh(record) if record is not None else False,
            "config_fingerprint": record.config_fingerprint if record is not None else "",
        }

    def _compatibility_view(
        self,
        record: ProviderModelCompatibilityRecord | None,
    ) -> dict[str, Any] | None:
        if record is None:
            return None
        return {
            "model": record.model,
            "status": record.status,
            "reason": record.reason,
            "tested_at": record.tested_at,
            "config_fingerprint": record.config_fingerprint,
            "probe_kind": record.probe_kind,
            "is_fresh": self.is_compatibility_record_fresh(record),
        }

    def _config_sort_key(self, cfg: ProviderRuntimeConfig) -> tuple[int, int]:
        try:
            provider_index = PROVIDER_ORDER.index(cfg.provider)
        except ValueError:
            provider_index = len(PROVIDER_ORDER)
        return (cfg.priority, provider_index)

    def _empty_model_cache_entry(self, provider_name: str) -> ProviderModelCacheEntry:
        spec = provider_spec(provider_name)
        if spec is None:
            raise RuntimeError(f"Unknown provider: {provider_name}")
        return ProviderModelCacheEntry(
            provider=provider_name,
            models=list(spec.static_models),
            source="static cache",
        )

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
        assert self._cipher is not None
        encrypted: dict[str, str] = {}
        preserved = preserved or {}
        for spec_field in spec.secret_fields:
            value = values.get(spec_field.name, "").strip()
            if value:
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
