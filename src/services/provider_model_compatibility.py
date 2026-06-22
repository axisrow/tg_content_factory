from __future__ import annotations

import hashlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.agent.provider_registry import (
    ProviderRuntimeConfig,
    provider_spec,
)
from src.utils.datetime import try_parse_datetime
from src.utils.json import safe_json_dumps

if TYPE_CHECKING:
    from src.services.provider_model_cache import ProviderModelCacheEntry

logger = logging.getLogger(__name__)

MODEL_COMPATIBILITY_FRESHNESS_HOURS = 24
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
COMMUNITY_COMPATIBILITY_CATALOG_PATH = (
    _PROJECT_ROOT / "data" / "deepagents_model_compatibility_catalog.json"
)


@dataclass(slots=True)
class ProviderModelCompatibilityRecord:
    model: str
    status: str
    reason: str = ""
    tested_at: str = ""
    config_fingerprint: str = ""
    probe_kind: str = "auto-select"


class ProviderModelCompatibilityMixin:
    """Compatibility-probe caching and catalog export for provider configs.

    Mixed into ``ProviderConfigService``; cross-mixin ``self`` calls
    (``load_model_cache``, ``save_model_cache``, ``_empty_model_cache_entry``,
    ``canonical_endpoint_fingerprint``, ``_config_sort_key``, ``_app_version``,
    ``normalize_provider_plain_fields``) resolve via the concrete class MRO.
    """

    def config_fingerprint(
        self,
        cfg: ProviderRuntimeConfig,
        *,
        model: str | None = None,
    ) -> str:
        # NB: this hashes the safe_json_dumps output, so it depends on that
        # serializer's exact byte format. The orjson migration (#956) made the
        # output compact, which invalidates fingerprints cached by the old spaced
        # format once — each provider is simply re-probed on first run, no data loss.
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
        tested_at = try_parse_datetime(record.tested_at)
        if tested_at is None:
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
