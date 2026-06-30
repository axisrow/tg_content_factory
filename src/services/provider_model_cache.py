from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import aiohttp

from src.agent.provider_registry import (
    ZAI_CODING_BASE_URL,
    ZAI_GENERAL_BASE_URL,
    ProviderRuntimeConfig,
    ProviderSpec,
    is_zai_legacy_anthropic_base_url,
    normalize_zai_base_url,
    provider_spec,
)
from src.services.provider_model_compatibility import ProviderModelCompatibilityRecord
from src.utils.json import safe_json_dumps

if TYPE_CHECKING:
    from typing import Protocol

    from src.database import Database

    class ProviderConfigService(Protocol):
        _db: Database

        async def load_provider_configs(self) -> list[ProviderRuntimeConfig]: ...

        async def load_model_cache(self) -> dict[str, ProviderModelCacheEntry]: ...

        async def save_model_cache(self, cache: dict[str, ProviderModelCacheEntry]) -> None: ...

        def _empty_model_cache_entry(self, provider_name: str) -> ProviderModelCacheEntry: ...

        async def _fetch_live_models(
            self,
            spec: ProviderSpec,
            cfg: ProviderRuntimeConfig | None,
        ) -> list[str]: ...

        async def _fetch_zai_models(self, base_url: str, api_key: str) -> list[str]: ...

        async def _fetch_openai_models(self, base_url: str, api_key: str) -> list[str]: ...

        async def _fetch_anthropic_models(self, api_key: str) -> list[str]: ...

        async def _fetch_google_genai_models(self, api_key: str) -> list[str]: ...

        async def _fetch_cohere_models(self, api_key: str) -> list[str]: ...

        async def _fetch_ollama_models(self, base_url: str, api_key: str) -> list[str]: ...

        async def _fetch_huggingface_models(self, api_key: str) -> list[str]: ...

        async def refresh_models_for_provider(
            self,
            provider_name: str,
            cfg: ProviderRuntimeConfig | None = None,
        ) -> ProviderModelCacheEntry: ...

        def normalize_ollama_base_url(self, base_url: str, api_key: str = "") -> str: ...

        async def _fetch_json(self, url: str, headers: dict[str, str] | None = None) -> Any: ...

logger = logging.getLogger(__name__)

MODEL_CACHE_SETTINGS_KEY = "agent_deepagents_model_cache_v1"

_MODELS_HTTP_TIMEOUT = aiohttp.ClientTimeout(total=20)


async def fetch_json(url: str, headers: dict[str, str] | None = None) -> Any:
    """GET *url* and return parsed JSON. Shared by LLM and image model listing."""
    async with aiohttp.ClientSession(timeout=_MODELS_HTTP_TIMEOUT) as session:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            return await response.json()


def _parse_openai_model_ids(payload: Any) -> list[str]:
    """Extract ``data[].id`` from an OpenAI-compatible ``/models`` response."""
    data = payload.get("data", []) if isinstance(payload, dict) else []
    return [str(item.get("id", "")).strip() for item in data if item.get("id")]


async def fetch_openai_model_ids(base_url: str, api_key: str) -> list[str]:
    """Fetch model ids from an OpenAI-compatible ``/models`` endpoint.

    Standalone helper (no DB / provider-config dependency) so both the LLM
    provider cache and the image-generation service share one request path —
    the single source of truth for "ask OpenAI which models exist".
    """
    headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    payload = await fetch_json(base_url.rstrip("/") + "/models", headers=headers)
    return _parse_openai_model_ids(payload)


@dataclass(slots=True)
class ProviderModelCacheEntry:
    provider: str
    models: list[str]
    source: str
    fetched_at: str = ""
    error: str = ""
    compatibility: dict[str, ProviderModelCompatibilityRecord] = field(default_factory=dict)


class ProviderModelCacheMixin:
    """Model-cache persistence and live-model fetch adapters for provider configs.

    Mixed into ``ProviderConfigService``; ``self`` calls into sibling helpers
    (``self._fetch_json``, ``self._fetch_live_models``, ``self.normalize_ollama_base_url``)
    resolve through the concrete class so both instance and class monkeypatching
    of these methods keeps working.
    """

    async def load_model_cache(self: "ProviderConfigService") -> dict[str, ProviderModelCacheEntry]:
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

    async def save_model_cache(
        self: "ProviderConfigService", cache: dict[str, ProviderModelCacheEntry]
    ) -> None:
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
        self: "ProviderConfigService",
        provider_name: str,
        cfg: ProviderRuntimeConfig | None = None,
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
        self: "ProviderConfigService",
        configs: list[ProviderRuntimeConfig] | None = None,
    ) -> dict[str, ProviderModelCacheEntry]:
        if configs is None:
            configs = await self.load_provider_configs()
        results: dict[str, ProviderModelCacheEntry] = {}
        for cfg in configs:
            results[cfg.provider] = await self.refresh_models_for_provider(cfg.provider, cfg)
        return results

    def _empty_model_cache_entry(self, provider_name: str) -> ProviderModelCacheEntry:
        spec = provider_spec(provider_name)
        if spec is None:
            raise RuntimeError(f"Unknown provider: {provider_name}")
        return ProviderModelCacheEntry(
            provider=provider_name,
            models=list(spec.static_models),
            source="static cache",
        )

    async def _fetch_live_models(
        self: "ProviderConfigService",
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
        return _parse_openai_model_ids(payload)

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

    async def _fetch_ollama_models(
        self: "ProviderConfigService", base_url: str, api_key: str
    ) -> list[str]:
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
