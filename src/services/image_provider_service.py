"""Settings-driven image generation provider management.

Stores provider API keys (encrypted) in the DB settings table and builds
image adapters from them, falling back to environment variables when no
DB config exists for a given provider.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from src.config import AppConfig, resolve_session_encryption_secret
from src.database import Database
from src.security import SessionCipher
from src.utils.json import safe_json_dumps

if TYPE_CHECKING:
    from src.services.provider_adapters import ImageAdapter

logger = logging.getLogger(__name__)

SETTINGS_KEY = "image_providers_v1"


@dataclass(slots=True)
class ImageProviderSpec:
    name: str
    display_name: str
    env_vars: list[str]


IMAGE_PROVIDER_SPECS: dict[str, ImageProviderSpec] = {
    "together": ImageProviderSpec("together", "Together AI", ["TOGETHER_API_KEY"]),
    "huggingface": ImageProviderSpec("huggingface", "HuggingFace", ["HUGGINGFACE_API_KEY", "HUGGINGFACE_TOKEN"]),
    "openai": ImageProviderSpec("openai", "OpenAI", ["OPENAI_API_KEY"]),
    "replicate": ImageProviderSpec("replicate", "Replicate", ["REPLICATE_API_TOKEN"]),
}

IMAGE_PROVIDER_ORDER: list[str] = ["together", "huggingface", "openai", "replicate"]


@dataclass(slots=True)
class ImageProviderConfig:
    provider: str
    enabled: bool = True
    api_key: str = ""
    _api_key_enc_preserved: str = ""  # raw encrypted value kept on decrypt failure


def image_provider_spec(name: str) -> ImageProviderSpec | None:
    return IMAGE_PROVIDER_SPECS.get(name)


class ImageProviderService:
    def __init__(self, db: Database, config: AppConfig) -> None:
        self._db = db
        secret = resolve_session_encryption_secret(config)
        self._cipher = SessionCipher(secret) if secret else None

    @property
    def writes_enabled(self) -> bool:
        return self._cipher is not None

    # ── load / save ──

    async def load_provider_configs(self) -> list[ImageProviderConfig]:
        raw = await self._db.get_setting(SETTINGS_KEY)
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Invalid image provider JSON in settings", exc_info=True)
            return []
        if not isinstance(parsed, list):
            return []

        configs: list[ImageProviderConfig] = []
        for item in parsed:
            if not isinstance(item, dict):
                continue
            provider = str(item.get("provider", "")).strip()
            if provider not in IMAGE_PROVIDER_SPECS:
                continue
            api_key = ""
            preserved_enc = ""
            enc_key = str(item.get("api_key_enc", "")).strip()
            if enc_key:
                try:
                    api_key = self._cipher.decrypt(enc_key) if self._cipher else ""
                except ValueError:
                    logger.warning("Failed to decrypt image provider key for %s", provider, exc_info=True)
                    preserved_enc = enc_key  # keep raw so save doesn't destroy it
            configs.append(
                ImageProviderConfig(
                    provider=provider,
                    enabled=bool(item.get("enabled", True)),
                    api_key=api_key,
                    _api_key_enc_preserved=preserved_enc,
                )
            )
        return configs

    async def save_provider_configs(self, configs: list[ImageProviderConfig]) -> None:
        if not self.writes_enabled:
            raise RuntimeError("SESSION_ENCRYPTION_KEY is required to manage image providers.")
        payload = []
        for cfg in configs:
            entry: dict[str, Any] = {
                "provider": cfg.provider,
                "enabled": cfg.enabled,
            }
            if cfg.api_key.strip():
                assert self._cipher is not None
                entry["api_key_enc"] = self._cipher.encrypt(cfg.api_key.strip())
            elif cfg._api_key_enc_preserved:
                entry["api_key_enc"] = cfg._api_key_enc_preserved
            payload.append(entry)
        await self._db.set_setting(SETTINGS_KEY, safe_json_dumps(payload, ensure_ascii=False))

    # ── UI helpers ──

    def create_empty_config(self, provider_name: str) -> ImageProviderConfig:
        return ImageProviderConfig(provider=provider_name, enabled=True, api_key="")

    def build_provider_views(
        self, configs: list[ImageProviderConfig]
    ) -> list[dict[str, Any]]:
        views = []
        for cfg in configs:
            spec = IMAGE_PROVIDER_SPECS.get(cfg.provider)
            if spec is None:
                continue
            env_var_set = any(os.environ.get(v, "").strip() for v in spec.env_vars)
            views.append(
                {
                    "provider": cfg.provider,
                    "display_name": spec.display_name,
                    "enabled": cfg.enabled,
                    "has_key": bool(cfg.api_key.strip()),
                    "env_var_set": env_var_set,
                    "env_var_names": ", ".join(spec.env_vars),
                }
            )
        return views

    def parse_provider_form(
        self,
        form: Any,
        existing: list[ImageProviderConfig],
    ) -> list[ImageProviderConfig]:
        existing_map = {cfg.provider: cfg for cfg in existing}
        configs: list[ImageProviderConfig] = []
        for name in IMAGE_PROVIDER_ORDER:
            if not form.get(f"img_provider_present__{name}"):
                continue
            enabled = str(form.get(f"img_provider_enabled__{name}", "")).strip() == "1"
            new_key = str(form.get(f"img_provider_secret__{name}__api_key", "")).strip()
            old_cfg = existing_map.get(name)
            preserved = ""
            if new_key:
                api_key = new_key
            elif old_cfg:
                api_key = old_cfg.api_key
                preserved = old_cfg._api_key_enc_preserved
            else:
                api_key = ""
            configs.append(
                ImageProviderConfig(
                    provider=name, enabled=enabled, api_key=api_key, _api_key_enc_preserved=preserved,
                )
            )
        return configs

    # ── adapter construction ──

    def build_adapters(self, configs: list[ImageProviderConfig]) -> dict[str, "ImageAdapter"]:
        """Build image adapters from DB configs with env-var fallback."""
        from src.services.provider_adapters import (
            make_huggingface_image_adapter,
            make_openai_image_adapter,
            make_replicate_image_adapter,
            make_together_image_adapter,
        )

        _factories: dict[str, tuple[Any, list[str]]] = {
            "together": (make_together_image_adapter, ["TOGETHER_API_KEY"]),
            "huggingface": (make_huggingface_image_adapter, ["HUGGINGFACE_API_KEY", "HUGGINGFACE_TOKEN"]),
            "openai": (make_openai_image_adapter, ["OPENAI_API_KEY"]),
            "replicate": (make_replicate_image_adapter, ["REPLICATE_API_TOKEN"]),
        }

        adapters: dict[str, "ImageAdapter"] = {}
        configured_providers = {cfg.provider for cfg in configs}

        # 1. DB-configured providers
        for cfg in configs:
            if not cfg.enabled:
                continue
            key = cfg.api_key.strip()
            if not key:
                # fallback to env var even for configured providers
                spec = IMAGE_PROVIDER_SPECS.get(cfg.provider)
                if spec:
                    key = next(
                        (os.environ.get(v, "").strip() for v in spec.env_vars if os.environ.get(v, "").strip()),
                        "",
                    )
            if key and cfg.provider in _factories:
                factory, _ = _factories[cfg.provider]
                adapters[cfg.provider] = factory(key)

        # 2. Env-var fallback for unconfigured providers
        for name, (factory, env_vars) in _factories.items():
            if name in configured_providers:
                continue
            key = next((os.environ.get(v, "").strip() for v in env_vars if os.environ.get(v, "").strip()), "")
            if key:
                adapters[name] = factory(key)

        return adapters
