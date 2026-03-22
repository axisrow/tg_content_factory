from __future__ import annotations

import logging
import os
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from src.services.provider_adapters import ImageAdapter

logger = logging.getLogger(__name__)


class ImageGenerationService:
    """Routes image generation requests to provider-specific HTTP adapters.

    Adapters are auto-registered from environment variables at init time.
    Model strings use ``provider:model_id`` convention (e.g.
    ``together:black-forest-labs/FLUX.1-schnell``).  Without a prefix the
    first available adapter is used as fallback.
    """

    def __init__(self) -> None:
        self._adapters: dict[str, ImageAdapter] = {}
        self._register_from_env()

    # ── public API ──

    async def generate(self, model: str | None, text: str) -> str | None:
        """Generate an image for *text* using *model*.  Returns URL/path or ``None``."""
        if not text or not self._adapters:
            return None
        provider_name, model_id = self._parse_model_string(model)
        adapter = self._resolve_adapter(provider_name)
        if adapter is None:
            logger.warning("No image adapter available for model=%s", model)
            return None
        try:
            return await adapter(text, model_id)
        except Exception:
            logger.exception("Image generation failed (model=%s)", model)
            return None

    async def is_available(self) -> bool:
        return len(self._adapters) > 0

    def register_adapter(self, name: str, adapter: ImageAdapter) -> None:
        """Register an adapter manually (useful for tests)."""
        self._adapters[name] = adapter

    @property
    def adapter_names(self) -> list[str]:
        return list(self._adapters.keys())

    # ── internals ──

    def _register_from_env(self) -> None:
        from src.services.provider_adapters import (
            make_huggingface_image_adapter,
            make_openai_image_adapter,
            make_replicate_image_adapter,
            make_together_image_adapter,
        )

        together_key = os.environ.get("TOGETHER_API_KEY")
        if together_key:
            self._adapters["together"] = make_together_image_adapter(together_key)

        hf_key = os.environ.get("HUGGINGFACE_API_KEY") or os.environ.get("HUGGINGFACE_TOKEN")
        if hf_key:
            self._adapters["huggingface"] = make_huggingface_image_adapter(hf_key)

        openai_key = os.environ.get("OPENAI_API_KEY")
        if openai_key:
            self._adapters["openai"] = make_openai_image_adapter(openai_key)

        replicate_token = os.environ.get("REPLICATE_API_TOKEN")
        if replicate_token:
            self._adapters["replicate"] = make_replicate_image_adapter(replicate_token)

    @staticmethod
    def _parse_model_string(model: Optional[str]) -> tuple[Optional[str], str]:
        """Split ``'provider:model_id'`` → ``(provider, model_id)``.

        If no colon is present, returns ``(None, model)`` so the fallback
        adapter is used.
        """
        if not model:
            return None, ""
        if ":" in model:
            provider, _, model_id = model.partition(":")
            return provider, model_id
        return None, model

    def _resolve_adapter(self, provider_name: Optional[str]) -> Optional[ImageAdapter]:
        if provider_name and provider_name in self._adapters:
            return self._adapters[provider_name]
        if self._adapters:
            return next(iter(self._adapters.values()))
        return None
