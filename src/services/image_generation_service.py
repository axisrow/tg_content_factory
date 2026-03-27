from __future__ import annotations

import asyncio
import logging
import os
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from src.services.provider_adapters import ImageAdapter
    from src.services.s3_store import S3Store

logger = logging.getLogger(__name__)


class ImageGenerationService:
    """Routes image generation requests to provider-specific HTTP adapters.

    Adapters are auto-registered from environment variables at init time.
    Model strings use ``provider:model_id`` convention (e.g.
    ``together:black-forest-labs/FLUX.1-schnell``).  Without a prefix the
    first available adapter is used as fallback.
    """

    def __init__(self, adapters: dict[str, "ImageAdapter"] | None = None) -> None:
        self._adapters: dict[str, ImageAdapter] = {}
        self._s3: S3Store | None = None
        if adapters is not None:
            self._adapters = dict(adapters)
        else:
            self._register_from_env()
        self._init_s3()

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
            result = await adapter(text, model_id)
        except (OSError, asyncio.TimeoutError) as exc:
            logger.warning("Image generation failed (model=%s): %s", model, exc)
            return None
        except Exception:
            logger.exception("Image generation unexpected error (model=%s)", model)
            return None
        # Upload local files to S3 when configured
        if result and not result.startswith("http") and getattr(self, "_s3", None) is not None:
            s3_url = await self._s3.upload_file(result)
            if s3_url:
                return s3_url
        return result

    async def is_available(self) -> bool:
        return len(self._adapters) > 0

    def register_adapter(self, name: str, adapter: ImageAdapter) -> None:
        """Register an adapter manually (useful for tests)."""
        self._adapters[name] = adapter

    @property
    def adapter_names(self) -> list[str]:
        return list(self._adapters.keys())

    # ── internals ──

    def _init_s3(self) -> None:
        from src.services.s3_store import S3Store
        self._s3 = S3Store.from_env()
        if self._s3:
            logger.info("S3 image storage configured")

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

    # ── model catalog ──

    async def search_models(self, provider: str, query: str = "", *, api_key: str = "") -> list[dict]:
        """Search available models for a provider. Returns list of dicts with name, description, etc."""
        import aiohttp

        if provider == "replicate":
            token = api_key or os.environ.get("REPLICATE_API_TOKEN", "")
            if not token:
                return []
            url = "https://api.replicate.com/v1/models"
            params = {}
            if query:
                params["query"] = query
            headers = {"Authorization": f"Bearer {token}"}
            timeout = aiohttp.ClientTimeout(total=15)
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, headers=headers, params=params, timeout=timeout) as resp:
                        if resp.status != 200:
                            return []
                        data = await resp.json()
                        results = data.get("results", [])
                        models = [
                            {
                                "id": f"{r.get('owner', '')}/{r.get('name', '')}",
                                "model_string": f"replicate:{r.get('owner', '')}/{r.get('name', '')}",
                                "description": (r.get("description") or "")[:200],
                                "run_count": r.get("run_count", 0),
                                "rank": r.get("rank"),
                            }
                            for r in results
                        ]
                        models.sort(key=lambda m: m.get("run_count", 0), reverse=True)
                        return models[:20]
            except Exception:
                logger.warning("Failed to search Replicate models", exc_info=True)
                return []

        # Static catalogs for providers without search API
        def _m(mid: str, provider: str, desc: str) -> dict:
            return {"id": mid, "model_string": f"{provider}:{mid}", "description": desc, "run_count": 0}

        catalogs: dict[str, list[dict]] = {
            "together": [
                _m("black-forest-labs/FLUX.1-schnell", "together", "FLUX.1 Schnell — fast"),
                _m("black-forest-labs/FLUX.1-dev", "together", "FLUX.1 Dev — high quality"),
            ],
            "openai": [
                _m("dall-e-3", "openai", "DALL-E 3 — OpenAI image generation"),
                _m("dall-e-2", "openai", "DALL-E 2 — OpenAI image generation"),
            ],
            "huggingface": [
                _m("stabilityai/stable-diffusion-xl-base-1.0", "huggingface", "Stable Diffusion XL"),
                _m("black-forest-labs/FLUX.1-dev", "huggingface", "FLUX.1 Dev on HuggingFace"),
            ],
        }
        models = catalogs.get(provider, [])
        if query:
            q = query.lower()
            models = [m for m in models if q in m["id"].lower() or q in m["description"].lower()]
        return models
