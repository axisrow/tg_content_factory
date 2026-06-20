from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from src.services.production_limits_service import ProductionLimitsService
    from src.services.provider_adapters import ImageAdapter
    from src.services.s3_store import S3Store

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ImageGenerationFailure:
    kind: str
    provider: str | None
    model: str | None
    message: str
    retryable: bool = True

    @property
    def is_timeout(self) -> bool:
        return self.kind == "timeout"

    def user_message(self, *, lang: str = "ru") -> str:
        """User-facing explanation of the failure, in Russian (``ru``) or English (``en``).

        Centralizes the timeout wording so the agent/CLI/web callers don't each
        re-derive it (and re-import the Codex timeout constant)."""
        if not self.is_timeout:
            return "Генерация не вернула результат." if lang == "ru" else "Generation failed — check logs"

        model = self.model or ("по умолчанию" if lang == "ru" else "default")
        if (self.model or "").startswith("codex:"):
            from src.services.provider_adapters import CODEX_IMAGE_TIMEOUT_SECONDS

            seconds = int(CODEX_IMAGE_TIMEOUT_SECONDS)
            if lang == "ru":
                return (
                    f"Генерация изображения через {model} не успела завершиться за {seconds} секунд. "
                    "Процесс Codex остановлен; попробуйте позже или передайте model другого провайдера."
                )
            return (
                f"Generation timed out for model={model} after {seconds}s. "
                "The Codex process was stopped; try again later or choose another image provider/model."
            )
        if lang == "ru":
            return (
                f"Генерация изображения через {model} не успела завершиться за таймаут. "
                "Попробуйте позже или выберите другую модель."
            )
        return (
            f"Generation timed out for model={model}. "
            "Try again later or choose another image provider/model."
        )


class ImageGenerationService:
    """Routes image generation requests to provider-specific HTTP adapters.

    Adapters are auto-registered from environment variables at init time.
    Model strings use ``provider:model_id`` convention (e.g.
    ``together:black-forest-labs/FLUX.1-schnell``).  Without a prefix the
    first available adapter is used as fallback.
    """

    def __init__(
        self,
        adapters: dict[str, "ImageAdapter"] | None = None,
        limits: "ProductionLimitsService | None" = None,
    ) -> None:
        self._adapters: dict[str, ImageAdapter] = {}
        self._s3: S3Store | None = None
        self._last_failure: ImageGenerationFailure | None = None
        # Optional opt-in rate-limit / daily cost cap (#814). None = unlimited
        # (the default), so generation behaves exactly as before unless an
        # operator enables production_limits in config.
        self._limits = limits
        if adapters is not None:
            self._adapters = dict(adapters)
        else:
            self._register_from_env()
        self._init_s3()

    # ── public API ──

    async def generate(self, model: str | None, text: str) -> str | None:
        """Generate an image for *text* using *model*.  Returns URL/path or ``None``."""
        self._last_failure = None
        if not text or not self._adapters:
            return None
        provider_name, model_id = self._parse_model_string(model)
        adapter = self._resolve_adapter(provider_name)
        if adapter is None:
            logger.warning("No image adapter available for model=%s", model)
            self._last_failure = ImageGenerationFailure(
                kind="no_adapter",
                provider=provider_name,
                model=model,
                message=f"No image adapter available for model={model}",
                retryable=False,
            )
            return None
        limits = getattr(self, "_limits", None)
        if limits is not None:
            allowed, error = await limits.acquire(is_image=True)
            if not allowed:
                logger.warning("Image generation blocked by production limits (model=%s): %s", model, error)
                self._last_failure = ImageGenerationFailure(
                    kind="rate_limited",
                    provider=provider_name,
                    model=model,
                    message=error or "production limit exceeded",
                    retryable=True,
                )
                return None
        try:
            result = await adapter(text, model_id)
        except TimeoutError as exc:
            logger.warning("Image generation timed out (model=%s): %s", model, exc)
            self._last_failure = ImageGenerationFailure(
                kind="timeout", provider=provider_name, model=model, message=str(exc),
            )
            return None
        except Exception as exc:
            # OSError is an expected I/O failure — log calmly without a traceback;
            # anything else is unexpected and warrants the full stack.
            if isinstance(exc, OSError):
                logger.warning("Image generation failed (model=%s): %s", model, exc)
            else:
                logger.exception("Image generation unexpected error (model=%s)", model)
            self._last_failure = ImageGenerationFailure(
                kind="error", provider=provider_name, model=model, message=str(exc),
            )
            return None
        # The paid generation succeeded → record its cost against the daily cap.
        if limits is not None and result:
            await limits.record_cost(is_image=True)
        s3 = getattr(self, "_s3", None)
        if result and s3 is not None:
            if result.startswith("http"):
                # Provider returned an ephemeral host URL (e.g. Replicate, expires
                # ~24h). Mirror it into durable S3 so saved runs don't 404 later
                # (audit #836/4); skip URLs already pointing at our S3 endpoint.
                if not s3.owns_url(result):
                    s3_url = await s3.upload_url(result)
                    if s3_url:
                        return s3_url
            else:
                s3_url = await s3.upload_file(result)
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

    @property
    def last_failure(self) -> ImageGenerationFailure | None:
        return self._last_failure

    # ── internals ──

    def _init_s3(self) -> None:
        from src.services.s3_store import S3Store
        self._s3 = S3Store.from_env()
        if self._s3:
            logger.info("S3 image storage configured")

    def _register_from_env(self) -> None:
        """Register adapters from the provider table, env-only (no DB configs).

        Same uniform rule as :meth:`ImageProviderService.build_adapters`: keyed
        providers come from their env vars, keyless providers (codex) from
        ``detect()``.
        """
        from src.services.image_provider_service import (
            IMAGE_PROVIDER_ORDER,
            IMAGE_PROVIDER_SPECS,
            _env_key,
        )

        for name in IMAGE_PROVIDER_ORDER:
            spec = IMAGE_PROVIDER_SPECS[name]
            if spec.keyless:
                if spec.detect() and spec.keyless_factory is not None:
                    self._adapters[name] = spec.keyless_factory()
            else:
                key = _env_key(spec.env_vars)
                if key and spec.keyed_factory is not None:
                    self._adapters[name] = spec.keyed_factory(key)

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
        if provider_name:
            # An explicit provider must use its own adapter or fail cleanly —
            # never silently route to another provider's adapter, which would
            # generate off-brand images from an incompatible model (audit #835/11).
            return self._adapters.get(provider_name)
        # Implicit fallback (no provider specified): first adapter that is allowed
        # to be a default. Spec-flagged explicit-only providers (e.g. Codex,
        # which spawns a blocking subprocess) are skipped so an unqualified
        # generate() never silently triggers them.
        from src.services.image_provider_service import image_provider_spec

        for name, adapter in self._adapters.items():
            spec = image_provider_spec(name)
            if spec is None or not spec.explicit_only:
                return adapter
        return None

    # ── model catalog ──

    async def search_models(
        self, provider: str, query: str = "", *, api_key: str = "", refresh: bool = False
    ) -> list[dict]:
        """Search available models for a provider. Returns list of dicts with name, description, etc.

        For OpenAI, ``refresh=True`` fetches the live model list from ``/v1/models``
        (filtered to image models) via the shared model-listing helper; otherwise the
        static fallback catalog is returned.
        """
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

        if provider == "huggingface":
            token = api_key or os.environ.get("HUGGINGFACE_API_KEY") or os.environ.get("HUGGINGFACE_TOKEN", "")
            if not token:
                return []
            try:
                from huggingface_hub import HfApi

                hf_api = HfApi()
                results = await asyncio.to_thread(
                    hf_api.list_models,
                    filter="text-to-image",
                    search=query if query else None,
                    sort="downloads",
                    limit=20,
                    token=token,
                )
                models = []
                for m in results:
                    model_id = m.id
                    models.append(
                        {
                            "id": model_id,
                            "model_string": f"huggingface:{model_id}",
                            "description": (m.cardData or {}).get("description", "")[:200] if m.cardData else "",
                            "run_count": m.downloads or 0,
                        }
                    )
                models.sort(key=lambda x: x.get("run_count", 0), reverse=True)
                return models[:20]
            except Exception:
                logger.warning("Failed to search HuggingFace models", exc_info=True)
                return []

        # Static catalogs + live-refresh fetchers are declared per provider on
        # the spec table. replicate/huggingface keep dedicated live-search
        # branches above (their API shapes differ enough that a fetch_models
        # adapter would not simplify them); everything else dispatches off the
        # spec here.
        from src.services.image_provider_service import image_provider_spec

        def _by_query(models: list[dict]) -> list[dict]:
            if not query:
                return models
            q = query.lower()
            return [m for m in models if q in m["id"].lower() or q in m["description"].lower()]

        spec = image_provider_spec(provider)
        if spec is None:
            return []

        if refresh and spec.fetch_models is not None:
            # A provider's live listing replaces the static catalog when available;
            # on a missing key / empty / failed fetch we fall through to it.
            live = await spec.fetch_models(self, api_key=api_key)
            if live:
                return _by_query(live)

        return _by_query(spec.static_catalog)

    @staticmethod
    async def _fetch_openai_image_models(api_key: str) -> list[dict]:
        """Fetch live OpenAI models via the shared helper, filtered to image models.

        Returns ``[]`` without a network call when no key is available, so callers
        can invoke it unconditionally.
        """
        if not api_key:
            return []
        from src.services.provider_model_cache import fetch_openai_model_ids

        base_url = os.environ.get("OPENAI_API_BASE", "https://api.openai.com/v1")
        try:
            ids = await fetch_openai_model_ids(base_url, api_key)
        except Exception:
            logger.warning("Failed to fetch OpenAI image models", exc_info=True)
            return []
        image_ids = [m for m in ids if m.startswith("gpt-image") or m.startswith("dall-e")]
        image_ids.sort()
        return [
            {
                "id": mid,
                "model_string": f"openai:{mid}",
                "description": "OpenAI image model",
                "run_count": 0,
            }
            for mid in image_ids
        ]

    @staticmethod
    async def _fetch_codex_models() -> list[dict]:
        """Fetch the live model list from the Codex SDK (blocking call off-loop)."""
        from src.services.provider_adapters import _codex_sdk_installed

        if not _codex_sdk_installed():
            return []

        def _list() -> list[dict]:
            from openai_codex import Codex

            with Codex() as codex:
                response = codex.models()
            out: list[dict] = []
            for m in getattr(response, "data", None) or []:
                mid = getattr(m, "id", None)
                if not mid:
                    continue
                out.append(
                    {
                        "id": str(mid),
                        "model_string": f"codex:{mid}",
                        "description": getattr(m, "description", None)
                        or getattr(m, "display_name", None)
                        or "Codex model",
                        "run_count": 0,
                    }
                )
            return out

        try:
            return await asyncio.to_thread(_list)
        except Exception:
            logger.warning("Failed to fetch Codex models", exc_info=True)
            return []
