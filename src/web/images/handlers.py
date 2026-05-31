"""Application orchestration for the image-generation web domain."""

from __future__ import annotations

import logging

from fastapi import Request

from src.services.image_generation_service import ImageGenerationService
from src.web import deps
from src.web.images.forms import parse_generate_form, parse_models_search
from src.web.images.responses import ImagesJson, ImagesTemplate

logger = logging.getLogger(__name__)


async def _get_image_service(request: Request) -> ImageGenerationService:
    """Build ImageGenerationService with DB-configured providers + env fallback."""
    try:
        from src.services.image_provider_service import ImageProviderService

        db = deps.get_db(request)
        config = request.app.state.config
        svc = ImageProviderService(db, config)
        configs = await svc.load_provider_configs()
        adapters = svc.build_adapters(configs)
        if adapters:
            return ImageGenerationService(adapters=adapters)
    except Exception:
        logger.warning("Failed to load image providers from DB", exc_info=True)
    return ImageGenerationService()


async def _get_provider_api_key(request: Request, provider: str) -> str:
    """Get API key for provider from DB config, falling back to env."""
    import os

    try:
        from src.services.image_provider_service import IMAGE_PROVIDER_SPECS, ImageProviderService

        db = deps.get_db(request)
        config = request.app.state.config
        svc = ImageProviderService(db, config)
        configs = await svc.load_provider_configs()
        for cfg in configs:
            if cfg.provider == provider and cfg.api_key:
                return cfg.api_key
        spec = IMAGE_PROVIDER_SPECS.get(provider)
        if spec:
            for var in spec.env_vars:
                val = os.environ.get(var, "").strip()
                if val:
                    return val
    except Exception:
        pass
    return ""


async def images_page(request: Request) -> ImagesTemplate:
    svc = await _get_image_service(request)
    return ImagesTemplate("images.html", {"providers": svc.adapter_names})


async def generate_image(request: Request) -> ImagesJson:
    form = await parse_generate_form(request)
    if not form.prompt:
        return ImagesJson({"ok": False, "error": "Prompt is required"}, status_code=400)

    svc = await _get_image_service(request)
    if not await svc.is_available():
        return ImagesJson({"ok": False, "error": "No image providers configured"}, status_code=409)

    result = await svc.generate(form.model or None, form.prompt)
    if result is None:
        return ImagesJson({"ok": False, "error": "Generation failed — check server logs"}, status_code=500)

    return ImagesJson({"ok": True, "url": result, "model": form.model, "prompt": form.prompt})


async def search_models(request: Request) -> ImagesJson:
    q = parse_models_search(request)
    if not q.provider:
        return ImagesJson({"ok": False, "error": "provider is required"}, status_code=400)

    api_key = await _get_provider_api_key(request, q.provider)
    svc = ImageGenerationService()
    models = await svc.search_models(q.provider, q.query, api_key=api_key)
    return ImagesJson({"ok": True, "models": models, "provider": q.provider})
