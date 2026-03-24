"""Image generation playground — test generation, browse models, view history."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.services.image_generation_service import ImageGenerationService
from src.web import deps

router = APIRouter()
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


@router.get("/", response_class=HTMLResponse)
async def images_page(request: Request):
    svc = await _get_image_service(request)
    return deps.get_templates(request).TemplateResponse(
        request,
        "images.html",
        {
            "providers": svc.adapter_names,
        },
    )


@router.post("/generate")
async def generate_image(request: Request):
    form = await request.form()
    prompt = str(form.get("prompt", "")).strip()
    model = str(form.get("model", "")).strip()
    if not prompt:
        return JSONResponse({"ok": False, "error": "Prompt is required"}, status_code=400)

    svc = await _get_image_service(request)
    if not await svc.is_available():
        return JSONResponse({"ok": False, "error": "No image providers configured"}, status_code=409)

    result = await svc.generate(model or None, prompt)
    if result is None:
        return JSONResponse({"ok": False, "error": "Generation failed — check server logs"}, status_code=500)

    return JSONResponse({"ok": True, "url": result, "model": model, "prompt": prompt})


@router.get("/models/search")
async def search_models_route(request: Request):
    provider = request.query_params.get("provider", "").strip()
    query = request.query_params.get("q", "").strip()
    if not provider:
        return JSONResponse({"ok": False, "error": "provider is required"}, status_code=400)

    api_key = await _get_provider_api_key(request, provider)
    svc = ImageGenerationService()
    models = await svc.search_models(provider, query, api_key=api_key)
    return JSONResponse({"ok": True, "models": models, "provider": provider})
