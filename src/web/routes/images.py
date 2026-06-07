"""Image generation playground — test generation, browse models, view history."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.web import deps
from src.web.images import handlers
from src.web.images.responses import images_response

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/generated", response_class=JSONResponse)
async def list_generated_images(request: Request, limit: int = 50):
    """List recently generated images as JSON (parity with CLI `image generated`)."""
    db = deps.get_db(request)
    images = await db.repos.generated_images.list_recent(limit=limit)
    return JSONResponse([img.model_dump(mode="json") for img in images])


@router.get("/", response_class=HTMLResponse)
async def images_page(request: Request):
    return images_response(request, await handlers.images_page(request))


@router.post("/generate")
async def generate_image(request: Request):
    return images_response(request, await handlers.generate_image(request))


@router.get("/models/search")
async def search_models_route(request: Request):
    return images_response(request, await handlers.search_models(request))
