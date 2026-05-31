"""Image generation playground — test generation, browse models, view history."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from src.web.images import handlers
from src.web.images.responses import images_response

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/", response_class=HTMLResponse)
async def images_page(request: Request):
    return images_response(request, await handlers.images_page(request))


@router.post("/generate")
async def generate_image(request: Request):
    return images_response(request, await handlers.generate_image(request))


@router.get("/models/search")
async def search_models_route(request: Request):
    return images_response(request, await handlers.search_models(request))
