import logging

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse

from src.web.channels import handlers
from src.web.channels.responses import channels_response

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def channels_list(request: Request):
    return channels_response(request, await handlers.channels_list(request))


@router.post("/add")
async def add_channel(request: Request, identifier: str = Form("")):
    return channels_response(request, await handlers.add_channel(request, identifier))


@router.get("/dialogs")
async def get_dialogs(request: Request):
    return channels_response(request, await handlers.get_dialogs(request))


@router.post("/add-bulk")
async def add_bulk(request: Request):
    return channels_response(request, await handlers.add_bulk(request))


@router.post("/{pk}/toggle")
async def toggle_channel(request: Request, pk: int):
    return channels_response(request, await handlers.toggle_channel(request, pk))


@router.post("/{pk}/delete")
async def delete_channel(request: Request, pk: int):
    return channels_response(request, await handlers.delete_channel(request, pk))


@router.post("/refresh-types")
async def refresh_channel_types(request: Request):
    return channels_response(request, await handlers.refresh_channel_types(request))


@router.post("/refresh-meta")
async def refresh_channel_meta(request: Request):
    return channels_response(request, await handlers.refresh_channel_meta(request))


# ── Tag endpoints ────────────────────────────────────────────────────────────

@router.get("/tags")
async def list_tags(request: Request):
    return channels_response(request, await handlers.list_tags(request))


@router.post("/tags")
async def create_tag(request: Request, name: str = Form("")):
    return channels_response(request, await handlers.create_tag(request, name))


@router.delete("/tags/{name}")
async def delete_tag(request: Request, name: str):
    return channels_response(request, await handlers.delete_tag(request, name))


@router.get("/{pk}/tags")
async def get_channel_tags(request: Request, pk: int):
    return channels_response(request, await handlers.get_channel_tags(request, pk))


@router.post("/{pk}/tags")
async def set_channel_tags(request: Request, pk: int):
    return channels_response(request, await handlers.set_channel_tags(request, pk))
