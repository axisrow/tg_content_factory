from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from src.web.dialogs import handlers
from src.web.dialogs.responses import dialog_response

router = APIRouter()

handle_send_message = handlers.send_message
handle_edit_message = handlers.edit_message
handle_forward_messages = handlers.forward_messages
handle_pin_message = handlers.pin_message
handle_unpin_message = handlers.unpin_message
handle_download_media = handlers.download_media
handle_get_participants = handlers.get_participants
handle_edit_admin = handlers.edit_admin
handle_edit_permissions = handlers.edit_permissions
handle_kick_participant = handlers.kick_participant


@router.get("/", response_class=HTMLResponse)
async def dialogs_page(
    request: Request,
    phone: str | None = None,
    left: int = 0,
    failed: int = 0,
):
    return dialog_response(request, await handlers.dialogs_page(request, phone, left, failed))


@router.post("/refresh")
async def refresh_dialogs(request: Request):
    return dialog_response(request, await handlers.refresh_dialogs(request))


@router.get("/cache-status")
async def cache_status(request: Request):
    return dialog_response(request, await handlers.cache_status(request))


@router.post("/cache-clear")
async def cache_clear(request: Request):
    return dialog_response(request, await handlers.cache_clear(request))


@router.post("/leave")
async def leave_dialogs(request: Request):
    return dialog_response(request, await handlers.leave_dialogs(request))


@router.post("/send")
async def send_message(request: Request):
    return dialog_response(request, await handle_send_message(request))


@router.post("/join")
async def join_dialog(request: Request):
    return dialog_response(request, await handlers.join_dialog(request))


@router.post("/resolve")
async def resolve_entity(request: Request):
    return dialog_response(request, await handlers.resolve_entity(request))


@router.post("/edit-message")
async def edit_message(request: Request):
    return dialog_response(request, await handle_edit_message(request))


@router.post("/delete-message")
async def delete_message(request: Request):
    return dialog_response(request, await handlers.delete_message(request))


@router.post("/forward-messages")
async def forward_messages(request: Request):
    return dialog_response(request, await handle_forward_messages(request))


@router.post("/pin-message")
async def pin_message(request: Request):
    return dialog_response(request, await handle_pin_message(request))


@router.post("/react")
async def react_message(request: Request):
    return dialog_response(request, await handlers.react_message(request))


@router.post("/queue/{command_id}/cancel")
async def cancel_queue_command(request: Request, command_id: int):
    return dialog_response(request, await handlers.cancel_queue_command(request, command_id))


@router.post("/queue/clear-pending")
async def clear_pending_queue_commands(request: Request):
    return dialog_response(request, await handlers.clear_pending_queue_commands(request))


@router.post("/unpin-message")
async def unpin_message(request: Request):
    return dialog_response(request, await handle_unpin_message(request))


@router.post("/download-media")
async def download_media(request: Request):
    return dialog_response(request, await handle_download_media(request))


@router.get("/participants")
async def get_participants(request: Request):
    return dialog_response(request, await handle_get_participants(request))


@router.post("/edit-admin")
async def edit_admin(request: Request):
    return dialog_response(request, await handle_edit_admin(request))


@router.post("/edit-permissions")
async def edit_permissions(request: Request):
    return dialog_response(request, await handle_edit_permissions(request))


@router.post("/kick")
async def kick_participant(request: Request):
    return dialog_response(request, await handle_kick_participant(request))


@router.get("/broadcast-stats")
async def broadcast_stats(request: Request):
    return dialog_response(request, await handlers.broadcast_stats(request))


@router.post("/archive")
async def archive_dialog(request: Request):
    return dialog_response(request, await handlers.archive_dialog(request))


@router.post("/unarchive")
async def unarchive_dialog(request: Request):
    return dialog_response(request, await handlers.unarchive_dialog(request))


@router.post("/mark-read")
async def mark_read(request: Request):
    return dialog_response(request, await handlers.mark_read(request))


@router.get("/create-channel", response_class=HTMLResponse)
async def create_channel_page(request: Request):
    return dialog_response(request, await handlers.create_channel_page(request))


@router.post("/create-channel")
async def create_channel(request: Request):
    return dialog_response(request, await handlers.create_channel(request))
