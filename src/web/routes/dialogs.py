from __future__ import annotations

import logging
import time
from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from src.database import Database
from src.models import AccountSessionStatus
from src.services.channel_service import ChannelService
from src.services.telegram_command_service import TelegramCommandService
from src.web import deps
from src.web.dialogs.forms import (
    CacheClearForm,
    ChatActionForm,
    CreateChannelForm,
    DownloadMediaForm,
    EditAdminForm,
    EditMessageForm,
    EditPermissionsForm,
    ForwardMessagesForm,
    KickParticipantForm,
    LeaveDialogsForm,
    MarkReadForm,
    MessageIdsForm,
    PinMessageForm,
    RefreshDialogsForm,
    SendMessageForm,
    UnpinMessageForm,
    parse_dialog_form,
)
from src.web.redirects import dialogs_redirect, redirect_see_other

router = APIRouter()
logger = logging.getLogger(__name__)


def _db_dep(request: Request) -> Database:
    return deps.get_db(request)


def _channel_service_dep(request: Request) -> ChannelService:
    return deps.channel_service(request)


def _templates_dep(request: Request):
    return deps.get_templates(request)


def _telegram_command_service_dep(request: Request) -> TelegramCommandService:
    return deps.telegram_command_service(request)


DbDep = Annotated[Database, Depends(_db_dep)]
ChannelServiceDep = Annotated[ChannelService, Depends(_channel_service_dep)]
TemplatesDep = Annotated[object, Depends(_templates_dep)]
TelegramCommandServiceDep = Annotated[TelegramCommandService, Depends(_telegram_command_service_dep)]


async def _enqueue_dialog_command(
    command_service: TelegramCommandService,
    command_type: str,
    *,
    payload: dict,
    phone: str | None = None,
    target_path: str = "/dialogs/",
) -> RedirectResponse:
    command_id = await command_service.enqueue(
        command_type,
        payload=payload,
        requested_by="web:dialogs",
    )
    params = {"command_id": command_id}
    if phone and "phone=" not in target_path:
        params["phone"] = phone
    return redirect_see_other(target_path, params)


async def _get_command_state(command_service: TelegramCommandService, command_id: str | None):
    if not command_id or not command_id.isdigit():
        return None
    return await command_service.get(int(command_id))


@router.get("/", response_class=HTMLResponse)
async def dialogs_page(
    request: Request,
    db: DbDep,
    channel_service: ChannelServiceDep,
    templates: TemplatesDep,
    command_service: TelegramCommandServiceDep,
    phone: str | None = None,
    left: int = 0,
    failed: int = 0,
):
    started_at = time.perf_counter()
    accounts = sorted(
        account.phone
        for account in await db.get_account_summaries(active_only=False)
        if account.session_status == AccountSessionStatus.OK
    )
    selected_phone = phone if phone in accounts else None
    dialogs = []
    dialogs_cached_at = None
    command = await _get_command_state(command_service, request.query_params.get("command_id"))
    if selected_phone:
        dialogs = await channel_service.get_my_dialogs(selected_phone)
        dialogs_cached_at = await db.repos.dialog_cache.get_cached_at(selected_phone)
    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    logger.info(
        "dialogs_page: phone=%s accounts=%d dialogs=%d duration_ms=%d",
        selected_phone,
        len(accounts),
        len(dialogs),
        elapsed_ms,
    )
    return templates.TemplateResponse(
        request,
        "dialogs.html",
        {
            "accounts": accounts,
            "selected_phone": selected_phone,
            "dialogs": dialogs,
            "dialogs_cached_at": dialogs_cached_at,
            "left": left,
            "failed": failed,
            "command": command,
        },
    )


@router.post("/refresh")
async def refresh_dialogs(request: Request, command_service: TelegramCommandServiceDep):
    form = await parse_dialog_form(request, RefreshDialogsForm)
    if not form.phone:
        return dialogs_redirect(error="missing_fields")
    return await _enqueue_dialog_command(
        command_service,
        "dialogs.refresh",
        payload={"phone": form.phone},
        phone=form.phone,
    )


@router.get("/cache-status")
async def cache_status(db: DbDep):
    phones = await db.repos.dialog_cache.get_all_phones()
    result = []
    for ph in sorted(phones):
        count = await db.repos.dialog_cache.count_dialogs(ph)
        cached_at = await db.repos.dialog_cache.get_cached_at(ph)
        result.append({
            "phone": ph,
            "count": count,
            "cached_at": cached_at.isoformat() if cached_at else None,
        })
    return JSONResponse(result)


@router.post("/cache-clear")
async def cache_clear(request: Request, command_service: TelegramCommandServiceDep):
    form = await parse_dialog_form(request, CacheClearForm)
    return await _enqueue_dialog_command(
        command_service,
        "dialogs.cache_clear",
        payload={"phone": form.phone},
        phone=form.phone or None,
    )


@router.post("/leave")
async def leave_dialogs(request: Request, command_service: TelegramCommandServiceDep):
    form = await parse_dialog_form(request, LeaveDialogsForm)
    return await _enqueue_dialog_command(
        command_service,
        "dialogs.leave",
        payload={
            "phone": form.phone,
            "dialogs": [{"dialog_id": dialog_id, "title": title} for dialog_id, title in form.dialogs],
        },
        phone=form.phone or None,
    )


@router.post("/send")
async def send_message(request: Request, command_service: TelegramCommandServiceDep):
    form = await parse_dialog_form(request, SendMessageForm)
    if not form.phone or not form.recipient or not form.text:
        return dialogs_redirect(form.phone, error="missing_fields")
    return await _enqueue_dialog_command(
        command_service,
        "dialogs.send",
        payload={"phone": form.phone, "recipient": form.recipient, "text": form.text},
        phone=form.phone,
    )


@router.post("/edit-message")
async def edit_message(request: Request, command_service: TelegramCommandServiceDep):
    form = await parse_dialog_form(request, EditMessageForm)
    if not form.phone or not form.chat_id or not form.message_id or not form.text:
        return dialogs_redirect(form.phone, error="missing_fields")
    return await _enqueue_dialog_command(
        command_service,
        "dialogs.edit_message",
        payload={"phone": form.phone, "chat_id": form.chat_id, "message_id": form.message_id, "text": form.text},
        phone=form.phone,
    )


@router.post("/delete-message")
async def delete_message(request: Request, command_service: TelegramCommandServiceDep):
    form = await parse_dialog_form(request, MessageIdsForm)
    if not form.phone or not form.chat_id or not form.message_ids:
        return dialogs_redirect(form.phone, error="missing_fields")
    if not form.ids:
        return dialogs_redirect(form.phone, error="invalid_ids")
    return await _enqueue_dialog_command(
        command_service,
        "dialogs.delete_message",
        payload={"phone": form.phone, "chat_id": form.chat_id, "message_ids": form.ids},
        phone=form.phone,
    )


@router.post("/forward-messages")
async def forward_messages(request: Request, command_service: TelegramCommandServiceDep):
    form = await parse_dialog_form(request, ForwardMessagesForm)
    if not form.phone or not form.from_chat or not form.to_chat or not form.message_ids:
        return dialogs_redirect(form.phone, error="missing_fields")
    if not form.ids:
        return dialogs_redirect(form.phone, error="invalid_ids")
    return await _enqueue_dialog_command(
        command_service,
        "dialogs.forward_messages",
        payload={"phone": form.phone, "from_chat": form.from_chat, "to_chat": form.to_chat, "message_ids": form.ids},
        phone=form.phone,
    )


@router.post("/pin-message")
async def pin_message(request: Request, command_service: TelegramCommandServiceDep):
    form = await parse_dialog_form(request, PinMessageForm)
    if not form.phone or not form.chat_id or not form.message_id:
        return dialogs_redirect(form.phone, error="missing_fields")
    return await _enqueue_dialog_command(
        command_service,
        "dialogs.pin_message",
        payload={"phone": form.phone, "chat_id": form.chat_id, "message_id": form.message_id, "notify": form.notify},
        phone=form.phone,
    )


@router.post("/unpin-message")
async def unpin_message(request: Request, command_service: TelegramCommandServiceDep):
    form = await parse_dialog_form(request, UnpinMessageForm)
    if not form.phone or not form.chat_id:
        return dialogs_redirect(form.phone, error="missing_fields")
    return await _enqueue_dialog_command(
        command_service,
        "dialogs.unpin_message",
        payload={"phone": form.phone, "chat_id": form.chat_id, "message_id": form.parsed_message_id},
        phone=form.phone,
    )


@router.post("/download-media")
async def download_media(request: Request, command_service: TelegramCommandServiceDep):
    form = await parse_dialog_form(request, DownloadMediaForm)
    if not form.phone or not form.chat_id or not form.message_id:
        return dialogs_redirect(form.phone, error="missing_fields")
    return await _enqueue_dialog_command(
        command_service,
        "dialogs.download_media",
        payload={"phone": form.phone, "chat_id": form.chat_id, "message_id": form.message_id},
        phone=form.phone,
    )


@router.get("/participants")
async def get_participants(
    request: Request,
    db: DbDep,
    command_service: TelegramCommandServiceDep,
):
    phone = request.query_params.get("phone", "")
    chat_id = request.query_params.get("chat_id", "")
    limit_str = request.query_params.get("limit", "")
    search = request.query_params.get("search", "")
    limit = int(limit_str) if limit_str and limit_str.isdigit() else 200
    if not phone or not chat_id:
        return JSONResponse({"error": "phone and chat_id required"}, status_code=400)
    scope = f"dialogs_participants:{phone}:{chat_id}"
    # The cached snapshot is keyed only by (phone, chat_id), so it does not
    # reflect a specific search string. When the caller asks for a filtered
    # search, bypass the cache and always enqueue a fresh command; otherwise
    # an older search result would be returned silently.
    if not search:
        snapshot = await db.repos.runtime_snapshots.get_snapshot("dialogs_participants", scope)
        if snapshot is not None:
            return JSONResponse(snapshot.payload)
    command_id = await command_service.enqueue(
        "dialogs.participants",
        payload={"phone": phone, "chat_id": chat_id, "limit": limit, "search": search},
        requested_by="web:dialogs",
    )
    return JSONResponse({"status": "queued", "command_id": command_id}, status_code=202)


@router.post("/edit-admin")
async def edit_admin(request: Request, command_service: TelegramCommandServiceDep):
    form = await parse_dialog_form(request, EditAdminForm)
    if not form.phone or not form.chat_id or not form.user_id:
        return dialogs_redirect(form.phone, error="missing_fields")
    return await _enqueue_dialog_command(
        command_service,
        "dialogs.edit_admin",
        payload={
            "phone": form.phone,
            "chat_id": form.chat_id,
            "user_id": form.user_id,
            "title": form.title,
            "is_admin": form.is_admin,
        },
        phone=form.phone,
    )


@router.post("/edit-permissions")
async def edit_permissions(request: Request, command_service: TelegramCommandServiceDep):
    form = await parse_dialog_form(request, EditPermissionsForm)
    if form.send_messages is None and form.send_media is None:
        return dialogs_redirect(form.phone, error="no_permission_flags")
    if not form.phone or not form.chat_id or not form.user_id:
        return dialogs_redirect(form.phone, error="missing_fields")
    payload = {"phone": form.phone, "chat_id": form.chat_id, "user_id": form.user_id}
    if form.until_date:
        payload["until_date"] = form.until_date
    if form.send_messages is not None:
        payload["send_messages"] = form.send_messages
    if form.send_media is not None:
        payload["send_media"] = form.send_media
    return await _enqueue_dialog_command(
        command_service,
        "dialogs.edit_permissions",
        payload=payload,
        phone=form.phone,
    )


@router.post("/kick")
async def kick_participant(request: Request, command_service: TelegramCommandServiceDep):
    form = await parse_dialog_form(request, KickParticipantForm)
    if not form.phone or not form.chat_id or not form.user_id:
        return dialogs_redirect(form.phone, error="missing_fields")
    return await _enqueue_dialog_command(
        command_service,
        "dialogs.kick",
        payload={"phone": form.phone, "chat_id": form.chat_id, "user_id": form.user_id},
        phone=form.phone,
    )


@router.get("/broadcast-stats")
async def broadcast_stats(request: Request, db: DbDep, command_service: TelegramCommandServiceDep):
    phone = request.query_params.get("phone", "")
    chat_id = request.query_params.get("chat_id", "")
    if not phone or not chat_id:
        return JSONResponse({"error": "phone and chat_id required"}, status_code=400)
    scope = f"dialogs_broadcast_stats:{phone}:{chat_id}"
    snapshot = await db.repos.runtime_snapshots.get_snapshot("dialogs_broadcast_stats", scope)
    if snapshot is not None:
        return JSONResponse(snapshot.payload)
    command_id = await command_service.enqueue(
        "dialogs.broadcast_stats",
        payload={"phone": phone, "chat_id": chat_id},
        requested_by="web:dialogs",
    )
    return JSONResponse({"status": "queued", "command_id": command_id}, status_code=202)


@router.post("/archive")
async def archive_dialog(request: Request, command_service: TelegramCommandServiceDep):
    form = await parse_dialog_form(request, ChatActionForm)
    if not form.phone or not form.chat_id:
        return dialogs_redirect(form.phone, error="missing_fields")
    return await _enqueue_dialog_command(
        command_service,
        "dialogs.archive",
        payload={"phone": form.phone, "chat_id": form.chat_id},
        phone=form.phone,
    )


@router.post("/unarchive")
async def unarchive_dialog(request: Request, command_service: TelegramCommandServiceDep):
    form = await parse_dialog_form(request, ChatActionForm)
    if not form.phone or not form.chat_id:
        return dialogs_redirect(form.phone, error="missing_fields")
    return await _enqueue_dialog_command(
        command_service,
        "dialogs.unarchive",
        payload={"phone": form.phone, "chat_id": form.chat_id},
        phone=form.phone,
    )


@router.post("/mark-read")
async def mark_read(request: Request, command_service: TelegramCommandServiceDep):
    form = await parse_dialog_form(request, MarkReadForm)
    if not form.phone or not form.chat_id:
        return dialogs_redirect(form.phone, error="missing_fields")
    return await _enqueue_dialog_command(
        command_service,
        "dialogs.mark_read",
        payload={"phone": form.phone, "chat_id": form.chat_id, "max_id": form.parsed_max_id},
        phone=form.phone,
    )


@router.get("/create-channel", response_class=HTMLResponse)
async def create_channel_page(
    request: Request,
    db: DbDep,
    templates: TemplatesDep,
    command_service: TelegramCommandServiceDep,
):
    accounts = sorted(
        account.phone
        for account in await db.get_account_summaries(active_only=False)
        if account.session_status == AccountSessionStatus.OK
    )
    command = await _get_command_state(command_service, request.query_params.get("command_id"))
    return templates.TemplateResponse(
        request,
        "dialogs_create_channel.html",
        {"accounts": accounts, "command": command},
    )


@router.post("/create-channel")
async def create_channel(
    request: Request,
    command_service: TelegramCommandServiceDep,
):
    form = await parse_dialog_form(request, CreateChannelForm)
    if not form.phone or not form.title:
        return redirect_see_other("/dialogs/create-channel", {"error": "missing_fields"})
    command_id = await command_service.enqueue(
        "dialogs.create_channel",
        payload={
            "phone": form.phone,
            "title": form.title,
            "about": form.about,
            "username": form.username,
        },
        requested_by="web:dialogs",
    )
    return redirect_see_other("/dialogs/create-channel", {"command_id": command_id})
