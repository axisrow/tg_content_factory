from __future__ import annotations

import logging
import time
from urllib.parse import quote

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from src.web import deps

router = APIRouter()
logger = logging.getLogger(__name__)


async def _enqueue_dialog_command(
    request: Request,
    command_type: str,
    *,
    payload: dict,
    phone: str | None = None,
    target_path: str = "/dialogs/",
) -> RedirectResponse:
    command_id = await deps.telegram_command_service(request).enqueue(
        command_type,
        payload=payload,
        requested_by="web:dialogs",
    )
    separator = "&" if "?" in target_path else "?"
    if phone and "phone=" not in target_path:
        target_path = f"{target_path}{separator}phone={quote(phone, safe='')}"
        separator = "&"
    return RedirectResponse(
        url=f"{target_path}{separator}command_id={command_id}",
        status_code=303,
    )


async def _get_command_state(request: Request, command_id: str | None):
    if not command_id or not command_id.isdigit():
        return None
    return await deps.telegram_command_service(request).get(int(command_id))


@router.get("/", response_class=HTMLResponse)
async def dialogs_page(
    request: Request,
    phone: str | None = None,
    left: int = 0,
    failed: int = 0,
):
    started_at = time.perf_counter()
    db = deps.get_db(request)
    accounts = sorted(account.phone for account in await db.get_accounts(active_only=False))
    selected_phone = phone if phone in accounts else None
    dialogs = []
    dialogs_cached_at = None
    command = await _get_command_state(request, request.query_params.get("command_id"))
    if selected_phone:
        dialogs = await deps.channel_service(request).get_my_dialogs(selected_phone)
        dialogs_cached_at = await db.repos.dialog_cache.get_cached_at(selected_phone)
    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    logger.info(
        "dialogs_page: phone=%s accounts=%d dialogs=%d duration_ms=%d",
        selected_phone,
        len(accounts),
        len(dialogs),
        elapsed_ms,
    )
    return deps.get_templates(request).TemplateResponse(
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
async def refresh_dialogs(request: Request, phone: str = Form(...)):
    return await _enqueue_dialog_command(
        request,
        "dialogs.refresh",
        payload={"phone": phone},
        phone=phone,
    )


@router.get("/cache-status")
async def cache_status(request: Request):
    db = deps.get_db(request)
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
async def cache_clear(request: Request, phone: str = Form("")):
    return await _enqueue_dialog_command(
        request,
        "dialogs.cache_clear",
        payload={"phone": phone},
        phone=phone or None,
    )


@router.post("/leave")
async def leave_dialogs(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    dialogs: list[tuple[int, str]] = []
    for item in form.getlist("channel_ids"):
        parts = item.split(":", 1)
        if len(parts) == 2 and parts[0].lstrip("-").isdigit():
            dialogs.append((int(parts[0]), parts[1]))
    return await _enqueue_dialog_command(
        request,
        "dialogs.leave",
        payload={
            "phone": phone,
            "dialogs": [{"dialog_id": dialog_id, "title": title} for dialog_id, title in dialogs],
        },
        phone=phone or None,
    )


@router.post("/send")
async def send_message(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    recipient = form.get("recipient", "")
    text = form.get("text", "")
    if not phone or not recipient or not text:
        return RedirectResponse(
            url=f"/dialogs/?phone={quote(phone, safe='')}&error=missing_fields",
            status_code=303,
        )
    return await _enqueue_dialog_command(
        request,
        "dialogs.send",
        payload={"phone": phone, "recipient": recipient, "text": text},
        phone=phone,
    )


@router.post("/edit-message")
async def edit_message(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    chat_id = form.get("chat_id", "")
    message_id = form.get("message_id", "")
    text = form.get("text", "")
    if not phone or not chat_id or not message_id or not text:
        return RedirectResponse(
            url=f"/dialogs/?phone={quote(phone, safe='')}&error=missing_fields",
            status_code=303,
        )
    return await _enqueue_dialog_command(
        request,
        "dialogs.edit_message",
        payload={"phone": phone, "chat_id": chat_id, "message_id": message_id, "text": text},
        phone=phone,
    )


@router.post("/delete-message")
async def delete_message(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    chat_id = form.get("chat_id", "")
    message_ids_str = form.get("message_ids", "")
    if not phone or not chat_id or not message_ids_str:
        return RedirectResponse(
            url=f"/dialogs/?phone={quote(phone, safe='')}&error=missing_fields",
            status_code=303,
        )
    ids = [int(x.strip()) for x in message_ids_str.split(",") if x.strip().isdigit()]
    if not ids:
        return RedirectResponse(
            url=f"/dialogs/?phone={quote(phone, safe='')}&error=invalid_ids",
            status_code=303,
        )
    return await _enqueue_dialog_command(
        request,
        "dialogs.delete_message",
        payload={"phone": phone, "chat_id": chat_id, "message_ids": ids},
        phone=phone,
    )


@router.post("/forward-messages")
async def forward_messages(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    from_chat = form.get("from_chat", "")
    to_chat = form.get("to_chat", "")
    message_ids_str = form.get("message_ids", "")
    if not phone or not from_chat or not to_chat or not message_ids_str:
        return RedirectResponse(
            url=f"/dialogs/?phone={quote(phone, safe='')}&error=missing_fields",
            status_code=303,
        )
    ids = [int(x.strip()) for x in message_ids_str.split(",") if x.strip().isdigit()]
    if not ids:
        return RedirectResponse(
            url=f"/dialogs/?phone={quote(phone, safe='')}&error=invalid_ids",
            status_code=303,
        )
    return await _enqueue_dialog_command(
        request,
        "dialogs.forward_messages",
        payload={"phone": phone, "from_chat": from_chat, "to_chat": to_chat, "message_ids": ids},
        phone=phone,
    )


@router.post("/pin-message")
async def pin_message(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    chat_id = form.get("chat_id", "")
    message_id = form.get("message_id", "")
    notify = form.get("notify", "") in ("1", "true", "on")
    if not phone or not chat_id or not message_id:
        return RedirectResponse(
            url=f"/dialogs/?phone={quote(phone, safe='')}&error=missing_fields", status_code=303
        )
    return await _enqueue_dialog_command(
        request,
        "dialogs.pin_message",
        payload={"phone": phone, "chat_id": chat_id, "message_id": message_id, "notify": notify},
        phone=phone,
    )


@router.post("/unpin-message")
async def unpin_message(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    chat_id = form.get("chat_id", "")
    message_id_str = form.get("message_id", "")
    message_id = int(message_id_str) if message_id_str and message_id_str.isdigit() else None
    if not phone or not chat_id:
        return RedirectResponse(
            url=f"/dialogs/?phone={quote(phone, safe='')}&error=missing_fields", status_code=303
        )
    return await _enqueue_dialog_command(
        request,
        "dialogs.unpin_message",
        payload={"phone": phone, "chat_id": chat_id, "message_id": message_id},
        phone=phone,
    )


@router.post("/download-media")
async def download_media(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    chat_id = form.get("chat_id", "")
    message_id = form.get("message_id", "")
    if not phone or not chat_id or not message_id:
        return RedirectResponse(
            url=f"/dialogs/?phone={quote(phone, safe='')}&error=missing_fields", status_code=303
        )
    return await _enqueue_dialog_command(
        request,
        "dialogs.download_media",
        payload={"phone": phone, "chat_id": chat_id, "message_id": message_id},
        phone=phone,
    )


@router.get("/participants")
async def get_participants(request: Request):
    phone = request.query_params.get("phone", "")
    chat_id = request.query_params.get("chat_id", "")
    limit_str = request.query_params.get("limit", "")
    search = request.query_params.get("search", "")
    limit = int(limit_str) if limit_str and limit_str.isdigit() else 200
    if not phone or not chat_id:
        return JSONResponse({"error": "phone and chat_id required"}, status_code=400)
    scope = f"dialogs_participants:{phone}:{chat_id}:{search}:{limit}"
    snapshot = await deps.get_db(request).repos.runtime_snapshots.get_snapshot("dialogs_participants", scope)
    if snapshot is not None:
        return JSONResponse(snapshot.payload)
    command_id = await deps.telegram_command_service(request).enqueue(
        "dialogs.participants",
        payload={"phone": phone, "chat_id": chat_id, "limit": limit, "search": search},
        requested_by="web:dialogs",
    )
    return JSONResponse({"status": "queued", "command_id": command_id}, status_code=202)


@router.post("/edit-admin")
async def edit_admin(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    chat_id = form.get("chat_id", "")
    user_id = form.get("user_id", "")
    title = form.get("title", "") or None
    is_admin = form.get("is_admin", "0") in ("1", "true", "on")
    if not phone or not chat_id or not user_id:
        return RedirectResponse(
            url=f"/dialogs/?phone={quote(phone, safe='')}&error=missing_fields", status_code=303
        )
    return await _enqueue_dialog_command(
        request,
        "dialogs.edit_admin",
        payload={"phone": phone, "chat_id": chat_id, "user_id": user_id, "title": title, "is_admin": is_admin},
        phone=phone,
    )


@router.post("/edit-permissions")
async def edit_permissions(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    chat_id = form.get("chat_id", "")
    user_id = form.get("user_id", "")
    until_date_str = form.get("until_date", "") or None
    send_messages_str = form.get("send_messages")
    send_media_str = form.get("send_media")
    if send_messages_str is None and send_media_str is None:
        return RedirectResponse(
            url=f"/dialogs/?phone={quote(phone, safe='')}&error=no_permission_flags", status_code=303
        )
    if not phone or not chat_id or not user_id:
        return RedirectResponse(
            url=f"/dialogs/?phone={quote(phone, safe='')}&error=missing_fields", status_code=303
        )
    payload = {"phone": phone, "chat_id": chat_id, "user_id": user_id}
    if until_date_str:
        payload["until_date"] = until_date_str
    if send_messages_str is not None:
        payload["send_messages"] = send_messages_str in ("1", "true", "on")
    if send_media_str is not None:
        payload["send_media"] = send_media_str in ("1", "true", "on")
    return await _enqueue_dialog_command(
        request,
        "dialogs.edit_permissions",
        payload=payload,
        phone=phone,
    )


@router.post("/kick")
async def kick_participant(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    chat_id = form.get("chat_id", "")
    user_id = form.get("user_id", "")
    if not phone or not chat_id or not user_id:
        return RedirectResponse(
            url=f"/dialogs/?phone={quote(phone, safe='')}&error=missing_fields", status_code=303
        )
    return await _enqueue_dialog_command(
        request,
        "dialogs.kick",
        payload={"phone": phone, "chat_id": chat_id, "user_id": user_id},
        phone=phone,
    )


@router.get("/broadcast-stats")
async def broadcast_stats(request: Request):
    phone = request.query_params.get("phone", "")
    chat_id = request.query_params.get("chat_id", "")
    if not phone or not chat_id:
        return JSONResponse({"error": "phone and chat_id required"}, status_code=400)
    scope = f"dialogs_broadcast_stats:{phone}:{chat_id}"
    snapshot = await deps.get_db(request).repos.runtime_snapshots.get_snapshot("dialogs_broadcast_stats", scope)
    if snapshot is not None:
        return JSONResponse(snapshot.payload)
    command_id = await deps.telegram_command_service(request).enqueue(
        "dialogs.broadcast_stats",
        payload={"phone": phone, "chat_id": chat_id},
        requested_by="web:dialogs",
    )
    return JSONResponse({"status": "queued", "command_id": command_id}, status_code=202)


@router.post("/archive")
async def archive_dialog(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    chat_id = form.get("chat_id", "")
    if not phone or not chat_id:
        return RedirectResponse(
            url=f"/dialogs/?phone={quote(phone, safe='')}&error=missing_fields", status_code=303
        )
    return await _enqueue_dialog_command(
        request,
        "dialogs.archive",
        payload={"phone": phone, "chat_id": chat_id},
        phone=phone,
    )


@router.post("/unarchive")
async def unarchive_dialog(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    chat_id = form.get("chat_id", "")
    if not phone or not chat_id:
        return RedirectResponse(
            url=f"/dialogs/?phone={quote(phone, safe='')}&error=missing_fields", status_code=303
        )
    return await _enqueue_dialog_command(
        request,
        "dialogs.unarchive",
        payload={"phone": phone, "chat_id": chat_id},
        phone=phone,
    )


@router.post("/mark-read")
async def mark_read(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    chat_id = form.get("chat_id", "")
    max_id_str = form.get("max_id", "") or None
    max_id = int(max_id_str) if max_id_str and max_id_str.isdigit() else None
    if not phone or not chat_id:
        return RedirectResponse(
            url=f"/dialogs/?phone={quote(phone, safe='')}&error=missing_fields", status_code=303
        )
    return await _enqueue_dialog_command(
        request,
        "dialogs.mark_read",
        payload={"phone": phone, "chat_id": chat_id, "max_id": max_id},
        phone=phone,
    )


@router.get("/create-channel", response_class=HTMLResponse)
async def create_channel_page(request: Request):
    db = deps.get_db(request)
    accounts = sorted(account.phone for account in await db.get_accounts(active_only=False))
    command = await _get_command_state(request, request.query_params.get("command_id"))
    return deps.get_templates(request).TemplateResponse(
        request,
        "dialogs_create_channel.html",
        {"accounts": accounts, "command": command},
    )


@router.post("/create-channel")
async def create_channel(
    request: Request,
    phone: str = Form(...),
    title: str = Form(...),
    about: str = Form(""),
    username: str = Form(""),
):
    command_id = await deps.telegram_command_service(request).enqueue(
        "dialogs.create_channel",
        payload={
            "phone": phone,
            "title": title,
            "about": about,
            "username": username,
        },
        requested_by="web:dialogs",
    )
    return RedirectResponse(
        url=f"/dialogs/create-channel?command_id={command_id}",
        status_code=303,
    )
