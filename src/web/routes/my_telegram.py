from __future__ import annotations

import logging
import time
from urllib.parse import quote

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from src.web import deps

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/", response_class=HTMLResponse)
async def my_telegram_page(
    request: Request,
    phone: str | None = None,
    left: int = 0,
    failed: int = 0,
):
    started_at = time.perf_counter()
    pool = deps.get_pool(request)
    accounts = sorted(pool.clients.keys())
    selected_phone = phone if phone in pool.clients else None
    dialogs = []
    dialogs_cached_at = None
    if selected_phone:
        dialogs = await deps.channel_service(request).get_my_dialogs(selected_phone)
        dialogs_cached_at = await deps.get_db(request).repos.dialog_cache.get_cached_at(
            selected_phone
        )
    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    logger.info(
        "my_telegram_page: phone=%s accounts=%d dialogs=%d duration_ms=%d",
        selected_phone,
        len(accounts),
        len(dialogs),
        elapsed_ms,
    )
    return deps.get_templates(request).TemplateResponse(
        request,
        "my_telegram.html",
        {
            "accounts": accounts,
            "selected_phone": selected_phone,
            "dialogs": dialogs,
            "dialogs_cached_at": dialogs_cached_at,
            "left": left,
            "failed": failed,
        },
    )


@router.post("/refresh")
async def refresh_dialogs(request: Request, phone: str = Form(...)):
    await deps.channel_service(request).get_my_dialogs(phone, refresh=True)
    return RedirectResponse(
        url=f"/my-telegram/?phone={quote(phone, safe='')}",
        status_code=303,
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
    pool = deps.get_pool(request)
    db = deps.get_db(request)
    if phone:
        pool.invalidate_dialogs_cache(phone)
        await db.repos.dialog_cache.clear_dialogs(phone)
    else:
        pool.invalidate_dialogs_cache()
        await db.repos.dialog_cache.clear_all_dialogs()
    redirect_phone = f"?phone={quote(phone, safe='')}" if phone else ""
    return RedirectResponse(
        url=f"/my-telegram/{redirect_phone}&msg=cache_cleared" if phone else "/my-telegram/?msg=cache_cleared",
        status_code=303,
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
    results = await deps.channel_service(request).leave_dialogs(phone, dialogs)
    left = sum(1 for v in results.values() if v)
    failed = len(results) - left
    return RedirectResponse(
        url=f"/my-telegram/?phone={quote(phone, safe='')}&left={left}&failed={failed}",
        status_code=303,
    )


@router.post("/send")
async def send_message(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    recipient = form.get("recipient", "")
    text = form.get("text", "")
    pool = deps.get_pool(request)
    if not phone or not recipient or not text:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=missing_fields",
            status_code=303,
        )
    result = await pool.get_native_client_by_phone(phone)
    if result is None:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=client_unavailable",
            status_code=303,
        )
    client, _ = result
    try:
        entity = await client.get_entity(recipient)
        await client.send_message(entity, text)
        logger.info("Message sent from %s to %s", phone, recipient)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&msg=message_sent",
            status_code=303,
        )
    except Exception as exc:
        logger.exception("Failed to send message: %s", exc)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=send_failed",
            status_code=303,
        )


@router.post("/edit-message")
async def edit_message(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    chat_id = form.get("chat_id", "")
    message_id = form.get("message_id", "")
    text = form.get("text", "")
    pool = deps.get_pool(request)
    if not phone or not chat_id or not message_id or not text:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=missing_fields",
            status_code=303,
        )
    result = await pool.get_native_client_by_phone(phone)
    if result is None:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=client_unavailable",
            status_code=303,
        )
    client, _ = result
    try:
        entity = await client.get_entity(chat_id)
        await client.edit_message(entity, int(message_id), text)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&msg=message_edited",
            status_code=303,
        )
    except Exception as exc:
        logger.exception("Failed to edit message: %s", exc)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=edit_failed",
            status_code=303,
        )


@router.post("/delete-message")
async def delete_message(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    chat_id = form.get("chat_id", "")
    message_ids_str = form.get("message_ids", "")
    pool = deps.get_pool(request)
    if not phone or not chat_id or not message_ids_str:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=missing_fields",
            status_code=303,
        )
    ids = [int(x.strip()) for x in message_ids_str.split(",") if x.strip().isdigit()]
    if not ids:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=invalid_ids",
            status_code=303,
        )
    result = await pool.get_native_client_by_phone(phone)
    if result is None:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=client_unavailable",
            status_code=303,
        )
    client, _ = result
    try:
        entity = await client.get_entity(chat_id)
        await client.delete_messages(entity, ids)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&msg=messages_deleted",
            status_code=303,
        )
    except Exception as exc:
        logger.exception("Failed to delete messages: %s", exc)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=delete_failed",
            status_code=303,
        )


@router.post("/pin-message")
async def pin_message(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    chat_id = form.get("chat_id", "")
    message_id = form.get("message_id", "")
    notify = form.get("notify", "") in ("1", "true", "on")
    pool = deps.get_pool(request)
    if not phone or not chat_id or not message_id:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=missing_fields", status_code=303
        )
    result = await pool.get_native_client_by_phone(phone)
    if result is None:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=client_unavailable", status_code=303
        )
    client, _ = result
    try:
        entity = await client.get_entity(chat_id)
        await client.pin_message(entity, int(message_id), notify=notify)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&msg=message_pinned", status_code=303
        )
    except Exception as exc:
        logger.exception("Failed to pin message: %s", exc)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=pin_failed", status_code=303
        )


@router.post("/unpin-message")
async def unpin_message(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    chat_id = form.get("chat_id", "")
    message_id_str = form.get("message_id", "")
    message_id = int(message_id_str) if message_id_str and message_id_str.isdigit() else None
    pool = deps.get_pool(request)
    if not phone or not chat_id:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=missing_fields", status_code=303
        )
    result = await pool.get_native_client_by_phone(phone)
    if result is None:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=client_unavailable", status_code=303
        )
    client, _ = result
    try:
        entity = await client.get_entity(chat_id)
        await client.unpin_message(entity, message_id)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&msg=message_unpinned", status_code=303
        )
    except Exception as exc:
        logger.exception("Failed to unpin message: %s", exc)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=unpin_failed", status_code=303
        )


@router.post("/download-media")
async def download_media(request: Request):
    import os
    import pathlib

    from fastapi.responses import FileResponse

    form = await request.form()
    phone = form.get("phone", "")
    chat_id = form.get("chat_id", "")
    message_id = form.get("message_id", "")
    pool = deps.get_pool(request)
    if not phone or not chat_id or not message_id:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=missing_fields", status_code=303
        )
    result = await pool.get_native_client_by_phone(phone)
    if result is None:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=client_unavailable", status_code=303
        )
    client, _ = result
    try:
        entity = await client.get_entity(chat_id)
        msg = None
        async for m in client._client.iter_messages(entity, ids=int(message_id)):
            msg = m
            break
        if msg is None:
            return RedirectResponse(
                url=f"/my-telegram/?phone={quote(phone, safe='')}&error=message_not_found", status_code=303
            )
        output_dir = pathlib.Path("data/downloads")
        output_dir.mkdir(parents=True, exist_ok=True)
        path = await client.download_media(msg, file=str(output_dir))
        if not path:
            return RedirectResponse(
                url=f"/my-telegram/?phone={quote(phone, safe='')}&error=no_media", status_code=303
            )
        return FileResponse(path=path, filename=os.path.basename(path))
    except Exception as exc:
        logger.exception("Failed to download media: %s", exc)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=download_failed", status_code=303
        )


@router.get("/participants")
async def get_participants(request: Request):
    phone = request.query_params.get("phone", "")
    chat_id = request.query_params.get("chat_id", "")
    limit_str = request.query_params.get("limit", "")
    search = request.query_params.get("search", "")
    limit = int(limit_str) if limit_str and limit_str.isdigit() else None
    pool = deps.get_pool(request)
    if not phone or not chat_id:
        return JSONResponse({"error": "phone and chat_id required"}, status_code=400)
    result = await pool.get_native_client_by_phone(phone)
    if result is None:
        return JSONResponse({"error": "client unavailable"}, status_code=503)
    client, _ = result
    try:
        entity = await client.get_entity(chat_id)
        participants = await client.get_participants(entity, limit=limit, search=search)
        data = [
            {
                "id": p.id,
                "first_name": getattr(p, "first_name", None) or "",
                "last_name": getattr(p, "last_name", None) or "",
                "username": getattr(p, "username", None) or "",
            }
            for p in participants
        ]
        return JSONResponse({"participants": data, "total": len(data)})
    except Exception as exc:
        logger.exception("Failed to get participants: %s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)


@router.post("/edit-admin")
async def edit_admin(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    chat_id = form.get("chat_id", "")
    user_id = form.get("user_id", "")
    title = form.get("title", "") or None
    pool = deps.get_pool(request)
    if not phone or not chat_id or not user_id:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=missing_fields", status_code=303
        )
    result = await pool.get_native_client_by_phone(phone)
    if result is None:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=client_unavailable", status_code=303
        )
    client, _ = result
    try:
        entity = await client.get_entity(chat_id)
        user = await client.get_entity(user_id)
        kwargs = {}
        if title:
            kwargs["title"] = title
        await client.edit_admin(entity, user, **kwargs)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&msg=admin_updated", status_code=303
        )
    except Exception as exc:
        logger.exception("Failed to edit admin: %s", exc)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=edit_admin_failed", status_code=303
        )


@router.post("/edit-permissions")
async def edit_permissions(request: Request):
    from datetime import datetime

    form = await request.form()
    phone = form.get("phone", "")
    chat_id = form.get("chat_id", "")
    user_id = form.get("user_id", "")
    until_date_str = form.get("until_date", "") or None
    pool = deps.get_pool(request)
    if not phone or not chat_id or not user_id:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=missing_fields", status_code=303
        )
    result = await pool.get_native_client_by_phone(phone)
    if result is None:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=client_unavailable", status_code=303
        )
    client, _ = result
    try:
        entity = await client.get_entity(chat_id)
        user = await client.get_entity(user_id)
        until_date = datetime.fromisoformat(until_date_str) if until_date_str else None
        await client.edit_permissions(entity, user, until_date=until_date)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&msg=permissions_updated", status_code=303
        )
    except Exception as exc:
        logger.exception("Failed to edit permissions: %s", exc)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=edit_permissions_failed", status_code=303
        )


@router.post("/kick")
async def kick_participant(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    chat_id = form.get("chat_id", "")
    user_id = form.get("user_id", "")
    pool = deps.get_pool(request)
    if not phone or not chat_id or not user_id:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=missing_fields", status_code=303
        )
    result = await pool.get_native_client_by_phone(phone)
    if result is None:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=client_unavailable", status_code=303
        )
    client, _ = result
    try:
        entity = await client.get_entity(chat_id)
        user = await client.get_entity(user_id)
        await client.kick_participant(entity, user)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&msg=user_kicked", status_code=303
        )
    except Exception as exc:
        logger.exception("Failed to kick participant: %s", exc)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=kick_failed", status_code=303
        )


@router.get("/broadcast-stats")
async def broadcast_stats(request: Request):
    phone = request.query_params.get("phone", "")
    chat_id = request.query_params.get("chat_id", "")
    pool = deps.get_pool(request)
    if not phone or not chat_id:
        return JSONResponse({"error": "phone and chat_id required"}, status_code=400)
    result = await pool.get_native_client_by_phone(phone)
    if result is None:
        return JSONResponse({"error": "client unavailable"}, status_code=503)
    client, _ = result
    try:
        entity = await client.get_entity(chat_id)
        stats = await client.get_broadcast_stats(entity)
        return JSONResponse({"stats": str(stats)})
    except Exception as exc:
        logger.exception("Failed to get broadcast stats: %s", exc)
        return JSONResponse({"error": str(exc)}, status_code=500)


@router.post("/archive")
async def archive_dialog(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    chat_id = form.get("chat_id", "")
    pool = deps.get_pool(request)
    if not phone or not chat_id:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=missing_fields", status_code=303
        )
    result = await pool.get_native_client_by_phone(phone)
    if result is None:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=client_unavailable", status_code=303
        )
    client, _ = result
    try:
        entity = await client.get_entity(chat_id)
        await client.edit_folder(entity, 1)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&msg=dialog_archived", status_code=303
        )
    except Exception as exc:
        logger.exception("Failed to archive dialog: %s", exc)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=archive_failed", status_code=303
        )


@router.post("/unarchive")
async def unarchive_dialog(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    chat_id = form.get("chat_id", "")
    pool = deps.get_pool(request)
    if not phone or not chat_id:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=missing_fields", status_code=303
        )
    result = await pool.get_native_client_by_phone(phone)
    if result is None:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=client_unavailable", status_code=303
        )
    client, _ = result
    try:
        entity = await client.get_entity(chat_id)
        await client.edit_folder(entity, 0)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&msg=dialog_unarchived", status_code=303
        )
    except Exception as exc:
        logger.exception("Failed to unarchive dialog: %s", exc)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=unarchive_failed", status_code=303
        )


@router.post("/mark-read")
async def mark_read(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    chat_id = form.get("chat_id", "")
    max_id_str = form.get("max_id", "") or None
    max_id = int(max_id_str) if max_id_str and max_id_str.isdigit() else None
    pool = deps.get_pool(request)
    if not phone or not chat_id:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=missing_fields", status_code=303
        )
    result = await pool.get_native_client_by_phone(phone)
    if result is None:
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=client_unavailable", status_code=303
        )
    client, _ = result
    try:
        entity = await client.get_entity(chat_id)
        await client.send_read_acknowledge(entity, max_id=max_id)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&msg=messages_marked_read", status_code=303
        )
    except Exception as exc:
        logger.exception("Failed to mark messages as read: %s", exc)
        return RedirectResponse(
            url=f"/my-telegram/?phone={quote(phone, safe='')}&error=mark_read_failed", status_code=303
        )


@router.get("/create-channel", response_class=HTMLResponse)
async def create_channel_page(request: Request):
    pool = deps.get_pool(request)
    accounts = sorted(pool.clients.keys())
    return deps.get_templates(request).TemplateResponse(
        request,
        "my_telegram_create_channel.html",
        {"accounts": accounts},
    )


@router.post("/create-channel")
async def create_channel(
    request: Request,
    phone: str = Form(...),
    title: str = Form(...),
    about: str = Form(""),
    username: str = Form(""),
):
    pool = deps.get_pool(request)
    client = pool.clients.get(phone)
    if client is None:
        return RedirectResponse(
            url="/my-telegram/create-channel?error=no_client",
            status_code=303,
        )
    try:
        from telethon.tl.functions.channels import CreateChannelRequest

        result = await client(
            CreateChannelRequest(
                title=title.strip(),
                about=about.strip(),
                broadcast=True,
                megagroup=False,
            )
        )
        channel = result.chats[0] if result.chats else None
        if channel is None:
            raise RuntimeError("Telegram returned empty response — channel may not have been created")
        channel_id = channel.id
        channel_username = getattr(channel, "username", None) or ""

        if username.strip() and channel_id:
            try:
                from telethon.tl.functions.channels import UpdateUsernameRequest

                await client(UpdateUsernameRequest(channel, username.strip()))
                channel_username = username.strip()
            except Exception:
                logger.warning(
                    "Could not set username %r for new channel id=%s", username, channel_id
                )

        logger.info("Created channel id=%s title=%r by %s", channel_id, title, phone)
        invite_link = f"https://t.me/{channel_username}" if channel_username else ""
        return deps.get_templates(request).TemplateResponse(
            request,
            "my_telegram_create_channel.html",
            {
                "accounts": sorted(pool.clients.keys()),
                "created": True,
                "channel_id": channel_id,
                "channel_title": title,
                "channel_username": channel_username,
                "invite_link": invite_link,
            },
        )
    except Exception as exc:
        logger.exception("Failed to create channel: %s", exc)
        return deps.get_templates(request).TemplateResponse(
            request,
            "my_telegram_create_channel.html",
            {
                "accounts": sorted(pool.clients.keys()),
                "error": str(exc)[:200],
            },
        )
