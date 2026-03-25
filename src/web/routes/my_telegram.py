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
