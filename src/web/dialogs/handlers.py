"""Application orchestration for the dialogs web domain."""

from __future__ import annotations

import logging
import time
from typing import cast

from fastapi import Request

from src.models import AccountSessionStatus
from src.telegram.reactions import TelegramReactionInvalidError, normalize_outgoing_reaction_emoji
from src.web import deps
from src.web.dialogs.forms import (
    CacheClearForm,
    ChatActionForm,
    CreateChannelForm,
    DeleteDialogsForm,
    DownloadMediaForm,
    EditAdminForm,
    EditMessageForm,
    EditPermissionsForm,
    ForwardMessagesForm,
    JoinDialogForm,
    KickParticipantForm,
    LeaveDialogsForm,
    MarkReadForm,
    MessageIdsForm,
    PinMessageForm,
    ReactionForm,
    RefreshDialogsForm,
    ResolveEntityForm,
    SendMessageForm,
    UnpinMessageForm,
    parse_dialog_form,
)
from src.web.dialogs.responses import (
    CommandRedirect,
    DialogJson,
    DialogRedirect,
    DialogTemplate,
    PathRedirect,
)

logger = logging.getLogger(__name__)


async def _enqueue(
    request: Request,
    command_type: str,
    *,
    payload: dict,
    phone: str | None = None,
    target_path: str = "/dialogs/",
) -> CommandRedirect:
    command_service = deps.telegram_command_service(request)
    command_id = await command_service.enqueue(
        command_type,
        payload=payload,
        requested_by="web:dialogs",
    )
    return CommandRedirect(command_id=command_id, phone=phone or "", target_path=target_path)


async def _get_command_state(request: Request, command_id: str | None):
    if not command_id or not command_id.isdigit():
        return None
    return await deps.telegram_command_service(request).get(int(command_id))


async def _ok_accounts(db) -> list[str]:
    return sorted(
        account.phone
        for account in await db.get_account_summaries(active_only=False)
        if account.session_status == AccountSessionStatus.OK
    )


async def dialogs_page(
    request: Request,
    phone: str | None = None,
    left: int = 0,
    failed: int = 0,
) -> DialogTemplate:
    """Skeleton — account selector only. The dialog list (a network/cache fetch) loads
    lazily via the fragment, so changing accounts no longer reloads the whole page (#756)."""
    db = deps.get_db(request)
    accounts = await _ok_accounts(db)
    selected_phone = phone if phone in accounts else None
    return DialogTemplate(
        "dialogs.html",
        {
            "accounts": accounts,
            "selected_phone": selected_phone,
        },
    )


async def dialogs_list_fragment(
    request: Request,
    phone: str | None = None,
    left: int = 0,
    failed: int = 0,
) -> DialogTemplate:
    """Heavy fragment: the selected account's dialog list (#756)."""
    started_at = time.perf_counter()
    db = deps.get_db(request)
    channel_service = deps.channel_service(request)
    accounts = await _ok_accounts(db)
    selected_phone = phone if phone in accounts else None
    dialogs = []
    dialogs_cached_at = None
    if selected_phone:
        dialogs = await channel_service.get_my_dialogs(selected_phone)
        dialogs_cached_at = await db.repos.dialog_cache.get_cached_at(selected_phone)
    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    logger.info(
        "dialogs_list_fragment: phone=%s accounts=%d dialogs=%d duration_ms=%d",
        selected_phone,
        len(accounts),
        len(dialogs),
        elapsed_ms,
    )
    return DialogTemplate(
        "dialogs/_list.html",
        {
            "selected_phone": selected_phone,
            "dialogs": dialogs,
            "dialogs_cached_at": dialogs_cached_at,
            "left": left,
            "failed": failed,
        },
    )


async def refresh_dialogs(request: Request) -> CommandRedirect | DialogRedirect:
    form = await parse_dialog_form(request, RefreshDialogsForm)
    if not form.phone:
        return DialogRedirect(error="missing_fields")
    return await _enqueue(request, "dialogs.refresh", payload={"phone": form.phone}, phone=form.phone)


async def cache_status(request: Request) -> DialogJson:
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
    return DialogJson(result)


async def cache_clear(request: Request) -> CommandRedirect:
    form = await parse_dialog_form(request, CacheClearForm)
    return await _enqueue(
        request, "dialogs.cache_clear", payload={"phone": form.phone}, phone=form.phone or None
    )


async def leave_dialogs(request: Request) -> CommandRedirect:
    form = await parse_dialog_form(request, LeaveDialogsForm)
    return await _enqueue(
        request,
        "dialogs.leave",
        payload={
            "phone": form.phone,
            "dialogs": [
                {"dialog_id": dialog_id, "channel_type": channel_type}
                for dialog_id, channel_type in form.dialogs
            ],
        },
        phone=form.phone or None,
    )


async def delete_dialogs(request: Request) -> CommandRedirect:
    form = await parse_dialog_form(request, DeleteDialogsForm)
    return await _enqueue(
        request,
        "dialogs.delete",
        payload={
            "phone": form.phone,
            "dialogs": [
                {"dialog_id": dialog_id, "channel_type": channel_type}
                for dialog_id, channel_type in form.dialogs
            ],
        },
        phone=form.phone or None,
    )


async def send_message(request: Request) -> CommandRedirect | DialogRedirect:
    form = await parse_dialog_form(request, SendMessageForm)
    if not form.phone or not form.recipient or not form.text:
        return DialogRedirect(form.phone, error="missing_fields")
    return await _enqueue(
        request,
        "dialogs.send",
        payload={"phone": form.phone, "recipient": form.recipient, "text": form.text},
        phone=form.phone,
    )


async def join_dialog(request: Request) -> CommandRedirect | DialogRedirect:
    form = await parse_dialog_form(request, JoinDialogForm)
    if not form.phone or not form.target:
        return DialogRedirect(form.phone, error="missing_fields")
    return await _enqueue(
        request,
        "dialogs.join",
        payload={"phone": form.phone, "target": form.target},
        phone=form.phone,
    )


async def resolve_entity(request: Request) -> CommandRedirect | DialogRedirect:
    form = await parse_dialog_form(request, ResolveEntityForm)
    if not form.identifier:
        return DialogRedirect(form.phone, error="missing_fields")
    return await _enqueue(
        request,
        "dialogs.resolve",
        payload={"phone": form.phone, "identifier": form.identifier},
        phone=form.phone,
    )


async def edit_message(request: Request) -> CommandRedirect | DialogRedirect:
    form = await parse_dialog_form(request, EditMessageForm)
    if not form.phone or not form.chat_id or not form.message_id or not form.text:
        return DialogRedirect(form.phone, error="missing_fields")
    return await _enqueue(
        request,
        "dialogs.edit_message",
        payload={"phone": form.phone, "chat_id": form.chat_id, "message_id": form.message_id, "text": form.text},
        phone=form.phone,
    )


async def delete_message(request: Request) -> CommandRedirect | DialogRedirect:
    form = await parse_dialog_form(request, MessageIdsForm)
    if not form.phone or not form.chat_id or not form.message_ids:
        return DialogRedirect(form.phone, error="missing_fields")
    if not form.ids:
        return DialogRedirect(form.phone, error="invalid_ids")
    return await _enqueue(
        request,
        "dialogs.delete_message",
        payload={"phone": form.phone, "chat_id": form.chat_id, "message_ids": form.ids},
        phone=form.phone,
    )


async def forward_messages(request: Request) -> CommandRedirect | DialogRedirect:
    form = await parse_dialog_form(request, ForwardMessagesForm)
    if not form.phone or not form.from_chat or not form.to_chat or not form.message_ids:
        return DialogRedirect(form.phone, error="missing_fields")
    if not form.ids:
        return DialogRedirect(form.phone, error="invalid_ids")
    return await _enqueue(
        request,
        "dialogs.forward_messages",
        payload={"phone": form.phone, "from_chat": form.from_chat, "to_chat": form.to_chat, "message_ids": form.ids},
        phone=form.phone,
    )


async def pin_message(request: Request) -> CommandRedirect | DialogRedirect:
    form = await parse_dialog_form(request, PinMessageForm)
    if not form.phone or not form.chat_id or not form.message_id:
        return DialogRedirect(form.phone, error="missing_fields")
    return await _enqueue(
        request,
        "dialogs.pin_message",
        payload={"phone": form.phone, "chat_id": form.chat_id, "message_id": form.message_id, "notify": form.notify},
        phone=form.phone,
    )


async def react_message(request: Request) -> CommandRedirect | DialogRedirect:
    form = await parse_dialog_form(request, ReactionForm)
    if not form.phone or not form.chat_id or not form.message_id or not form.emoji:
        return DialogRedirect(form.phone, error="missing_fields")
    if not form.message_id.isdigit():
        return DialogRedirect(form.phone, error="invalid_message_id")
    try:
        emoji = normalize_outgoing_reaction_emoji(form.emoji)
    except TelegramReactionInvalidError:
        return DialogRedirect(form.phone, error="invalid_reaction")
    return await _enqueue(
        request,
        "dialogs.react",
        payload={"phone": form.phone, "chat_id": form.chat_id, "message_id": int(form.message_id), "emoji": emoji},
        phone=form.phone,
    )


async def cancel_queue_command(request: Request, command_id: int) -> DialogRedirect:
    """Cancel a pending Telegram command living in `telegram_commands` (issue #621)."""
    form = await request.form()
    phone = cast(str, form.get("phone") or "") if hasattr(form, "get") else ""
    ok = await deps.telegram_command_service(request).cancel(command_id)
    error = None if ok else "command_not_cancellable"
    msg = "command_cancelled" if ok else None
    return DialogRedirect(phone, msg=msg, error=error)


async def clear_pending_queue_commands(request: Request) -> DialogRedirect:
    """Bulk-cancel pending Telegram commands. Optional filters: command_type, phone."""
    form = await request.form()
    if hasattr(form, "get"):
        phone = str(form.get("phone") or "").strip() or None
        command_type = str(form.get("command_type") or "").strip() or None
    else:
        phone = None
        command_type = None
    cancelled = await deps.telegram_command_service(request).cancel_pending(
        command_type=command_type, phone=phone
    )
    msg = "pending_commands_cancelled" if cancelled > 0 else "pending_commands_empty"
    return DialogRedirect(phone or "", msg=msg)


async def unpin_message(request: Request) -> CommandRedirect | DialogRedirect:
    form = await parse_dialog_form(request, UnpinMessageForm)
    if not form.phone or not form.chat_id:
        return DialogRedirect(form.phone, error="missing_fields")
    return await _enqueue(
        request,
        "dialogs.unpin_message",
        payload={"phone": form.phone, "chat_id": form.chat_id, "message_id": form.parsed_message_id},
        phone=form.phone,
    )


async def download_media(request: Request) -> CommandRedirect | DialogRedirect:
    form = await parse_dialog_form(request, DownloadMediaForm)
    if not form.phone or not form.chat_id or not form.message_id:
        return DialogRedirect(form.phone, error="missing_fields")
    return await _enqueue(
        request,
        "dialogs.download_media",
        payload={"phone": form.phone, "chat_id": form.chat_id, "message_id": form.message_id},
        phone=form.phone,
    )


async def get_participants(request: Request) -> DialogJson:
    db = deps.get_db(request)
    command_service = deps.telegram_command_service(request)
    phone = request.query_params.get("phone", "")
    chat_id = request.query_params.get("chat_id", "")
    limit_str = request.query_params.get("limit", "")
    search = request.query_params.get("search", "")
    limit = int(limit_str) if limit_str and limit_str.isdigit() else 200
    if not phone or not chat_id:
        return DialogJson({"error": "phone and chat_id required"}, status_code=400)
    scope = f"dialogs_participants:{phone}:{chat_id}"
    # The cached snapshot is keyed only by (phone, chat_id), so it does not
    # reflect a specific search string. When the caller asks for a filtered
    # search, bypass the cache and always enqueue a fresh command; otherwise
    # an older search result would be returned silently.
    if not search:
        snapshot = await db.repos.runtime_snapshots.get_snapshot("dialogs_participants", scope)
        if snapshot is not None:
            return DialogJson(snapshot.payload)
    command_id = await command_service.enqueue(
        "dialogs.participants",
        payload={"phone": phone, "chat_id": chat_id, "limit": limit, "search": search},
        requested_by="web:dialogs",
    )
    return DialogJson({"status": "queued", "command_id": command_id}, status_code=202)


async def edit_admin(request: Request) -> CommandRedirect | DialogRedirect:
    form = await parse_dialog_form(request, EditAdminForm)
    if not form.phone or not form.chat_id or not form.user_id:
        return DialogRedirect(form.phone, error="missing_fields")
    return await _enqueue(
        request,
        "dialogs.edit_admin",
        payload={
            "phone": form.phone, "chat_id": form.chat_id, "user_id": form.user_id,
            "title": form.title, "is_admin": form.is_admin,
        },
        phone=form.phone,
    )


async def edit_permissions(request: Request) -> CommandRedirect | DialogRedirect:
    form = await parse_dialog_form(request, EditPermissionsForm)
    if form.send_messages is None and form.send_media is None:
        return DialogRedirect(form.phone, error="no_permission_flags")
    if not form.phone or not form.chat_id or not form.user_id:
        return DialogRedirect(form.phone, error="missing_fields")
    payload: dict[str, object] = {"phone": form.phone, "chat_id": form.chat_id, "user_id": form.user_id}
    if form.until_date:
        payload["until_date"] = form.until_date
    if form.send_messages is not None:
        payload["send_messages"] = form.send_messages
    if form.send_media is not None:
        payload["send_media"] = form.send_media
    return await _enqueue(request, "dialogs.edit_permissions", payload=payload, phone=form.phone)


async def kick_participant(request: Request) -> CommandRedirect | DialogRedirect:
    form = await parse_dialog_form(request, KickParticipantForm)
    if not form.phone or not form.chat_id or not form.user_id:
        return DialogRedirect(form.phone, error="missing_fields")
    return await _enqueue(
        request,
        "dialogs.kick",
        payload={"phone": form.phone, "chat_id": form.chat_id, "user_id": form.user_id},
        phone=form.phone,
    )


async def broadcast_stats(request: Request) -> DialogJson:
    db = deps.get_db(request)
    command_service = deps.telegram_command_service(request)
    phone = request.query_params.get("phone", "")
    chat_id = request.query_params.get("chat_id", "")
    if not phone or not chat_id:
        return DialogJson({"error": "phone and chat_id required"}, status_code=400)
    scope = f"dialogs_broadcast_stats:{phone}:{chat_id}"
    snapshot = await db.repos.runtime_snapshots.get_snapshot("dialogs_broadcast_stats", scope)
    if snapshot is not None:
        return DialogJson(snapshot.payload)
    command_id = await command_service.enqueue(
        "dialogs.broadcast_stats",
        payload={"phone": phone, "chat_id": chat_id},
        requested_by="web:dialogs",
    )
    return DialogJson({"status": "queued", "command_id": command_id}, status_code=202)


async def archive_dialog(request: Request) -> CommandRedirect | DialogRedirect:
    form = await parse_dialog_form(request, ChatActionForm)
    if not form.phone or not form.chat_id:
        return DialogRedirect(form.phone, error="missing_fields")
    return await _enqueue(
        request,
        "dialogs.archive",
        payload={"phone": form.phone, "chat_id": form.chat_id},
        phone=form.phone,
    )


async def unarchive_dialog(request: Request) -> CommandRedirect | DialogRedirect:
    form = await parse_dialog_form(request, ChatActionForm)
    if not form.phone or not form.chat_id:
        return DialogRedirect(form.phone, error="missing_fields")
    return await _enqueue(
        request,
        "dialogs.unarchive",
        payload={"phone": form.phone, "chat_id": form.chat_id},
        phone=form.phone,
    )


async def mark_read(request: Request) -> CommandRedirect | DialogRedirect:
    form = await parse_dialog_form(request, MarkReadForm)
    if not form.phone or not form.chat_id:
        return DialogRedirect(form.phone, error="missing_fields")
    return await _enqueue(
        request,
        "dialogs.mark_read",
        payload={"phone": form.phone, "chat_id": form.chat_id, "max_id": form.parsed_max_id},
        phone=form.phone,
    )


async def create_channel_page(request: Request) -> DialogTemplate:
    db = deps.get_db(request)
    accounts = await _ok_accounts(db)
    command = await _get_command_state(request, request.query_params.get("command_id"))
    return DialogTemplate(
        "dialogs_create_channel.html",
        {"accounts": accounts, "command": command},
    )


async def create_channel(request: Request) -> CommandRedirect | PathRedirect:
    form = await parse_dialog_form(request, CreateChannelForm)
    if not form.phone or not form.title:
        return PathRedirect("/dialogs/create-channel", {"error": "missing_fields"})
    return await _enqueue(
        request,
        "dialogs.create_channel",
        payload={
            "phone": form.phone,
            "title": form.title,
            "about": form.about,
            "username": form.username,
        },
        target_path="/dialogs/create-channel",
    )
