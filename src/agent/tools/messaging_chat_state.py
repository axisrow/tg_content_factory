from __future__ import annotations

import inspect
from typing import Any

from claude_agent_sdk import tool

from src.agent.tools._formatters import format_sender_identity
from src.agent.tools._registry import _text_response, require_confirmation, resolve_entity, resolve_live_read_phone
from src.agent.tools._telegram_runtime import find_single_dialog_id_by_title
from src.agent.tools.messaging_schemas import (
    ARCHIVE_CHAT_SCHEMA,
    GET_BROADCAST_STATS_SCHEMA,
    MARK_READ_SCHEMA,
    READ_MESSAGES_SCHEMA,
    UNARCHIVE_CHAT_SCHEMA,
)
from src.telegram.flood_wait import HandledFloodWaitError, run_with_flood_wait
from src.telegram.identity import extract_message_sender_identity


def register_chat_state_read_tools(db: Any, ctx: Any, client_pool: Any) -> list[Any]:
    tools: list[Any] = []

    @tool(
        "get_broadcast_stats",
        "Get broadcast statistics (followers, views, reactions) for a Telegram channel. "
        "Requires admin/owner rights on the channel.",
        GET_BROADCAST_STATS_SCHEMA,
    )
    async def get_broadcast_stats(args):
        live_gate = ctx.require_live_runtime("Получение статистики", tool_name="get_broadcast_stats")
        if live_gate:
            return live_gate
        phone, err = await ctx.resolve_phone(args.get("phone", ""))
        if err:
            return err
        perm_gate = await ctx.require_phone_permission(phone, "get_broadcast_stats")
        if perm_gate:
            return perm_gate
        chat_id = args.get("chat_id", "")
        if not chat_id:
            return _text_response("Ошибка: chat_id обязателен.")
        try:
            client, entity, err = await resolve_entity(client_pool, phone, chat_id)
            if err:
                return err
            stats = await client.get_broadcast_stats(entity)
            fields = {}
            for attr in ("followers", "views_per_post", "shares_per_post", "reactions_per_post", "forwards_per_post"):
                val = getattr(stats, attr, None)
                if val is not None:
                    current = getattr(val, "current", None)
                    previous = getattr(val, "previous", None)
                    if current is not None:
                        fields[attr] = f"{current} (prev: {previous})"
                    else:
                        fields[attr] = str(val)
            period = getattr(stats, "period", None)
            if period is not None:
                min_d = getattr(period, "min_date", None)
                max_d = getattr(period, "max_date", None)
                fields["period"] = f"{min_d} — {max_d}"
            en = getattr(stats, "enabled_notifications", None)
            if en is not None:
                fields["enabled_notifications"] = str(en)
            if not fields:
                fields["raw"] = str(stats)
            lines = [f"Статистика канала {chat_id}:"]
            for k, v in fields.items():
                lines.append(f"  {k}: {v}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения статистики: {e}")

    tools.append(get_broadcast_stats)

    @tool(
        "archive_chat",
        "Archive a Telegram dialog (move to archive folder). "
        "Ask user for confirmation first.",
        ARCHIVE_CHAT_SCHEMA,
    )
    async def archive_chat(args):
        live_gate = ctx.require_live_runtime("Архивирование чата", tool_name="archive_chat")
        if live_gate:
            return live_gate
        phone, err = await ctx.resolve_phone(args.get("phone", ""))
        if err:
            return err
        perm_gate = await ctx.require_phone_permission(phone, "archive_chat")
        if perm_gate:
            return perm_gate
        chat_id = args.get("chat_id", "")
        if not chat_id:
            return _text_response("Ошибка: chat_id обязателен.")
        gate = require_confirmation(f"архивирует чат {chat_id}", args)
        if gate:
            return gate
        try:
            client, entity, err = await resolve_entity(client_pool, phone, chat_id)
            if err:
                return err
            await client.edit_folder(entity, 1)
            return _text_response(f"Чат {chat_id} архивирован.")
        except Exception as e:
            return _text_response(f"Ошибка архивирования: {e}")

    tools.append(archive_chat)

    @tool(
        "unarchive_chat",
        "Unarchive a Telegram dialog (move back to main folder). "
        "Ask user for confirmation first.",
        UNARCHIVE_CHAT_SCHEMA,
    )
    async def unarchive_chat(args):
        live_gate = ctx.require_live_runtime("Разархивирование чата", tool_name="unarchive_chat")
        if live_gate:
            return live_gate
        phone, err = await ctx.resolve_phone(args.get("phone", ""))
        if err:
            return err
        perm_gate = await ctx.require_phone_permission(phone, "unarchive_chat")
        if perm_gate:
            return perm_gate
        chat_id = args.get("chat_id", "")
        if not chat_id:
            return _text_response("Ошибка: chat_id обязателен.")
        gate = require_confirmation(f"разархивирует чат {chat_id}", args)
        if gate:
            return gate
        try:
            client, entity, err = await resolve_entity(client_pool, phone, chat_id)
            if err:
                return err
            await client.edit_folder(entity, 0)
            return _text_response(f"Чат {chat_id} разархивирован.")
        except Exception as e:
            return _text_response(f"Ошибка разархивирования: {e}")

    tools.append(unarchive_chat)

    @tool(
        "mark_read",
        "Mark messages as read in a Telegram chat. "
        "max_id marks all messages up to that ID as read; omit to mark all.",
        MARK_READ_SCHEMA,
    )
    async def mark_read(args):
        live_gate = ctx.require_live_runtime("Отметка сообщений как прочитанных", tool_name="mark_read")
        if live_gate:
            return live_gate
        phone, err = await ctx.resolve_phone(args.get("phone", ""))
        if err:
            return err
        perm_gate = await ctx.require_phone_permission(phone, "mark_read")
        if perm_gate:
            return perm_gate
        chat_id = args.get("chat_id", "")
        max_id = args.get("max_id") or None
        if not chat_id:
            return _text_response("Ошибка: chat_id обязателен.")
        try:
            client, entity, err = await resolve_entity(client_pool, phone, chat_id)
            if err:
                return err
            await client.send_read_acknowledge(entity, max_id=max_id)
            return _text_response(f"Сообщения отмечены как прочитанные в {chat_id}.")
        except Exception as e:
            return _text_response(f"Ошибка отметки сообщений: {e}")

    tools.append(mark_read)

    @tool(
        "read_messages",
        "Preview last N messages from any Telegram chat/channel (not stored in DB). "
        "chat_id accepts @username, t.me link, numeric ID, or 'me'. "
        "To save messages to DB for search, use add_channel + collect_channel instead.",
        READ_MESSAGES_SCHEMA,
    )
    async def read_messages(args):
        live_gate = ctx.require_live_runtime("Чтение сообщений", tool_name="read_messages")
        if live_gate:
            return live_gate
        phone, err = await resolve_live_read_phone(db, client_pool, args.get("phone", ""), tool_name="read_messages")
        if err:
            return err
        perm_gate = await ctx.require_phone_permission(phone, "read_messages")
        if perm_gate:
            return perm_gate
        chat_id = args.get("chat_id", "")
        try:
            limit = max(1, min(int(args.get("limit") or 100), 500))
        except (TypeError, ValueError):
            limit = 100
        if not chat_id:
            return _text_response("Ошибка: chat_id обязателен.")
        try:
            client, entity, err = await resolve_entity(client_pool, phone, chat_id)
            if err:
                fallback_chat_id, fallback_err = await find_single_dialog_id_by_title(db, client_pool, phone, chat_id)
                if fallback_err:
                    return fallback_err
                if fallback_chat_id:
                    client, entity, err = await resolve_entity(client_pool, phone, fallback_chat_id)
                    if not err:
                        chat_id = fallback_chat_id
                if err:
                    return err
            lines = [f"Последние {limit} сообщений из {chat_id}:\n"]
            count = 0
            total_chars = 0
            budget = 50_000
            sender_cache: dict[int, object | None] = {}

            async def _resolve_message_sender(msg):
                sender = getattr(msg, "sender", None)
                if sender is not None:
                    return sender

                sender_id = getattr(msg, "sender_id", None)
                if sender_id is not None:
                    try:
                        cache_key = int(sender_id)
                    except (TypeError, ValueError):
                        cache_key = None
                    if cache_key is not None and cache_key in sender_cache:
                        return sender_cache[cache_key]
                else:
                    cache_key = None

                getter = getattr(msg, "get_sender", None)
                if not callable(getter):
                    return None
                try:
                    result = getter()
                    sender = await result if inspect.isawaitable(result) else result
                except Exception:
                    sender = None
                if cache_key is not None:
                    sender_cache[cache_key] = sender
                return sender

            async def _read_recent() -> None:
                nonlocal count, total_chars
                async for msg in client.iter_messages(entity, limit=limit):
                    if not msg.text:
                        continue
                    sender_entity = await _resolve_message_sender(msg)
                    sender_identity = extract_message_sender_identity(msg, sender=sender_entity)
                    sender = f" {format_sender_identity(sender_identity)}"
                    date_str = msg.date.strftime("%Y-%m-%d %H:%M") if msg.date else ""
                    preview = msg.text[:500]
                    line = f"#{msg.id} {date_str}{sender}: {preview}"
                    lines.append(line)
                    total_chars += len(line)
                    count += 1
                    if total_chars >= budget:
                        lines.append(f"\n[Вывод обрезан после {count} сообщений, достигнут лимит символов]")
                        break

            try:
                await run_with_flood_wait(
                    _read_recent(),
                    operation="agent_read_recent_messages",
                    phone=phone,
                    pool=client_pool,
                )
            except HandledFloodWaitError as exc:
                return _text_response(f"Flood wait: {exc.info.detail}")
            if count == 0:
                return _text_response("Сообщений с текстом не найдено.")
            lines.append(f"\nИтого: {count} сообщений.")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка чтения сообщений: {e}")

    tools.append(read_messages)
    return tools
