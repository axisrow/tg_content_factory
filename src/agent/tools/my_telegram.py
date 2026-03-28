"""Agent tools for My Telegram — dialogs, topics, channel creation."""

from __future__ import annotations

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._registry import (
    _text_response,
    normalize_phone,
    require_confirmation,
    require_phone_permission,
    require_pool,
    resolve_phone,
)
from src.parsers import normalize_identifier

_TYPE_ALIASES: dict[str, set[str]] = {
    "channels": {"channel"},
    "groups": {"group", "supergroup", "gigagroup", "forum", "monoforum"},
    "chats": {"dm", "bot", "saved"},
}


def register(db, client_pool, embedding_service, **kwargs):
    tools = []

    @tool(
        "search_my_telegram",
        "Search your Telegram account dialogs — channels, groups, private chats, bots, saved messages. "
        "This does NOT search message content (use search_messages for that). "
        "Params: search — find by @username, t.me/username, https://t.me/username, numeric ID, "
        "or title substring (case-insensitive); matches username field exactly or title as substring; "
        "type — filter: 'channels' (channel), 'groups' (group/supergroup/gigagroup/forum/monoforum), "
        "'chats' (dm/bot/saved), or exact type like 'supergroup', 'dm', 'bot'; "
        "limit — cap results (default: all).",
        {"phone": str, "search": str, "type": str, "limit": int},
    )
    async def search_my_telegram(args):
        pool_gate = require_pool(client_pool, "Поиск диалогов")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "search_my_telegram")
        if perm_gate:
            return perm_gate
        try:
            from src.services.channel_service import ChannelService

            svc = ChannelService(db, client_pool, None)
            dialogs = await svc.get_my_dialogs(phone)
            total = len(dialogs)
            type_filter = (args.get("type") or "").strip().lower()
            if type_filter:
                allowed = _TYPE_ALIASES.get(type_filter, {type_filter})
                dialogs = [d for d in dialogs if (d.get("channel_type") or "") in allowed]
            raw_query = (args.get("search") or "").strip()
            search_query = raw_query.lower()  # kept for error message display
            if raw_query:
                clean, kind = normalize_identifier(raw_query)
                if kind == "username":
                    dialogs = [
                        d for d in dialogs
                        if clean == (d.get("username") or "").lower()
                        or clean in (d.get("title") or "").lower()
                    ]
                elif kind == "numeric_id":
                    # channel_id stores Telethon entity.id (bare positive int).
                    # Users may provide Bot API format (-1001234567890) where -100 is a prefix.
                    bare_id = clean[4:] if clean.startswith("-100") else clean.lstrip("-")
                    dialogs = [d for d in dialogs if str(d.get("channel_id", "")) == bare_id]
                else:
                    dialogs = [
                        d for d in dialogs
                        if clean in (d.get("title") or "").lower()
                        or clean in (d.get("username") or "").lower()
                    ]
            limit = args.get("limit")
            if limit is not None:
                try:
                    limit = max(1, int(limit))
                    dialogs = dialogs[:limit]
                except (TypeError, ValueError):
                    pass
            if not dialogs:
                if total == 0:
                    return _text_response(f"Диалоги для {phone} не найдены.")
                parts = []
                if type_filter:
                    parts.append(f"тип: {type_filter}")
                if search_query:
                    parts.append(f"поиск: '{search_query}'")
                filters_desc = ", ".join(parts)
                return _text_response(
                    f"Нет диалогов по запросу ({filters_desc}). "
                    f"Всего диалогов для {phone}: {total}."
                )
            lines = [f"Диалоги ({len(dialogs)}):"]
            for d in dialogs:
                title = d.get("title", "?")
                did = d.get("channel_id", "?")
                dtype = d.get("channel_type", "?")
                lines.append(f"- id={did}, type={dtype}: {title}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения диалогов: {e}")

    tools.append(search_my_telegram)

    @tool(
        "refresh_dialogs",
        "Refresh cached dialog list for an account from Telegram. "
        "Use when messaging tools fail to resolve a numeric chat_id.",
        {"phone": str},
    )
    async def refresh_dialogs(args):
        pool_gate = require_pool(client_pool, "Обновление диалогов")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "refresh_dialogs")
        if perm_gate:
            return perm_gate
        try:
            from src.services.channel_service import ChannelService

            svc = ChannelService(db, client_pool, None)
            dialogs = await svc.get_my_dialogs(phone, refresh=True)
            return _text_response(f"Диалоги обновлены: {len(dialogs)} шт.")
        except Exception as e:
            return _text_response(f"Ошибка обновления диалогов: {e}")

    tools.append(refresh_dialogs)

    @tool(
        "leave_dialogs",
        "⚠️ DANGEROUS: Leave (unsubscribe from) Telegram channels/groups. "
        "dialog_ids = comma-separated Telegram chat IDs (get from search_my_telegram). "
        "Always ask user for confirmation first.",
        {"phone": str, "dialog_ids": str, "confirm": bool},
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def leave_dialogs(args):
        pool_gate = require_pool(client_pool, "Выход из диалогов")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        dialog_ids_str = args.get("dialog_ids", "")
        if not dialog_ids_str:
            return _text_response("Ошибка: dialog_ids обязателен.")
        perm_gate = await require_phone_permission(db, phone, "leave_dialogs")
        if perm_gate:
            return perm_gate
        gate = require_confirmation(
            f"выйдет из {len(dialog_ids_str.split(','))} диалогов на аккаунте {phone}", args
        )
        if gate:
            return gate
        try:
            from src.services.channel_service import ChannelService

            svc = ChannelService(db, client_pool, None)
            dialog_ids = [(int(x.strip()), "") for x in dialog_ids_str.split(",") if x.strip()]
            results = await svc.leave_dialogs(phone, dialog_ids)
            success = sum(1 for v in results.values() if v)
            return _text_response(f"Выход завершён: {success}/{len(results)} диалогов покинуты.")
        except Exception as e:
            return _text_response(f"Ошибка выхода из диалогов: {e}")

    tools.append(leave_dialogs)

    @tool(
        "create_telegram_channel",
        "⚠️ Create a new Telegram channel via a connected account. "
        "Ask user for confirmation first.",
        {"phone": str, "title": str, "about": str, "username": str, "confirm": bool},
    )
    async def create_telegram_channel(args):
        pool_gate = require_pool(client_pool, "Создание канала")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        title = args.get("title", "")
        if not title:
            return _text_response("Ошибка: title обязателен.")
        perm_gate = await require_phone_permission(db, phone, "create_telegram_channel")
        if perm_gate:
            return perm_gate
        gate = require_confirmation(f"создаст новый Telegram-канал '{title}'", args)
        if gate:
            return gate
        try:
            about = args.get("about", "")
            username = args.get("username", "")
            result = await client_pool.get_native_client_by_phone(phone)
            if result is None:
                return _text_response(f"Клиент для {phone} не найден или flood-wait активен.")
            client, _ = result
            from telethon.tl.functions.channels import CreateChannelRequest

            result = await client(CreateChannelRequest(
                title=title,
                about=about,
                broadcast=True,
            ))
            channel = result.chats[0]
            username_note = ""
            if username:
                try:
                    from telethon.tl.functions.channels import UpdateUsernameRequest

                    await client(UpdateUsernameRequest(channel, username))
                    username_note = f"\n- Username: @{username}"
                except Exception as ue:
                    username_note = f"\n- Username: не удалось установить ({ue})"
            return _text_response(
                f"Канал создан!\n- ID: {channel.id}\n- Title: {title}{username_note}"
            )
        except Exception as e:
            return _text_response(f"Ошибка создания канала: {e}")

    tools.append(create_telegram_channel)

    @tool(
        "get_forum_topics",
        "Get forum topics for a supergroup with topics enabled. "
        "channel_id = Telegram numeric ID (from list_channels or search_my_telegram).",
        {"channel_id": int},
    )
    async def get_forum_topics(args):
        channel_id = args.get("channel_id")
        if channel_id is None:
            return _text_response("Ошибка: channel_id обязателен.")
        try:
            topics = await db.get_forum_topics(int(channel_id))
            if not topics:
                return _text_response(f"Топики для канала {channel_id} не найдены.")
            lines = [f"Топики канала {channel_id} ({len(topics)}):"]
            for t in topics:
                lines.append(f"- id={t['topic_id']}: {t['title']}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения топиков: {e}")

    tools.append(get_forum_topics)

    @tool(
        "clear_dialog_cache",
        "⚠️ Clear cached dialog list for an account. Ask user for confirmation first.",
        {"phone": str, "confirm": bool},
    )
    async def clear_dialog_cache(args):
        phone = normalize_phone(args.get("phone", ""))
        if phone:
            perm_gate = await require_phone_permission(db, phone, "clear_dialog_cache")
            if perm_gate:
                return perm_gate
        gate = require_confirmation(
            f"очистит кеш диалогов{' для ' + phone if phone else ' для всех аккаунтов'}", args
        )
        if gate:
            return gate
        try:
            if phone:
                if client_pool is not None:
                    client_pool.invalidate_dialogs_cache(phone)
                await db.repos.dialog_cache.clear_dialogs(phone)
            else:
                if client_pool is not None:
                    client_pool.invalidate_dialogs_cache()
                await db.repos.dialog_cache.clear_all_dialogs()
            return _text_response("Кеш диалогов очищен.")
        except Exception as e:
            return _text_response(f"Ошибка очистки кеша: {e}")

    tools.append(clear_dialog_cache)

    @tool("get_cache_status", "Show dialog cache status: DB entries and age per account", {})
    async def get_cache_status(args):
        try:
            phones = await db.repos.dialog_cache.get_all_phones()
            if not phones:
                return _text_response("Кеш диалогов пуст.")
            lines = ["Статус кеша диалогов:"]
            for ph in sorted(phones):
                count = await db.repos.dialog_cache.count_dialogs(ph)
                cached_at = await db.repos.dialog_cache.get_cached_at(ph)
                cached_at_str = cached_at.strftime("%Y-%m-%d %H:%M:%S UTC") if cached_at else "—"
                lines.append(f"- {ph}: {count} записей, обновлён {cached_at_str}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения статуса кеша: {e}")

    tools.append(get_cache_status)

    return tools
