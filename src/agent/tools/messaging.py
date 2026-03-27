"""Agent tools for Messaging — send, edit, delete messages via Telegram."""

from __future__ import annotations

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._registry import (
    _text_response,
    require_confirmation,
    require_phone_permission,
    require_pool,
    resolve_phone,
)


def register(db, client_pool, embedding_service, **kwargs):
    tools = []

    @tool(
        "send_message",
        "Send a direct message to a Telegram user or chat. "
        "Recipient can be a username (@user), phone number, or numeric ID. "
        "Ask user for confirmation first.",
        {"phone": str, "recipient": str, "text": str, "confirm": bool},
    )
    async def send_message(args):
        pool_gate = require_pool(client_pool, "Отправка сообщения")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "send_message")
        if perm_gate:
            return perm_gate
        recipient = args.get("recipient", "")
        text = args.get("text", "")
        if not recipient or not text:
            return _text_response("Ошибка: recipient и text обязательны.")
        preview = text[:120] + ("..." if len(text) > 120 else "")
        gate = require_confirmation(
            f"отправит сообщение от {phone} пользователю {recipient}: «{preview}»", args
        )
        if gate:
            return gate
        try:
            result = await client_pool.get_native_client_by_phone(phone)
            if result is None:
                return _text_response(f"Клиент для {phone} не найден или flood-wait активен.")
            client, _ = result
            entity = await client.get_entity(recipient)
            await client.send_message(entity, text)
            return _text_response(f"Сообщение отправлено: {recipient}")
        except Exception as e:
            return _text_response(f"Ошибка отправки сообщения: {e}")

    tools.append(send_message)

    @tool(
        "edit_message",
        "Edit a previously sent message in a Telegram chat. "
        "Ask user for confirmation first.",
        {"phone": str, "chat_id": str, "message_id": int, "text": str, "confirm": bool},
    )
    async def edit_message(args):
        pool_gate = require_pool(client_pool, "Редактирование сообщения")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "edit_message")
        if perm_gate:
            return perm_gate
        chat_id = args.get("chat_id", "")
        message_id = args.get("message_id")
        text = args.get("text", "")
        if not chat_id or not message_id or not text:
            return _text_response("Ошибка: chat_id, message_id и text обязательны.")
        preview = text[:120] + ("..." if len(text) > 120 else "")
        gate = require_confirmation(
            f"отредактирует сообщение #{message_id} в чате {chat_id}: «{preview}»", args
        )
        if gate:
            return gate
        try:
            result = await client_pool.get_native_client_by_phone(phone)
            if result is None:
                return _text_response(f"Клиент для {phone} не найден или flood-wait активен.")
            client, _ = result
            entity = await client.get_entity(chat_id)
            await client.edit_message(entity, int(message_id), text)
            return _text_response(f"Сообщение #{message_id} отредактировано.")
        except Exception as e:
            return _text_response(f"Ошибка редактирования сообщения: {e}")

    tools.append(edit_message)

    @tool(
        "delete_message",
        "⚠️ DANGEROUS: Delete messages from a Telegram chat. "
        "Pass comma-separated message IDs. Always ask user for confirmation first.",
        {"phone": str, "chat_id": str, "message_ids": str, "confirm": bool},
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def delete_message(args):
        pool_gate = require_pool(client_pool, "Удаление сообщений")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "delete_message")
        if perm_gate:
            return perm_gate
        chat_id = args.get("chat_id", "")
        message_ids_str = args.get("message_ids", "")
        if not chat_id or not message_ids_str:
            return _text_response("Ошибка: chat_id и message_ids обязательны.")
        ids = [int(x.strip()) for x in message_ids_str.split(",") if x.strip().isdigit()]
        if not ids:
            return _text_response("Ошибка: не указаны валидные message_ids.")
        gate = require_confirmation(
            f"удалит {len(ids)} сообщений из чата {chat_id}: {ids}", args
        )
        if gate:
            return gate
        try:
            result = await client_pool.get_native_client_by_phone(phone)
            if result is None:
                return _text_response(f"Клиент для {phone} не найден или flood-wait активен.")
            client, _ = result
            entity = await client.get_entity(chat_id)
            await client.delete_messages(entity, ids)
            return _text_response(f"Удалено {len(ids)} сообщений из чата {chat_id}.")
        except Exception as e:
            return _text_response(f"Ошибка удаления сообщений: {e}")

    tools.append(delete_message)

    @tool(
        "forward_messages",
        "Forward messages from one Telegram chat to another. "
        "Pass comma-separated message IDs. Always ask user for confirmation first.",
        {"phone": str, "from_chat": str, "to_chat": str, "message_ids": str, "confirm": bool},
    )
    async def forward_messages(args):
        pool_gate = require_pool(client_pool, "Пересылка сообщений")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "forward_messages")
        if perm_gate:
            return perm_gate
        from_chat = args.get("from_chat", "")
        to_chat = args.get("to_chat", "")
        message_ids_str = args.get("message_ids", "")
        if not from_chat or not to_chat or not message_ids_str:
            return _text_response("Ошибка: from_chat, to_chat и message_ids обязательны.")
        ids = [int(x.strip()) for x in message_ids_str.split(",") if x.strip().isdigit()]
        if not ids:
            return _text_response("Ошибка: не указаны валидные message_ids.")
        gate = require_confirmation(
            f"перешлёт {len(ids)} сообщений из {from_chat} в {to_chat}: {ids}", args
        )
        if gate:
            return gate
        try:
            result = await client_pool.get_native_client_by_phone(phone)
            if result is None:
                return _text_response(f"Клиент для {phone} не найден или flood-wait активен.")
            client, _ = result
            from_entity = await client.get_entity(from_chat)
            to_entity = await client.get_entity(to_chat)
            await client.forward_messages(to_entity, ids, from_entity)
            return _text_response(f"Переслано {len(ids)} сообщений из {from_chat} в {to_chat}.")
        except Exception as e:
            return _text_response(f"Ошибка пересылки сообщений: {e}")

    tools.append(forward_messages)

    @tool(
        "pin_message",
        "Pin a message in a Telegram chat. Ask user for confirmation first.",
        {"phone": str, "chat_id": str, "message_id": int, "notify": bool, "confirm": bool},
    )
    async def pin_message(args):
        pool_gate = require_pool(client_pool, "Закрепление сообщения")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "pin_message")
        if perm_gate:
            return perm_gate
        chat_id = args.get("chat_id", "")
        message_id = args.get("message_id")
        notify = args.get("notify", False)
        if not chat_id or not message_id:
            return _text_response("Ошибка: chat_id и message_id обязательны.")
        gate = require_confirmation(f"закрепит сообщение #{message_id} в чате {chat_id}", args)
        if gate:
            return gate
        try:
            result = await client_pool.get_native_client_by_phone(phone)
            if result is None:
                return _text_response(f"Клиент для {phone} не найден.")
            client, _ = result
            entity = await client.get_entity(chat_id)
            await client.pin_message(entity, int(message_id), notify=notify)
            return _text_response(f"Сообщение #{message_id} закреплено в {chat_id}.")
        except Exception as e:
            return _text_response(f"Ошибка закрепления сообщения: {e}")

    tools.append(pin_message)

    @tool(
        "unpin_message",
        "Unpin a message in a Telegram chat. Omit message_id to unpin all. "
        "Ask user for confirmation first.",
        {"phone": str, "chat_id": str, "message_id": int, "confirm": bool},
    )
    async def unpin_message(args):
        pool_gate = require_pool(client_pool, "Открепление сообщения")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "unpin_message")
        if perm_gate:
            return perm_gate
        chat_id = args.get("chat_id", "")
        message_id = args.get("message_id") or None
        if not chat_id:
            return _text_response("Ошибка: chat_id обязателен.")
        target = f"#{message_id}" if message_id else "все сообщения"
        gate = require_confirmation(f"открепит {target} в чате {chat_id}", args)
        if gate:
            return gate
        try:
            result = await client_pool.get_native_client_by_phone(phone)
            if result is None:
                return _text_response(f"Клиент для {phone} не найден.")
            client, _ = result
            entity = await client.get_entity(chat_id)
            await client.unpin_message(entity, message_id)
            return _text_response(f"Сообщение(я) откреплено в {chat_id}.")
        except Exception as e:
            return _text_response(f"Ошибка открепления сообщения: {e}")

    tools.append(unpin_message)

    @tool(
        "download_media",
        "Download media from a Telegram message. Returns the local file path. "
        "Use chat_id='me' for Saved Messages (Избранное).",
        {"phone": str, "chat_id": str, "message_id": int},
    )
    async def download_media(args):
        import pathlib

        pool_gate = require_pool(client_pool, "Загрузка медиа")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "download_media")
        if perm_gate:
            return perm_gate
        chat_id = args.get("chat_id", "")
        message_id = args.get("message_id")
        if not chat_id or not message_id:
            return _text_response("Ошибка: chat_id и message_id обязательны.")
        try:
            result = await client_pool.get_native_client_by_phone(phone)
            if result is None:
                return _text_response(f"Клиент для {phone} не найден.")
            client, _ = result
            entity = await client.get_entity(chat_id)
            msg = None
            async for m in client.iter_messages(entity, ids=int(message_id)):
                msg = m
                break
            if msg is None:
                return _text_response(f"Сообщение #{message_id} не найдено.")
            output_dir = pathlib.Path(__file__).resolve().parents[3] / "data" / "downloads"
            output_dir.mkdir(parents=True, exist_ok=True)
            path = await client.download_media(msg, file=str(output_dir))
            if not path:
                return _text_response("В сообщении нет медиа.")
            resolved = pathlib.Path(path).resolve()
            if not resolved.is_relative_to(output_dir.resolve()):
                return _text_response("Ошибка: путь загрузки вне допустимой директории.")
            return _text_response(f"Медиа загружено: {path}")
        except Exception as e:
            return _text_response(f"Ошибка загрузки медиа: {e}")

    tools.append(download_media)

    @tool(
        "get_participants",
        "Get list of participants in a Telegram channel or group.",
        {"phone": str, "chat_id": str, "limit": int, "search": str},
    )
    async def get_participants(args):
        pool_gate = require_pool(client_pool, "Получение участников")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "get_participants")
        if perm_gate:
            return perm_gate
        chat_id = args.get("chat_id", "")
        limit = args.get("limit") or 200
        search = args.get("search", "")
        if not chat_id:
            return _text_response("Ошибка: chat_id обязателен.")
        try:
            result = await client_pool.get_native_client_by_phone(phone)
            if result is None:
                return _text_response(f"Клиент для {phone} не найден.")
            client, _ = result
            entity = await client.get_entity(chat_id)
            participants = await client.get_participants(entity, limit=limit, search=search)
            if not participants:
                return _text_response("Участники не найдены.")
            lines = [f"Участники {chat_id} ({len(participants)}):"]
            for p in participants[:50]:
                name = " ".join(filter(None, [
                    getattr(p, "first_name", None) or "",
                    getattr(p, "last_name", None) or "",
                ]))
                username = f" (@{p.username})" if getattr(p, "username", None) else ""
                lines.append(f"  {p.id}: {name}{username}")
            if len(participants) > 50:
                lines.append(f"  ... и ещё {len(participants) - 50}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения участников: {e}")

    tools.append(get_participants)

    @tool(
        "edit_admin",
        "Promote or demote a user as admin in a Telegram channel/group. "
        "Set is_admin=true to promote (grants all permissions), is_admin=false to demote. "
        "Ask user for confirmation first.",
        {"phone": str, "chat_id": str, "user_id": str, "is_admin": bool, "title": str, "confirm": bool},
    )
    async def edit_admin(args):
        pool_gate = require_pool(client_pool, "Изменение прав администратора")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "edit_admin")
        if perm_gate:
            return perm_gate
        chat_id = args.get("chat_id", "")
        user_id = args.get("user_id", "")
        is_admin = args.get("is_admin", True)
        title = args.get("title") or None
        if not chat_id or not user_id:
            return _text_response("Ошибка: chat_id и user_id обязательны.")
        action = "повысит" if is_admin else "понизит"
        gate = require_confirmation(f"{action} {user_id} в {chat_id}", args)
        if gate:
            return gate
        try:
            result = await client_pool.get_native_client_by_phone(phone)
            if result is None:
                return _text_response(f"Клиент для {phone} не найден.")
            client, _ = result
            entity = await client.get_entity(chat_id)
            user = await client.get_entity(user_id)
            kwargs = {"is_admin": is_admin}
            if title:
                kwargs["title"] = title
            await client.edit_admin(entity, user, **kwargs)
            return _text_response(f"Права администратора обновлены для {user_id} в {chat_id}.")
        except Exception as e:
            return _text_response(f"Ошибка изменения прав администратора: {e}")

    tools.append(edit_admin)

    @tool(
        "edit_permissions",
        "Restrict or unrestrict a user in a Telegram group. "
        "Set send_messages=false to mute, send_media=false to block media, etc. "
        "To unrestrict, set all flags to true. Ask user for confirmation first.",
        {
            "phone": str, "chat_id": str, "user_id": str,
            "send_messages": bool, "send_media": bool,
            "until_date": str, "confirm": bool,
        },
    )
    async def edit_permissions(args):
        from datetime import datetime

        pool_gate = require_pool(client_pool, "Изменение ограничений пользователя")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "edit_permissions")
        if perm_gate:
            return perm_gate
        chat_id = args.get("chat_id", "")
        user_id = args.get("user_id", "")
        until_date_str = args.get("until_date") or None
        send_messages = args.get("send_messages")
        send_media = args.get("send_media")
        if not chat_id or not user_id:
            return _text_response("Ошибка: chat_id и user_id обязательны.")
        if send_messages is None and send_media is None:
            return _text_response(
                "Ошибка: укажите хотя бы один флаг ограничения "
                "(send_messages, send_media)."
            )
        gate = require_confirmation(f"изменит ограничения для {user_id} в {chat_id}", args)
        if gate:
            return gate
        try:
            result = await client_pool.get_native_client_by_phone(phone)
            if result is None:
                return _text_response(f"Клиент для {phone} не найден.")
            client, _ = result
            entity = await client.get_entity(chat_id)
            user = await client.get_entity(user_id)
            until_date = datetime.fromisoformat(until_date_str) if until_date_str else None
            kwargs = {"until_date": until_date}
            if send_messages is not None:
                kwargs["send_messages"] = send_messages
            if send_media is not None:
                kwargs["send_media"] = send_media
            await client.edit_permissions(entity, user, **kwargs)
            return _text_response(f"Ограничения обновлены для {user_id} в {chat_id}.")
        except Exception as e:
            return _text_response(f"Ошибка изменения ограничений: {e}")

    tools.append(edit_permissions)

    @tool(
        "kick_participant",
        "⚠️ DANGEROUS: Kick a participant from a Telegram chat. "
        "Always ask user for confirmation first.",
        {"phone": str, "chat_id": str, "user_id": str, "confirm": bool},
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def kick_participant(args):
        pool_gate = require_pool(client_pool, "Исключение участника")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "kick_participant")
        if perm_gate:
            return perm_gate
        chat_id = args.get("chat_id", "")
        user_id = args.get("user_id", "")
        if not chat_id or not user_id:
            return _text_response("Ошибка: chat_id и user_id обязательны.")
        gate = require_confirmation(f"исключит {user_id} из чата {chat_id}", args)
        if gate:
            return gate
        try:
            result = await client_pool.get_native_client_by_phone(phone)
            if result is None:
                return _text_response(f"Клиент для {phone} не найден.")
            client, _ = result
            entity = await client.get_entity(chat_id)
            user = await client.get_entity(user_id)
            await client.kick_participant(entity, user)
            return _text_response(f"{user_id} исключён из {chat_id}.")
        except Exception as e:
            return _text_response(f"Ошибка исключения участника: {e}")

    tools.append(kick_participant)

    @tool(
        "get_broadcast_stats",
        "Get broadcast statistics for a Telegram channel.",
        {"phone": str, "chat_id": str},
    )
    async def get_broadcast_stats(args):
        pool_gate = require_pool(client_pool, "Получение статистики")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "get_broadcast_stats")
        if perm_gate:
            return perm_gate
        chat_id = args.get("chat_id", "")
        if not chat_id:
            return _text_response("Ошибка: chat_id обязателен.")
        try:
            result = await client_pool.get_native_client_by_phone(phone)
            if result is None:
                return _text_response(f"Клиент для {phone} не найден.")
            client, _ = result
            entity = await client.get_entity(chat_id)
            stats = await client.get_broadcast_stats(entity)
            fields = {}
            for attr in ("followers", "views_per_post", "shares_per_post",
                         "reactions_per_post", "forwards_per_post"):
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
        {"phone": str, "chat_id": str, "confirm": bool},
    )
    async def archive_chat(args):
        pool_gate = require_pool(client_pool, "Архивирование чата")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "archive_chat")
        if perm_gate:
            return perm_gate
        chat_id = args.get("chat_id", "")
        if not chat_id:
            return _text_response("Ошибка: chat_id обязателен.")
        gate = require_confirmation(f"архивирует чат {chat_id}", args)
        if gate:
            return gate
        try:
            result = await client_pool.get_native_client_by_phone(phone)
            if result is None:
                return _text_response(f"Клиент для {phone} не найден.")
            client, _ = result
            entity = await client.get_entity(chat_id)
            await client.edit_folder(entity, 1)
            return _text_response(f"Чат {chat_id} архивирован.")
        except Exception as e:
            return _text_response(f"Ошибка архивирования: {e}")

    tools.append(archive_chat)

    @tool(
        "unarchive_chat",
        "Unarchive a Telegram dialog (move back to main folder). "
        "Ask user for confirmation first.",
        {"phone": str, "chat_id": str, "confirm": bool},
    )
    async def unarchive_chat(args):
        pool_gate = require_pool(client_pool, "Разархивирование чата")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "unarchive_chat")
        if perm_gate:
            return perm_gate
        chat_id = args.get("chat_id", "")
        if not chat_id:
            return _text_response("Ошибка: chat_id обязателен.")
        gate = require_confirmation(f"разархивирует чат {chat_id}", args)
        if gate:
            return gate
        try:
            result = await client_pool.get_native_client_by_phone(phone)
            if result is None:
                return _text_response(f"Клиент для {phone} не найден.")
            client, _ = result
            entity = await client.get_entity(chat_id)
            await client.edit_folder(entity, 0)
            return _text_response(f"Чат {chat_id} разархивирован.")
        except Exception as e:
            return _text_response(f"Ошибка разархивирования: {e}")

    tools.append(unarchive_chat)

    @tool(
        "mark_read",
        "Mark messages as read in a Telegram chat.",
        {"phone": str, "chat_id": str, "max_id": int},
    )
    async def mark_read(args):
        pool_gate = require_pool(client_pool, "Отметка сообщений как прочитанных")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "mark_read")
        if perm_gate:
            return perm_gate
        chat_id = args.get("chat_id", "")
        max_id = args.get("max_id") or None
        if not chat_id:
            return _text_response("Ошибка: chat_id обязателен.")
        try:
            result = await client_pool.get_native_client_by_phone(phone)
            if result is None:
                return _text_response(f"Клиент для {phone} не найден.")
            client, _ = result
            entity = await client.get_entity(chat_id)
            await client.send_read_acknowledge(entity, max_id=max_id)
            return _text_response(f"Сообщения отмечены как прочитанные в {chat_id}.")
        except Exception as e:
            return _text_response(f"Ошибка отметки сообщений: {e}")

    tools.append(mark_read)

    @tool(
        "read_messages",
        "Read the last N messages from any Telegram chat or channel without storing them in the database. "
        "Useful to preview content before deciding whether to collect it. "
        "chat_id can be a username (@channel), t.me link, numeric ID, or 'me' for Saved Messages. "
        "Default limit is 100 messages.",
        {"phone": str, "chat_id": str, "limit": int},
    )
    async def read_messages(args):
        pool_gate = require_pool(client_pool, "Чтение сообщений")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "read_messages")
        if perm_gate:
            return perm_gate
        chat_id = args.get("chat_id", "")
        limit = args.get("limit") or 100
        if not chat_id:
            return _text_response("Ошибка: chat_id обязателен.")
        try:
            result = await client_pool.get_native_client_by_phone(phone)
            if result is None:
                return _text_response(f"Клиент для {phone} не найден или flood-wait активен.")
            client, _ = result
            entity = await client.get_entity(chat_id)
            lines = [f"Последние {limit} сообщений из {chat_id}:\n"]
            count = 0
            async for msg in client.iter_messages(entity, limit=limit):
                if not msg.text:
                    continue
                sender = f" [id:{msg.sender_id}]" if msg.sender_id else ""
                date_str = msg.date.strftime("%Y-%m-%d %H:%M") if msg.date else ""
                preview = msg.text[:500]
                lines.append(f"#{msg.id} {date_str}{sender}: {preview}")
                count += 1
            if count == 0:
                return _text_response("Сообщений с текстом не найдено.")
            lines.append(f"\nИтого: {count} сообщений.")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка чтения сообщений: {e}")

    tools.append(read_messages)

    return tools
