"""Agent tools for Messaging — send, edit, delete messages via Telegram."""

from __future__ import annotations

from typing import Annotated

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._registry import (
    ToolInputError,
    _text_response,
    arg_csv_ints,
    arg_str,
    get_tool_context,
    require_confirmation,
    require_phone_permission,
    require_pool,
    resolve_entity,
    resolve_phone,
)
from src.telegram.flood_wait import HandledFloodWaitError, run_with_flood_wait


def register(db, client_pool, embedding_service, **kwargs):
    ctx = get_tool_context(kwargs, db=db, client_pool=client_pool, embedding_service=embedding_service)
    tools = []

    async def _prepare_telegram_tool(args, *, tool_name: str, action: str) -> tuple[str, dict | None]:
        pool_gate = ctx.require_pool(action)
        if pool_gate:
            return "", pool_gate
        phone, err = await ctx.resolve_phone(args.get("phone", ""))
        if err:
            return "", err
        perm_gate = await ctx.require_phone_permission(phone, tool_name)
        if perm_gate:
            return "", perm_gate
        return phone, None

    @tool(
        "send_message",
        "Send a message from a connected account (phone = sender's phone). "
        "recipient accepts @username, phone number, or numeric ID. Ask user for confirmation first.",
        {
            "phone": Annotated[str, "Номер телефона аккаунта (например +79001234567)"],
            "recipient": Annotated[str, "Получатель (@username, телефон или числовой ID)"],
            "text": Annotated[str, "Текст сообщения"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
    )
    async def send_message(args):
        phone, err = await _prepare_telegram_tool(args, tool_name="send_message", action="Отправка сообщения")
        if err:
            return err
        try:
            recipient = arg_str(args, "recipient", required=True)
            text = arg_str(args, "text", required=True)
        except ToolInputError:
            return _text_response("Ошибка: recipient и text обязательны.")
        preview = text[:120] + ("..." if len(text) > 120 else "")
        gate = require_confirmation(
            f"отправит сообщение от {phone} пользователю {recipient}: «{preview}»", args
        )
        if gate:
            return gate
        try:
            client, entity, err = await resolve_entity(client_pool, phone, recipient)
            if err:
                return err
            await client.send_message(entity, text)
            return _text_response(f"Сообщение отправлено: {recipient}")
        except Exception as e:
            return _text_response(f"Ошибка отправки сообщения: {e}")

    tools.append(send_message)

    @tool(
        "edit_message",
        "Edit a previously sent message. "
        "chat_id accepts @username, t.me link, numeric ID, or 'me'. Ask user for confirmation first.",
        {
            "phone": Annotated[str, "Номер телефона аккаунта (например +79001234567)"],
            "chat_id": Annotated[str, "ID чата (@username, t.me ссылка, числовой ID или me)"],
            "message_id": Annotated[int, "ID сообщения в Telegram"],
            "text": Annotated[str, "Текст сообщения"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
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
            client, entity, err = await resolve_entity(client_pool, phone, chat_id)
            if err:
                return err
            await client.edit_message(entity, int(message_id), text)
            return _text_response(f"Сообщение #{message_id} отредактировано.")
        except Exception as e:
            return _text_response(f"Ошибка редактирования сообщения: {e}")

    tools.append(edit_message)

    @tool(
        "delete_message",
        "⚠️ DANGEROUS: Delete messages from a Telegram chat. "
        "chat_id accepts @username, numeric ID, or 'me'. "
        "message_ids = comma-separated integers. Always ask user for confirmation first.",
        {
            "phone": Annotated[str, "Номер телефона аккаунта (например +79001234567)"],
            "chat_id": Annotated[str, "ID чата (@username, t.me ссылка, числовой ID или me)"],
            "message_ids": Annotated[str, "ID сообщений через запятую"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def delete_message(args):
        phone, err = await _prepare_telegram_tool(args, tool_name="delete_message", action="Удаление сообщений")
        if err:
            return err
        try:
            chat_id = arg_str(args, "chat_id", required=True)
            arg_str(args, "message_ids", required=True)
        except ToolInputError:
            return _text_response("Ошибка: chat_id и message_ids обязательны.")
        try:
            ids = arg_csv_ints(args, "message_ids", required=True)
        except ToolInputError:
            return _text_response("Ошибка: не указаны валидные message_ids.")
        gate = require_confirmation(
            f"удалит {len(ids)} сообщений из чата {chat_id}: {ids}", args
        )
        if gate:
            return gate
        try:
            client, entity, err = await resolve_entity(client_pool, phone, chat_id)
            if err:
                return err
            await client.delete_messages(entity, ids)
            return _text_response(f"Удалено {len(ids)} сообщений из чата {chat_id}.")
        except Exception as e:
            return _text_response(f"Ошибка удаления сообщений: {e}")

    tools.append(delete_message)

    @tool(
        "forward_messages",
        "Forward messages from one Telegram chat to another. "
        "Pass comma-separated message IDs. Always ask user for confirmation first.",
        {
            "phone": Annotated[str, "Номер телефона аккаунта (например +79001234567)"],
            "from_chat": Annotated[str, "ID чата-источника (@username, числовой ID)"],
            "to_chat": Annotated[str, "ID чата-получателя (@username, числовой ID)"],
            "message_ids": Annotated[str, "ID сообщений через запятую"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
    )
    async def forward_messages(args):
        phone, err = await _prepare_telegram_tool(args, tool_name="forward_messages", action="Пересылка сообщений")
        if err:
            return err
        try:
            from_chat = arg_str(args, "from_chat", required=True)
            to_chat = arg_str(args, "to_chat", required=True)
            arg_str(args, "message_ids", required=True)
        except ToolInputError:
            return _text_response("Ошибка: from_chat, to_chat и message_ids обязательны.")
        try:
            ids = arg_csv_ints(args, "message_ids", required=True)
        except ToolInputError:
            return _text_response("Ошибка: не указаны валидные message_ids.")
        gate = require_confirmation(
            f"перешлёт {len(ids)} сообщений из {from_chat} в {to_chat}: {ids}", args
        )
        if gate:
            return gate
        try:
            client, from_entity, err = await resolve_entity(client_pool, phone, from_chat)
            if err:
                return err
            _, to_entity, err = await resolve_entity(client_pool, phone, to_chat)
            if err:
                return err
            await client.forward_messages(to_entity, ids, from_entity)
            return _text_response(f"Переслано {len(ids)} сообщений из {from_chat} в {to_chat}.")
        except Exception as e:
            return _text_response(f"Ошибка пересылки сообщений: {e}")

    tools.append(forward_messages)

    @tool(
        "pin_message",
        "Pin a message in a Telegram chat. notify=true sends a notification to all members. "
        "chat_id accepts @username, numeric ID, or 'me'. Ask user for confirmation first.",
        {
            "phone": Annotated[str, "Номер телефона аккаунта (например +79001234567)"],
            "chat_id": Annotated[str, "ID чата (@username, t.me ссылка, числовой ID или me)"],
            "message_id": Annotated[int, "ID сообщения в Telegram"],
            "notify": Annotated[bool, "Отправить уведомление участникам"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
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
            client, entity, err = await resolve_entity(client_pool, phone, chat_id)
            if err:
                return err
            await client.pin_message(entity, int(message_id), notify=notify)
            return _text_response(f"Сообщение #{message_id} закреплено в {chat_id}.")
        except Exception as e:
            return _text_response(f"Ошибка закрепления сообщения: {e}")

    tools.append(pin_message)

    @tool(
        "unpin_message",
        "Unpin a message in a Telegram chat. Omit message_id to unpin all. "
        "Ask user for confirmation first.",
        {
            "phone": Annotated[str, "Номер телефона аккаунта (например +79001234567)"],
            "chat_id": Annotated[str, "ID чата (@username, t.me ссылка, числовой ID или me)"],
            "message_id": Annotated[int, "ID сообщения в Telegram"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
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
            client, entity, err = await resolve_entity(client_pool, phone, chat_id)
            if err:
                return err
            await client.unpin_message(entity, message_id)
            return _text_response(f"Сообщение(я) откреплено в {chat_id}.")
        except Exception as e:
            return _text_response(f"Ошибка открепления сообщения: {e}")

    tools.append(unpin_message)

    @tool(
        "download_media",
        "Download media from a Telegram message. Returns the local file path. "
        "Use chat_id='me' for Saved Messages (Избранное).",
        {
            "phone": Annotated[str, "Номер телефона аккаунта (например +79001234567)"],
            "chat_id": Annotated[str, "ID чата (@username, t.me ссылка, числовой ID или me)"],
            "message_id": Annotated[int, "ID сообщения в Telegram"],
        },
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
            client, entity, err = await resolve_entity(client_pool, phone, chat_id)
            if err:
                return err
            msg = None

            async def _lookup_message() -> None:
                nonlocal msg
                async for m in client.iter_messages(entity, ids=int(message_id)):
                    msg = m
                    break

            try:
                await run_with_flood_wait(
                    _lookup_message(),
                    operation="agent_download_media_lookup",
                    phone=phone,
                    pool=client_pool,
                )
            except HandledFloodWaitError as exc:
                return _text_response(f"Flood wait: {exc.info.detail}")
            if msg is None:
                return _text_response(f"Сообщение #{message_id} не найдено.")
            output_dir = pathlib.Path(__file__).resolve().parents[3] / "data" / "downloads"
            try:
                path = await run_with_flood_wait(
                    client.download_media(msg, file=str(output_dir)),
                    operation="agent_download_media",
                    phone=phone,
                    pool=client_pool,
                )
            except HandledFloodWaitError as exc:
                return _text_response(f"Flood wait: {exc.info.detail}")
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
        "Get list of participants in a Telegram group (not broadcast channels). "
        "search filters by name/username substring.",
        {
            "phone": Annotated[str, "Номер телефона аккаунта (например +79001234567)"],
            "chat_id": Annotated[str, "ID чата (@username, t.me ссылка, числовой ID или me)"],
            "limit": Annotated[int, "Максимальное количество результатов"],
            "search": Annotated[str, "Фильтр по имени/username участника"],
        },
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
            client, entity, err = await resolve_entity(client_pool, phone, chat_id)
            if err:
                return err
            participants = await client.get_participants(entity, limit=limit, search=search)
            if not participants:
                return _text_response("Участники не найдены.")
            lines = [f"Участники {chat_id} ({len(participants)}):"]
            for p in participants:
                name = " ".join(filter(None, [
                    getattr(p, "first_name", None) or "",
                    getattr(p, "last_name", None) or "",
                ]))
                username = f" (@{p.username})" if getattr(p, "username", None) else ""
                lines.append(f"  {p.id}: {name}{username}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения участников: {e}")

    tools.append(get_participants)

    @tool(
        "edit_admin",
        "Promote (is_admin=true) or demote (is_admin=false) a user as admin. "
        "user_id accepts @username or numeric ID. title sets a custom admin badge. "
        "Requires admin rights. Ask for confirmation first.",
        {
            "phone": Annotated[str, "Номер телефона аккаунта (например +79001234567)"],
            "chat_id": Annotated[str, "ID чата (@username, t.me ссылка, числовой ID или me)"],
            "user_id": Annotated[str, "ID пользователя (@username или числовой ID)"],
            "is_admin": Annotated[bool, "true — назначить админом, false — снять права"],
            "title": Annotated[str, "Кастомный бейдж администратора"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
    )
    async def edit_admin(args):
        phone, err = await _prepare_telegram_tool(
            args,
            tool_name="edit_admin",
            action="Изменение прав администратора",
        )
        if err:
            return err
        chat_id = arg_str(args, "chat_id")
        user_id = arg_str(args, "user_id")
        is_admin = args.get("is_admin", True)
        title = args.get("title") or None
        if not chat_id or not user_id:
            return _text_response("Ошибка: chat_id и user_id обязательны.")
        action = "повысит" if is_admin else "понизит"
        gate = require_confirmation(f"{action} {user_id} в {chat_id}", args)
        if gate:
            return gate
        try:
            client, entity, err = await resolve_entity(client_pool, phone, chat_id)
            if err:
                return err
            _, user, err = await resolve_entity(client_pool, phone, user_id, is_user=True)
            if err:
                return err
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
        "Restrict (send_messages=false) or unrestrict (all true) a user in a Telegram group. "
        "until_date = ISO datetime (e.g. '2025-12-31T23:59:59') for temporary restriction. "
        "Ask for confirmation first.",
        {
            "phone": Annotated[str, "Номер телефона аккаунта (например +79001234567)"],
            "chat_id": Annotated[str, "ID чата (@username, t.me ссылка, числовой ID или me)"],
            "user_id": Annotated[str, "ID пользователя (@username или числовой ID)"],
            "send_messages": Annotated[bool, "Разрешить отправку сообщений"],
            "send_media": Annotated[bool, "Разрешить отправку медиа"],
            "until_date": Annotated[str, "Дата окончания ограничения в формате ISO (YYYY-MM-DDTHH:MM:SS)"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
    )
    async def edit_permissions(args):
        from datetime import datetime

        phone, err = await _prepare_telegram_tool(
            args,
            tool_name="edit_permissions",
            action="Изменение ограничений пользователя",
        )
        if err:
            return err
        chat_id = arg_str(args, "chat_id")
        user_id = arg_str(args, "user_id")
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
            client, entity, err = await resolve_entity(client_pool, phone, chat_id)
            if err:
                return err
            _, user, err = await resolve_entity(client_pool, phone, user_id, is_user=True)
            if err:
                return err
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
        "user_id accepts @username or numeric ID. Always ask user for confirmation first.",
        {
            "phone": Annotated[str, "Номер телефона аккаунта (например +79001234567)"],
            "chat_id": Annotated[str, "ID чата (@username, t.me ссылка, числовой ID или me)"],
            "user_id": Annotated[str, "ID пользователя (@username или числовой ID)"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def kick_participant(args):
        phone, err = await _prepare_telegram_tool(args, tool_name="kick_participant", action="Исключение участника")
        if err:
            return err
        chat_id = arg_str(args, "chat_id")
        user_id = arg_str(args, "user_id")
        if not chat_id or not user_id:
            return _text_response("Ошибка: chat_id и user_id обязательны.")
        gate = require_confirmation(f"исключит {user_id} из чата {chat_id}", args)
        if gate:
            return gate
        try:
            client, entity, err = await resolve_entity(client_pool, phone, chat_id)
            if err:
                return err
            _, user, err = await resolve_entity(client_pool, phone, user_id, is_user=True)
            if err:
                return err
            await client.kick_participant(entity, user)
            return _text_response(f"{user_id} исключён из {chat_id}.")
        except Exception as e:
            return _text_response(f"Ошибка исключения участника: {e}")

    tools.append(kick_participant)

    @tool(
        "get_broadcast_stats",
        "Get broadcast statistics (followers, views, reactions) for a Telegram channel. "
        "Requires admin/owner rights on the channel.",
        {
            "phone": Annotated[str, "Номер телефона аккаунта (например +79001234567)"],
            "chat_id": Annotated[str, "ID чата (@username, t.me ссылка, числовой ID или me)"],
        },
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
            client, entity, err = await resolve_entity(client_pool, phone, chat_id)
            if err:
                return err
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
        {
            "phone": Annotated[str, "Номер телефона аккаунта (например +79001234567)"],
            "chat_id": Annotated[str, "ID чата (@username, t.me ссылка, числовой ID или me)"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
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
        {
            "phone": Annotated[str, "Номер телефона аккаунта (например +79001234567)"],
            "chat_id": Annotated[str, "ID чата (@username, t.me ссылка, числовой ID или me)"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
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
        {
            "phone": Annotated[str, "Номер телефона аккаунта (например +79001234567)"],
            "chat_id": Annotated[str, "ID чата (@username, t.me ссылка, числовой ID или me)"],
            "max_id": Annotated[int, "Отметить прочитанными до этого ID включительно"],
        },
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
        {
            "phone": Annotated[str, "Номер телефона аккаунта (например +79001234567)"],
            "chat_id": Annotated[str, "ID чата (@username, t.me ссылка, числовой ID или me)"],
            "limit": Annotated[int, "Максимальное количество результатов"],
        },
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
        try:
            limit = max(1, min(int(args.get("limit") or 100), 500))
        except (TypeError, ValueError):
            limit = 100
        if not chat_id:
            return _text_response("Ошибка: chat_id обязателен.")
        try:
            client, entity, err = await resolve_entity(client_pool, phone, chat_id)
            if err:
                return err
            lines = [f"Последние {limit} сообщений из {chat_id}:\n"]
            count = 0
            total_chars = 0
            budget = 50_000

            async def _read_recent() -> None:
                nonlocal count, total_chars
                async for msg in client.iter_messages(entity, limit=limit):
                    if not msg.text:
                        continue
                    sender = f" [id:{msg.sender_id}]" if msg.sender_id else ""
                    date_str = msg.date.strftime("%Y-%m-%d %H:%M") if msg.date else ""
                    preview = msg.text[:500]
                    line = f"#{msg.id} {date_str}{sender}: {preview}"
                    lines.append(line)
                    total_chars += len(line)
                    count += 1
                    if total_chars >= budget:
                        lines.append(
                            f"\n[Вывод обрезан после {count} сообщений, достигнут лимит символов]"
                        )
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
