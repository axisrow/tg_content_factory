from __future__ import annotations

from typing import Any

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._registry import _text_response, arg_str, require_confirmation, resolve_entity
from src.agent.tools._telegram_runtime import prepare_telegram_tool
from src.agent.tools.messaging_schemas import (
    EDIT_ADMIN_SCHEMA,
    EDIT_PERMISSIONS_SCHEMA,
    GET_PARTICIPANTS_SCHEMA,
    KICK_PARTICIPANT_SCHEMA,
)
from src.utils.datetime import parse_datetime


def register_admin_moderation_tools(ctx: Any, client_pool: Any) -> list[Any]:
    tools: list[Any] = []

    @tool(
        "get_participants",
        "Get list of participants in a Telegram group (not broadcast channels). "
        "search filters by name/username substring.",
        GET_PARTICIPANTS_SCHEMA,
    )
    async def get_participants(args):
        live_gate = ctx.require_live_runtime("Получение участников", tool_name="get_participants")
        if live_gate:
            return live_gate
        phone, err = await ctx.resolve_phone(args.get("phone", ""))
        if err:
            return err
        perm_gate = await ctx.require_phone_permission(phone, "get_participants")
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
                name = " ".join(
                    filter(
                        None,
                        [
                            getattr(p, "first_name", None) or "",
                            getattr(p, "last_name", None) or "",
                        ],
                    )
                )
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
        EDIT_ADMIN_SCHEMA,
    )
    async def edit_admin(args):
        phone, err = await prepare_telegram_tool(
            ctx,
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
        EDIT_PERMISSIONS_SCHEMA,
    )
    async def edit_permissions(args):
        phone, err = await prepare_telegram_tool(
            ctx,
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
            return _text_response("Ошибка: укажите хотя бы один флаг ограничения (send_messages, send_media).")
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
            kwargs = {"until_date": parse_datetime(until_date_str)}
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
        KICK_PARTICIPANT_SCHEMA,
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def kick_participant(args):
        phone, err = await prepare_telegram_tool(
            ctx,
            args,
            tool_name="kick_participant",
            action="Исключение участника",
        )
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
    return tools
