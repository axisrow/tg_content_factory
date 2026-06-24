from __future__ import annotations

from typing import Any

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._registry import _text_response, arg_bool, arg_str, is_affirmative, require_confirmation
from src.agent.tools._telegram_runtime import prepare_telegram_tool
from src.agent.tools.messaging_schemas import (
    EDIT_ADMIN_SCHEMA,
    EDIT_PERMISSIONS_SCHEMA,
    GET_PARTICIPANTS_SCHEMA,
    KICK_PARTICIPANT_SCHEMA,
)
from src.services.telegram_actions import TelegramActionClientUnavailableError, TelegramActionService
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
            result = await TelegramActionService(client_pool).get_participants(
                phone=phone,
                chat_id=chat_id,
                limit=limit,
                search=search,
            )
            participants = result.participants
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
        except TelegramActionClientUnavailableError:
            return _text_response(f"Клиент для {phone} не найден или flood-wait активен.")
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
        # Coerce explicitly: a model emitting the JSON string "false" must demote,
        # not promote — bool("false") is True would silently invert the action (#1115).
        is_admin = arg_bool(args, "is_admin", True)
        title = args.get("title") or None
        if not chat_id or not user_id:
            return _text_response("Ошибка: chat_id и user_id обязательны.")
        action = "повысит" if is_admin else "понизит"
        gate = require_confirmation(f"{action} {user_id} в {chat_id}", args)
        if gate:
            return gate
        try:
            await TelegramActionService(client_pool).edit_admin(
                phone=phone,
                chat_id=chat_id,
                user_id=user_id,
                is_admin=is_admin,
                title=title,
            )
            return _text_response(f"Права администратора обновлены для {user_id} в {chat_id}.")
        except TelegramActionClientUnavailableError:
            return _text_response(f"Клиент для {phone} не найден или flood-wait активен.")
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
        # Preserve the three-state (None=unchanged / True / False): only coerce when
        # the flag is present, so a JSON string "false" restricts instead of being
        # treated as truthy and silently allowing the action (#1115).
        raw_send_messages = args.get("send_messages")
        raw_send_media = args.get("send_media")
        send_messages = is_affirmative(raw_send_messages) if raw_send_messages is not None else None
        send_media = is_affirmative(raw_send_media) if raw_send_media is not None else None
        if not chat_id or not user_id:
            return _text_response("Ошибка: chat_id и user_id обязательны.")
        if send_messages is None and send_media is None:
            return _text_response("Ошибка: укажите хотя бы один флаг ограничения (send_messages, send_media).")
        gate = require_confirmation(f"изменит ограничения для {user_id} в {chat_id}", args)
        if gate:
            return gate
        try:
            await TelegramActionService(client_pool).edit_permissions(
                phone=phone,
                chat_id=chat_id,
                user_id=user_id,
                until_date=parse_datetime(until_date_str),
                send_messages=send_messages,
                send_media=send_media,
            )
            return _text_response(f"Ограничения обновлены для {user_id} в {chat_id}.")
        except TelegramActionClientUnavailableError:
            return _text_response(f"Клиент для {phone} не найден или flood-wait активен.")
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
            await TelegramActionService(client_pool).kick_participant(
                phone=phone,
                chat_id=chat_id,
                user_id=user_id,
            )
            return _text_response(f"{user_id} исключён из {chat_id}.")
        except TelegramActionClientUnavailableError:
            return _text_response(f"Клиент для {phone} не найден или flood-wait активен.")
        except Exception as e:
            return _text_response(f"Ошибка исключения участника: {e}")

    tools.append(kick_participant)
    return tools
