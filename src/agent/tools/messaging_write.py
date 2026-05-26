from __future__ import annotations

from typing import Any

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._registry import (
    ToolInputError,
    _text_response,
    arg_csv_ints,
    arg_str,
    get_accounts_with_flood_cleanup,
    is_flood_wait_active,
    normalize_flood_wait_until,
    require_confirmation,
)
from src.agent.tools._telegram_runtime import prepare_telegram_tool
from src.agent.tools.messaging_schemas import (
    DELETE_MESSAGE_SCHEMA,
    EDIT_MESSAGE_SCHEMA,
    FORWARD_MESSAGES_SCHEMA,
    SEND_MESSAGE_SCHEMA,
    SEND_REACTION_SCHEMA,
)
from src.services.telegram_actions import TelegramActionClientUnavailableError, TelegramActionService
from src.services.telegram_command_service import TelegramCommandService


def _command_status_text(status: object) -> str:
    value = getattr(status, "value", status)
    return str(value or "unknown")


def _explicit_pool_method(client_pool: Any, name: str) -> Any | None:
    instance_attrs = getattr(client_pool, "__dict__", {})
    if isinstance(instance_attrs, dict) and name in instance_attrs:
        candidate = instance_attrs[name]
    elif callable(getattr(type(client_pool), name, None)):
        candidate = getattr(client_pool, name)
    else:
        return None
    return candidate if callable(candidate) else None


async def _reaction_queue_status_hint(ctx: Any, phone: str, client_pool: Any) -> str:
    lines: list[str] = []
    try:
        accounts = await get_accounts_with_flood_cleanup(ctx.db)
    except Exception:
        accounts = []
    account = next((item for item in accounts if str(getattr(item, "phone", "")) == phone), None)
    if account is not None and is_flood_wait_active(account):
        flood_until = normalize_flood_wait_until(getattr(account, "flood_wait_until", None))
        if flood_until is not None:
            lines.append(f"Аккаунт сейчас во flood-wait до {flood_until.isoformat()}; задача подождёт.")

    is_warming = _explicit_pool_method(client_pool, "is_warming")
    if callable(is_warming):
        try:
            if bool(is_warming()):
                lines.append("Сейчас идёт прогрев диалогов; задача останется в очереди до готовности аккаунта.")
        except Exception:
            pass
    return "\n".join(lines)


def register_message_write_tools(ctx: Any, client_pool: Any) -> list[Any]:
    tools: list[Any] = []

    @tool(
        "send_message",
        "Send a message from a connected account (phone = sender's phone). "
        "recipient accepts @username, phone number, or numeric ID. Ask user for confirmation first.",
        SEND_MESSAGE_SCHEMA,
    )
    async def send_message(args):
        phone, err = await prepare_telegram_tool(ctx, args, tool_name="send_message", action="Отправка сообщения")
        if err:
            return err
        try:
            recipient = arg_str(args, "recipient", required=True)
            text = arg_str(args, "text", required=True)
        except ToolInputError:
            return _text_response("Ошибка: recipient и text обязательны.")
        preview = text[:120] + ("..." if len(text) > 120 else "")
        gate = require_confirmation(f"отправит сообщение от {phone} пользователю {recipient}: «{preview}»", args)
        if gate:
            return gate
        try:
            await TelegramActionService(client_pool).send_message(
                phone=phone,
                recipient=recipient,
                text=text,
            )
            return _text_response(f"Сообщение отправлено: {recipient}")
        except TelegramActionClientUnavailableError:
            return _text_response(f"Клиент для {phone} не найден или flood-wait активен.")
        except Exception as e:
            return _text_response(f"Ошибка отправки сообщения: {e}")

    tools.append(send_message)

    @tool(
        "edit_message",
        "Edit a previously sent message. "
        "chat_id accepts @username, t.me link, numeric ID, or 'me'. Ask user for confirmation first.",
        EDIT_MESSAGE_SCHEMA,
    )
    async def edit_message(args):
        live_gate = ctx.require_live_runtime("Редактирование сообщения", tool_name="edit_message")
        if live_gate:
            return live_gate
        phone, err = await ctx.resolve_phone(args.get("phone", ""))
        if err:
            return err
        perm_gate = await ctx.require_phone_permission(phone, "edit_message")
        if perm_gate:
            return perm_gate
        chat_id = args.get("chat_id", "")
        message_id = args.get("message_id")
        text = args.get("text", "")
        if not chat_id or not message_id or not text:
            return _text_response("Ошибка: chat_id, message_id и text обязательны.")
        preview = text[:120] + ("..." if len(text) > 120 else "")
        gate = require_confirmation(f"отредактирует сообщение #{message_id} в чате {chat_id}: «{preview}»", args)
        if gate:
            return gate
        try:
            await TelegramActionService(client_pool).edit_message(
                phone=phone,
                chat_id=chat_id,
                message_id=int(message_id),
                text=text,
            )
            return _text_response(f"Сообщение #{message_id} отредактировано.")
        except TelegramActionClientUnavailableError:
            return _text_response(f"Клиент для {phone} не найден или flood-wait активен.")
        except Exception as e:
            return _text_response(f"Ошибка редактирования сообщения: {e}")

    tools.append(edit_message)

    @tool(
        "delete_message",
        "⚠️ DANGEROUS: Delete messages from a Telegram chat. "
        "chat_id accepts @username, numeric ID, or 'me'. "
        "message_ids = comma-separated integers. Always ask user for confirmation first.",
        DELETE_MESSAGE_SCHEMA,
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def delete_message(args):
        phone, err = await prepare_telegram_tool(ctx, args, tool_name="delete_message", action="Удаление сообщений")
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
        gate = require_confirmation(f"удалит {len(ids)} сообщений из чата {chat_id}: {ids}", args)
        if gate:
            return gate
        try:
            await TelegramActionService(client_pool).delete_messages(
                phone=phone,
                chat_id=chat_id,
                message_ids=ids,
            )
            return _text_response(f"Удалено {len(ids)} сообщений из чата {chat_id}.")
        except TelegramActionClientUnavailableError:
            return _text_response(f"Клиент для {phone} не найден или flood-wait активен.")
        except Exception as e:
            return _text_response(f"Ошибка удаления сообщений: {e}")

    tools.append(delete_message)

    @tool(
        "forward_messages",
        "Forward messages from one Telegram chat to another. "
        "Pass comma-separated message IDs. Always ask user for confirmation first.",
        FORWARD_MESSAGES_SCHEMA,
    )
    async def forward_messages(args):
        phone, err = await prepare_telegram_tool(ctx, args, tool_name="forward_messages", action="Пересылка сообщений")
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
        gate = require_confirmation(f"перешлёт {len(ids)} сообщений из {from_chat} в {to_chat}: {ids}", args)
        if gate:
            return gate
        try:
            await TelegramActionService(client_pool).forward_messages(
                phone=phone,
                from_chat=from_chat,
                to_chat=to_chat,
                message_ids=ids,
            )
            return _text_response(f"Переслано {len(ids)} сообщений из {from_chat} в {to_chat}.")
        except TelegramActionClientUnavailableError:
            return _text_response(f"Клиент для {phone} не найден или flood-wait активен.")
        except Exception as e:
            return _text_response(f"Ошибка пересылки сообщений: {e}")

    tools.append(forward_messages)

    @tool(
        "send_reaction",
        "Set an emoji reaction on a Telegram message. "
        "chat_id accepts @username, t.me link, numeric ID, or 'me'. Ask user for confirmation first.",
        SEND_REACTION_SCHEMA,
    )
    async def send_reaction(args):
        pool_gate = ctx.require_pool("Реакция на сообщение")
        if pool_gate:
            return pool_gate
        phone, err = await ctx.resolve_phone(args.get("phone", ""))
        if err:
            return err
        perm_gate = await ctx.require_phone_permission(phone, "send_reaction")
        if perm_gate:
            return perm_gate
        try:
            chat_id = arg_str(args, "chat_id", required=True)
            emoji = arg_str(args, "emoji", required=True)
        except ToolInputError:
            return _text_response("Ошибка: chat_id и emoji обязательны.")
        message_id = args.get("message_id")
        if not message_id:
            return _text_response("Ошибка: message_id обязателен.")
        try:
            message_id_int = int(message_id)
        except (TypeError, ValueError):
            return _text_response("Ошибка: message_id должен быть целым числом.")
        gate = require_confirmation(
            f"поставит реакцию {emoji!r} на сообщение #{message_id_int} в чате {chat_id}",
            args,
        )
        if gate:
            return gate
        try:
            payload = {
                "phone": phone,
                "chat_id": chat_id,
                "message_id": message_id_int,
                "emoji": emoji,
            }
            command_id = await TelegramCommandService(ctx.db).enqueue(
                "dialogs.react",
                payload=payload,
                requested_by="agent:send_reaction",
                deduplicate=True,
            )
            command = await TelegramCommandService(ctx.db).get(command_id)
            status = _command_status_text(getattr(command, "status", None))
            suffix = await _reaction_queue_status_hint(ctx, phone, client_pool)
            lines = [
                f"Реакция {emoji!r} принята в очередь: задача #{command_id}, статус {status}.",
                "Если такая же реакция уже ждала выполнения, использована существующая задача.",
            ]
            if suffix:
                lines.append(suffix)
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка постановки реакции в очередь: {e}")

    tools.append(send_reaction)
    return tools
