from __future__ import annotations

from typing import Any

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._registry import (
    ToolInputError,
    _text_response,
    arg_csv_ints,
    arg_int,
    arg_str,
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
        "send_reaction",
        "Set an emoji reaction on a Telegram message. "
        "chat_id accepts @username, t.me link, numeric ID, or 'me'. Ask user for confirmation first.",
        SEND_REACTION_SCHEMA,
    )
    async def send_reaction(args):
        phone, err = await prepare_telegram_tool(ctx, args, tool_name="send_reaction", action="Реакция на сообщение")
        if err:
            return err
        try:
            chat_id = arg_str(args, "chat_id", required=True)
            message_id = arg_int(args, "message_id", required=True)
            emoji = arg_str(args, "emoji", required=True)
        except ToolInputError:
            return _text_response("Ошибка: chat_id, message_id и emoji обязательны.")
        gate = require_confirmation(
            f"поставит реакцию {emoji} на сообщение #{message_id} в чате {chat_id} от аккаунта {phone}",
            args,
        )
        if gate:
            return gate
        try:
            await TelegramActionService(client_pool).send_reaction(
                phone=phone,
                chat_id=chat_id,
                message_id=int(message_id),
                emoji=emoji,
            )
            return _text_response(f"Реакция {emoji} поставлена на сообщение #{message_id} в чате {chat_id}.")
        except TelegramActionClientUnavailableError:
            return _text_response(f"Клиент для {phone} не найден или flood-wait активен.")
        except Exception as e:
            return _text_response(f"Ошибка установки реакции: {e}")

    tools.append(send_reaction)

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
    return tools
