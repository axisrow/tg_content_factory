from __future__ import annotations

from typing import Any

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._registry import (
    ToolInputError,
    _text_response,
    arg_csv_ints,
    arg_str,
    require_confirmation,
    resolve_entity,
)
from src.agent.tools._telegram_runtime import prepare_telegram_tool
from src.agent.tools.messaging_schemas import (
    DELETE_MESSAGE_SCHEMA,
    EDIT_MESSAGE_SCHEMA,
    FORWARD_MESSAGES_SCHEMA,
    SEND_MESSAGE_SCHEMA,
)


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
    return tools
