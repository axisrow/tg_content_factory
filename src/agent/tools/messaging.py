"""Agent tools for Messaging — send, edit, delete messages via Telegram."""

from __future__ import annotations

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._registry import _text_response, require_confirmation, require_pool


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
        phone = args.get("phone", "")
        recipient = args.get("recipient", "")
        text = args.get("text", "")
        if not phone or not recipient or not text:
            return _text_response("Ошибка: phone, recipient и text обязательны.")
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
        phone = args.get("phone", "")
        chat_id = args.get("chat_id", "")
        message_id = args.get("message_id")
        text = args.get("text", "")
        if not phone or not chat_id or not message_id or not text:
            return _text_response("Ошибка: phone, chat_id, message_id и text обязательны.")
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
        phone = args.get("phone", "")
        chat_id = args.get("chat_id", "")
        message_ids_str = args.get("message_ids", "")
        if not phone or not chat_id or not message_ids_str:
            return _text_response("Ошибка: phone, chat_id и message_ids обязательны.")
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

    return tools
