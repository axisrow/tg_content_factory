"""Agent tools for Messaging — sending direct messages via Telegram."""

from __future__ import annotations

from claude_agent_sdk import tool

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
        gate = require_confirmation(
            f"отправит сообщение от {phone} пользователю {recipient}", args
        )
        if gate:
            return gate
        try:
            client = await client_pool.get_client_for_phone(phone)
            if client is None:
                return _text_response(f"Клиент для {phone} не найден.")
            entity = await client.get_entity(recipient)
            await client.send_message(entity, text)
            return _text_response(f"Сообщение отправлено: {recipient}")
        except Exception as e:
            return _text_response(f"Ошибка отправки сообщения: {e}")

    tools.append(send_message)

    return tools
