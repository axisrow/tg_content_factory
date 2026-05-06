from __future__ import annotations

from typing import Any

from claude_agent_sdk import tool

from src.agent.tools._registry import _text_response, require_confirmation, resolve_entity
from src.agent.tools.messaging_schemas import DOWNLOAD_MEDIA_SCHEMA, PIN_MESSAGE_SCHEMA, UNPIN_MESSAGE_SCHEMA
from src.telegram.flood_wait import HandledFloodWaitError, run_with_flood_wait


def register_pin_media_tools(ctx: Any, client_pool: Any) -> list[Any]:
    tools: list[Any] = []

    @tool(
        "pin_message",
        "Pin a message in a Telegram chat. notify=true sends a notification to all members. "
        "chat_id accepts @username, numeric ID, or 'me'. Ask user for confirmation first.",
        PIN_MESSAGE_SCHEMA,
    )
    async def pin_message(args):
        live_gate = ctx.require_live_runtime("Закрепление сообщения", tool_name="pin_message")
        if live_gate:
            return live_gate
        phone, err = await ctx.resolve_phone(args.get("phone", ""))
        if err:
            return err
        perm_gate = await ctx.require_phone_permission(phone, "pin_message")
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
        UNPIN_MESSAGE_SCHEMA,
    )
    async def unpin_message(args):
        live_gate = ctx.require_live_runtime("Открепление сообщения", tool_name="unpin_message")
        if live_gate:
            return live_gate
        phone, err = await ctx.resolve_phone(args.get("phone", ""))
        if err:
            return err
        perm_gate = await ctx.require_phone_permission(phone, "unpin_message")
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
        DOWNLOAD_MEDIA_SCHEMA,
    )
    async def download_media(args):
        import pathlib

        live_gate = ctx.require_live_runtime("Загрузка медиа", tool_name="download_media")
        if live_gate:
            return live_gate
        phone, err = await ctx.resolve_phone(args.get("phone", ""))
        if err:
            return err
        perm_gate = await ctx.require_phone_permission(phone, "download_media")
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
    return tools
