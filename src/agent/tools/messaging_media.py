from __future__ import annotations

from typing import Any

from claude_agent_sdk import tool

from src.agent.tools._registry import _text_response, require_confirmation
from src.agent.tools.messaging_schemas import DOWNLOAD_MEDIA_SCHEMA, PIN_MESSAGE_SCHEMA, UNPIN_MESSAGE_SCHEMA
from src.services.telegram_actions import (
    TelegramActionClientUnavailableError,
    TelegramActionMessageNotFoundError,
    TelegramActionNoMediaError,
    TelegramActionPathEscapeError,
    TelegramActionService,
)
from src.telegram.flood_wait import HandledFloodWaitError


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
            await TelegramActionService(client_pool).pin_message(
                phone=phone,
                chat_id=chat_id,
                message_id=int(message_id),
                notify=notify,
            )
            return _text_response(f"Сообщение #{message_id} закреплено в {chat_id}.")
        except TelegramActionClientUnavailableError:
            return _text_response(f"Клиент для {phone} не найден или flood-wait активен.")
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
            await TelegramActionService(client_pool).unpin_message(
                phone=phone,
                chat_id=chat_id,
                message_id=int(message_id) if message_id is not None else None,
            )
            return _text_response(f"Сообщение(я) откреплено в {chat_id}.")
        except TelegramActionClientUnavailableError:
            return _text_response(f"Клиент для {phone} не найден или flood-wait активен.")
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
            output_dir = pathlib.Path(__file__).resolve().parents[3] / "data" / "downloads"
            result = await TelegramActionService(client_pool).download_media(
                phone=phone,
                chat_id=chat_id,
                message_id=int(message_id),
                output_dir=output_dir,
                operation_prefix="agent_download_media",
            )
            return _text_response(f"Медиа загружено: {result.path}")
        except TelegramActionClientUnavailableError:
            return _text_response(f"Клиент для {phone} не найден или flood-wait активен.")
        except TelegramActionMessageNotFoundError:
            return _text_response(f"Сообщение #{message_id} не найдено.")
        except TelegramActionNoMediaError:
            return _text_response("В сообщении нет медиа.")
        except TelegramActionPathEscapeError:
            return _text_response("Ошибка: путь загрузки вне допустимой директории.")
        except HandledFloodWaitError as exc:
            return _text_response(f"Flood wait: {exc.info.detail}")
        except Exception as e:
            return _text_response(f"Ошибка загрузки медиа: {e}")

    tools.append(download_media)
    return tools
