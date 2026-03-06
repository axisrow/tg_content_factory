from __future__ import annotations

import logging
import re
from collections.abc import AsyncGenerator

from telethon import TelegramClient
from telethon.tl.custom import Message

logger = logging.getLogger(__name__)

BOTFATHER = "BotFather"
_TOKEN_RE = re.compile(r"(\d{8,}:[A-Za-z0-9_\-]{35,})")


async def _await_event(client: TelegramClient, event_filter, coro) -> Message:
    """Register an event handler, execute coro, and return the first matched event."""
    import asyncio

    loop = asyncio.get_event_loop()
    future: asyncio.Future = loop.create_future()

    @client.on(event_filter)
    async def _handler(event):  # type: ignore[misc]
        if not future.done():
            future.set_result(event)

    try:
        await coro
        return await asyncio.wait_for(future, timeout=60)
    finally:
        client.remove_event_handler(_handler, event_filter)


async def _iter_buttons(client: TelegramClient) -> AsyncGenerator:
    """Async generator yielding (button_text, button_data) from /mybots inline keyboard."""
    async with client.conversation(BOTFATHER, timeout=30) as conv:
        await conv.send_message("/mybots")
        resp = await conv.get_response()
        if not resp.reply_markup:
            return
        for row in resp.reply_markup.rows:
            for button in row.buttons:
                text = getattr(button, "text", "") or ""
                data = getattr(button, "data", None)
                if text and data:
                    yield text, data


async def _get_bot_menu(client: TelegramClient, bot_username: str) -> Message | None:
    """Navigate to a specific bot's menu in BotFather. Returns the menu message or None."""
    async with client.conversation(BOTFATHER, timeout=30) as conv:
        await conv.send_message("/mybots")
        resp = await conv.get_response()
        if not resp.reply_markup:
            return None
        for row in resp.reply_markup.rows:
            for button in row.buttons:
                btn_text = getattr(button, "text", "") or ""
                if bot_username.lstrip("@").lower() in btn_text.lower():
                    await resp.click(data=getattr(button, "data", None))
                    return await conv.get_edit()
        return None


async def create_bot(client: TelegramClient, name: str, username: str) -> str:
    """Create a bot via BotFather. Returns the bot token."""
    async with client.conversation(BOTFATHER, timeout=60) as conv:
        await conv.send_message("/newbot")
        resp = await conv.get_response()

        # BotFather asks for display name
        if _is_error(resp.text):
            raise RuntimeError(f"BotFather: {resp.text}")

        await conv.send_message(name)
        resp = await conv.get_response()

        # BotFather asks for username (must end in 'bot')
        if _is_error(resp.text):
            raise RuntimeError(f"BotFather: {resp.text}")

        await conv.send_message(username)
        resp = await conv.get_response()

        m = _TOKEN_RE.search(resp.text)
        if not m:
            raise RuntimeError(f"Could not extract token from BotFather response: {resp.text}")

        logger.info("Bot @%s created successfully", username)
        return m.group(1)


async def delete_bot(client: TelegramClient, bot_username: str) -> None:
    """Delete a bot via BotFather by username."""
    async with client.conversation(BOTFATHER, timeout=60) as conv:
        await conv.send_message("/mybots")
        resp = await conv.get_response()

        # Click on the bot button
        _click_inline(resp, bot_username.lstrip("@"))
        resp = await conv.get_edit()

        # Click "Delete Bot"
        _click_inline(resp, "Delete Bot")
        resp = await conv.get_response()

        # Confirm — click "Yes, I am totally sure."
        _click_inline(resp, "sure")
        await conv.get_response()

        logger.info("Bot @%s deleted via BotFather", bot_username)


def _click_inline(message: Message, text: str) -> None:
    """Click the first inline button whose text contains *text* (case-insensitive).

    Note: this fires the click without awaiting — callers must await the resulting
    edit/response separately via the conversation object.
    """
    if not message.reply_markup:
        raise RuntimeError(f"No inline keyboard on message: {message.text[:80]!r}")
    for row in message.reply_markup.rows:
        for button in row.buttons:
            btn_text = getattr(button, "text", "") or ""
            if text.lower() in btn_text.lower():
                import asyncio

                data = getattr(button, "data", None)
                asyncio.ensure_future(message.click(data=data))
                return
    raise RuntimeError(f"Button containing {text!r} not found")


def _is_error(text: str) -> bool:
    low = text.lower()
    return any(w in low for w in ("sorry", "invalid", "taken", "already", "error"))
