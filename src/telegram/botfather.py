from __future__ import annotations

import logging
import re

from telethon import TelegramClient
from telethon.tl.custom import Message

logger = logging.getLogger(__name__)

BOTFATHER = "BotFather"
_TOKEN_RE = re.compile(r"(\d{8,}:[A-Za-z0-9_\-]{35,})")


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
        await _click_inline(resp, bot_username.lstrip("@"))
        resp = await conv.get_edit()

        # Click "Delete Bot"
        await _click_inline(resp, "Delete Bot")
        resp = await conv.get_response()

        # Confirm — click "Yes, I am totally sure."
        await _click_inline(resp, "sure")
        await conv.get_response()

        logger.info("Bot @%s deleted via BotFather", bot_username)


async def _click_inline(message: Message, text: str) -> None:
    """Click the first inline button whose text contains *text* (case-insensitive)."""
    if not message.reply_markup:
        raise RuntimeError(f"No inline keyboard on message: {message.text[:80]!r}")
    for row in message.reply_markup.rows:
        for button in row.buttons:
            btn_text = getattr(button, "text", "") or ""
            if text.lower() in btn_text.lower():
                data = getattr(button, "data", None)
                await message.click(data=data)
                return
    raise RuntimeError(f"Button containing {text!r} not found")


def _is_error(text: str) -> bool:
    low = text.lower()
    return any(w in low for w in ("sorry", "invalid", "taken", "already", "error"))
