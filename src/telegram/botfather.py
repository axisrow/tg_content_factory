from __future__ import annotations

import logging
import re

from telethon import TelegramClient
from telethon.tl.custom import Message

logger = logging.getLogger(__name__)

BOTFATHER = "BotFather"
_TOKEN_RE = re.compile(r"(\d{8,}:[A-Za-z0-9_\-]{35,})")


class BotNotFoundError(RuntimeError):
    """The requested bot is absent from BotFather's ``/mybots`` list.

    Raised by :func:`delete_bot` when the bot button is missing from the
    ``/mybots`` keyboard — i.e. the bot does not exist in Telegram for this
    account (typically because it was already deleted). This is a *distinct,
    recoverable* signal, NOT a generic BotFather failure: a repeat teardown can
    treat it as "the Telegram delete already happened" and safely proceed to the
    local DB cleanup, making teardown idempotent (issue #1085, follow-up #1041).
    """

# Redaction is intentionally BROADER than _TOKEN_RE: the orphan branch fires
# precisely when BotFather's reply drifted from the expected token format, so a
# valid token may still be present in the text yet not match the strict regex.
# We scrub any "<8+ digits><sep><20+ token-ish chars>" run — colon OR space/
# dash/equals separators — before that text is logged or raised, so a format
# change can never leak the bot credential into logs (#1041 review follow-up).
_TOKEN_REDACT_RE = re.compile(r"\d{8,}[\s:=\-][A-Za-z0-9_\-]{20,}")


def _redact_tokens(text: str) -> str:
    """Replace any bot-token-like substring with a placeholder for safe logging."""
    return _TOKEN_REDACT_RE.sub("<redacted-token>", text)


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

        token_match = _TOKEN_RE.search(resp.text)
        if not token_match:
            # The username step already succeeded, so BotFather has CREATED the
            # bot in Telegram. A regex miss here (e.g. BotFather changed its
            # reply format) means we have a live orphan bot we can't record or
            # use — flag it loudly with the username so it can be deleted by
            # hand (issue #1041).
            safe_response = _redact_tokens(resp.text)
            logger.error(
                "Orphan bot @%s: BotFather created it but the token could not be "
                "parsed from the response; delete it manually. Response: %s",
                username,
                safe_response,
            )
            raise RuntimeError(
                f"Could not extract token from BotFather response — orphan bot "
                f"@{username} was created in Telegram but has no token and must "
                f"be deleted manually. Response: {safe_response}"
            )

        logger.info("Bot @%s created successfully", username)
        return token_match.group(1)


def _has_inline(message: Message, text: str) -> bool:
    """Return True if any inline button's text contains *text* (case-insensitive).

    Mirrors the matching used by :func:`_click_inline` so callers can probe for a
    button (e.g. the bot entry in ``/mybots``) without clicking it.
    """
    if not message.reply_markup:
        return False
    low = text.lower()
    for row in message.reply_markup.rows:
        for button in row.buttons:
            btn_text = getattr(button, "text", "") or ""
            if low in btn_text.lower():
                return True
    return False


async def delete_bot(client: TelegramClient, bot_username: str) -> None:
    """Delete a bot via BotFather by username.

    Raises :class:`BotNotFoundError` if the bot is absent from ``/mybots`` (it
    no longer exists in Telegram for this account); the caller can treat that as
    an already-completed delete and continue with local cleanup (#1085).
    """
    target = bot_username.lstrip("@")
    async with client.conversation(BOTFATHER, timeout=60) as conv:
        await conv.send_message("/mybots")
        resp = await conv.get_response()

        # BotNotFoundError is the *recoverable* signal (caller may then delete the
        # DB row), so it must mean "the bot is genuinely absent from a valid
        # /mybots listing" — NOT "BotFather gave an unexpected reply". A reply
        # WITHOUT an inline keyboard is not a bot list at all (an error/rate-limit
        # message, or format drift); the live bot may still exist, so raise a
        # GENERIC error that teardown will NOT forgive, leaving the DB row intact.
        # Mapping this to BotNotFoundError would risk wiping a live bot's row
        # (#1085 review, Codex finding).
        if not resp.reply_markup:
            safe = _redact_tokens(resp.text or "")
            raise RuntimeError(
                f"Unexpected BotFather reply to /mybots (no bot list): {safe[:200]!r}"
            )

        # The reply IS a /mybots listing. If our bot's button is absent from it,
        # the bot was already deleted in Telegram — that is the recoverable
        # BotNotFoundError the caller can treat as "TG step already done".
        if not _has_inline(resp, target):
            logger.info("Bot @%s not found in /mybots — already deleted in Telegram", target)
            raise BotNotFoundError(f"Bot @{target} not found in /mybots")
        await _click_inline(resp, target)
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
