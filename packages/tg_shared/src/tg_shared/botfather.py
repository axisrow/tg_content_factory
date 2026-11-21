from __future__ import annotations

import logging
import re

from telethon import TelegramClient
from telethon.tl.custom import Message

logger = logging.getLogger(__name__)

BOTFATHER = "BotFather"
_TOKEN_RE = re.compile(r"(\d{8,}:[A-Za-z0-9_\-]{35,})")


class BotNotFoundError(RuntimeError):
    """The requested bot is absent from BotFather's list of this account's bots.

    Raised by :func:`delete_bot` when the bot button is missing from the
    keyboard BotFather offers, or BotFather says the account has no bots — i.e.
    the bot does not exist in Telegram for this
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


def _button_texts(message: Message) -> list[str]:
    """Texts of the keyboard buttons under *message*; empty when it offers none.

    A reply that only removes the keyboard carries a markup without rows.
    """
    rows = getattr(message.reply_markup, "rows", None) or []
    return [getattr(button, "text", "") or "" for row in rows for button in row.buttons]


_CONFIRM_DELETE = "Yes, I am totally sure."


def _shown(message: Message) -> str:
    """BotFather's reply text, safe to put into an error or a log line."""
    return repr(_redact_tokens(message.text or "")[:200])


async def delete_bot(client: TelegramClient, bot_username: str) -> None:
    """Delete a bot via BotFather by username.

    A plain message exchange: ``/deletebot``, the bot's username, the
    confirmation phrase. The ``/mybots`` menu does the same with button
    presses that BotFather answers by editing its message, and on a live run
    those edits never reached the waiting conversation (#1868).

    Raises :class:`BotNotFoundError` if BotFather does not list the bot, or says
    the account has no bots at all (it no longer exists in Telegram for this
    account); the caller can treat that as an already-completed delete and
    continue with local cleanup (#1085).
    """
    target = bot_username.lstrip("@")
    async with client.conversation(BOTFATHER, timeout=60) as conv:
        await conv.send_message("/deletebot")
        resp = await conv.get_response()

        # BotNotFoundError is the *recoverable* signal (caller may then delete the
        # DB row), so it must mean "the bot is genuinely absent from a valid
        # listing" — NOT "BotFather gave an unexpected reply". A reply WITHOUT a
        # keyboard is not a bot list at all (an error/rate-limit message, or
        # format drift); the live bot may still exist, so raise a GENERIC error
        # that teardown will NOT forgive, leaving the DB row intact. Mapping this
        # to BotNotFoundError would risk wiping a live bot's row (#1085 review,
        # Codex finding).
        bots = _button_texts(resp)
        if not bots:
            # The one exception is BotFather saying in its own words that the
            # account has no bots ("You don't have any bots yet. Use the /newbot
            # command to create a new bot first."): an empty list, so ours is gone.
            if "have any bots" in (resp.text or ""):
                logger.info("BotFather has no bots on this account — @%s is already deleted in Telegram", target)
                raise BotNotFoundError(f"BotFather has no bots on this account, so @{target} no longer exists")
            raise RuntimeError(f"Unexpected BotFather reply to /deletebot (no bot list): {_shown(resp)}")

        # The reply IS a listing. If our bot's button is absent from it, the bot
        # was already deleted in Telegram — that is the recoverable
        # BotNotFoundError the caller can treat as "TG step already done".
        if not any(target.lower() in text.lower() for text in bots):
            logger.info("Bot @%s is not in BotFather's list — already deleted in Telegram", target)
            raise BotNotFoundError(f"Bot @{target} not found in BotFather's list")

        await conv.send_message(f"@{target}")
        resp = await conv.get_response()
        # The phrase below deletes whichever bot BotFather has selected, and
        # that cannot be undone: it goes out only for a reply naming this bot.
        if f"@{target}".lower() not in (resp.text or "").lower():
            raise RuntimeError(f"BotFather did not select @{target} for deletion: {_shown(resp)}")

        await conv.send_message(_CONFIRM_DELETE)
        resp = await conv.get_response()
        if "gone" not in (resp.text or "").lower():
            raise RuntimeError(f"BotFather did not confirm that @{target} is deleted: {_shown(resp)}")

        logger.info("Bot @%s deleted via BotFather", bot_username)


def _is_error(text: str) -> bool:
    low = text.lower()
    return any(w in low for w in ("sorry", "invalid", "taken", "already", "error"))
