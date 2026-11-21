"""Unit tests for tg_shared.botfather (#1421, wave A).

Moved verbatim from the factory's ``tests/test_notification.py`` — they
exercise the BotFather conversation helper only; the factory-side
notification-service flows (``test_setup_bot_*`` etc.) stayed behind in
``tests/test_notification.py``.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from tg_shared import botfather

pytestmark = pytest.mark.native_backend_allowed


def _make_message(button_rows: list[list[str]], text: str = "") -> MagicMock:
    msg = MagicMock()
    msg.text = text
    msg.reply_markup = MagicMock()
    rows = []
    for row_labels in button_rows:
        buttons = []
        for label in row_labels:
            btn = MagicMock()
            btn.text = label
            btn.data = label.encode()
            buttons.append(btn)
        row_mock = MagicMock()
        row_mock.buttons = buttons
        rows.append(row_mock)
    msg.reply_markup.rows = rows
    msg.click = AsyncMock()
    return msg


_VALID_TOKEN = "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi"


def _make_conv(*get_response_values) -> AsyncMock:
    mock_conv = AsyncMock()
    mock_conv.__aenter__ = AsyncMock(return_value=mock_conv)
    mock_conv.__aexit__ = AsyncMock(return_value=None)
    mock_conv.get_response = AsyncMock(side_effect=list(get_response_values))
    return mock_conv


def test_is_error_matches_sorry():
    assert botfather._is_error("Sorry, I can't do that.") is True


def test_is_error_matches_taken():
    assert botfather._is_error("This username is already taken.") is True


def test_is_error_matches_invalid():
    assert botfather._is_error("Invalid bot name.") is True


def test_is_error_ok():
    assert botfather._is_error("Done! Congratulations on your new bot.") is False


async def test_create_bot_success():
    mock_conv = _make_conv(
        MagicMock(text="Alright, send me the name."),
        MagicMock(text="Good. Now choose a username."),
        MagicMock(text=f"Done! Use this token:\n{_VALID_TOKEN}"),
    )
    mock_client = MagicMock()
    mock_client.conversation.return_value = mock_conv

    result = await botfather.create_bot(mock_client, "MyBot", "mybot_bot")
    assert result == _VALID_TOKEN


async def test_create_bot_botfather_error_on_name():
    mock_conv = _make_conv(MagicMock(text="Sorry, too many attempts."))
    mock_client = MagicMock()
    mock_client.conversation.return_value = mock_conv

    with pytest.raises(RuntimeError, match="BotFather"):
        await botfather.create_bot(mock_client, "MyBot", "mybot_bot")


async def test_create_bot_no_token_in_response():
    mock_conv = _make_conv(
        MagicMock(text="Alright, send me the name."),
        MagicMock(text="Good. Now choose a username."),
        MagicMock(text="Something went wrong, no token here."),
    )
    mock_client = MagicMock()
    mock_client.conversation.return_value = mock_conv

    with pytest.raises(RuntimeError, match="Could not extract token"):
        await botfather.create_bot(mock_client, "MyBot", "mybot_bot")


# ---------------------------------------------------------------------------
# Brittle BotFather parsing (issue #1041) — failure modes around create_bot's
# token regex and _is_error coverage. The token
# regex in particular is a blind spot: by the time it runs, BotFather has
# ALREADY created the bot, so a regex miss orphans a live bot in Telegram.
# ---------------------------------------------------------------------------


async def test_create_bot_token_miss_warns_about_orphan(caplog):
    """RED→GREEN (#1041): a token-regex miss leaves a live orphan bot.

    By the time ``create_bot`` reaches the final response, the username has been
    accepted and BotFather has created the bot. If ``_TOKEN_RE`` fails to match
    (BotFather changed its reply format), the bot exists in Telegram but we have
    no token and no record — an orphan. The raised error must name the bot
    username and the word "orphan" so the operator can find and delete it.
    """
    import logging

    mock_conv = _make_conv(
        MagicMock(text="Alright, send me the name."),
        MagicMock(text="Good. Now choose a username."),
        MagicMock(text="Your bot is ready! (but the token format changed)"),
    )
    mock_client = MagicMock()
    mock_client.conversation.return_value = mock_conv

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError) as exc_info:
            await botfather.create_bot(mock_client, "MyBot", "mybot_orphan_bot")

    message = str(exc_info.value).lower()
    logged = " ".join(r.getMessage() for r in caplog.records).lower()
    combined = message + " " + logged
    assert "orphan" in combined
    assert "mybot_orphan_bot" in combined


async def test_create_bot_token_miss_redacts_token_from_logs_and_error(caplog):
    """RED→GREEN (#1041 review): orphan branch must not LEAK a token.

    The orphan branch fires exactly when the token regex drifts — which means a
    valid token may still be present in the reply (e.g. BotFather changed the
    delimiter). Logging/raising the raw response would then write the bot
    credential to logs. The response must be redacted before it is logged or
    embedded in the raised error.
    """
    import logging

    # A real-shaped token but with a SPACE delimiter, so _TOKEN_RE (which wants
    # a colon) misses it — the exact format-drift the orphan branch handles.
    leaked = "123456789 ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi"
    mock_conv = _make_conv(
        MagicMock(text="Alright, send me the name."),
        MagicMock(text="Good. Now choose a username."),
        MagicMock(text=f"Here is your bot! Token: {leaked}"),
    )
    mock_client = MagicMock()
    mock_client.conversation.return_value = mock_conv

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError) as exc_info:
            await botfather.create_bot(mock_client, "MyBot", "mybot_leak_bot")

    logged = " ".join(r.getMessage() for r in caplog.records)
    message = str(exc_info.value)
    # The token-like substring must NOT appear in either surface.
    assert leaked not in logged
    assert leaked not in message
    assert "<redacted-token>" in logged
    assert "<redacted-token>" in message


def test_redact_tokens_scrubs_token_variants():
    """_redact_tokens scrubs colon/space/dash-delimited token-like runs (#1041)."""
    assert "<redacted-token>" == botfather._redact_tokens(
        "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi"
    )
    assert "<redacted-token>" == botfather._redact_tokens(
        "123456789 ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi"
    )
    # Plain prose without a token run is left untouched.
    assert botfather._redact_tokens("Sorry, that username is taken.") == (
        "Sorry, that username is taken."
    )


def test_is_error_does_not_false_positive_on_token_reply():
    """A successful token reply must not be misread as an error (#1041).

    ``_is_error`` runs on the name/username step replies. The substring list
    must not flag a legitimate BotFather success message — otherwise create_bot
    raises AFTER the bot is created, orphaning it. Guard the known-good replies.
    """
    assert botfather._is_error("Good. Now let's choose a username for your bot.") is False
    assert botfather._is_error("Alright! A new bot. How are we going to call it?") is False
    assert (
        botfather._is_error(
            "Done! Congratulations on your new bot. Use this token to access the HTTP API:"
        )
        is False
    )


def test_is_error_matches_too_many_requests():
    """Rate-limit replies are errors too — currently uncovered (#1041)."""
    # BotFather rate-limit / generic failure wordings beyond the original set.
    assert botfather._is_error("Sorry, too many attempts. Please try again later.") is True


_PROMPT = (
    "OK, you selected @mybot_bot. Are you sure?\n\n"
    "Send 'Yes, I am totally sure.' to confirm you really want to delete this bot."
)
_DONE = "Done! The bot is gone. /help"


def _listing(*usernames: str) -> MagicMock:
    """BotFather's "Choose a bot to delete." reply: one keyboard button per bot."""
    return _make_message([[name] for name in usernames], text="Choose a bot to delete.")


def _reply(text: str) -> MagicMock:
    msg = MagicMock()
    msg.text = text
    msg.reply_markup = None
    return msg


def _client_with(conv: AsyncMock) -> MagicMock:
    client = MagicMock()
    client.conversation.return_value = conv
    return client


def _sent(conv: AsyncMock) -> list[str]:
    return [call.args[0] for call in conv.send_message.await_args_list]


@pytest.mark.parametrize("asked", ["@mybot_bot", "mybot_bot", "@MyBot_Bot"], ids=["at", "bare", "other-case"])
async def test_delete_bot_needs_no_message_edits(asked):
    """A plain message exchange, with no button presses and no waiting for edits.

    Found live (#1868): BotFather's edits of its ``/mybots`` message never
    reached the waiting conversation and the delete timed out twice, while its
    plain replies kept arriving.
    """
    conv = _make_conv(_listing("@mybot_bot"), _reply(_PROMPT), _reply(_DONE))
    conv.get_edit = AsyncMock(side_effect=asyncio.TimeoutError)

    await botfather.delete_bot(_client_with(conv), asked)

    assert _sent(conv) == ["/deletebot", "@" + asked.lstrip("@"), "Yes, I am totally sure."]


async def test_delete_bot_missing_from_the_list_raises_bot_not_found():
    """RED→GREEN (#1085): a bot absent from BotFather's list is a distinct, recoverable signal.

    When ``teardown_bot``'s first BotFather call already destroyed the bot but the
    DB-delete then failed, the row becomes an orphan (issue #1041). A *repeat*
    teardown calls ``delete_bot`` again, but the bot is gone from Telegram, so
    BotFather no longer lists it.

    That condition must be a *typed* signal (``BotNotFoundError``) — separable
    from a real BotFather failure — so the caller can treat it as "TG step
    already done, proceed to DB cleanup" instead of a hard error that strands
    the orphan row forever.
    """
    conv = _make_conv(_listing("@someone_else_bot"))

    with pytest.raises(botfather.BotNotFoundError):
        await botfather.delete_bot(_client_with(conv), "@leadhunter_gone_bot")

    assert _sent(conv) == ["/deletebot"]


@pytest.mark.parametrize(
    "prompt",
    [
        "OK, you selected @other_bot. Are you sure?\n\nSend 'Yes, I am totally sure.' to confirm.",
        "Invalid bot selected. Please try again.",
        "Sorry, too many attempts. Please try again later.",
    ],
    ids=["another-bot", "refused", "rate-limited"],
)
async def test_delete_bot_never_confirms_an_unexpected_choice(prompt):
    """The confirmation is irreversible: it is sent only for a prompt that names our bot."""
    conv = _make_conv(_listing("@mybot_bot"), _reply(prompt))

    with pytest.raises(RuntimeError) as exc_info:
        await botfather.delete_bot(_client_with(conv), "@mybot_bot")

    assert not isinstance(exc_info.value, botfather.BotNotFoundError)
    assert _sent(conv) == ["/deletebot", "@mybot_bot"]


async def test_delete_bot_reports_a_deletion_botfather_did_not_confirm():
    """Without BotFather's "gone" the bot may still live, so its DB row must stay."""
    conv = _make_conv(_listing("@mybot_bot"), _reply(_PROMPT), _reply("Sorry, something went wrong."))

    with pytest.raises(RuntimeError, match="did not confirm") as exc_info:
        await botfather.delete_bot(_client_with(conv), "@mybot_bot")

    assert not isinstance(exc_info.value, botfather.BotNotFoundError)


@pytest.mark.parametrize(
    "markup",
    [None, MagicMock(spec=[])],
    ids=["no-markup", "keyboard-removed"],
)
async def test_delete_bot_keyboardless_reply_is_generic_error_not_bot_not_found(markup):
    """RED→GREEN (#1085, Codex finding): an error/keyboard-less reply is NOT "bot gone".

    ``BotNotFoundError`` is the *recoverable* signal that lets ``teardown_bot``
    skip the Telegram step and delete the DB row. It must therefore mean "the bot
    is genuinely absent from a valid listing" — NOT "BotFather replied with
    something we didn't expect". If ``/deletebot`` returns an error, a
    rate-limit, or any reply WITHOUT keyboard buttons (so we never saw a bot
    list at all — a markup that only removes the keyboard has no rows), the
    live bot may still exist. Mapping that to ``BotNotFoundError`` would let
    teardown wipe the DB row while the bot is alive — a data-loss orphan flip.
    Such a reply must raise a *generic* RuntimeError (which ``teardown_bot``
    does NOT forgive), leaving the row intact.
    """
    # A plain text BotFather reply (rate-limit / transient error) — no bot list.
    err_msg = _reply("Sorry, too many requests. Please try again later.")
    err_msg.reply_markup = markup
    conv = _make_conv(err_msg)

    with pytest.raises(RuntimeError, match="no bot list") as exc_info:
        await botfather.delete_bot(_client_with(conv), "@leadhunter_alive_bot")
    # Must NOT be the recoverable subclass — the bot might still be live.
    assert not isinstance(exc_info.value, botfather.BotNotFoundError)
    assert _sent(conv) == ["/deletebot"]
