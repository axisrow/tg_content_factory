from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.models import NotificationBot
from src.services.notification_service import NotificationService
from src.services.notification_target_service import NotificationTargetService
from src.telegram import botfather
from src.telegram.notifier import Notifier
from tests.helpers import FakeCliTelethonClient

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


def _make_conv(*get_response_values, get_edit_value=None) -> AsyncMock:
    mock_conv = AsyncMock()
    mock_conv.__aenter__ = AsyncMock(return_value=mock_conv)
    mock_conv.__aexit__ = AsyncMock(return_value=None)
    mock_conv.get_response = AsyncMock(side_effect=list(get_response_values))
    if get_edit_value is not None:
        mock_conv.get_edit = AsyncMock(return_value=get_edit_value)
    return mock_conv


async def _connect_notification_account(
    harness,
    *,
    phone: str,
    session_string: str,
    me_id: int,
    me_username: str | None,
    entity_id: int = 987654321,
    is_primary: bool = False,
):
    # Pool client must have me/entity configured since use_client() now
    # routes through get_client_by_phone which reuses the pool session.
    harness.queue_cli_client(
        phone=phone,
        client=FakeCliTelethonClient(
            me=SimpleNamespace(id=me_id, username=me_username),
            entity_resolver=lambda _peer: SimpleNamespace(id=entity_id),
        ),
    )
    native_client = harness.queue_native_client(
        session_string=session_string,
        client=FakeCliTelethonClient(
            me=SimpleNamespace(id=me_id, username=me_username),
            entity_resolver=lambda _peer: SimpleNamespace(id=entity_id),
        ),
    )
    await harness.add_account(
        phone=phone,
        session_string=session_string,
        is_primary=is_primary,
    )
    await harness.initialize_connected_accounts()
    return native_client


def test_is_error_matches_sorry():
    assert botfather._is_error("Sorry, I can't do that.") is True


def test_is_error_matches_taken():
    assert botfather._is_error("This username is already taken.") is True


def test_is_error_matches_invalid():
    assert botfather._is_error("Invalid bot name.") is True


def test_is_error_ok():
    assert botfather._is_error("Done! Congratulations on your new bot.") is False


async def test_click_inline_finds_button():
    msg = _make_message([["Delete Bot", "Cancel"]])
    await botfather._click_inline(msg, "delete")
    msg.click.assert_awaited_once()


async def test_click_inline_case_insensitive():
    msg = _make_message([["Yes, I am totally sure."]])
    await botfather._click_inline(msg, "sure")
    msg.click.assert_awaited_once()


async def test_click_inline_no_keyboard():
    msg = MagicMock()
    msg.text = "plain text"
    msg.reply_markup = None
    with pytest.raises(RuntimeError, match="No inline keyboard"):
        await botfather._click_inline(msg, "delete")


async def test_click_inline_button_not_found():
    msg = _make_message([["Cancel", "Back"]])
    with pytest.raises(RuntimeError, match="not found"):
        await botfather._click_inline(msg, "delete")


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
# token regex, _click_inline button matching, and _is_error coverage. The token
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


async def test_click_inline_localized_label_raises_clearly():
    """_click_inline matches by English substring; a localized keyboard misses.

    This documents the brittleness called out in #1041: if BotFather renders
    the keyboard in another language, the substring match fails and we get a
    clear 'not found' error rather than a silent wrong-button click.
    """
    # Russian "Удалить бота" instead of "Delete Bot".
    msg = _make_message([["Удалить бота", "Отмена"]])
    with pytest.raises(RuntimeError, match="not found"):
        await botfather._click_inline(msg, "Delete Bot")
    msg.click.assert_not_awaited()


async def test_delete_bot_success():
    bot_msg = _make_message([["@mybot_bot"]])
    options_msg = _make_message([["Bot Info", "Delete Bot"]])
    confirm_msg = _make_message([["Yes, I am totally sure."]])
    done_msg = MagicMock(text="Bot deleted!")

    mock_conv = _make_conv(
        bot_msg,
        confirm_msg,
        done_msg,
        get_edit_value=options_msg,
    )
    mock_client = MagicMock()
    mock_client.conversation.return_value = mock_conv

    await botfather.delete_bot(mock_client, "@mybot_bot")

    bot_msg.click.assert_awaited_once()
    options_msg.click.assert_awaited_once()
    confirm_msg.click.assert_awaited_once()


@pytest.mark.anyio
async def test_setup_bot_success(db, real_pool_harness_factory):
    harness = real_pool_harness_factory()
    await _connect_notification_account(
        harness,
        phone="+70001111111",
        session_string="session-1",
        me_id=111,
        me_username="alice",
        is_primary=True,
    )
    svc = NotificationService(db, NotificationTargetService(db, harness.pool))

    with patch(
        "src.services.notification_service.botfather.create_bot",
        new_callable=AsyncMock,
        return_value="111111111:AABBCCDDEEFFaabbccddeeffAABBCCDDEEFF",
    ):
        bot = await svc.setup_bot()

    assert bot.tg_user_id == 111
    assert bot.bot_username == "leadhunter_alice_bot"
    assert bot.bot_id == 987654321

    saved = await db.get_notification_bot(111)
    assert saved is not None
    assert saved.bot_username == "leadhunter_alice_bot"


@pytest.mark.anyio
async def test_setup_bot_custom_prefix(db, real_pool_harness_factory):
    harness = real_pool_harness_factory()
    await _connect_notification_account(
        harness,
        phone="+70001111111",
        session_string="session-1",
        me_id=222,
        me_username="bob",
        is_primary=True,
    )
    svc = NotificationService(
        db,
        NotificationTargetService(db, harness.pool),
        bot_name_prefix="Acme",
        bot_username_prefix="acme_",
    )

    with patch(
        "src.services.notification_service.botfather.create_bot",
        new_callable=AsyncMock,
        return_value="222222222:AABBCCDDEEFFaabbccddeeffAABBCCDDEEFF",
    ) as mock_create:
        bot = await svc.setup_bot()

    assert bot.bot_username == "acme_bob_bot"
    created_client = mock_create.await_args.args[0]
    # use_client() now returns pool session (reuses persistent connection)
    pool_session = harness.pool.clients["+70001111111"]
    assert created_client.raw_client is pool_session.raw_client
    assert mock_create.await_args.args[1:] == ("Acme (bob)", "acme_bob_bot")


@pytest.mark.anyio
async def test_setup_bot_slug_truncated(db, real_pool_harness_factory):
    long_username = "averylongusernamethatexceeds17"
    harness = real_pool_harness_factory()
    await _connect_notification_account(
        harness,
        phone="+70001111111",
        session_string="session-1",
        me_id=333,
        me_username=long_username,
        is_primary=True,
    )
    svc = NotificationService(db, NotificationTargetService(db, harness.pool))

    with patch(
        "src.services.notification_service.botfather.create_bot",
        new_callable=AsyncMock,
        return_value="333333333:AABBCCDDEEFFaabbccddeeffAABBCCDDEEFF",
    ):
        bot = await svc.setup_bot()

    slug = long_username[:17]
    assert bot.bot_username == f"leadhunter_{slug}_bot"
    assert len(bot.bot_username) <= 32


@pytest.mark.anyio
async def test_setup_bot_no_client(db, real_pool_harness_factory):
    harness = real_pool_harness_factory()
    svc = NotificationService(db, NotificationTargetService(db, harness.pool))

    with pytest.raises(RuntimeError, match="Primary-аккаунт"):
        await svc.setup_bot()


@pytest.mark.anyio
async def test_setup_bot_bot_id_none_if_entity_fails(db, real_pool_harness_factory):
    harness = real_pool_harness_factory()
    harness.queue_cli_client(
        phone="+70001111111",
        client=FakeCliTelethonClient(
            me=SimpleNamespace(id=444, username="carol"),
            entity_resolver=lambda _peer: Exception("peer not found"),
        ),
    )
    harness.queue_native_client(
        session_string="session-1",
        client=FakeCliTelethonClient(
            me=SimpleNamespace(id=444, username="carol"),
            entity_resolver=lambda _peer: Exception("peer not found"),
        ),
    )
    await harness.add_account(
        phone="+70001111111",
        session_string="session-1",
        is_primary=True,
    )
    await harness.initialize_connected_accounts()

    svc = NotificationService(db, NotificationTargetService(db, harness.pool))
    with patch(
        "src.services.notification_service.botfather.create_bot",
        new_callable=AsyncMock,
        return_value="444444444:AABBCCDDEEFFaabbccddeeffAABBCCDDEEFF",
    ):
        bot = await svc.setup_bot()

    assert bot.bot_id is None


@pytest.mark.anyio
async def test_get_status_no_bot(db, real_pool_harness_factory):
    harness = real_pool_harness_factory()
    await _connect_notification_account(
        harness,
        phone="+70001111111",
        session_string="session-1",
        me_id=555,
        me_username="alice",
        is_primary=True,
    )
    svc = NotificationService(db, NotificationTargetService(db, harness.pool))
    result = await svc.get_status()
    assert result is None


@pytest.mark.anyio
async def test_get_status_returns_bot(db, real_pool_harness_factory):
    saved = NotificationBot(
        tg_user_id=666,
        tg_username="dave",
        bot_id=111,
        bot_username="leadhunter_dave_bot",
        bot_token="666666666:AABBCCDDEEFFaabbccddeeffAABBCCDDEEFF",
    )
    await db.save_notification_bot(saved)

    harness = real_pool_harness_factory()
    await _connect_notification_account(
        harness,
        phone="+70001111111",
        session_string="session-1",
        me_id=666,
        me_username="dave",
        is_primary=True,
    )
    svc = NotificationService(db, NotificationTargetService(db, harness.pool))
    result = await svc.get_status()

    assert result is not None
    assert result.tg_user_id == 666
    assert result.bot_username == "leadhunter_dave_bot"


@pytest.mark.anyio
async def test_teardown_bot_success(db, real_pool_harness_factory):
    saved = NotificationBot(
        tg_user_id=777,
        tg_username="eve",
        bot_id=222,
        bot_username="leadhunter_eve_bot",
        bot_token="777777777:AABBCCDDEEFFaabbccddeeffAABBCCDDEEFF",
    )
    await db.save_notification_bot(saved)

    harness = real_pool_harness_factory()
    await _connect_notification_account(
        harness,
        phone="+70001111111",
        session_string="session-1",
        me_id=777,
        me_username="eve",
        is_primary=True,
    )
    svc = NotificationService(db, NotificationTargetService(db, harness.pool))

    with patch(
        "src.services.notification_service.botfather.delete_bot",
        new_callable=AsyncMock,
    ):
        await svc.teardown_bot()

    assert await db.get_notification_bot(777) is None


@pytest.mark.anyio
async def test_teardown_bot_db_delete_failure_warns_about_orphan(
    db, real_pool_harness_factory, caplog
):
    """RED→GREEN (#1041): a DB-delete failure AFTER BotFather succeeds must not
    fail silently — it leaves an orphan DB row pointing at a bot that no longer
    exists in Telegram.

    ``teardown_bot`` deletes the live bot via BotFather first, then removes the
    DB row. If the DB delete raises after BotFather already destroyed the bot,
    the row survives: ``get_status`` will keep reporting the bot as configured
    while ``send_notification`` silently can't reach it. The service must log a
    loud orphan warning (so the operator can clean the row) and surface the
    failure rather than swallow it.
    """
    saved = NotificationBot(
        tg_user_id=999,
        tg_username="frank",
        bot_id=333,
        bot_username="leadhunter_frank_bot",
        bot_token="999999999:AABBCCDDEEFFaabbccddeeffAABBCCDDEEFF",
    )
    await db.save_notification_bot(saved)

    harness = real_pool_harness_factory()
    await _connect_notification_account(
        harness,
        phone="+70001111111",
        session_string="session-1",
        me_id=999,
        me_username="frank",
        is_primary=True,
    )
    svc = NotificationService(db, NotificationTargetService(db, harness.pool))

    # BotFather succeeds (bot is gone from Telegram) but the DB delete blows up.
    with (
        patch(
            "src.services.notification_service.botfather.delete_bot",
            new_callable=AsyncMock,
        ),
        patch.object(
            svc._notifications.notification_bots,
            "delete_bot",
            new_callable=AsyncMock,
            side_effect=RuntimeError("db locked"),
        ),
    ):
        import logging

        with caplog.at_level(logging.ERROR):
            with pytest.raises(RuntimeError):
                await svc.teardown_bot()

    # The operator must be told the live bot is gone but the row remains.
    combined = " ".join(r.getMessage() for r in caplog.records).lower()
    assert "orphan" in combined
    assert "leadhunter_frank_bot" in combined or "999" in combined


@pytest.mark.anyio
async def test_teardown_bot_no_bot_raises(db, real_pool_harness_factory):
    harness = real_pool_harness_factory()
    await _connect_notification_account(
        harness,
        phone="+70001111111",
        session_string="session-1",
        me_id=888,
        me_username="alice",
        is_primary=True,
    )
    svc = NotificationService(db, NotificationTargetService(db, harness.pool))

    with pytest.raises(RuntimeError, match="No notification bot"):
        await svc.teardown_bot()


@pytest.mark.anyio
async def test_notifier_uses_primary_account_by_default(db, real_pool_harness_factory):
    harness = real_pool_harness_factory()
    await _connect_notification_account(
        harness,
        phone="+70001111111",
        session_string="session-1",
        me_id=111,
        me_username="alice",
        is_primary=True,
    )

    notifier = Notifier(NotificationTargetService(db, harness.pool), admin_chat_id=123456)
    sent = await notifier.notify("hello")

    assert sent is True
    # Verify send went through the pool session's raw_client (cli_client)
    pool_session = harness.pool.clients["+70001111111"]
    pool_session.raw_client.send_message.assert_awaited_once_with(123456, "hello")


@pytest.mark.anyio
async def test_notifier_does_not_fallback_from_selected_account(db, real_pool_harness_factory):
    harness = real_pool_harness_factory()
    primary_client = await _connect_notification_account(
        harness,
        phone="+70001111111",
        session_string="session-primary",
        me_id=111,
        me_username="primary",
        is_primary=True,
    )
    await _connect_notification_account(
        harness,
        phone="+70002222222",
        session_string="session-selected",
        me_id=222,
        me_username="selected",
        is_primary=False,
    )
    await db.set_setting("notification_account_phone", "+70002222222")
    # Put selected account in flood wait so describe_target() returns non-available
    await db.update_account_flood("+70002222222", datetime.now(timezone.utc) + timedelta(seconds=300))

    notifier = Notifier(NotificationTargetService(db, harness.pool), admin_chat_id=123456)
    sent = await notifier.notify("hello")

    assert sent is False
    primary_client.send_message.assert_not_awaited()


@pytest.mark.anyio
async def test_notification_service_uses_selected_account(db, real_pool_harness_factory):
    harness = real_pool_harness_factory()
    await _connect_notification_account(
        harness,
        phone="+70001111111",
        session_string="session-primary",
        me_id=111,
        me_username="primary",
        is_primary=True,
    )
    await _connect_notification_account(
        harness,
        phone="+70002222222",
        session_string="session-selected",
        me_id=222,
        me_username="selected",
        entity_id=222333444,
        is_primary=False,
    )
    await db.set_setting("notification_account_phone", "+70002222222")

    svc = NotificationService(db, NotificationTargetService(db, harness.pool))
    with patch(
        "src.services.notification_service.botfather.create_bot",
        new_callable=AsyncMock,
        return_value="222222222:AABBCCDDEEFFaabbccddeeffAABBCCDDEEFF",
    ):
        bot = await svc.setup_bot()

    assert bot.tg_user_id == 222


# ---------------------------------------------------------------------------
# send_notification — backs CLI `notification test` and agent test_notification
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_send_notification_delivers_via_notifier():
    target_service = MagicMock()
    svc = NotificationService(MagicMock(), target_service)
    notifier = MagicMock()
    notifier.notify = AsyncMock(return_value=True)
    with patch("src.telegram.notifier.Notifier", return_value=notifier) as mock_notifier:
        ok = await svc.send_notification("Custom message")
    assert ok is True
    notifier.notify.assert_awaited_once_with("Custom message")
    # Notifier wired with the service's target_service and notification bundle.
    assert mock_notifier.call_args.args[0] is target_service


@pytest.mark.anyio
async def test_send_notification_defaults_blank_message():
    svc = NotificationService(MagicMock(), MagicMock())
    notifier = MagicMock()
    notifier.notify = AsyncMock(return_value=True)
    with patch("src.telegram.notifier.Notifier", return_value=notifier):
        await svc.send_notification("   ")
    sent = notifier.notify.await_args.args[0]
    assert sent.strip() != ""


@pytest.mark.anyio
async def test_send_notification_raises_on_failure():
    svc = NotificationService(MagicMock(), MagicMock())
    notifier = MagicMock()
    notifier.notify = AsyncMock(return_value=False)
    with patch("src.telegram.notifier.Notifier", return_value=notifier):
        with pytest.raises(RuntimeError, match="notification_test_failed"):
            await svc.send_notification("boom")
