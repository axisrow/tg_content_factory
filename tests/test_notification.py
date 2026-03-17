from __future__ import annotations

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
    harness.queue_cli_client(phone=phone, client=FakeCliTelethonClient())
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


@pytest.mark.asyncio
async def test_setup_bot_success(db, real_pool_harness_factory):
    harness = real_pool_harness_factory()
    native_client = await _connect_notification_account(
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
    native_client.disconnect.assert_awaited_once()

    saved = await db.get_notification_bot(111)
    assert saved is not None
    assert saved.bot_username == "leadhunter_alice_bot"


@pytest.mark.asyncio
async def test_setup_bot_custom_prefix(db, real_pool_harness_factory):
    harness = real_pool_harness_factory()
    native_client = await _connect_notification_account(
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
    mock_create.assert_awaited_once_with(native_client, "Acme (bob)", "acme_bob_bot")


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_setup_bot_no_client(db, real_pool_harness_factory):
    harness = real_pool_harness_factory()
    svc = NotificationService(db, NotificationTargetService(db, harness.pool))

    with pytest.raises(RuntimeError, match="Primary-аккаунт"):
        await svc.setup_bot()


@pytest.mark.asyncio
async def test_setup_bot_bot_id_none_if_entity_fails(db, real_pool_harness_factory):
    harness = real_pool_harness_factory()
    harness.queue_cli_client(phone="+70001111111", client=FakeCliTelethonClient())
    native_client = harness.queue_native_client(
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
    native_client.disconnect.assert_awaited_once()


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
    native_client = await _connect_notification_account(
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
    native_client.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_teardown_bot_no_bot_raises(db, real_pool_harness_factory):
    harness = real_pool_harness_factory()
    native_client = await _connect_notification_account(
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

    native_client.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_notifier_uses_primary_account_by_default(db, real_pool_harness_factory):
    harness = real_pool_harness_factory()
    native_client = await _connect_notification_account(
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
    native_client.send_message.assert_awaited_once_with(123456, "hello")
    native_client.disconnect.assert_awaited_once()


@pytest.mark.asyncio
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
    harness.native_auth_spy.by_session.pop("session-selected", None)

    def _raise_native(session_string: str):
        if session_string == "session-selected":
            raise ConnectionError("selected account unavailable")
        return FakeCliTelethonClient(
            me=SimpleNamespace(id=111, username="primary"),
        )

    harness.native_auth_spy.factory = _raise_native

    notifier = Notifier(NotificationTargetService(db, harness.pool), admin_chat_id=123456)
    sent = await notifier.notify("hello")

    assert sent is False
    primary_client.send_message.assert_not_awaited()


@pytest.mark.asyncio
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
    selected_client = await _connect_notification_account(
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
    assert selected_client.disconnect.await_count == 1
