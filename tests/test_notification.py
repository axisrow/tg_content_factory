from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.models import Account, NotificationBot
from src.services.notification_service import NotificationService
from src.services.notification_target_service import NotificationTargetService
from src.telegram import botfather
from src.telegram.notifier import Notifier

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_message(button_rows: list[list[str]], text: str = "") -> MagicMock:
    """Build a Message-like mock with an inline keyboard."""
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


def _make_pool(
    me_id: int = 111,
    me_username: str = "alice",
    phone: str = "+70001111111",
) -> tuple:
    """Return (mock_pool, mock_client) with get_me pre-configured."""
    me = MagicMock()
    me.id = me_id
    me.username = me_username

    entity = MagicMock()
    entity.id = 987654321

    mock_client = AsyncMock()
    mock_client.get_me = AsyncMock(return_value=me)
    mock_client.send_message = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=entity)

    pool = AsyncMock()
    pool.clients = {phone: mock_client}
    pool.get_available_client = AsyncMock(return_value=(mock_client, phone))
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, phone))
    pool.release_client = AsyncMock()
    return pool, mock_client


async def _add_account(
    db,
    phone: str = "+70001111111",
    *,
    is_primary: bool = True,
    is_active: bool = True,
) -> None:
    await db.add_account(
        Account(
            phone=phone,
            session_string=f"session-{phone}",
            is_primary=is_primary,
            is_active=is_active,
        )
    )


# ---------------------------------------------------------------------------
# botfather._is_error
# ---------------------------------------------------------------------------


def test_is_error_matches_sorry():
    assert botfather._is_error("Sorry, I can't do that.") is True


def test_is_error_matches_taken():
    assert botfather._is_error("This username is already taken.") is True


def test_is_error_matches_invalid():
    assert botfather._is_error("Invalid bot name.") is True


def test_is_error_ok():
    assert botfather._is_error("Done! Congratulations on your new bot.") is False


# ---------------------------------------------------------------------------
# botfather._click_inline
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# botfather.create_bot
# ---------------------------------------------------------------------------

_VALID_TOKEN = "123456789:ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghi"


def _make_conv(*get_response_values, get_edit_value=None) -> AsyncMock:
    """Build an AsyncMock that works as an async context manager (conv = mock_conv)."""
    mock_conv = AsyncMock()
    mock_conv.__aenter__ = AsyncMock(return_value=mock_conv)
    mock_conv.__aexit__ = AsyncMock(return_value=None)
    mock_conv.get_response = AsyncMock(side_effect=list(get_response_values))
    if get_edit_value is not None:
        mock_conv.get_edit = AsyncMock(return_value=get_edit_value)
    return mock_conv


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
# botfather.delete_bot
# ---------------------------------------------------------------------------


async def test_delete_bot_success():
    bot_msg = _make_message([["@mybot_bot"]])
    options_msg = _make_message([["Bot Info", "Delete Bot"]])
    confirm_msg = _make_message([["Yes, I am totally sure."]])
    done_msg = MagicMock(text="Bot deleted!")

    mock_conv = _make_conv(
        bot_msg, confirm_msg, done_msg,
        get_edit_value=options_msg,
    )
    mock_client = MagicMock()
    mock_client.conversation.return_value = mock_conv

    await botfather.delete_bot(mock_client, "@mybot_bot")

    bot_msg.click.assert_awaited_once()
    options_msg.click.assert_awaited_once()
    confirm_msg.click.assert_awaited_once()


# ---------------------------------------------------------------------------
# NotificationService.setup_bot
# ---------------------------------------------------------------------------


async def test_setup_bot_success(db):
    pool, _ = _make_pool(me_id=111, me_username="alice")
    await _add_account(db)
    svc = NotificationService(db, NotificationTargetService(db, pool))

    with patch(
        "src.services.notification_service.botfather.create_bot",
        new_callable=AsyncMock,
        return_value="111111111:AABBCCDDEEFFaabbccddeeffAABBCCDDEEFF",
    ):
        bot = await svc.setup_bot()

    assert bot.tg_user_id == 111
    assert bot.bot_username == "leadhunter_alice_bot"
    assert bot.bot_id == 987654321
    pool.release_client.assert_awaited_once_with("+70001111111")

    saved = await db.get_notification_bot(111)
    assert saved is not None
    assert saved.bot_username == "leadhunter_alice_bot"


async def test_setup_bot_custom_prefix(db):
    pool, mock_client = _make_pool(me_id=222, me_username="bob")
    await _add_account(db)
    svc = NotificationService(
        db,
        NotificationTargetService(db, pool),
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
    mock_create.assert_awaited_once_with(mock_client, "Acme (bob)", "acme_bob_bot")


async def test_setup_bot_slug_truncated(db):
    long_username = "averylongusernamethatexceeds17"
    pool, _ = _make_pool(me_id=333, me_username=long_username)
    await _add_account(db)
    svc = NotificationService(db, NotificationTargetService(db, pool))

    with patch(
        "src.services.notification_service.botfather.create_bot",
        new_callable=AsyncMock,
        return_value="333333333:AABBCCDDEEFFaabbccddeeffAABBCCDDEEFF",
    ):
        bot = await svc.setup_bot()

    slug = long_username[:17]
    assert bot.bot_username == f"leadhunter_{slug}_bot"
    assert len(bot.bot_username) <= 32


async def test_setup_bot_no_client(db):
    pool = AsyncMock()
    pool.clients = {}
    pool.get_client_by_phone = AsyncMock(return_value=None)
    svc = NotificationService(db, NotificationTargetService(db, pool))

    with pytest.raises(RuntimeError, match="Primary-аккаунт"):
        await svc.setup_bot()


async def test_setup_bot_bot_id_none_if_entity_fails(db):
    me = MagicMock()
    me.id = 444
    me.username = "carol"

    mock_client = AsyncMock()
    mock_client.get_me = AsyncMock(return_value=me)
    mock_client.send_message = AsyncMock()
    mock_client.get_entity = AsyncMock(side_effect=Exception("peer not found"))

    pool = AsyncMock()
    pool.clients = {"+70001111111": mock_client}
    pool.get_client_by_phone = AsyncMock(return_value=(mock_client, "+70001111111"))
    pool.release_client = AsyncMock()

    await _add_account(db)
    svc = NotificationService(db, NotificationTargetService(db, pool))
    with patch(
        "src.services.notification_service.botfather.create_bot",
        new_callable=AsyncMock,
        return_value="444444444:AABBCCDDEEFFaabbccddeeffAABBCCDDEEFF",
    ):
        bot = await svc.setup_bot()

    assert bot.bot_id is None


# ---------------------------------------------------------------------------
# NotificationService.get_status
# ---------------------------------------------------------------------------


async def test_get_status_no_bot(db):
    pool, _ = _make_pool(me_id=555)
    await _add_account(db)
    svc = NotificationService(db, NotificationTargetService(db, pool))
    result = await svc.get_status()
    assert result is None


async def test_get_status_returns_bot(db):
    saved = NotificationBot(
        tg_user_id=666,
        tg_username="dave",
        bot_id=111,
        bot_username="leadhunter_dave_bot",
        bot_token="666666666:AABBCCDDEEFFaabbccddeeffAABBCCDDEEFF",
    )
    await db.save_notification_bot(saved)

    pool, _ = _make_pool(me_id=666, me_username="dave")
    await _add_account(db)
    svc = NotificationService(db, NotificationTargetService(db, pool))
    result = await svc.get_status()

    assert result is not None
    assert result.tg_user_id == 666
    assert result.bot_username == "leadhunter_dave_bot"


# ---------------------------------------------------------------------------
# NotificationService.teardown_bot
# ---------------------------------------------------------------------------


async def test_teardown_bot_success(db):
    saved = NotificationBot(
        tg_user_id=777,
        tg_username="eve",
        bot_id=222,
        bot_username="leadhunter_eve_bot",
        bot_token="777777777:AABBCCDDEEFFaabbccddeeffAABBCCDDEEFF",
    )
    await db.save_notification_bot(saved)

    pool, _ = _make_pool(me_id=777, me_username="eve")
    await _add_account(db)
    svc = NotificationService(db, NotificationTargetService(db, pool))

    with patch(
        "src.services.notification_service.botfather.delete_bot",
        new_callable=AsyncMock,
    ):
        await svc.teardown_bot()

    assert await db.get_notification_bot(777) is None
    pool.release_client.assert_awaited_once_with("+70001111111")


async def test_teardown_bot_no_bot_raises(db):
    pool, _ = _make_pool(me_id=888)
    await _add_account(db)
    svc = NotificationService(db, NotificationTargetService(db, pool))

    with pytest.raises(RuntimeError, match="No notification bot"):
        await svc.teardown_bot()

    pool.release_client.assert_awaited_once_with("+70001111111")


async def test_notifier_uses_primary_account_by_default(db):
    pool, mock_client = _make_pool()
    await _add_account(db)

    notifier = Notifier(NotificationTargetService(db, pool), admin_chat_id=123456)
    sent = await notifier.notify("hello")

    assert sent is True
    pool.get_client_by_phone.assert_awaited_once_with("+70001111111")
    mock_client.send_message.assert_awaited_once_with(123456, "hello")
    pool.release_client.assert_awaited_once_with("+70001111111")


async def test_notifier_does_not_fallback_from_selected_account(db):
    _, primary_client = _make_pool(phone="+70001111111")

    pool = AsyncMock()
    pool.clients = {"+70001111111": primary_client}
    pool.get_client_by_phone = AsyncMock(return_value=None)
    pool.release_client = AsyncMock()

    await _add_account(db, phone="+70001111111", is_primary=True)
    await _add_account(db, phone="+70002222222", is_primary=False)
    await db.set_setting("notification_account_phone", "+70002222222")

    notifier = Notifier(NotificationTargetService(db, pool), admin_chat_id=123456)
    sent = await notifier.notify("hello")

    assert sent is False
    primary_client.send_message.assert_not_awaited()
    pool.get_client_by_phone.assert_not_awaited()


async def test_notification_service_uses_selected_account(db):
    _, primary_client = _make_pool(
        me_id=111,
        me_username="primary",
        phone="+70001111111",
    )
    selected_me = MagicMock()
    selected_me.id = 222
    selected_me.username = "selected"
    selected_entity = MagicMock()
    selected_entity.id = 222333444
    selected_client = AsyncMock()
    selected_client.get_me = AsyncMock(return_value=selected_me)
    selected_client.send_message = AsyncMock()
    selected_client.get_entity = AsyncMock(return_value=selected_entity)

    pool = AsyncMock()
    pool.clients = {
        "+70001111111": primary_client,
        "+70002222222": selected_client,
    }
    pool.get_client_by_phone = AsyncMock(
        side_effect=lambda phone: (
            (selected_client, phone) if phone == "+70002222222" else (primary_client, phone)
        )
    )
    pool.release_client = AsyncMock()

    await _add_account(db, phone="+70001111111", is_primary=True)
    await _add_account(db, phone="+70002222222", is_primary=False)
    await db.set_setting("notification_account_phone", "+70002222222")

    svc = NotificationService(db, NotificationTargetService(db, pool))
    with patch(
        "src.services.notification_service.botfather.create_bot",
        new_callable=AsyncMock,
        return_value="222222222:AABBCCDDEEFFaabbccddeeffAABBCCDDEEFF",
    ):
        bot = await svc.setup_bot()

    assert bot.tg_user_id == 222
    pool.get_client_by_phone.assert_awaited_once_with("+70002222222")
