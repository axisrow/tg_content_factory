from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.helpers import FakeCliTelethonClient
from tg_agent.models import NotificationBot
from tg_agent.services.notification_service import NotificationService
from tg_agent.services.notification_target_service import NotificationTargetService
from tg_agent.telegram.notifier import Notifier
from tg_shared import botfather

pytestmark = pytest.mark.native_backend_allowed



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
        "tg_agent.services.notification_service.botfather.create_bot",
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
async def test_setup_bot_start_send_timeout_is_unconfirmed_not_fatal(db, real_pool_harness_factory, caplog):
    """Orphaned-request guard (issues #1267/#1268/#1260, class #1239).

    The ``/start`` sent to the freshly created bot goes through
    ``asyncio.wait_for(client.send_message(...), timeout=30.0)``. The timeout is
    kept (with ``connection_retries=None`` a dead-connection send would hang
    forever and freeze bot setup), but a client-side timeout cancels only the
    local wait — the ``/start`` may already have reached Telegram. So a timeout
    here is an UNCONFIRMED delivery, logged as such, and setup PROCEEDS (a
    duplicate ``/start`` is idempotent and harmless, so it is never re-sent).
    This mirrors the #1239/#1253 fix for ``publish_service``.

    Mutation-proven: narrowing the ``except asyncio.TimeoutError`` back into a
    generic ``except Exception`` still passes, but the log message no longer
    says "unconfirmed" — the caplog assertion below fails.
    """
    harness = real_pool_harness_factory()
    # Same wiring as _connect_notification_account, but the bot /start send times
    # out. get_me / get_entity (read-only, reversible) keep working normally.
    harness.queue_cli_client(
        phone="+70001111111",
        client=FakeCliTelethonClient(
            me=SimpleNamespace(id=555, username="dave"),
            entity_resolver=lambda _peer: SimpleNamespace(id=987654321),
            send_message_side_effect=asyncio.TimeoutError(),
        ),
    )
    harness.queue_native_client(
        session_string="session-1",
        client=FakeCliTelethonClient(
            me=SimpleNamespace(id=555, username="dave"),
            entity_resolver=lambda _peer: SimpleNamespace(id=987654321),
            send_message_side_effect=asyncio.TimeoutError(),
        ),
    )
    await harness.add_account(phone="+70001111111", session_string="session-1", is_primary=True)
    await harness.initialize_connected_accounts()

    svc = NotificationService(db, NotificationTargetService(db, harness.pool))

    with caplog.at_level("WARNING", logger="tg_agent.services.notification_service"):
        with patch(
            "tg_agent.services.notification_service.botfather.create_bot",
            new_callable=AsyncMock,
            return_value="555555555:AABBCCDDEEFFaabbccddeeffAABBCCDDEEFF",
        ):
            bot = await svc.setup_bot()

    # Setup completed despite the /start timeout — the bot is created and saved.
    assert bot.tg_user_id == 555
    assert bot.bot_username == "leadhunter_dave_bot"
    saved = await db.get_notification_bot(555)
    assert saved is not None

    # The timeout was logged as an unconfirmed delivery, distinct from a failure.
    assert any(
        "unconfirmed" in r.message.lower() and "/start" in r.message.lower() for r in caplog.records
    ), "a /start send timeout must be logged as an unconfirmed delivery, not a plain failure"


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
        "tg_agent.services.notification_service.botfather.create_bot",
        new_callable=AsyncMock,
        return_value="222222222:AABBCCDDEEFFaabbccddeeffAABBCCDDEEFF",
    ) as mock_create:
        bot = await svc.setup_bot()

    assert bot.bot_username == "acme_bob_bot"
    created = mock_create.await_args
    assert created is not None
    created_client = created.args[0]
    # use_client() now returns pool session (reuses persistent connection)
    pool_session = harness.pool.clients["+70001111111"]
    assert created_client.raw_client is pool_session.raw_client
    assert created.args[1:] == ("Acme (bob)", "acme_bob_bot")


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
        "tg_agent.services.notification_service.botfather.create_bot",
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
        "tg_agent.services.notification_service.botfather.create_bot",
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


async def _connect_counted_account(harness, phone: str, me_id: int, *, is_primary: bool = False):
    """Connect an account and return its pool client, whose `get_me` awaits are counted."""
    client = FakeCliTelethonClient(me=SimpleNamespace(id=me_id, username=None))
    harness.queue_cli_client(phone=phone, client=client)
    await harness.add_account(phone=phone, session_string=f"session-{me_id}", is_primary=is_primary)
    await harness.initialize_connected_accounts()
    return client


@pytest.mark.anyio
async def test_get_status_asks_telegram_for_the_account_once(db, real_pool_harness_factory):
    """The worker reads the bot on every heartbeat; an account's user id never changes.

    Found live (#1868): a `get_me` on each read used up the notification
    account's hourly flood budget and locked the account out for about an hour.
    """
    harness = real_pool_harness_factory()
    client = await _connect_counted_account(harness, "+70001111111", 555, is_primary=True)
    target = NotificationTargetService(db, harness.pool)
    asked_before = client.get_me.await_count

    for _ in range(3):  # three heartbeats, each with its own NotificationService, as in the worker
        assert await NotificationService(db, target).get_status() is None

    assert client.get_me.await_count - asked_before == 1


@pytest.mark.anyio
async def test_get_status_follows_a_change_of_the_notification_account(db, real_pool_harness_factory):
    for user_id in (555, 777):
        await db.save_notification_bot(NotificationBot(
            tg_user_id=user_id, tg_username=None, bot_id=user_id + 1, bot_username=f"bot_{user_id}",
            bot_token=f"{user_id}:AABBCCDDEEFFaabbccddeeffAABBCCDDEEFF",
        ))
    harness = real_pool_harness_factory()
    await _connect_counted_account(harness, "+70001111111", 555, is_primary=True)
    await _connect_counted_account(harness, "+70002222222", 777)
    target = NotificationTargetService(db, harness.pool)
    svc = NotificationService(db, target)

    first = await svc.get_status()
    await target.set_configured_phone("+70002222222")
    second = await svc.get_status()

    assert first is not None and second is not None
    assert (first.tg_user_id, second.tg_user_id) == (555, 777)


@pytest.mark.anyio
async def test_get_status_refuses_an_unavailable_account_it_has_seen(db, real_pool_harness_factory):
    """A remembered user id must not make a disconnected account look usable."""
    harness = real_pool_harness_factory()
    await _connect_counted_account(harness, "+70001111111", 555, is_primary=True)
    svc = NotificationService(db, NotificationTargetService(db, harness.pool))
    await svc.get_status()
    harness.pool.clients.pop("+70001111111")

    with pytest.raises(RuntimeError, match="не подключён"):
        await svc.get_status()


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
        "tg_agent.services.notification_service.botfather.delete_bot",
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
            "tg_agent.services.notification_service.botfather.delete_bot",
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
async def test_teardown_bot_idempotent_after_orphan_cleans_db(db, real_pool_harness_factory):
    """RED→GREEN (#1085): a repeat teardown must clean the orphan DB row.

    Scenario from the issue: the first teardown deleted the live bot via
    BotFather, then the DB-delete failed (#1041) — leaving an orphan row. The
    bot is now gone from Telegram. A repeat ``teardown_bot`` calls
    ``botfather.delete_bot`` again, which can no longer find the bot in
    ``/mybots`` and raises ``BotNotFoundError``.

    Before the fix that error propagated *before* the DB-delete ran, so the
    orphan row could never be removed via the normal flow. After the fix the
    "bot missing in Telegram" signal is treated as "TG step already done" and
    teardown proceeds to delete the DB row — making the operation idempotent.
    """
    saved = NotificationBot(
        tg_user_id=1010,
        tg_username="grace",
        bot_id=444,
        bot_username="leadhunter_grace_bot",
        bot_token="101010101:AABBCCDDEEFFaabbccddeeffAABBCCDDEEFF",
    )
    await db.save_notification_bot(saved)

    harness = real_pool_harness_factory()
    await _connect_notification_account(
        harness,
        phone="+70001111111",
        session_string="session-1",
        me_id=1010,
        me_username="grace",
        is_primary=True,
    )
    svc = NotificationService(db, NotificationTargetService(db, harness.pool))

    # The bot was already deleted in Telegram on the first (failed) teardown, so
    # BotFather can't find it in /mybots — signalled by BotNotFoundError.
    with patch(
        "tg_agent.services.notification_service.botfather.delete_bot",
        new_callable=AsyncMock,
        side_effect=botfather.BotNotFoundError("@leadhunter_grace_bot not in /mybots"),
    ):
        await svc.teardown_bot()

    # The orphan row must be gone — teardown is idempotent.
    assert await db.get_notification_bot(1010) is None


@pytest.mark.anyio
async def test_teardown_bot_real_botfather_error_does_not_delete_db(db, real_pool_harness_factory):
    """GUARD (#1085): a genuine BotFather failure must NOT delete the DB row.

    The idempotent path only forgives the specific "bot missing in Telegram"
    signal (``BotNotFoundError``). Any *other* BotFather failure means the live
    bot may still exist (the irreversible TG delete did NOT happen), so we must
    keep the DB row and propagate the error — never run the DB-delete after a
    failed TG-delete. This guards against over-broadening the forgiving branch.
    """
    saved = NotificationBot(
        tg_user_id=1111,
        tg_username="heidi",
        bot_id=555,
        bot_username="leadhunter_heidi_bot",
        bot_token="111111222:AABBCCDDEEFFaabbccddeeffAABBCCDDEEFF",
    )
    await db.save_notification_bot(saved)

    harness = real_pool_harness_factory()
    await _connect_notification_account(
        harness,
        phone="+70001111111",
        session_string="session-1",
        me_id=1111,
        me_username="heidi",
        is_primary=True,
    )
    svc = NotificationService(db, NotificationTargetService(db, harness.pool))

    with patch(
        "tg_agent.services.notification_service.botfather.delete_bot",
        new_callable=AsyncMock,
        side_effect=RuntimeError("BotFather: Sorry, something went wrong"),
    ):
        with pytest.raises(RuntimeError, match="something went wrong"):
            await svc.teardown_bot()

    # DB row must survive — the bot might still be live in Telegram.
    assert await db.get_notification_bot(1111) is not None


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
        "tg_agent.services.notification_service.botfather.create_bot",
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
    with patch("tg_agent.telegram.notifier.Notifier", return_value=notifier) as mock_notifier:
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
    with patch("tg_agent.telegram.notifier.Notifier", return_value=notifier):
        await svc.send_notification("   ")
    sent = notifier.notify.await_args.args[0]
    assert sent.strip() != ""


@pytest.mark.anyio
async def test_send_notification_raises_on_failure():
    svc = NotificationService(MagicMock(), MagicMock())
    notifier = MagicMock()
    notifier.notify = AsyncMock(return_value=False)
    with patch("tg_agent.telegram.notifier.Notifier", return_value=notifier):
        with pytest.raises(RuntimeError, match="notification_test_failed"):
            await svc.send_notification("boom")
