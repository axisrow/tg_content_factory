from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from telethon.errors import FloodWaitError
from telethon_cli.errors import CLIError

from src.services.notification_target_service import NotificationTargetService
from src.telegram.backends import TelegramTransportSession
from src.telegram.flood_wait import HandledFloodWaitError
from tests.helpers import FakeCliTelethonClient


@pytest.mark.asyncio
async def test_initialize_uses_runtime_backend_without_persistent_clients(
    real_pool_harness_factory,
):
    harness = real_pool_harness_factory()
    cli_client = harness.queue_cli_client(
        phone="+70000000001",
        client=FakeCliTelethonClient(
            me=MagicMock(premium=True),
        ),
    )

    await harness.add_account(
        phone="+70000000001",
        session_string="session-1",
        is_primary=True,
    )
    await harness.initialize_connected_accounts()

    assert "+70000000001" in harness.pool.clients
    # After initialize(), the session is kept alive in the pool (persistent connection)
    assert harness.pool._direct_session("+70000000001") is not None
    accounts = await harness.db.get_accounts()
    assert accounts[0].is_premium is True
    cli_client.disconnect.assert_not_awaited()  # persistent; no disconnect during initialize
    assert len(harness.telethon_cli_spy.created) == 1


@pytest.mark.asyncio
async def test_add_client_supports_session_override_before_db_write(
    real_pool_harness_factory,
):
    harness = real_pool_harness_factory()
    cli_client = harness.queue_cli_client(
        client=FakeCliTelethonClient(),
    )

    await harness.auth_connect_account("+79990000001", session_string="session-override")
    acquired = await harness.pool.get_client_by_phone("+79990000001")

    assert acquired is not None
    session, phone = acquired
    assert phone == "+79990000001"
    # add_client keeps the connection alive; get_client_by_phone reuses the same session
    assert session.raw_client is cli_client
    assert "+79990000001" in harness.pool.clients
    cli_client.disconnect.assert_not_awaited()  # persistent session; no disconnect on add

    await harness.pool.release_client("+79990000001")
    cli_client.disconnect.assert_not_awaited()  # release doesn't disconnect persistent sessions
    assert len(harness.telethon_cli_spy.created) == 1


@pytest.mark.asyncio
async def test_shared_lease_keeps_phone_busy_until_last_release(
    real_pool_harness_factory,
):
    harness = real_pool_harness_factory()
    harness.queue_cli_client(phone="+70000000001", client=FakeCliTelethonClient())
    harness.queue_cli_client(phone="+70000000002", client=FakeCliTelethonClient())

    await harness.add_account("+70000000001", session_string="session-a", is_primary=True)
    await harness.add_account("+70000000002", session_string="session-b")
    await harness.initialize_connected_accounts()

    first = await harness.pool.get_client_by_phone("+70000000001")
    second = await harness.pool.get_client_by_phone("+70000000001")

    assert first is not None
    assert second is not None
    assert first[1] == "+70000000001"
    assert second[1] == "+70000000001"

    await harness.pool.release_client("+70000000001")
    third = await harness.pool.get_available_client()

    assert third is not None
    assert third[1] == "+70000000002"


@pytest.mark.asyncio
async def test_premium_client_does_not_disable_generic_flood_reporting_on_persistent_session(
    real_pool_harness_factory,
):
    harness = real_pool_harness_factory()
    cli_client = harness.queue_cli_client(
        phone="+70000000001",
        client=FakeCliTelethonClient(me=MagicMock(premium=True)),
    )
    await harness.add_account(
        "+70000000001",
        session_string="session-a",
        is_primary=True,
        is_premium=True,
    )
    await harness.initialize_connected_accounts()
    persistent_session = harness.pool.clients["+70000000001"]

    acquired = await harness.pool.get_premium_client()

    assert acquired is not None
    premium_session, phone = acquired
    assert phone == "+70000000001"
    assert premium_session is not persistent_session
    assert premium_session.raw_client is cli_client
    assert premium_session._report_flood_wait is False
    assert persistent_session._report_flood_wait is True

    await harness.pool.release_client("+70000000001")


@pytest.mark.asyncio
@pytest.mark.native_backend_allowed
async def test_native_client_by_phone_returns_flood_aware_session(
    real_pool_harness_factory,
):
    err = FloodWaitError(request=None, capture=0)
    err.seconds = 11
    harness = real_pool_harness_factory()
    harness.queue_cli_client(phone="+70000000001", client=FakeCliTelethonClient())
    harness.queue_native_client(
        session_string="session-native",
        client=FakeCliTelethonClient(entity_resolver=lambda _arg: err),
    )
    await harness.add_account(
        "+70000000001",
        session_string="session-native",
        is_primary=True,
    )
    await harness.initialize_connected_accounts()

    result = await harness.pool.get_native_client_by_phone("+70000000001")
    assert result is not None
    session, phone = result

    assert phone == "+70000000001"
    assert isinstance(session, TelegramTransportSession)
    with pytest.raises(HandledFloodWaitError):
        await session.get_entity("@flooded")

    accounts = await harness.db.get_accounts()
    assert accounts[0].flood_wait_until is not None


@pytest.mark.asyncio
@pytest.mark.native_backend_allowed
async def test_auto_mode_falls_back_to_native_when_cli_acquire_fails(
    real_pool_harness_factory,
):
    harness = real_pool_harness_factory()

    def _raise_cli(_namespace):
        raise CLIError("telethon-cli unavailable")

    harness.telethon_cli_spy.factory = _raise_cli
    native_client = harness.queue_native_client(
        session_string="session-auto",
        client=FakeCliTelethonClient(),
    )

    await harness.add_account(
        "+70000000001",
        session_string="session-auto",
        is_primary=True,
    )
    await harness.initialize_connected_accounts()

    assert "+70000000001" in harness.pool.clients
    assert harness.native_auth_spy.created == [("session-auto", native_client)]


@pytest.mark.asyncio
@pytest.mark.native_backend_allowed
async def test_auto_mode_falls_back_to_native_for_subprocess_transport(
    real_pool_harness_factory,
):
    harness = real_pool_harness_factory(cli_transport="subprocess")
    native_client = harness.queue_native_client(
        session_string="session-subprocess",
        client=FakeCliTelethonClient(),
    )

    await harness.add_account(
        "+70000000001",
        session_string="session-subprocess",
        is_primary=True,
    )
    await harness.initialize_connected_accounts()

    assert "+70000000001" in harness.pool.clients
    assert harness.native_auth_spy.created == [("session-subprocess", native_client)]


@pytest.mark.asyncio
@pytest.mark.native_backend_allowed
async def test_get_native_client_by_phone_uses_native_backend_and_disconnects(
    real_pool_harness_factory,
):
    harness = real_pool_harness_factory()
    harness.queue_cli_client(phone="+70000000001", client=FakeCliTelethonClient())
    native_client = harness.queue_native_client(
        session_string="session-native",
        client=FakeCliTelethonClient(),
    )

    await harness.add_account(
        "+70000000001",
        session_string="session-native",
        is_primary=True,
    )
    await harness.initialize_connected_accounts()

    acquired = await harness.pool.get_native_client_by_phone("+70000000001")

    assert acquired is not None
    session, phone = acquired
    assert isinstance(session, TelegramTransportSession)
    assert session.raw_client is native_client
    assert phone == "+70000000001"

    await harness.pool.release_client("+70000000001")
    native_client.disconnect.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.native_backend_allowed
async def test_notification_target_uses_real_pool_native_getter(
    real_pool_harness_factory,
):
    harness = real_pool_harness_factory()
    harness.queue_cli_client(phone="+70000000001", client=FakeCliTelethonClient())
    native_client = harness.queue_native_client(
        session_string="session-1",
        client=FakeCliTelethonClient(),
    )

    await harness.add_account(
        phone="+70000000001",
        session_string="session-1",
        is_primary=True,
    )
    await harness.initialize_connected_accounts()

    service = NotificationTargetService(harness.db, harness.pool)

    async with service.use_client() as (acquired_client, phone):
        assert isinstance(acquired_client, TelegramTransportSession)
        assert acquired_client.raw_client is native_client
        assert phone == "+70000000001"

    native_client.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_acquire_reconnects_disconnected_client(
    real_pool_harness_factory,
):
    """When cached client is disconnected, _acquire_from_lease auto-reconnects it."""
    harness = real_pool_harness_factory()
    cli_client = harness.queue_cli_client(
        phone="+70000000001",
        client=FakeCliTelethonClient(),
    )
    await harness.add_account("+70000000001", session_string="s1", is_primary=True)
    await harness.initialize_connected_accounts()

    # Simulate connection drop: is_connected() is called once in _acquire_from_lease
    cli_client.is_connected = MagicMock(return_value=False)

    acquired = await harness.pool.get_client_by_phone("+70000000001")

    assert acquired is not None
    _, phone = acquired
    assert phone == "+70000000001"
    cli_client.connect.assert_awaited()


@pytest.mark.asyncio
async def test_acquire_falls_back_to_backend_when_reconnect_fails(
    real_pool_harness_factory,
):
    """When auto-reconnect fails, pool falls back to backend router for a fresh client."""
    harness = real_pool_harness_factory()
    cli_client = harness.queue_cli_client(
        phone="+70000000001",
        client=FakeCliTelethonClient(),
    )
    await harness.add_account("+70000000001", session_string="s1", is_primary=True)
    await harness.initialize_connected_accounts()

    # Simulate: disconnected + reconnect raises
    cli_client.is_connected = MagicMock(return_value=False)
    cli_client.connect = AsyncMock(side_effect=ConnectionError("reconnect failed"))

    # Queue a fresh client for the backend fallback
    fresh_client = harness.queue_cli_client(
        phone="+70000000001",
        client=FakeCliTelethonClient(),
    )

    acquired = await harness.pool.get_client_by_phone("+70000000001")

    assert acquired is not None
    _, phone = acquired
    assert phone == "+70000000001"
    # The dead client was evicted from self.clients
    assert harness.pool.clients["+70000000001"].raw_client is fresh_client
