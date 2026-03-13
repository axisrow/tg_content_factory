from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from telethon.errors import FloodWaitError

from tests.helpers import FakeCliTelethonClient


def _channel_entity(
    channel_id: int,
    *,
    title: str = "Test Channel",
    username: str | None = "test_chan",
):
    return SimpleNamespace(
        id=channel_id,
        title=title,
        username=username,
        broadcast=True,
        megagroup=False,
        gigagroup=False,
        forum=False,
        monoforum=False,
        scam=False,
        fake=False,
        restricted=False,
    )


@pytest.mark.asyncio
async def test_pool_initialize_no_accounts(real_pool_harness_factory):
    harness = real_pool_harness_factory()
    await harness.initialize_connected_accounts()
    assert len(harness.pool.clients) == 0


@pytest.mark.asyncio
async def test_pool_get_available_no_clients(real_pool_harness_factory):
    harness = real_pool_harness_factory()
    result = await harness.pool.get_available_client()
    assert result is None


@pytest.mark.asyncio
async def test_stats_availability_no_connected_active(real_pool_harness_factory):
    harness = real_pool_harness_factory()
    await harness.add_account("+70000000001", session_string="s1", is_primary=True)

    availability = await harness.pool.get_stats_availability()
    assert availability.state == "no_connected_active"
    assert availability.retry_after_sec is None


@pytest.mark.asyncio
async def test_stats_availability_all_flooded(real_pool_harness_factory):
    harness = real_pool_harness_factory()
    harness.queue_cli_client(phone="+70000000002", client=FakeCliTelethonClient())
    await harness.add_account("+70000000002", session_string="s2", is_primary=True)
    await harness.initialize_connected_accounts()

    until = datetime.now(timezone.utc) + timedelta(seconds=120)
    await harness.db.update_account_flood("+70000000002", until)

    availability = await harness.pool.get_stats_availability()
    assert availability.state == "all_flooded"
    assert availability.retry_after_sec is not None
    assert availability.retry_after_sec >= 1
    assert availability.next_available_at_utc is not None


@pytest.mark.asyncio
async def test_pool_report_flood(real_pool_harness_factory):
    harness = real_pool_harness_factory()
    await harness.add_account("+71234567890", session_string="session1", is_primary=True)

    await harness.pool.report_flood("+71234567890", 120)

    accounts = await harness.db.get_accounts()
    assert accounts[0].flood_wait_until is not None


@pytest.mark.asyncio
async def test_pool_disconnect_all_releases_active_leases(real_pool_harness_factory):
    harness = real_pool_harness_factory()
    cli_client = harness.queue_cli_client(
        phone="+71234567890",
        client=FakeCliTelethonClient(),
    )
    await harness.add_account("+71234567890", session_string="session1", is_primary=True)
    await harness.initialize_connected_accounts()

    acquired = await harness.pool.get_client_by_phone("+71234567890")
    assert acquired is not None

    await harness.pool.disconnect_all()

    assert len(harness.pool.clients) == 0
    # disconnect_all disconnects the persistent pool session exactly once
    assert cli_client.disconnect.await_count == 1


@pytest.mark.asyncio
async def test_pool_skips_flooded_returns_next(real_pool_harness_factory):
    harness = real_pool_harness_factory()
    harness.queue_cli_client(phone="+70001111111", client=FakeCliTelethonClient())
    harness.queue_cli_client(phone="+70002222222", client=FakeCliTelethonClient())
    await harness.add_account("+70001111111", session_string="s1", is_primary=True)
    await harness.add_account("+70002222222", session_string="s2")
    await harness.initialize_connected_accounts()

    await harness.pool.report_flood("+70001111111", 120)

    result = await harness.pool.get_available_client()
    assert result is not None
    assert result[1] == "+70002222222"


@pytest.mark.asyncio
async def test_resolve_channel_returns_raw_id(real_pool_harness_factory):
    harness = real_pool_harness_factory()
    client = harness.queue_cli_client(
        phone="+71234567890",
        client=FakeCliTelethonClient(
            entity_resolver=lambda _peer: _channel_entity(1970788983),
        ),
    )
    await harness.add_account("+71234567890", session_string="session1", is_primary=True)
    await harness.initialize_connected_accounts()

    result = await harness.pool.resolve_channel("@test_chan")

    assert result is not None
    assert result["channel_id"] == 1970788983
    assert result["title"] == "Test Channel"
    assert result["username"] == "test_chan"
    client.get_entity.assert_awaited_with("@test_chan")


@pytest.mark.asyncio
async def test_resolve_channel_no_client_raises(real_pool_harness_factory):
    harness = real_pool_harness_factory()

    with pytest.raises(RuntimeError, match="no_client"):
        await harness.pool.resolve_channel("@test_chan")


@pytest.mark.asyncio
async def test_resolve_channel_entity_not_found_returns_none(real_pool_harness_factory):
    harness = real_pool_harness_factory()
    harness.queue_cli_client(
        phone="+71234567890",
        client=FakeCliTelethonClient(
            entity_resolver=lambda _peer: ValueError("No user has ..."),
        ),
    )
    await harness.add_account("+71234567890", session_string="session1", is_primary=True)
    await harness.initialize_connected_accounts()

    result = await harness.pool.resolve_channel("@nonexistent")
    assert result is None


@pytest.mark.asyncio
async def test_resolve_channel_flood_rotates(real_pool_harness_factory):
    harness = real_pool_harness_factory()
    flood_err = FloodWaitError(request=None, capture=0)
    flood_err.seconds = 60
    harness.queue_cli_client(
        phone="+70001111111",
        client=FakeCliTelethonClient(
            entity_resolver=lambda _peer: flood_err,
        ),
    )
    harness.queue_cli_client(
        phone="+70002222222",
        client=FakeCliTelethonClient(
            entity_resolver=lambda _peer: _channel_entity(123456, title="Test", username="test"),
        ),
    )
    await harness.add_account("+70001111111", session_string="s1", is_primary=True)
    await harness.add_account("+70002222222", session_string="s2")
    await harness.initialize_connected_accounts()

    result = await harness.pool.resolve_channel("@test")

    assert result is not None
    assert result["channel_id"] == 123456


@pytest.mark.asyncio
async def test_resolve_channel_user_returns_none(real_pool_harness_factory):
    harness = real_pool_harness_factory()
    harness.queue_cli_client(
        phone="+71234567890",
        client=FakeCliTelethonClient(
            entity_resolver=lambda _peer: SimpleNamespace(id=999, first_name="Alex"),
        ),
    )
    await harness.add_account("+71234567890", session_string="session1", is_primary=True)
    await harness.initialize_connected_accounts()

    result = await harness.pool.resolve_channel("@AlexP87")
    assert result is None


@pytest.mark.asyncio
async def test_get_premium_client_fallback_when_in_use(real_pool_harness_factory):
    harness = real_pool_harness_factory()
    harness.queue_cli_client(
        phone="+70001111111",
        client=FakeCliTelethonClient(me=SimpleNamespace(premium=True)),
    )
    await harness.add_account(
        "+70001111111",
        session_string="s1",
        is_primary=True,
        is_premium=True,
    )
    await harness.initialize_connected_accounts()

    first = await harness.pool.get_premium_client()
    second = await harness.pool.get_premium_client()

    assert first is not None
    assert second is not None
    assert first[1] == "+70001111111"
    assert second[1] == "+70001111111"


@pytest.mark.asyncio
async def test_resolve_channel_strips_post_id_from_url(real_pool_harness_factory):
    harness = real_pool_harness_factory()
    client = harness.queue_cli_client(
        phone="+71234567890",
        client=FakeCliTelethonClient(
            entity_resolver=lambda _peer: _channel_entity(
                555,
                title="Arms Channel",
                username="ruarms_com",
            ),
        ),
    )
    await harness.add_account("+71234567890", session_string="session1", is_primary=True)
    await harness.initialize_connected_accounts()

    result = await harness.pool.resolve_channel("https://t.me/ruarms_com/24")

    assert result is not None
    assert result["channel_id"] == 555
    client.get_entity.assert_awaited_with("https://t.me/ruarms_com")
