from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from telethon.errors import FloodWaitError

from src.models import Account
from src.telegram.client_pool import ClientPool


@pytest.mark.asyncio
async def test_pool_initialize_no_accounts(db):
    auth = MagicMock()
    pool = ClientPool(auth, db)
    await pool.initialize()
    assert len(pool.clients) == 0


@pytest.mark.asyncio
async def test_pool_get_available_no_clients(db):
    auth = MagicMock()
    pool = ClientPool(auth, db)
    result = await pool.get_available_client()
    assert result is None


@pytest.mark.asyncio
async def test_stats_availability_no_connected_active(db):
    await db.add_account(Account(phone="+70000000001", session_string="s1", is_primary=True))
    auth = MagicMock()
    pool = ClientPool(auth, db)

    availability = await pool.get_stats_availability()
    assert availability.state == "no_connected_active"
    assert availability.retry_after_sec is None


@pytest.mark.asyncio
async def test_stats_availability_all_flooded(db):
    acc = Account(phone="+70000000002", session_string="s2", is_primary=True)
    await db.add_account(acc)

    auth = MagicMock()
    pool = ClientPool(auth, db)
    pool.clients["+70000000002"] = AsyncMock()

    until = datetime.now(timezone.utc) + timedelta(seconds=120)
    await db.update_account_flood("+70000000002", until)

    availability = await pool.get_stats_availability()
    assert availability.state == "all_flooded"
    assert availability.retry_after_sec is not None
    assert availability.retry_after_sec >= 1
    assert availability.next_available_at_utc is not None


@pytest.mark.asyncio
async def test_pool_report_flood(db):
    acc = Account(phone="+71234567890", session_string="session1", is_primary=True)
    await db.add_account(acc)

    auth = MagicMock()
    pool = ClientPool(auth, db)
    await pool.report_flood("+71234567890", 120)

    accounts = await db.get_accounts()
    assert accounts[0].flood_wait_until is not None


@pytest.mark.asyncio
async def test_pool_disconnect_all(db):
    auth = MagicMock()
    pool = ClientPool(auth, db)

    mock_client = AsyncMock()
    pool.clients["+71234567890"] = mock_client

    await pool.disconnect_all()
    assert len(pool.clients) == 0
    mock_client.disconnect.assert_awaited_once()


@pytest.mark.asyncio
async def test_pool_skips_flooded_returns_next(db):
    acc1 = Account(phone="+70001111111", session_string="s1", is_primary=True)
    acc2 = Account(phone="+70002222222", session_string="s2")
    await db.add_account(acc1)
    await db.add_account(acc2)

    auth = MagicMock()
    pool = ClientPool(auth, db)
    pool.clients["+70001111111"] = AsyncMock()
    pool.clients["+70002222222"] = AsyncMock()

    await pool.report_flood("+70001111111", 120)

    result = await pool.get_available_client()
    assert result is not None
    client, phone = result
    assert phone == "+70002222222"


@pytest.mark.asyncio
async def test_resolve_channel_returns_raw_id(db):
    """resolve_channel returns entity.id as-is (raw positive int)."""
    acc = Account(phone="+71234567890", session_string="session1", is_primary=True)
    await db.add_account(acc)

    mock_entity = MagicMock()
    mock_entity.id = 1970788983
    mock_entity.title = "Test Channel"
    mock_entity.username = "test_chan"

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=mock_entity)

    auth = MagicMock()
    pool = ClientPool(auth, db)
    pool.clients["+71234567890"] = mock_client

    result = await pool.resolve_channel("@test_chan")
    assert result is not None
    assert result["channel_id"] == 1970788983
    assert result["title"] == "Test Channel"
    assert result["username"] == "test_chan"


@pytest.mark.asyncio
async def test_resolve_channel_no_client_raises(db):
    """resolve_channel raises RuntimeError('no_client') when pool is empty."""
    auth = MagicMock()
    pool = ClientPool(auth, db)

    with pytest.raises(RuntimeError, match="no_client"):
        await pool.resolve_channel("@test_chan")


@pytest.mark.asyncio
async def test_resolve_channel_entity_not_found_returns_none(db):
    """resolve_channel returns None when get_entity raises ValueError."""
    acc = Account(phone="+71234567890", session_string="session1", is_primary=True)
    await db.add_account(acc)

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(side_effect=ValueError("No user has ..."))

    auth = MagicMock()
    pool = ClientPool(auth, db)
    pool.clients["+71234567890"] = mock_client

    result = await pool.resolve_channel("@nonexistent")
    assert result is None


@pytest.mark.asyncio
async def test_resolve_channel_flood_rotates(db):
    """resolve_channel rotates to another client on FloodWaitError."""
    acc1 = Account(phone="+70001111111", session_string="s1", is_primary=True)
    acc2 = Account(phone="+70002222222", session_string="s2")
    await db.add_account(acc1)
    await db.add_account(acc2)

    mock_entity = MagicMock()
    mock_entity.id = 123456
    mock_entity.title = "Test"
    mock_entity.username = "test"
    mock_entity.broadcast = True
    mock_entity.megagroup = False

    flood_err = FloodWaitError(request=None, capture=0)
    flood_err.seconds = 60

    mock_client1 = AsyncMock()
    mock_client1.get_entity = AsyncMock(side_effect=flood_err)

    mock_client2 = AsyncMock()
    mock_client2.get_entity = AsyncMock(return_value=mock_entity)

    auth = MagicMock()
    pool = ClientPool(auth, db)
    pool.clients["+70001111111"] = mock_client1
    pool.clients["+70002222222"] = mock_client2

    result = await pool.resolve_channel("@test")
    assert result is not None
    assert result["channel_id"] == 123456


@pytest.mark.asyncio
async def test_resolve_channel_user_returns_none(db):
    """resolve_channel returns None when get_entity returns a User (no title attr)."""
    acc = Account(phone="+71234567890", session_string="session1", is_primary=True)
    await db.add_account(acc)

    mock_user = MagicMock(spec=[])  # no attributes by default
    mock_user.id = 999
    mock_user.first_name = "Alex"
    # User objects have no 'title' attribute

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=mock_user)

    auth = MagicMock()
    pool = ClientPool(auth, db)
    pool.clients["+71234567890"] = mock_client

    result = await pool.resolve_channel("@AlexP87")
    assert result is None

@pytest.mark.asyncio
async def test_get_premium_client_fallback_when_in_use(db):
    """get_premium_client() may reuse an in-use premium client as a single-account fallback."""
    acc = Account(phone="+70001111111", session_string="s1", is_primary=True, is_premium=True)
    await db.add_account(acc)

    auth = MagicMock()
    pool = ClientPool(auth, db)
    pool.clients["+70001111111"] = AsyncMock()
    pool._in_use.add("+70001111111")  # Simulate concurrent usage

    result = await pool.get_premium_client()
    assert result is not None
    client, phone = result
    assert phone == "+70001111111"


@pytest.mark.asyncio
async def test_resolve_channel_strips_post_id_from_url(db):
    """resolve_channel normalizes t.me URLs by stripping post IDs."""
    acc = Account(phone="+71234567890", session_string="session1", is_primary=True)
    await db.add_account(acc)

    mock_entity = MagicMock()
    mock_entity.id = 555
    mock_entity.title = "Arms Channel"
    mock_entity.username = "ruarms_com"
    mock_entity.broadcast = True
    mock_entity.megagroup = False

    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=mock_entity)

    auth = MagicMock()
    pool = ClientPool(auth, db)
    pool.clients["+71234567890"] = mock_client

    result = await pool.resolve_channel("https://t.me/ruarms_com/24")
    assert result is not None
    assert result["channel_id"] == 555
    # Verify get_entity was called with the normalized URL (without /24)
    mock_client.get_entity.assert_awaited_with("https://t.me/ruarms_com")
