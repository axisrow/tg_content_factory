from unittest.mock import AsyncMock, MagicMock

import pytest

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
