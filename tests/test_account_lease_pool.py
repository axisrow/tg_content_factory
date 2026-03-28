"""Tests for AccountLeasePool flood wait logic."""

from datetime import datetime, timedelta, timezone

import pytest

from src.models import Account
from src.telegram.account_lease_pool import AccountLeasePool


@pytest.fixture
async def pool(db):
    return AccountLeasePool(db, set())


@pytest.mark.asyncio
async def test_acquire_available_skips_active_flood(db, pool):
    """Account with future flood_wait_until is not returned."""
    await db.add_account(Account(phone="+70001", session_string="s1", is_active=True))
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    await db.update_account_flood("+70001", future)

    result = await pool.acquire_available({"+70001"})
    assert result is None


@pytest.mark.asyncio
async def test_acquire_available_clears_expired_flood(db, pool):
    """Expired flood_wait_until is cleared from DB and account is returned."""
    await db.add_account(Account(phone="+70002", session_string="s2", is_active=True))
    past = datetime.now(timezone.utc) - timedelta(hours=1)
    await db.update_account_flood("+70002", past)

    result = await pool.acquire_available({"+70002"})

    # Account should be returned despite having a non-null flood_wait_until
    assert result is not None
    assert result.account.phone == "+70002"

    # DB value should be cleared
    accounts = await db.get_accounts()
    acc = next(a for a in accounts if a.phone == "+70002")
    assert acc.flood_wait_until is None


@pytest.mark.asyncio
async def test_acquire_available_returns_non_flooded_when_one_flooded(db):
    """With two accounts, the flooded one is skipped and the other returned."""
    pool = AccountLeasePool(db, set())

    await db.add_account(Account(phone="+70003", session_string="s3", is_active=True))
    await db.add_account(Account(phone="+70004", session_string="s4", is_active=True))

    future = datetime.now(timezone.utc) + timedelta(hours=1)
    await db.update_account_flood("+70003", future)

    result = await pool.acquire_available({"+70003", "+70004"})
    assert result is not None
    assert result.account.phone == "+70004"


@pytest.mark.asyncio
async def test_is_flood_waited_returns_false_for_expired(db):
    """_is_flood_waited correctly ignores expired timestamps."""
    past = datetime.now(timezone.utc) - timedelta(seconds=1)
    account = Account(phone="+70005", session_string="s5", is_active=True, flood_wait_until=past)
    assert not AccountLeasePool._is_flood_waited(account)


@pytest.mark.asyncio
async def test_is_flood_waited_returns_true_for_future(db):
    """_is_flood_waited correctly detects active flood wait."""
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    account = Account(phone="+70006", session_string="s6", is_active=True, flood_wait_until=future)
    assert AccountLeasePool._is_flood_waited(account)
