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


@pytest.mark.asyncio
async def test_acquire_available_round_robin_distributes(db, pool):
    """Sequential acquire/release calls cycle through all available accounts."""
    phones = ["+71001", "+71002", "+71003"]
    for p in phones:
        await db.add_account(Account(phone=p, session_string=p, is_active=True))
    connected = set(phones)

    seen: list[str] = []
    for _ in range(4):
        lease = await pool.acquire_available(connected)
        assert lease is not None
        seen.append(lease.account.phone)
        await pool.release(lease.account.phone)

    # First three calls hit each phone exactly once.
    assert set(seen[:3]) == set(phones)
    # Fourth call wraps around to the first phone.
    assert seen[3] == seen[0]


@pytest.mark.asyncio
async def test_round_robin_skips_in_use(db):
    """Phones already in_use are skipped by the round-robin walk."""
    phones = ["+72001", "+72002", "+72003"]
    for p in phones:
        await db.add_account(Account(phone=p, session_string=p, is_active=True))

    pool = AccountLeasePool(db, {"+72002"})
    connected = set(phones)

    lease1 = await pool.acquire_available(connected)
    assert lease1 is not None
    assert lease1.account.phone == "+72001"
    await pool.release("+72001")

    # cursor now sits at +72001; next exclusive should skip +72002 (in_use)
    # and return +72003.
    lease2 = await pool.acquire_available(connected)
    assert lease2 is not None
    assert lease2.account.phone == "+72003"


@pytest.mark.asyncio
async def test_round_robin_skips_flood_waited(db, pool):
    """Flood-waited accounts are skipped during round-robin selection."""
    phones = ["+73001", "+73002", "+73003"]
    for p in phones:
        await db.add_account(Account(phone=p, session_string=p, is_active=True))

    future = datetime.now(timezone.utc) + timedelta(hours=1)
    await db.update_account_flood("+73002", future)
    connected = set(phones)

    seen: list[str] = []
    for _ in range(2):
        lease = await pool.acquire_available(connected)
        assert lease is not None
        seen.append(lease.account.phone)
        await pool.release(lease.account.phone)

    assert set(seen) == {"+73001", "+73003"}
    assert "+73002" not in seen


@pytest.mark.asyncio
async def test_round_robin_skips_disconnected(db, pool):
    """Phones missing from connected_phones are skipped."""
    phones = ["+74001", "+74002", "+74003"]
    for p in phones:
        await db.add_account(Account(phone=p, session_string=p, is_active=True))

    connected = {"+74001", "+74003"}

    seen: list[str] = []
    for _ in range(2):
        lease = await pool.acquire_available(connected)
        assert lease is not None
        seen.append(lease.account.phone)
        await pool.release(lease.account.phone)

    assert set(seen) == {"+74001", "+74003"}


@pytest.mark.asyncio
async def test_round_robin_survives_account_change(db, pool):
    """Cursor based on phone (not index) survives account list mutations."""
    a_id = await db.add_account(Account(phone="+75001", session_string="A", is_active=True))
    b_id = await db.add_account(Account(phone="+75002", session_string="B", is_active=True))
    c_id = await db.add_account(Account(phone="+75003", session_string="C", is_active=True))
    assert a_id and b_id and c_id

    connected = {"+75001", "+75002", "+75003"}

    lease1 = await pool.acquire_available(connected)
    assert lease1 is not None
    assert lease1.account.phone == "+75001"
    await pool.release("+75001")

    # Remove B from the DB. Cursor (last_phone="+75001") should still find A's
    # position in the new list (now [A, C]) and advance to C.
    await db.delete_account(b_id)

    lease2 = await pool.acquire_available(connected)
    assert lease2 is not None
    assert lease2.account.phone == "+75003"


@pytest.mark.asyncio
async def test_cursor_unchanged_when_all_flooded(db, pool):
    """When no exclusive account is available, cursor must not advance."""
    phones = ["+76001", "+76002", "+76003"]
    for p in phones:
        await db.add_account(Account(phone=p, session_string=p, is_active=True))

    future = datetime.now(timezone.utc) + timedelta(hours=1)
    for p in phones:
        await db.update_account_flood(p, future)

    connected = set(phones)

    # All accounts flooded → exclusive returns None (no cursor advance), then
    # falls through to shared (which DOES return one, but shared also must
    # not advance the cursor).
    result = await pool.acquire_available(connected)
    # shared is allowed to return any flooded account — actually no, shared
    # also skips flood-waited; so result is None.
    assert result is None
    assert pool._last_phone is None

    # Clear flood on the middle phone. Next acquire returns it (cursor was
    # never moved, so the walk starts from index 0; +76002 is the first
    # eligible candidate).
    await db.update_account_flood("+76002", None)
    lease = await pool.acquire_available(connected)
    assert lease is not None
    assert lease.account.phone == "+76002"


@pytest.mark.asyncio
async def test_shared_fallback_uses_round_robin_order(db):
    """When every account is in use, shared leases still rotate instead of preferring primary."""
    phones = ["+77001", "+77002", "+77003"]
    for p in phones:
        await db.add_account(Account(phone=p, session_string=p, is_active=True))

    pool = AccountLeasePool(db, set(phones))
    connected = set(phones)

    seen: list[str] = []
    for _ in range(4):
        lease = await pool.acquire_available(connected)
        assert lease is not None
        assert lease.shared is True
        seen.append(lease.account.phone)

    assert seen[:3] == phones
    assert seen[3] == phones[0]
