"""Premium-flag freshness (#1749).

The factory ``is_premium`` flag is a *cache of a Telegram fact*, and three
paths used to let it go stale or get corrupted:

* the gateway-catalog mirror (``sync_gateway_catalog``) overwrote it verbatim,
  so a stale gateway side flipped a Premium account to No at every pool start
  (the repository-level no-downgrade rule is pinned in
  ``tests/repositories/test_accounts_repository.py``);
* pool startup verified premium live for *new* accounts only;
* premium-gated operations trusted the stored flag and refused without a
  live re-check.

These tests pin the live-refresh behaviors: pool startup re-verifies every
connected account, ``get_premium_client`` heals a stale flag before refusing,
both re-checks are TTL-suppressed, and the gateway ScopedPool does the same
against gateway storage.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.helpers import FakeCliTelethonClient
from tg_agent.gateway.actions import ScopedPool, _premium_recheck_at
from tg_agent.gateway.pool_storage import GatewayPoolStorage
from tg_agent.models import Account

PHONE = "+70000000001"


def _flip_premium(client: FakeCliTelethonClient, premium: bool) -> AsyncMock:
    """Replace get_me with a counting double reporting the given premium flag."""
    client.get_me = AsyncMock(return_value=SimpleNamespace(premium=premium))
    return client.get_me


@pytest.mark.anyio
async def test_pool_initialize_refreshes_premium_for_preexisting_accounts(real_pool_harness_factory):
    """A connected account whose live premium status changed is re-verified at
    every pool start — not only accounts new to the pool (#1749)."""
    harness = real_pool_harness_factory()
    client = harness.queue_cli_client(phone=PHONE, client=FakeCliTelethonClient())
    await harness.add_account(PHONE, session_string="s1", is_primary=True, is_premium=False)
    await harness.initialize_connected_accounts()
    summaries = await harness.db.get_account_summaries(active_only=False)
    assert summaries[0].is_premium is False

    # Live status flips while the pool stays up; the next initialize (worker
    # restart-free re-init, dispatcher accounts.toggle, …) must heal the flag.
    _flip_premium(client, premium=True)
    await harness.pool.initialize()

    summaries = await harness.db.get_account_summaries(active_only=False)
    assert summaries[0].is_premium is True


@pytest.mark.anyio
async def test_get_premium_client_heals_stale_flag_before_refusing(real_pool_harness_factory):
    """Incident replay: the stale mirror flipped Premium to No; the premium-
    gated call must live re-check and heal instead of refusing."""
    harness = real_pool_harness_factory()
    client = harness.queue_cli_client(
        phone=PHONE, client=FakeCliTelethonClient(me=SimpleNamespace(premium=True))
    )
    await harness.add_account(PHONE, session_string="s1", is_primary=True, is_premium=True)
    await harness.initialize_connected_accounts()
    # The corruption: as the blind catalog mirror did before #1749.
    await harness.db.update_account_premium(PHONE, False)

    result = await harness.pool.get_premium_client()

    assert result is not None
    session, phone = result
    assert phone == PHONE
    assert session.raw_client is client
    summaries = await harness.db.get_account_summaries(active_only=False)
    assert summaries[0].is_premium is True


@pytest.mark.anyio
async def test_get_premium_client_recheck_is_ttl_suppressed(real_pool_harness_factory):
    """When the re-check finds no Premium, repeated refusals do not re-fetch."""
    harness = real_pool_harness_factory()
    client = harness.queue_cli_client(phone=PHONE, client=FakeCliTelethonClient())
    await harness.add_account(PHONE, session_string="s1", is_primary=True, is_premium=False)
    await harness.initialize_connected_accounts()
    fetches = _flip_premium(client, premium=False)
    baseline = fetches.await_count

    assert await harness.pool.get_premium_client() is None
    after_first = fetches.await_count
    assert after_first == baseline + 1

    assert await harness.pool.get_premium_client() is None
    assert fetches.await_count == after_first, "re-check must stay TTL-suppressed"


@pytest.fixture
def _clear_scoped_pool_recheck_ttl():
    _premium_recheck_at.clear()
    yield
    _premium_recheck_at.clear()


def _gateway_scoped_pool(account_id: str, *, live_premium: bool):
    """ScopedPool over a fake gateway ClientPool with one connected account."""
    session = SimpleNamespace(fetch_me=AsyncMock(return_value=SimpleNamespace(premium=live_premium)))
    stale = Account(phone=account_id, session_string="s", is_premium=False)
    healed = Account(phone=account_id, session_string="s", is_premium=True)
    gateway_db = SimpleNamespace(update_account_premium=AsyncMock())
    pool = SimpleNamespace(
        clients={account_id: session},
        # First read sees the stale flag, the re-read after healing sees True.
        _get_account_for_phone=AsyncMock(side_effect=[stale, healed]),
        _db=gateway_db,
        get_client_by_phone=AsyncMock(return_value=(session, account_id)),
    )
    access = SimpleNamespace(_pool=pool, auth=MagicMock())
    return ScopedPool(access, account_id), session, gateway_db


@pytest.mark.anyio
async def test_scoped_pool_get_premium_client_heals_stale_flag(_clear_scoped_pool_recheck_ttl):
    scoped, session, gateway_db = _gateway_scoped_pool("uuid-1", live_premium=True)

    result = await scoped.get_premium_client()

    assert result == (session, "uuid-1")
    gateway_db.update_account_premium.assert_awaited_once_with("uuid-1", True)


@pytest.mark.anyio
async def test_scoped_pool_get_premium_client_refuses_and_stays_ttl_suppressed(
    _clear_scoped_pool_recheck_ttl,
):
    scoped, session, gateway_db = _gateway_scoped_pool("uuid-2", live_premium=False)

    assert await scoped.get_premium_client() is None
    gateway_db.update_account_premium.assert_awaited_once_with("uuid-2", False)

    fetches = session.fetch_me
    await scoped.get_premium_client()
    assert fetches.await_count == 1, "re-check must stay TTL-suppressed"


@pytest.mark.anyio
async def test_gateway_pool_storage_premium_key_is_account_id():
    """The settings key is ``premium:<account-uuid>`` — the parameter is the
    gateway account id, not a phone (#1749 naming trap)."""
    storage = GatewayPoolStorage(SimpleNamespace(_connection=MagicMock()))
    storage.set_setting = AsyncMock()

    await storage.update_account_premium("uuid-1", True)
    storage.set_setting.assert_awaited_with("premium:uuid-1", "1")

    await storage.update_account_premium("uuid-1", False)
    storage.set_setting.assert_awaited_with("premium:uuid-1", "0")
