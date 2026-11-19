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
both re-checks are TTL-suppressed
against gateway storage.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.helpers import FakeCliTelethonClient
from tg_agent.gateway.pool_storage import GatewayPoolStorage

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
