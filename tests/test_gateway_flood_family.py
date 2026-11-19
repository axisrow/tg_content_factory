"""Read-side flood scoping in the gateway's storage (#1844) — offline.

#1834 scopes a read-side FloodWait to its family so that publishing from the
same account stays available. The gateway's own pool keeps its floods in
``GatewayPoolStorage``, which must implement the per-family slice. Since #1868
projects run their requests on the gateway's pool over the relay, so the same
native flood scoping applies there.
"""

from datetime import datetime, timedelta, timezone

import pytest

from tg_agent.gateway.pool_storage import GatewayPoolStorage
from tg_agent.gateway.store import GatewayStore
from tg_agent.telegram.flood_scope import family_floods_repo

BARE = 1111111111


async def _account_flood(storage):
    accounts = await storage.get_accounts()
    assert len(accounts) == 1
    return accounts[0].flood_wait_until


# ---------------------------------------------------------------------------
# The storage slice
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_gateway_storage_implements_the_family_flood_slice(tmp_path):
    """Same contract as AccountsRepository: set / list active / clear."""
    store = GatewayStore(tmp_path / "gateway")
    await store.open()
    try:
        storage = GatewayPoolStorage(store)
        await storage.initialize()
        now = datetime.now(timezone.utc)
        later = now + timedelta(hours=1)

        assert family_floods_repo(storage) is storage

        await storage.set_family_flood("acc-1", "history", later)
        await storage.set_family_flood("acc-1", "resolve", now - timedelta(seconds=5))
        await storage.set_family_flood("acc-2", "dialogs", later)

        floods = await storage.list_active_family_floods()
        assert set(floods) == {"acc-1", "acc-2"}
        # Expired deadlines are filtered out by the read.
        assert set(floods["acc-1"]) == {"history"}
        assert abs((floods["acc-1"]["history"] - later).total_seconds()) < 1
        assert floods["acc-1"]["history"].tzinfo is not None
        assert set(await storage.list_active_family_floods(phone="acc-2")) == {"acc-2"}
        # ``now`` moves the cut-off.
        assert await storage.list_active_family_floods(now=later + timedelta(seconds=1)) == {}

        # A later deadline for the same family replaces the earlier one.
        await storage.set_family_flood("acc-1", "history", later + timedelta(hours=1))
        floods = await storage.list_active_family_floods(phone="acc-1")
        assert floods["acc-1"]["history"] > later

        await storage.clear_family_floods("acc-2", family="history")
        assert "acc-2" in await storage.list_active_family_floods()
        await storage.clear_family_floods("acc-2", family="dialogs")
        await storage.clear_family_floods("acc-1")
        assert await storage.list_active_family_floods() == {}
    finally:
        await store.close()


# ---------------------------------------------------------------------------
# Executed through the gateway
# ---------------------------------------------------------------------------

