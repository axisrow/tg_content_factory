"""Per-family flood state on the project side (#1845) — offline.

#1844 records read-side floods per (account, family) inside the gateway
process. The project mirror only carried a single ``flood_until``, so
``GatewayPool.select`` routed reads into family-flooded accounts (a local
``deferred`` receipt and a rotation were needed to recover), capacity
counters overestimated, and the flood CLI could neither show nor clear the
gateway's deadlines. These tests pin the exposure channel (``/v1/status``
rows), the read routing, and the flood-clear endpoint.
"""

import secrets
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from tg_agent.config import AppConfig
from tg_agent.database import Database
from tg_agent.gateway.client import TelegramGatewayClient
from tg_agent.gateway.pool import GatewayPool
from tg_agent.gateway.pool_storage import GatewayPoolStorage
from tg_agent.gateway.server import create_app
from tg_agent.gateway.store import GatewayStore
from tg_agent.telegram.collector_types import AllCollectionClientsFloodedError


class Transport:
    async def execute(self, project, operation):
        return {"messages": []}

    async def close(self):
        pass


@pytest.fixture
async def gateway(tmp_path):
    store = GatewayStore(tmp_path / "gateway")
    await store.open()
    storage = GatewayPoolStorage(store)
    await storage.initialize()
    app = create_app(store, Transport())
    first = await store.import_account("+100", "s1")
    second = await store.import_account("+200", "s2")
    project = str(uuid4())
    token = secrets.token_urlsafe(32)
    await store.register_project(project, tmp_path / project, token)
    await store.grant(project, first, primary=True)
    await store.grant(project, second)
    client = TelegramGatewayClient("http://gateway", token, transport=httpx.ASGITransport(app=app))
    db = Database(str(tmp_path / "project.db"))
    await db.initialize()
    pool = GatewayPool(AppConfig(), db, client=client)
    try:
        async with app.router.lifespan_context(app):
            await pool.initialize()
            yield SimpleNamespace(store=store, storage=storage, app=app, client=client, db=db,
                                  pool=pool, first=first, second=second)
    finally:
        await pool.disconnect_all()
        await db.close()
        await client.close()
        await store.close()


def _later(hours=1):
    return datetime.now(timezone.utc) + timedelta(hours=hours)


async def test_status_rows_expose_family_floods(gateway):
    until = _later()
    await gateway.storage.set_family_flood(str(gateway.first), "history", until)
    status = await gateway.client.status()
    rows = {row["id"]: row for row in status["accounts"]}
    assert rows[str(gateway.first)]["family_floods"] == {"history": until.timestamp()}
    assert rows[str(gateway.second)]["family_floods"] == {}


async def test_read_routing_skips_the_family_flooded_account(gateway):
    await gateway.storage.set_family_flood(str(gateway.first), "history", _later())
    await gateway.pool.initialize()

    # The primary (+100) is flooded for history reads; routing must land on +200.
    assert gateway.pool.select(family="history")["phone"] == "+200"
    # A flood in another family must not hide the account from unrelated reads.
    assert gateway.pool.select(family="resolve")["phone"] == "+100"
    # Writes and family-less selections keep the primary available.
    assert gateway.pool.select()["phone"] == "+100"

    # Capacity for collection (history reads) no longer overestimates.
    assert await gateway.pool.available_collection_client_count() == 1


async def test_routing_rotation_skips_the_family_flooded_replacement(gateway):
    """The read_request replacement pick honours the operation's family too."""
    pool = gateway.pool
    await gateway.storage.set_family_flood(str(gateway.first), "history", _later())
    await gateway.storage.set_family_flood(str(gateway.second), "history", _later())
    await pool.initialize()
    excluded = {str(gateway.first)}
    with pytest.raises(AllCollectionClientsFloodedError):
        pool.select(exclude=excluded, family="history")


async def test_all_accounts_flooded_for_a_family_report_the_family_deadline(gateway):
    first_until = _later(hours=1)
    second_until = _later(hours=2)
    await gateway.storage.set_family_flood(str(gateway.first), "history", first_until)
    await gateway.storage.set_family_flood(str(gateway.second), "history", second_until)
    await gateway.pool.initialize()
    with pytest.raises(AllCollectionClientsFloodedError) as exc:
        gateway.pool.select(family="history")
    assert exc.value.retry_after_sec > 0
    # The contract reports the earliest moment any account frees up (min).
    assert exc.value.next_available_at.timestamp() == pytest.approx(first_until.timestamp(), abs=2)


async def test_flood_clear_clears_gateway_account_and_family_floods(gateway):
    await gateway.storage.set_family_flood(str(gateway.first), "history", _later())
    await gateway.store.flood(str(gateway.first), time.time() + 3600)

    response = await gateway.client.http.post(f"/v1/accounts/{gateway.first}/flood-clear")
    assert response.status_code == 200
    assert await gateway.storage.list_active_family_floods() == {}
    accounts = await gateway.storage.get_accounts()
    assert accounts[0].flood_wait_until is None

    # An account outside the project's grants is refused.
    stranger = await gateway.store.import_account("+300", "s3")
    refused = await gateway.client.http.post(f"/v1/accounts/{stranger}/flood-clear")
    assert refused.status_code == 403


# ---------------------------------------------------------------------------
# The flood CLI in gateway mode (#1845)
# ---------------------------------------------------------------------------


async def _cli_env(gateway, monkeypatch, tmp_path):
    from tg_agent.cli.commands import account as account_cli
    from tg_agent.gateway.client import TelegramGatewayClient

    db = Database(str(tmp_path / "cli-project.db"))
    await db.initialize()
    await db.repos.accounts.sync_gateway_catalog(await gateway.client.accounts())

    config = SimpleNamespace(gateway={"root": str(tmp_path / "gateway"), "token_file": "unused"})

    async def fake_init(_config_path):
        return config, db

    async def fake_from_config(cls, _config):
        return gateway.client

    monkeypatch.setattr(account_cli.runtime, "init_db", fake_init)
    monkeypatch.setattr(TelegramGatewayClient, "from_config", classmethod(fake_from_config))
    return config, db


async def test_flood_status_gateway_mode_lists_family_floods(gateway, monkeypatch, capsys, tmp_path):
    from tg_agent.cli.commands.account import flood_status_impl

    await gateway.storage.set_family_flood(str(gateway.first), "history", _later())
    _, db = await _cli_env(gateway, monkeypatch, tmp_path)
    try:
        await flood_status_impl("config.yaml")
    finally:
        await db.close()
    out = capsys.readouterr().out
    assert "+100" in out and "+200" in out
    assert "Per-family read floods" in out
    assert "history" in out


async def test_flood_clear_gateway_mode_clears_the_gateway_state(gateway, monkeypatch, capsys, tmp_path):
    from tg_agent.cli.commands.account import flood_clear_impl

    await gateway.storage.set_family_flood(str(gateway.first), "history", _later())
    await gateway.store.flood(str(gateway.first), time.time() + 3600)
    db_path = tmp_path / "cli-project.db"
    await _cli_env(gateway, monkeypatch, tmp_path)
    # The impl closes the DB itself (legacy contract) — assert via a fresh handle.
    await flood_clear_impl("config.yaml", phone="+100")
    reopened = Database(str(db_path))
    await reopened.initialize()
    try:
        accounts = await reopened.get_account_summaries(active_only=False)
        assert accounts[0].flood_wait_until is None
    finally:
        await reopened.close()
    assert await gateway.storage.list_active_family_floods() == {}
    out = capsys.readouterr().out
    assert "Flood wait cleared for +100" in out
    assert "cleared family flood: history" in out
