"""Full offline CLI/HTTP/Telethon/SQLite flood and delivery scenarios."""
import asyncio
import secrets
from contextlib import asynccontextmanager
from types import SimpleNamespace
from uuid import UUID, uuid4

import httpx
import pytest
import typer
from telethon_floodgate import (
    HandledFloodWaitError,
    RateLimitSpec,
    ResolveRateLimiter,
    TelegramRateLimitedError,
    TelegramRateLimitGate,
    run_with_flood_wait,
)

from tests.test_rpc_guard_integration import PEER, make_session
from tg_agent.config import AppConfig
from tg_agent.gateway.client import TelegramGatewayClient
from tg_agent.gateway.owned_pool import GatewayClientPool
from tg_agent.gateway.pool import GatewayPool
from tg_agent.gateway.pool_storage import GatewayPoolStorage
from tg_agent.gateway.protocol import Send
from tg_agent.gateway.server import create_app
from tg_agent.gateway.store import GatewayStore
from tg_agent.gateway.telegram import LiveTelegramAccess
from tg_shared.auth import TelegramAuth

pytestmark = [pytest.mark.anyio, pytest.mark.aiosqlite_serial]


@asynccontextmanager
async def gateway_world(tmp_path, *, send_cap=None, **transport_options):
    store = GatewayStore(tmp_path / "gateway")
    await store.open()
    storage = GatewayPoolStorage(store)
    await storage.initialize()
    account = await store.import_account("+100", "offline-session")
    token, project = secrets.token_urlsafe(32), str(uuid4())
    await store.register_project(project, tmp_path / project, token)
    await store.grant(project, account, primary=True)
    auth = TelegramAuth(api_id=1, api_hash="offline")
    pool = GatewayClientPool(auth, storage)
    await pool.initialize(phones=[])
    pool._resolve_rate_limiter = ResolveRateLimiter(max_calls=2, jitter_sec=0)
    if send_cap is not None:
        pool._rate_limit_gate = TelegramRateLimitGate(category_limits={"send": RateLimitSpec(send_cap, 60)})
    session, sent = make_session(pool, phone=account, **transport_options)
    session.raw_client.is_connected = lambda: True  # never connect to Telegram
    pool.clients[account] = session
    access = LiveTelegramAccess(store, auth)
    access._pool, access.sessions = pool, pool.clients
    app = create_app(store, access)
    client = TelegramGatewayClient("http://gateway", token, transport=httpx.ASGITransport(app=app))
    world = SimpleNamespace(store=store, storage=storage, account=account, client=client,
                            pool=pool, session=session, sent=sent, access=access)
    try:
        async with app.router.lifespan_context(app):
            yield world
    finally:
        await client.close()
        await store.close()


async def test_cli_nested_resolve_flood_reaches_receipt_and_family_database(tmp_path, db, capsys):
    from tg_agent.cli.commands.dialogs import _dialogs_send

    async with gateway_world(tmp_path, flood=3600) as world:
        project_pool = GatewayPool(AppConfig(), db, client=world.client)
        await project_pool.initialize()
        try:
            with pytest.raises(typer.Exit):
                await _dialogs_send(SimpleNamespace(phone="+100", recipient="42", yes=True,
                                                     text="[private canary](@audituser)"), db, project_pool)
            error = capsys.readouterr().err
            assert "ResolveUsernameRequest" in error and "retry" in error
            assert "private canary" not in error
            floods = await world.storage.list_active_family_floods(phone=world.account)
            assert "resolve" in floods[world.account]
            async with world.store.transaction() as conn:
                async with conn.execute("SELECT flood_until FROM accounts WHERE id=?", (world.account,)) as cursor:
                    assert (await cursor.fetchone())[0] == 0
            # Known-peer publishing remains available while resolves are blocked.
            request = uuid4()
            operation = Send(kind="send", account_id=UUID(world.account), peer=42, peer_type="user", text="plain")
            await world.client.submit(request, operation)
            assert (await world.client.wait(request, timeout=5))["state"] == "succeeded"
            assert [type(r).__name__ for r in world.sent] == ["ResolveUsernameRequest", "SendMessageRequest"]
        finally:
            await project_pool.disconnect_all()


async def test_guard_refusal_is_unsent_and_same_intent_is_not_duplicated(tmp_path):
    async with gateway_world(tmp_path) as world:
        for _ in range(2):
            assert world.pool._resolve_rate_limiter.try_acquire(world.account) == 0
        request = uuid4()
        operation = Send(kind="send", account_id=UUID(world.account), peer=42, peer_type="user",
                         text="[private canary](@audituser)")
        await world.client.submit(request, operation)
        receipt = await world.client.wait(request, timeout=5)
        assert receipt["state"] == "deferred"
        assert receipt["result"]["diagnostics"]["delivery_state"] == "not_sent"
        assert receipt["result"]["diagnostics"]["source"] == "local"
        assert receipt["result"]["diagnostics"]["rpc"] == "ResolveUsernameRequest"
        assert "private canary" not in str(receipt)
        assert (await world.client.submit(request, operation))["state"] == "deferred"
        assert world.sent == []


async def test_write_flood_is_account_wide_and_never_replayed(tmp_path):
    async with gateway_world(tmp_path, send_flood=120) as world:
        request = uuid4()
        operation = Send(kind="send", account_id=UUID(world.account), peer=42, peer_type="user", text="plain")
        await world.client.submit(request, operation)
        receipt = await world.client.wait(request, timeout=5)
        assert receipt["state"] == "uncertain"
        assert receipt["result"]["diagnostics"]["rpc"] == "SendMessageRequest"
        assert (await world.client.submit(request, operation))["state"] == "uncertain"
        assert [type(r).__name__ for r in world.sent] == ["SendMessageRequest"]
        async with world.store.transaction() as conn:
            async with conn.execute("SELECT flood_until FROM accounts WHERE id=?", (world.account,)) as cursor:
                assert (await cursor.fetchone())[0] > 0


async def test_cancellation_after_send_keeps_uncertain_intent(tmp_path):
    async with gateway_world(tmp_path, pending_send=True) as world:
        request = uuid4()
        operation = Send(kind="send", account_id=UUID(world.account), peer=42, peer_type="user", text="plain")
        await world.client.submit(request, operation)
        async with asyncio.timeout(5):
            while not world.sent:
                await asyncio.sleep(0.01)
        await world.client.cancel(request)
        receipt = await world.client.wait(request, timeout=5)
        assert receipt["state"] == "uncertain"
        assert (await world.client.submit(request, operation))["state"] == "uncertain"
        assert len(world.sent) == 1


async def test_gateway_family_flood_and_resolve_budget_survive_restart(tmp_path):
    async with gateway_world(tmp_path, flood=3600) as world:
        account = world.account
        with pytest.raises(HandledFloodWaitError):
            await world.session.send_message(PEER, "[x](@audituser)")
        assert "resolve" in (await world.storage.list_active_family_floods(phone=account))[account]
    store = GatewayStore(tmp_path / "gateway")
    await store.open()
    storage = GatewayPoolStorage(store)
    await storage.initialize()
    pool = GatewayClientPool(TelegramAuth(api_id=1, api_hash="offline"), storage)
    await pool.initialize(phones=[])
    session, sent = make_session(pool, phone=account)
    try:
        with pytest.raises(TelegramRateLimitedError):
            await session.send_message(PEER, "[x](@differentuser)")
        assert sent == []
        assert pool._resolve_rate_limiter.used(account) >= 1
        await session.send_message(PEER, "plain")
        assert [type(r).__name__ for r in sent] == ["SendMessageRequest"]
    finally:
        await session.close()
        await pool.disconnect_all()
        await store.close()


async def test_partial_write_followed_by_refusal_never_replays_first_write(tmp_path, monkeypatch):
    async with gateway_world(tmp_path, send_cap=1) as world:
        async def execute(project, operation):
            await world.session.send_message(PEER, "first")
            await world.session.send_message(PEER, "second")

        monkeypatch.setattr(world.access, "execute", execute)
        request = uuid4()
        operation = Send(kind="send", account_id=UUID(world.account), peer=42, peer_type="user", text="plain")
        await world.client.submit(request, operation)
        receipt = await world.client.wait(request, timeout=5)
        assert receipt["state"] == "uncertain"
        assert receipt["result"]["diagnostics"]["delivery_state"] == "partial"
        assert (await world.client.submit(request, operation))["state"] == "uncertain"
        assert len(world.sent) == 1


async def test_legacy_outer_helper_does_not_turn_resolve_flood_into_account_flood(tmp_path):
    async with gateway_world(tmp_path, flood=3600) as world:
        with pytest.raises(HandledFloodWaitError) as caught:
            await run_with_flood_wait(world.session.raw_client.send_message(PEER, "[x](@audituser)"),
                                     operation="legacy_outer_send", phone=world.account, pool=world.pool)
        assert caught.value.info.rpc == "ResolveUsernameRequest"
        assert "resolve" in (await world.storage.list_active_family_floods(phone=world.account))[world.account]
        async with world.store.transaction() as conn:
            async with conn.execute("SELECT flood_until FROM accounts WHERE id=?", (world.account,)) as cursor:
                assert (await cursor.fetchone())[0] == 0


async def test_operator_account_clear_reaches_an_existing_guard(tmp_path):
    from datetime import datetime, timedelta, timezone

    async with gateway_world(tmp_path) as world:
        await world.pool.report_flood(world.account, 3600)
        with pytest.raises(TelegramRateLimitedError):
            await world.session.send_message(PEER, "blocked")
        await world.storage.update_account_flood(world.account, datetime.now(timezone.utc) - timedelta(seconds=1))
        world.pool._rpc_accounts_loaded_at = None  # expire the 60s refresh cache
        await world.session.send_message(PEER, "plain")
        assert len(world.sent) == 1


async def test_shorter_flood_signal_cannot_shorten_an_existing_deadline(tmp_path):
    async with gateway_world(tmp_path) as world:
        await world.pool.report_family_flood(world.account, 3600, "resolve")
        family_until = await world.pool.family_flood_deadline(world.account, "resolve")
        await world.pool.report_family_flood(world.account, 10, "resolve")
        assert await world.pool.family_flood_deadline(world.account, "resolve") >= family_until
        await world.pool.report_flood(world.account, 3600)
        until = (await world.storage.get_accounts())[0].flood_wait_until
        await world.pool.report_flood(world.account, 10)
        assert (await world.storage.get_accounts())[0].flood_wait_until >= until


async def test_repeated_initialization_does_not_replay_own_resolve_snapshot(tmp_path):
    async with gateway_world(tmp_path) as world:
        await world.session.send_message(PEER, "[x](@audituser)")
        await world.pool.persist_resolve_limiter_snapshot()
        assert world.pool._resolve_rate_limiter.used(world.account) == 1
        await world.pool.initialize(phones=[])
        assert world.pool._resolve_rate_limiter.used(world.account) == 1
