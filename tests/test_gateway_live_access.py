"""The gateway's production connection cache with a local transport double (#1868).

Projects reach an account through ``LiveTelegramAccess.connection`` (the relay
hub asks for it on every request): one live connection per account however many
projects use it, one DM handler, and every granted project sees each DM event.
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, cast

import pytest
from telethon_floodgate import TelegramRateLimitedError, TelegramRpcDeferredError

from tg_agent.gateway import runtime
from tg_agent.gateway.store import GatewayStore
from tg_agent.gateway.telegram import LiveTelegramAccess
from tg_agent.telegram.session_lock import release_all_session_locks
from tg_shared.auth import TelegramAuth

pytestmark = pytest.mark.aiosqlite_serial

PROJECTS = ("11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222")


class LocalClient:
    def __init__(self):
        self.disconnected = False
        self.handlers = []

    def add_event_handler(self, callback, builder):
        self.handlers.append(callback)

    async def get_me(self):
        return SimpleNamespace(id=10, first_name="Shared", last_name="Account", username="shared", premium=True)

    def is_connected(self):
        return not self.disconnected

    async def disconnect(self):
        self.disconnected = True


class LocalAuth:
    def __init__(self):
        self.connections = []

    async def create_client_from_session(self, session, **kwargs):
        client = LocalClient()
        self.connections.append(client)
        return client

    async def cleanup(self):
        pass


async def test_projects_share_one_live_connection_and_every_project_sees_each_dm(tmp_path, monkeypatch):
    monkeypatch.setenv("TGCF_SESSION_LOCK_DIR", str(tmp_path / "locks"))
    store = GatewayStore(tmp_path / "gateway")
    await store.open()
    auth = LocalAuth()
    access = LiveTelegramAccess(store, cast(TelegramAuth, auth))
    notified = []

    async def on_event(account):
        notified.append(account)

    access.on_event = on_event
    try:
        account = await store.import_account("+100", "local-transport-session")
        for index, project in enumerate(PROJECTS):
            await store.register_project(project, tmp_path / project, str(index) * 40)
            await store.grant(project, account)

        sessions = await asyncio.gather(*(access.connection(project, account) for project in PROJECTS))

        assert sessions[0].raw_client is sessions[1].raw_client
        assert len(auth.connections) == 1
        assert len(auth.connections[0].handlers) == 2  # NewMessage + MessageDeleted
        event = SimpleNamespace(chat_id=123, message=SimpleNamespace(
            id=7, raw_text="incoming DM", out=False, date=datetime.now(timezone.utc),
        ))
        await auth.connections[0].handlers[0](event)
        await auth.connections[0].handlers[1](SimpleNamespace(chat_id=123, deleted_ids=[7]))
        delivered = [await store.events(project, 0) for project in PROJECTS]
        assert delivered[0] == delivered[1]
        assert delivered[0][0]["payload"]["text"] == "incoming DM"
        assert delivered[0][1]["payload"]["kind"] == "deleted"
        assert notified == [account, account]
        assert (await access.connection(PROJECTS[0], account)).raw_client is sessions[0].raw_client
        assert len(auth.connections) == 1

        await access.close()

        assert auth.connections[0].disconnected
    finally:
        await access.close()
        await store.close()
        release_all_session_locks()


async def test_housekeeping_reconnects_accounts_that_could_not_connect_at_startup(monkeypatch):
    """An account flooded when the gateway started connects once the wait is over."""
    monkeypatch.setattr(runtime, "FLOOD_GUARD_PERSIST_INTERVAL_SEC", 0.01)
    retried = asyncio.Event()
    starts = []

    class Store:
        async def expire_events(self, *, now):
            pass

    class Access:
        async def start(self):
            starts.append(1)
            if len(starts) >= 2:
                retried.set()

    housekeeping = asyncio.create_task(runtime._housekeeping(cast(GatewayStore, Store()), Access()))
    try:
        async with asyncio.timeout(5):
            await retried.wait()
    finally:
        housekeeping.cancel()
        await asyncio.gather(housekeeping, return_exceptions=True)


async def test_a_repeated_account_failure_is_reported_once(tmp_path, monkeypatch, caplog):
    store = GatewayStore(tmp_path / "gateway")
    await store.open()
    access = LiveTelegramAccess(store, cast(TelegramAuth, LocalAuth()))

    async def unavailable(project, account):
        raise ConnectionError("network down")

    monkeypatch.setattr(access, "connection", unavailable)
    try:
        with caplog.at_level(logging.DEBUG, logger="tg_agent.gateway.telegram"):
            await access._warm(PROJECTS[0], "acc-1")
            await access._warm(PROJECTS[0], "acc-1")  # the next housekeeping round
        warning, repeat = caplog.records
        assert [warning.levelno, repeat.levelno] == [logging.WARNING, logging.DEBUG]
        # The first report names the account and the cause, with the traceback;
        # the repeat stays a one-line debug note.
        assert "acc-1" in warning.getMessage() and "network down" in warning.getMessage()
        assert warning.exc_info and not repeat.exc_info
    finally:
        await access.close()
        await store.close()


async def test_a_deferred_warm_waits_out_the_gate_window(tmp_path, monkeypatch, caplog):
    """A floodgate deferral paces the warm, not the poll cadence (#1867).

    Every ``status`` request used to respawn the warm the moment the previous
    one died: the gate's "retry in ~Ns" was re-entered every few seconds and
    logged as a failure with a traceback. Now the window from the deferral is
    honored, the deferral is one info line without a traceback.
    """
    store = GatewayStore(tmp_path / "gateway")
    await store.open()
    access = LiveTelegramAccess(store, cast(TelegramAuth, LocalAuth()))
    project = PROJECTS[0]
    attempts = []

    async def deferred(project, account):
        attempts.append(1)
        raise TelegramRpcDeferredError(account, "get_users", 500.0, "GetUsersRequest")

    monkeypatch.setattr(access, "connection", deferred)
    try:
        await store.register_project(project, tmp_path / project, "a" * 40)
        account = await store.import_account("+100", "gated-session")
        await store.grant(project, account)
        with caplog.at_level(logging.DEBUG, logger="tg_agent.gateway.telegram"):
            await access.start()
            while account not in access._warm_retry_at:
                await asyncio.sleep(0.01)
            for _ in range(5):  # every poll used to respawn the warm
                await access.start()
            await asyncio.sleep(0.05)
        assert attempts == [1]
        remaining = access._warm_retry_at[account] - time.monotonic()
        assert 499 < remaining <= 500  # the gate's estimate, not the retry floor
        (record,) = [rec for rec in caplog.records if "deferred by the floodgate" in rec.getMessage()]
        assert record.levelno == logging.INFO
        assert not record.exc_info
        # Once the window is out, the next start warms again.
        access._warm_retry_at[account] = 0.0
        await access.start()
        while len(attempts) < 2:
            await asyncio.sleep(0.01)
        assert attempts == [1, 1]
    finally:
        await access.close()
        await store.close()


async def test_a_failed_warm_is_paced_so_status_polls_cannot_storm_it(tmp_path, monkeypatch, caplog):
    """A plain connect failure also paces the warm; the #1875 report is unchanged."""
    store = GatewayStore(tmp_path / "gateway")
    await store.open()
    access = LiveTelegramAccess(store, cast(TelegramAuth, LocalAuth()))
    project = PROJECTS[0]
    attempts = []

    async def unavailable(project, account):
        attempts.append(1)
        raise ConnectionError("network down")

    monkeypatch.setattr(access, "connection", unavailable)
    try:
        await store.register_project(project, tmp_path / project, "a" * 40)
        account = await store.import_account("+100", "failing-session")
        await store.grant(project, account)
        with caplog.at_level(logging.DEBUG, logger="tg_agent.gateway.telegram"):
            await access.start()
            while account not in access._warm_retry_at:
                await asyncio.sleep(0.01)
            for _ in range(5):  # every poll used to respawn the warm
                await access.start()
            await asyncio.sleep(0.05)
        assert attempts == [1]
        remaining = access._warm_retry_at[account] - time.monotonic()
        assert 29 < remaining <= 30
        warning = next(rec for rec in caplog.records if rec.levelno == logging.WARNING)
        assert warning.exc_info  # a real failure still carries its traceback
    finally:
        await access.close()
        await store.close()


async def test_startup_connects_are_staggered(tmp_path, monkeypatch):
    """#1847: the startup account connects space out instead of starting at once.

    The stagger waits inside each connect task, so start() itself returns
    immediately (the housekeeping loop and the status op call it)."""
    monkeypatch.setattr("tg_agent.gateway.telegram.GATEWAY_CONNECT_STAGGER_SEC", 0.05)
    store = GatewayStore(tmp_path / "gateway")
    await store.open()
    auth = LocalAuth()
    access = LiveTelegramAccess(store, cast(TelegramAuth, auth))
    project = PROJECTS[0]
    try:
        await store.register_project(project, tmp_path / project, "a" * 40)
        acc_a = await store.import_account("+100", "a-session")
        acc_b = await store.import_account("+200", "b-session")
        await store.grant(project, acc_a)
        await store.grant(project, acc_b)

        delays: list[tuple[str, float]] = []
        original_warm = access._warm

        async def spy_warm(warm_project, account, *, delay: float = 0.0):
            delays.append((account, delay))
            await original_warm(warm_project, account, delay=delay)

        monkeypatch.setattr(access, "_warm", spy_warm)

        await asyncio.wait_for(access.start(), timeout=1.0)
        assert delays == [], "start() must spawn the connect tasks"
        await asyncio.gather(*access._startup)

        assert [delay for _, delay in delays] == [0.0, pytest.approx(0.05)]
        assert len(auth.connections) == 2
    finally:
        await access.close()
        await store.close()
        release_all_session_locks()


async def test_cold_account_connection_respects_a_pending_flood_write(tmp_path, monkeypatch):
    monkeypatch.setenv("TGCF_SESSION_LOCK_DIR", str(tmp_path / "locks"))
    store = GatewayStore(tmp_path / "gateway")
    await store.open()
    auth = LocalAuth()
    access = LiveTelegramAccess(store, cast(TelegramAuth, auth))
    project = PROJECTS[0]
    try:
        await store.register_project(project, tmp_path / project, "a" * 40)
        ready = await store.import_account("+100", "ready-session")
        waiting = await store.import_account("+200", "waiting-session")
        await store.grant(project, ready)
        await store.grant(project, waiting)
        await access.connection(project, ready)
        # The RPC guard creates this attribute on the pool at run time (rpc_guard.py).
        cast(Any, access._pool)._rpc_pending_account_deadlines = {
            waiting: datetime.now(timezone.utc) + timedelta(minutes=10),
        }

        with pytest.raises(TelegramRateLimitedError) as caught:
            await access.connection(project, waiting)

        assert 590 < caught.value.retry_after_sec <= 600
        assert len(auth.connections) == 1
    finally:
        await access.close()
        await store.close()
        release_all_session_locks()
