"""The gateway's production connection cache with a local transport double (#1868).

Projects reach an account through ``LiveTelegramAccess.connection`` (the relay
hub asks for it on every request): one live connection per account however many
projects use it, one DM handler, and every granted project sees each DM event.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any, cast

import pytest
from telethon_floodgate import TelegramRateLimitedError

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
