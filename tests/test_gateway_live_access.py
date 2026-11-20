"""The gateway's production connection cache with a local transport double (#1868).

Projects reach an account through ``LiveTelegramAccess.connection`` (the relay
hub asks for it on every request): one live connection per account however many
projects use it, one DM handler, and every granted project sees each DM event.
"""

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from telethon_floodgate import TelegramRateLimitedError

from tg_agent.gateway.store import GatewayStore
from tg_agent.gateway.telegram import LiveTelegramAccess
from tg_agent.telegram.session_lock import release_all_session_locks

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
    access = LiveTelegramAccess(store, auth)
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
        assert len(auth.connections[0].handlers) == 1
        event = SimpleNamespace(chat_id=123, message=SimpleNamespace(
            id=7, raw_text="incoming DM", out=False, date=datetime.now(timezone.utc),
        ))
        await auth.connections[0].handlers[0](event)
        delivered = [await store.events(project, 0) for project in PROJECTS]
        assert delivered[0] == delivered[1]
        assert delivered[0][0]["payload"]["text"] == "incoming DM"
        assert notified == [account]
        assert (await access.connection(PROJECTS[0], account)).raw_client is sessions[0].raw_client
        assert len(auth.connections) == 1

        await access.close()

        assert auth.connections[0].disconnected
    finally:
        await access.close()
        await store.close()
        release_all_session_locks()


async def test_cold_account_connection_respects_a_pending_flood_write(tmp_path, monkeypatch):
    monkeypatch.setenv("TGCF_SESSION_LOCK_DIR", str(tmp_path / "locks"))
    store = GatewayStore(tmp_path / "gateway")
    await store.open()
    auth = LocalAuth()
    access = LiveTelegramAccess(store, auth)
    project = PROJECTS[0]
    try:
        await store.register_project(project, tmp_path / project, "a" * 40)
        ready = await store.import_account("+100", "ready-session")
        waiting = await store.import_account("+200", "waiting-session")
        await store.grant(project, ready)
        await store.grant(project, waiting)
        await access.connection(project, ready)
        access._pool._rpc_pending_account_deadlines = {
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
