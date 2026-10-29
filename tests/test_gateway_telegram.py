"""Exercise the production connection cache with a local transport double."""

import asyncio
import secrets
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

from tg_agent.gateway.protocol import Send
from tg_agent.gateway.store import GatewayStore
from tg_agent.gateway.telegram import LiveTelegramAccess
from tg_agent.telegram.session_lock import release_all_session_locks


class LocalClient:
    def __init__(self):
        self.sent = []
        self.disconnected = False
        self.handlers = []

    def add_event_handler(self, callback, builder):
        self.handlers.append(callback)

    async def get_entity(self, peer):
        return SimpleNamespace(id=peer)

    async def send_message(self, entity, text, **kwargs):
        self.sent.append((entity.id, text))
        return SimpleNamespace(id=len(self.sent))

    async def disconnect(self):
        self.disconnected = True


class LocalAuth:
    def __init__(self):
        self.connections = []

    async def create_client_from_session(self, session):
        client = LocalClient()
        self.connections.append(client)
        return client

    async def cleanup(self):
        pass


async def test_two_projects_borrow_one_live_connection(tmp_path, monkeypatch):
    monkeypatch.setenv("TGCF_SESSION_LOCK_DIR", str(tmp_path / "locks"))
    store = GatewayStore(tmp_path / "gateway")
    await store.open()
    auth = LocalAuth()
    access = LiveTelegramAccess(store, auth)
    try:
        account = await store.import_account("+100", "local-transport-session")
        projects = [str(uuid4()), str(uuid4())]
        for project in projects:
            await store.register_project(project, tmp_path / project, secrets.token_urlsafe(32))
            await store.grant(project, account)
        operation = Send(kind="send", account_id=UUID(account), peer=123, text="message")
        results = await asyncio.gather(*(access.execute(project, operation) for project in projects))
        assert len(auth.connections) == 1
        assert len(auth.connections[0].sent) == 2
        assert len(auth.connections[0].handlers) == 1
        event = SimpleNamespace(chat_id=123, message=SimpleNamespace(
            id=7, raw_text="incoming DM", out=False, date=datetime.now(timezone.utc),
        ))
        await auth.connections[0].handlers[0](event)
        delivered = [await store.events(project, 0) for project in projects]
        assert delivered[0] == delivered[1]
        assert delivered[0][0]["payload"]["text"] == "incoming DM"
        assert {result["message_id"] for result in results} == {1, 2}
        assert not auth.connections[0].disconnected
        await access.close()
        assert auth.connections[0].disconnected
    finally:
        await access.close()
        await store.close()
        release_all_session_locks()
