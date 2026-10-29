"""Exercise the production connection cache with a local transport double."""

import asyncio
import secrets
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

from tg_agent.gateway.artifacts import ArtifactStore
from tg_agent.gateway.protocol import ACTION_MODELS, MessagePage, Profile, Publish, Send
from tg_agent.gateway.store import GatewayStore
from tg_agent.gateway.telegram import LiveTelegramAccess
from tg_agent.telegram.session_lock import release_all_session_locks


class LocalClient:
    def __init__(self):
        self.sent = []
        self.disconnected = False
        self.handlers = []
        self.files = []

    def add_event_handler(self, callback, builder):
        self.handlers.append(callback)

    async def get_entity(self, peer):
        return SimpleNamespace(id=getattr(peer, "channel_id", peer))

    get_input_entity = get_entity

    async def get_me(self):
        return SimpleNamespace(id=10, first_name="Shared", last_name="Account", username="shared", premium=True)

    async def download_profile_photo(self, entity, *, file):
        return b"image"

    async def send_file(self, entity, files, **kwargs):
        self.files.append(([Path(path).name for path in files], kwargs))
        return [SimpleNamespace(id=20 + index) for index, _ in enumerate(files)]

    async def iter_messages(self, entity, **kwargs):
        class Message(SimpleNamespace):
            def __getattr__(self, name):
                return None

        yield Message(id=7, text="shared source message", raw_text="shared source message",
                      date=datetime.now(timezone.utc), views=3, peer_id=SimpleNamespace(channel_id=123))

    def is_connected(self):
        return not self.disconnected

    async def send_message(self, entity, text, **kwargs):
        self.sent.append((entity.id, text))
        return SimpleNamespace(id=len(self.sent))

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
        action = ACTION_MODELS["send_message"](account_id=UUID(account),
                                                arguments={"recipient": "@source", "text": "business action"})
        response = await access.execute(projects[0], action)
        assert response["message_id"] == 3
        assert len(auth.connections) == 1
        profile = await access.execute(projects[0], Profile(kind="profile", account_id=UUID(account),
                                                           include_avatar=True))
        assert profile["user_id"] == 10 and profile["is_premium"]
        page = await access.execute(projects[1], MessagePage(kind="message_page", account_id=UUID(account), peer=123,
                                                            peer_type="channel"))
        assert page["messages"][0]["text"] == "shared source message"
        assert page["messages"][0]["channel_id"] == 123
        storage, artifact, temporary = ArtifactStore(store.root), uuid4(), uuid4()
        with storage.create(projects[0], temporary) as handle:
            handle.write(b"image")
        storage.finish(projects[0], artifact, storage.path(projects[0], temporary),
                       filename="photo.png", content_type="image/png", digest="test")
        published = await access.execute(projects[0], Publish(kind="publish", account_id=UUID(account), peer=123,
            peer_type="channel", artifact_ids=[artifact], reply_to=7, force_document=True))
        assert published == {"message_ids": [20]}
        assert auth.connections[0].files[0][0] == ["0-photo.png"]
        assert auth.connections[0].files[0][1]["reply_to"] == 7
        assert auth.connections[0].files[0][1]["force_document"] is True
        assert len(auth.connections) == 1
        borrowed = await access._pool.get_native_client_by_phone(account)
        assert borrowed is not None
        await access._pool.release_client(account)
        assert len(auth.connections) == 1
        await access.close()
        assert auth.connections[0].disconnected
    finally:
        await access.close()
        await store.close()
        release_all_session_locks()


async def test_broadcast_percent_values_are_normalized_before_crossing_gateway(monkeypatch):
    import json

    from tg_agent.gateway.actions import execute_action
    from tg_agent.services.telegram_actions import BroadcastStatsResult, TelegramActionService

    account = str(uuid4())

    class Access:
        auth = None
        _pool = SimpleNamespace(clients={account: object()})

        async def connection(self, project, selected):
            assert selected == account

    async def broadcast(self, *, phone, chat_id) -> BroadcastStatsResult:
        return BroadcastStatsResult(phone=phone, stats=SimpleNamespace(
            followers=SimpleNamespace(current=10, previous=8),
            enabled_notifications=SimpleNamespace(part=1.0, total=2.0)))

    monkeypatch.setattr(TelegramActionService, "get_broadcast_stats", broadcast)
    operation = ACTION_MODELS["get_broadcast_stats"](account_id=UUID(account), arguments={"chat_id": 123})
    result = await execute_action(Access(), "project", operation)
    encoded = json.loads(json.dumps(result))
    assert encoded["stats"]["enabled_notifications"] == {"part": 1.0, "total": 2.0}
