"""Two real HTTP clients, isolated project DBs, one injected Telegram transport."""

import asyncio
import secrets
import time
from uuid import UUID, uuid4

import httpx
import pytest

from tg_agent.database import Database
from tg_agent.gateway.client import TelegramGatewayClient
from tg_agent.gateway.outbox import ProjectOutbox
from tg_agent.gateway.protocol import History, Send
from tg_agent.gateway.server import create_app
from tg_agent.gateway.store import GatewayStore
from tg_agent.models import Message


class Transport:
    def __init__(self):
        self.connections = set()
        self.sends = []
        self.fail_after_send = False
        self.block = asyncio.Event()

    async def execute(self, project, operation):
        self.connections.add(str(operation.account_id))
        if isinstance(operation, History):
            return {"messages": [{"channel_id": 123, "message_id": operation.after_id + 1,
                                  "text": f"post for {project}", "date": "2026-10-29T00:00:00Z"}]}
        self.sends.append(operation.text)
        if self.fail_after_send:
            raise ConnectionError("potentially sensitive upstream error")
        return {"message_id": len(self.sends)}

    async def close(self):
        pass


@pytest.fixture
async def gateway(tmp_path):
    store = GatewayStore(tmp_path / "gateway")
    await store.open()
    transport = Transport()
    account = await store.import_account("+100", "test-session")
    tokens = [secrets.token_urlsafe(32), secrets.token_urlsafe(32)]
    for token in tokens:
        project = str(uuid4())
        await store.register_project(project, tmp_path / project, token)
        await store.grant(project, account, primary=True)
    app = create_app(store, transport)
    clients = [TelegramGatewayClient("http://gateway", token, transport=httpx.ASGITransport(app=app))
               for token in tokens]
    try:
        async with app.router.lifespan_context(app):
            yield store, transport, account, clients
    finally:
        for client in clients:
            await client.close()
        await store.close()


async def test_two_projects_collect_into_distinct_databases(gateway, tmp_path):
    _, transport, account, clients = gateway
    databases = [Database(str(tmp_path / f"project-{index}.db")) for index in range(2)]
    try:
        for db in databases:
            await db.initialize()
        requests = [uuid4(), uuid4()]
        for client, request in zip(clients, requests, strict=True):
            await client.submit(request, History(kind="history", account_id=UUID(account), peer=123))
        receipts = await asyncio.gather(*(client.wait(request, timeout=5)
                                         for client, request in zip(clients, requests, strict=True)))
        for db, receipt in zip(databases, receipts, strict=True):
            assert receipt["state"] == "succeeded"
            messages = [Message.model_validate(row) for row in receipt["result"]["messages"]]
            assert await db.insert_messages_batch(messages) == 1
            assert await db.insert_messages_batch(messages) == 0
        texts = []
        for db in databases:
            async with db.db.execute("SELECT text FROM messages") as cur:
                texts.append((await cur.fetchone())[0])
        assert texts[0] != texts[1]
        assert transport.connections == {account}
    finally:
        for db in databases:
            await db.close()


async def test_write_replay_and_uncertainty(gateway):
    _, transport, account, clients = gateway
    client = clients[0]
    request = uuid4()
    operation = Send(kind="send", account_id=UUID(account), peer=123, text="hello")
    transport.fail_after_send = True
    await client.submit(request, operation)
    receipt = await client.wait(request, timeout=5)
    assert receipt["state"] == "uncertain"
    assert "sensitive" not in str(receipt)
    assert (await client.submit(request, operation))["state"] == "uncertain"
    assert transport.sends == ["hello"]
    with pytest.raises(httpx.HTTPStatusError) as error:
        await clients[1].receipt(request)
    assert error.value.response.status_code == 404
    with pytest.raises(httpx.HTTPStatusError) as error:
        await client.submit(request, operation.model_copy(update={"text": "changed"}))
    assert error.value.response.status_code == 409


async def test_tokens_permissions_and_payload_validation(gateway):
    store, _, account, clients = gateway
    client = clients[0]
    with pytest.raises(httpx.HTTPStatusError) as error:
        await client.submit(uuid4(), History(kind="history", account_id=uuid4(), peer=123))
    assert error.value.response.status_code == 403
    response = await client.http.post("/v1/operations", json={"request_id": str(uuid4()), "operation": {
        "kind": "invoke_request", "account_id": account, "request": "anything"}})
    assert response.status_code == 422
    client.http.headers["Authorization"] = "Bearer invalid"
    response = await client.http.get("/v1/status")
    assert response.status_code == 401
    assert await store.pending() == []


async def test_outbox_recovers_same_intent_after_project_restart(gateway, tmp_path):
    _, transport, account, clients = gateway
    operation = Send(kind="send", account_id=UUID(account), peer=123, text="persisted intent")
    path = str(tmp_path / "project.db")
    db = Database(path)
    await db.initialize()
    try:
        outbox = ProjectOutbox(db)
        await outbox.initialize()
        first = await outbox.execute(clients[0], "publish:run-1:target-123", operation)
    finally:
        await db.close()
    db = Database(path)
    await db.initialize()
    try:
        outbox = ProjectOutbox(db)
        await outbox.initialize()
        second = await outbox.execute(clients[0], "publish:run-1:target-123", operation)
        assert first == second
        assert transport.sends == ["persisted intent"]
        with pytest.raises(ValueError, match="different intent"):
            await outbox.prepare("publish:run-1:target-123", operation.model_copy(update={"text": "other"}))
    finally:
        await db.close()


async def test_events_fan_out_and_automatic_reply_has_one_owner(gateway):
    store, transport, account, clients = gateway
    projects = [await store.authenticate(client.http.headers["Authorization"].removeprefix("Bearer "))
                for client in clients]
    now = time.time()
    await store.record_event(account, "123:1:in", {"text": "incoming", "dialog_id": 123}, now=now)
    await store.record_event(account, "123:1:in", {"text": "incoming", "dialog_id": 123}, now=now)
    first, second = await asyncio.gather(*(client.events() for client in clients))
    assert first == second
    assert len(first) == 1
    await clients[0].acknowledge(first[0]["id"])
    assert await clients[0].events(first[0]["id"]) == []
    assert await clients[1].events() == second
    await store.assign_reply_owner(projects[0], account, 123)
    operation = Send(kind="send", account_id=UUID(account), peer=123, text="auto reply", automatic=True)
    requests = [uuid4(), uuid4()]
    for client, request in zip(clients, requests, strict=True):
        await client.submit(request, operation)
    receipts = await asyncio.gather(*(client.wait(request, timeout=5)
                                     for client, request in zip(clients, requests, strict=True)))
    assert [receipt["state"] for receipt in receipts] == ["succeeded", "failed"]
    assert transport.sends == ["auto reply"]
    await store.expire_events(now=now + 8 * 86400)
    with pytest.raises(httpx.HTTPStatusError) as error:
        await clients[1].events()
    assert error.value.response.status_code == 410


async def test_pending_cancellation_is_scoped_to_its_project(gateway):
    store, transport, account, clients = gateway
    request = uuid4()
    project = await store.authenticate(clients[0].http.headers["Authorization"].removeprefix("Bearer "))
    operation = Send(kind="send", account_id=UUID(account), peer=123, text="cancel me")
    # Persist before waking the dispatcher so cancellation deterministically wins.
    await store.submit(project, str(request), operation.model_dump(mode="json"))
    with pytest.raises(httpx.HTTPStatusError) as error:
        await clients[1].cancel(request)
    assert error.value.response.status_code == 404
    assert (await clients[0].cancel(request))["state"] == "cancelled"
    assert transport.sends == []


async def test_artifacts_are_accessible_only_to_the_uploading_project(gateway):
    _, _, _, clients = gateway
    uploaded = await clients[0].http.post("/v1/artifacts", content=b"local file contents")
    assert uploaded.status_code == 201
    artifact = uploaded.json()["artifact_id"]
    own = await clients[0].http.get(f"/v1/artifacts/{artifact}")
    assert own.content == b"local file contents"
    foreign = await clients[1].http.get(f"/v1/artifacts/{artifact}")
    assert foreign.status_code == 404


async def test_shared_flood_defers_both_projects_without_sending(gateway):
    store, transport, account, clients = gateway
    deadline = time.time() + 100
    await store.flood(account, deadline)
    operation = Send(kind="send", account_id=UUID(account), peer=123, text="later")
    requests = [uuid4(), uuid4()]
    for client, request in zip(clients, requests, strict=True):
        await client.submit(request, operation)
    receipts = await asyncio.gather(*(client.wait(request, timeout=5)
                                     for client, request in zip(clients, requests, strict=True)))
    assert [receipt["state"] for receipt in receipts] == ["deferred", "deferred"]
    assert all(receipt["result"]["retry_at"] == deadline for receipt in receipts)
    assert transport.sends == []
