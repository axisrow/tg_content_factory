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


@pytest.mark.aiosqlite_serial
async def test_dm_fanout_commits_locally_before_ack_and_deduplicates(gateway, tmp_path, monkeypatch):
    from tg_agent.config import AppConfig
    from tg_agent.gateway.dm_consumer import GatewayDmConsumer
    from tg_agent.gateway.pool import GatewayPool

    store, _, account, clients = gateway
    databases = [Database(str(tmp_path / f"dm-{index}.db")) for index in range(2)]
    try:
        for db in databases:
            await db.initialize()
        await store.record_event(account, "123:9:0", {"dialog_id": 123, "message_id": 9,
            "text": "hello both projects", "out": False, "date": "2026-10-29T00:00:00Z"}, now=time.time())
        for db, client in zip(databases, clients, strict=True):
            pool = GatewayPool(AppConfig(), db, client=client)
            consumer = GatewayDmConsumer(pool, db)
            original = client.acknowledge

            async def acknowledge(event, original=original, db=db):
                assert await db.repos.dm_messages.count() == 1
                assert int(await db.get_setting("gateway_event_cursor")) == event
                await original(event)

            monkeypatch.setattr(client, "acknowledge", acknowledge)
            await consumer.consume()
            # Simulate an old local cursor after redelivery; the unique key is authoritative.
            await db.set_setting("gateway_event_cursor", "0")
            await consumer.consume()
            assert await db.repos.dm_messages.count() == 1
    finally:
        for db in databases:
            await db.close()


@pytest.mark.aiosqlite_serial
async def test_pipeline_publication_replays_saved_intent_after_pool_recreation(gateway, tmp_path):
    from tg_agent.config import AppConfig
    from tg_agent.gateway.pool import GatewayPool
    from tg_agent.models import GenerationRun, PipelineTarget
    from tg_agent.services.publish_service import PublishService

    _, transport, _, clients = gateway
    db = Database(str(tmp_path / "publication.db"))
    await db.initialize()
    pools = []
    try:
        run = GenerationRun(id=42, generated_text="persisted publication")
        target = PipelineTarget(pipeline_id=1, phone="+100", dialog_id=123)
        for _ in range(2):
            pool = GatewayPool(AppConfig(), db, client=clients[0])
            await pool.initialize()
            pools.append(pool)
            result = await PublishService(db, pool)._publish_to_target(run, target)
            assert result.success and result.message_id == 1
        assert transport.sends == ["persisted publication"]
    finally:
        for pool in pools:
            await pool.disconnect_all()
        await db.close()


async def test_identified_artifact_retry_retains_filename_and_rejects_changed_content(gateway):
    store, _, _, clients = gateway
    client = clients[0]
    artifact = uuid4()
    headers = {"X-Filename": "../photo.jpg", "Content-Type": "image/jpeg"}
    first = await client.http.put(f"/v1/artifacts/{artifact}", content=b"jpeg", headers=headers)
    assert first.status_code == 201
    assert (await client.http.put(f"/v1/artifacts/{artifact}", content=b"jpeg", headers=headers)).status_code == 201
    assert (await client.http.put(f"/v1/artifacts/{artifact}", content=b"changed", headers=headers)).status_code == 409
    response = await client.http.get(f"/v1/artifacts/{artifact}")
    assert response.content == b"jpeg"
    assert 'filename="photo.jpg"' in response.headers["content-disposition"]
    assert response.headers["content-type"] == "image/jpeg"
    assert (await clients[1].http.get(f"/v1/artifacts/{artifact}")).status_code == 404
    assert not list(store.root.rglob("*.tmp"))


async def test_auth_does_not_return_pending_or_authorized_session(gateway, monkeypatch):
    from types import SimpleNamespace

    store, transport, _, clients = gateway

    class Login:
        def __init__(self, *args, **kwargs):
            pass

        async def send_code(self, phone):
            return {"phone_code_hash": "hash", "session_str": "pending-secret", "code_type": "Telegram"}

        async def verify_code(self, phone, code, code_hash, password):
            assert (code, code_hash, password) == ("12345", "hash", None)
            return "authorized-session-secret"

        async def cleanup(self):
            pass

    transport.auth = SimpleNamespace(api_id=1, api_hash="hash", proxy=None)
    monkeypatch.setattr("tg_agent.gateway.auth.TelegramAuth", Login)
    client = clients[0]
    response = await client.http.post("/v1/auth", json={"action": "send_code", "phone": "+200"})
    assert response.status_code == 200 and "secret" not in response.text
    response = await client.http.post("/v1/auth", json={"action": "verify_code", "phone": "+200",
                                                       "code": "12345", "phone_code_hash": "hash"})
    assert response.status_code == 200 and "secret" not in response.text
    assert "+200" in {row["phone"] for row in await client.accounts()}
    assert "+200" not in {row["phone"] for row in await clients[1].accounts()}
    account = await store.account_for_phone("+200")
    project = await store.authenticate(client.http.headers["Authorization"].removeprefix("Bearer "))
    assert await store.session(project, account) == "authorized-session-secret"


@pytest.mark.aiosqlite_serial
async def test_expired_dm_journal_recovers_history_before_advancing_cursor(gateway, tmp_path, monkeypatch):
    from tg_agent.config import AppConfig
    from tg_agent.gateway.dm_consumer import GatewayDmConsumer
    from tg_agent.gateway.pool import GatewayPool

    store, transport, account, clients = gateway
    await store.record_event(account, "123:4:0", {"dialog_id": 123, "message_id": 4, "out": False,
        "text": "expired event", "date": "2026-10-20T00:00:00Z"}, now=time.time() - 8 * 86400)

    async def execute(project, operation):
        if operation.kind == "dialogs":
            return {"dialogs": [{"channel_id": 123, "channel_type": "dm"}]}
        assert operation.kind == "dm_history"
        return {"messages": [{"dialog_id": 123, "message_id": 4, "out": False, "text": "recovered from Telegram",
                              "date": "2026-10-20T00:00:00Z"}] if operation.after_id < 4 else []}

    monkeypatch.setattr(transport, "execute", execute)
    db = Database(str(tmp_path / "dm-recovery.db"))
    await db.initialize()
    pool = GatewayPool(AppConfig(), db, client=clients[0])
    try:
        await pool.initialize()
        consumer = GatewayDmConsumer(pool, db)
        await consumer.consume()
        assert await db.repos.dm_messages.count() == 1
        assert int(await db.get_setting("gateway_event_cursor")) == await store.event_floor()
        async with db.db.execute("SELECT text,processed FROM incoming_dms") as cursor:
            assert tuple(await cursor.fetchone()) == ("recovered from Telegram", 1)
        await consumer.consume()
        assert await db.repos.dm_messages.count() == 1
        assert consumer.status()["resync_required"] is False
    finally:
        await pool.disconnect_all()
        await db.close()


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


async def test_large_result_pages_remain_project_scoped(gateway, monkeypatch):
    from tg_agent.gateway.protocol import Dialogs

    _, transport, account, clients = gateway

    async def execute(project, operation):
        return {"dialogs": [{"channel_id": index, "title": str(index)} for index in range(250)]}

    monkeypatch.setattr(transport, "execute", execute)
    request = uuid4()
    await clients[0].submit(request, Dialogs(kind="dialogs", account_id=UUID(account)))
    completed = await clients[0].wait(request)
    assert len(completed["result"]["dialogs"]) == 250
    raw = await clients[0].http.get(f"/v1/operations/{request}")
    assert len(raw.json()["result"]["dialogs"]) == 100
    page = await clients[0].http.get(f"/v1/operations/{request}/results/dialogs", params={"offset": 100})
    assert len(page.json()["items"]) == 100 and page.json()["items"][0]["channel_id"] == 100
    assert (await clients[1].http.get(f"/v1/operations/{request}/results/dialogs")).status_code == 404


@pytest.mark.aiosqlite_serial
async def test_read_rotation_uses_only_granted_accounts(gateway, tmp_path, monkeypatch):
    from tg_agent.config import AppConfig
    from tg_agent.gateway.pool import GatewayPool

    store, transport, first, clients = gateway
    project = await store.authenticate(clients[0].http.headers["Authorization"].removeprefix("Bearer "))
    second = await store.import_account("+200", "second-session")
    forbidden = await store.import_account("+300", "forbidden-session")
    await store.grant(project, second)
    seen = []

    async def execute(project, operation):
        seen.append(str(operation.account_id))
        if str(operation.account_id) == first:
            raise ValueError("entity unavailable on the primary")
        return {"messages": [{"message_id": 7, "channel_id": 123, "text": "from the granted fallback",
                              "date": "2026-10-29T00:00:00Z"}], "next_id": 7}

    monkeypatch.setattr(transport, "execute", execute)
    db = Database(str(tmp_path / "rotation.db"))
    await db.initialize()
    pool = GatewayPool(AppConfig(), db, client=clients[0])
    try:
        await pool.initialize()
        result = await pool.read_request(History(kind="history", account_id=UUID(first), peer=123),
                                         "collection-page", rotate=True)
        assert result["messages"][0]["text"] == "from the granted fallback"
        assert seen == [first, second] and forbidden not in seen
    finally:
        await pool.disconnect_all()
        await db.close()


async def test_gateway_auth_preserves_two_factor_challenge(gateway, monkeypatch):
    from types import SimpleNamespace

    from tg_agent.gateway.pool import GatewayPool
    from tg_shared.auth import TwoFactorRequiredError

    _, transport, _, clients = gateway

    class Login:
        def __init__(self, *args, **kwargs):
            pass

        async def verify_code(self, phone, code, code_hash, password):
            raise TwoFactorRequiredError()

        async def cleanup(self):
            pass

    transport.auth = SimpleNamespace(api_id=1, api_hash="hash", proxy=None)
    monkeypatch.setattr("tg_agent.gateway.auth.TelegramAuth", Login)
    pool = GatewayPool(None, None, client=clients[0])
    with pytest.raises(TwoFactorRequiredError):
        await pool.authenticate("verify_code", {"phone": "+200", "code": "12345", "phone_code_hash": "hash"})
    from tg_agent.services.telegram_command_dispatcher import TelegramCommandDispatcher

    dispatcher = object.__new__(TelegramCommandDispatcher)
    dispatcher._pool = pool
    payload = {"phone": "+200", "code": "12345", "phone_code_hash": "hash", "password_2fa": "test-secret"}
    with pytest.raises(TwoFactorRequiredError):
        await dispatcher._dispatch("auth.verify_code", payload)
    assert payload["password_2fa"] == payload["code"] == ""
