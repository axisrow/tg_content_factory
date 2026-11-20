"""The shared gateway served over the session-owner relay (#1868).

Projects open one relay connection with their token: their Telegram requests run
on the gateway's connection of a granted account, and the control plane (catalog,
grants, logins, DM events, reply ownership) is made of relay operations. The
administrator token reaches only the ``admin.*`` operations. New DM events are
pushed to the project's DM consumer instead of being polled for.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from telethon import functions, types

from tests.test_relay import FakeOwnerClient
from tg_agent.gateway import dm_consumer
from tg_agent.gateway.catalog import GatewayAdministrator
from tg_agent.gateway.client import GatewayRequestError, TelegramGatewayClient
from tg_agent.gateway.dm_consumer import GatewayDmConsumer
from tg_agent.gateway.server import GatewayHub
from tg_agent.gateway.store import GatewayStore, private_key
from tg_agent.models import Account
from tg_agent.telegram import backends
from tg_agent.telegram.client_pool import ClientPool
from tg_agent.telegram.relay import (
    RELAY_PROTOCOL,
    RelayAccountUnavailableError,
    RelayConnection,
    RelayEndpoint,
    RelayServer,
    RelayTelegramClient,
    RelayUnauthorizedError,
)
from tg_shared.auth import TelegramAuth

pytestmark = pytest.mark.aiosqlite_serial

PROJECT_A = "11111111-1111-4111-8111-111111111111"
PROJECT_B = "22222222-2222-4222-8222-222222222222"
TOKEN_A = "a" * 40
TOKEN_B = "b" * 40
PHONE = "+10000000001"
OTHER = "+10000000002"


class Access:
    """The gateway's Telegram side: one fake owner connection per shared account."""

    def __init__(self):
        self.clients: dict[str, FakeOwnerClient] = {}
        self.dropped: list[str] = []
        self.on_event = None

    async def start(self):
        pass

    async def remove_account(self, account):
        self.dropped.append(account)

    async def connection(self, project, account):
        return SimpleNamespace(raw_client=self.clients.setdefault(account, FakeOwnerClient()))

    def account_status(self, account):
        return {"connected": account in self.clients}

    async def close(self):
        pass


@pytest.fixture
async def gateway(tmp_path):
    root = tmp_path / "shared"
    store = GatewayStore(root)
    admin_token = private_key(root, "gateway-admin.token")
    await store.open()
    access = Access()
    hub = GatewayHub(store, access, admin_token=admin_token)
    server = RelayServer(hub, ops=hub.ops)
    port = await server.start()
    await store.register_project(PROJECT_A, tmp_path / "a", TOKEN_A)
    await store.register_project(PROJECT_B, tmp_path / "b", TOKEN_B)
    account = await store.import_account(PHONE, "session-one")
    other = await store.import_account(OTHER, "session-two")
    await store.grant(PROJECT_A, account, primary=True)
    await store.grant(PROJECT_B, other)
    connections: list[RelayConnection] = []

    async def connect(token: str) -> RelayConnection:
        endpoint = RelayEndpoint(port=port, token=token, pid=os.getpid(), protocol=RELAY_PROTOCOL)
        connection = await RelayConnection.open(endpoint)
        connections.append(connection)
        return connection

    try:
        yield SimpleNamespace(store=store, hub=hub, access=access, connect=connect, admin_token=admin_token,
                              account=account, other=other)
    finally:
        for connection in connections:
            await connection.close()
        await server.stop()
        await hub.close()
        await store.close()


async def _telegram(connection: RelayConnection, phone: str) -> RelayTelegramClient:
    client = RelayTelegramClient(connection, phone, flood_sleep_threshold=0)
    await client.connect()
    return client


async def test_a_project_token_reaches_only_its_granted_accounts(gateway):
    connection = await gateway.connect(TOKEN_A)

    assert connection.phones == [PHONE]
    rows = await TelegramGatewayClient(connection).accounts()
    assert [(row["id"], row["phone"]) for row in rows] == [(gateway.account, PHONE)]

    state = await (await _telegram(connection, PHONE))(functions.updates.GetStateRequest())
    assert isinstance(state, types.updates.State)
    assert len(gateway.access.clients[gateway.account].calls) == 1
    with pytest.raises(RelayAccountUnavailableError):
        await (await _telegram(connection, OTHER))(functions.updates.GetStateRequest())
    assert gateway.other not in gateway.access.clients


async def test_a_disabled_grant_stops_requests_at_once(gateway):
    connection = await gateway.connect(TOKEN_A)
    telegram = await _telegram(connection, PHONE)
    await telegram(functions.updates.GetStateRequest())

    await TelegramGatewayClient(connection).update_account(gateway.account, active=False)

    with pytest.raises(RelayAccountUnavailableError):
        await telegram(functions.updates.GetStateRequest())


async def test_unknown_tokens_are_refused_at_handshake(gateway):
    with pytest.raises(RelayUnauthorizedError):
        await gateway.connect("z" * 40)


async def test_project_and_administrator_operations_are_separate(gateway):
    project = TelegramGatewayClient(await gateway.connect(TOKEN_A))
    with pytest.raises(GatewayRequestError) as refused:
        await project.call("admin.accounts.list")
    assert refused.value.status == 403

    admin_connection = await gateway.connect(gateway.admin_token)
    assert admin_connection.phones == []
    with pytest.raises(GatewayRequestError) as refused:
        await TelegramGatewayClient(admin_connection).accounts()
    assert refused.value.status == 403
    with pytest.raises(RelayAccountUnavailableError):
        await (await _telegram(admin_connection, PHONE))(functions.updates.GetStateRequest())


async def test_a_project_cannot_touch_another_projects_account(gateway):
    project = TelegramGatewayClient(await gateway.connect(TOKEN_A))

    with pytest.raises(GatewayRequestError) as refused:
        await project.export_session(gateway.other)
    assert refused.value.status == 403
    with pytest.raises(GatewayRequestError) as refused:
        await project.clear_flood(gateway.other)
    assert refused.value.status == 403
    assert await project.export_session(gateway.account) == "session-one"


async def test_new_dm_events_are_pushed_to_subscribed_projects(gateway):
    project_a = TelegramGatewayClient(await gateway.connect(TOKEN_A))
    project_b = TelegramGatewayClient(await gateway.connect(TOKEN_B))
    pushed = asyncio.Event()
    await project_a.subscribe_events(pushed.set)
    payload = {"kind": "dm", "dialog_id": 5, "message_id": 9, "text": "hi", "out": False, "date": None}

    await gateway.store.record_event(gateway.account, "5:9:0", payload, now=time.time())
    await gateway.access.on_event(gateway.account)  # what LiveTelegramAccess does after journaling

    async with asyncio.timeout(5):
        await pushed.wait()
    events = await project_a.events(0)
    assert [(event["account_id"], event["payload"]["text"]) for event in events] == [(gateway.account, "hi")]
    assert await project_b.events(0) == []
    await project_a.acknowledge(events[0]["id"])


async def test_dm_pushes_skip_projects_without_a_grant_on_the_account(gateway):
    project_b = TelegramGatewayClient(await gateway.connect(TOKEN_B))
    pushed = asyncio.Event()
    await project_b.subscribe_events(pushed.set)
    payload = {"kind": "dm", "dialog_id": 5, "message_id": 9, "text": "hi", "out": False, "date": None}
    await gateway.store.record_event(gateway.account, "5:9:0", payload, now=time.time())

    await gateway.access.on_event(gateway.account)  # an account granted only to project A
    await asyncio.sleep(0.1)
    assert not pushed.is_set()

    await gateway.access.on_event(gateway.other)  # project B's own account
    async with asyncio.timeout(5):
        await pushed.wait()


async def test_an_expired_event_cursor_asks_for_history_recovery(gateway):
    project = TelegramGatewayClient(await gateway.connect(TOKEN_A))
    payload = {"kind": "dm", "dialog_id": 5, "message_id": 1, "text": "old", "out": False, "date": None}
    await gateway.store.record_event(gateway.account, "5:1:0", payload, now=time.time() - 8 * 86400)

    with pytest.raises(GatewayRequestError) as expired:
        await project.events(0)

    assert expired.value.status == 410
    assert await project.event_floor() == 1


async def test_automatic_replies_have_one_owner_per_dialog(gateway):
    await gateway.store.grant(PROJECT_B, gateway.account)
    project_a = TelegramGatewayClient(await gateway.connect(TOKEN_A))
    project_b = TelegramGatewayClient(await gateway.connect(TOKEN_B))

    assert await project_a.reply_owner(gateway.account, 77) is True
    with pytest.raises(GatewayRequestError) as taken:
        await project_b.reply_owner(gateway.account, 77)
    assert taken.value.status == 409
    assert await project_a.can_auto_reply(gateway.account, 77)
    assert not await project_b.can_auto_reply(gateway.account, 77)

    await project_a.reply_owner(gateway.account, 77, release=True)
    assert await project_b.reply_owner(gateway.account, 77) is True


async def test_the_administrator_imports_grants_and_retires_accounts(gateway):
    admin = GatewayAdministrator(await gateway.connect(gateway.admin_token))

    account = await admin.call("accounts.import", {"phone": "+10000000003", "session": "session-three"})
    assert await admin.call("grants", {"project_id": PROJECT_B, "phone": "+10000000003"}) == account
    assert {row["phone"] for row in await admin.call("accounts.list")} == {PHONE, OTHER, "+10000000003"}
    assert await admin.call("accounts.export", phone="+10000000003") == "session-three"
    project_b = await gateway.connect(TOKEN_B)
    assert set(project_b.phones) == {OTHER, "+10000000003"}

    await admin.call("accounts.update", {"phone": "+10000000003", "active": False})

    assert account in gateway.access.dropped
    assert await project_b.refresh_phones() == [OTHER]
    with pytest.raises(GatewayRequestError) as invalid:
        await admin.call("grants", {"project_id": "not-a-uuid", "phone": OTHER})
    assert invalid.value.status == 422


async def test_a_project_pool_on_the_gateway_relay_runs_on_the_shared_connection(gateway, db, monkeypatch):
    async def no_lock(*_args, **_kwargs):
        raise AssertionError("a gateway project never takes the session lock")

    monkeypatch.setattr(backends, "acquire_session_lock", no_lock)
    me = types.User(id=4242, access_hash=1, first_name="Shared", is_self=True)
    gateway.access.clients[gateway.account] = FakeOwnerClient(lambda request: [me])
    await db.add_account(Account(phone=PHONE, session_string="", is_active=True))
    pool = ClientPool(TelegramAuth(1, "hash"), db, relay=await gateway.connect(TOKEN_A))
    try:
        await pool.initialize()
        raw = pool.clients[PHONE].raw_client
        assert isinstance(raw, RelayTelegramClient)
        assert (await raw.get_me()).id == 4242
    finally:
        await pool.disconnect_all()


async def _rows(db, sql, params=()):
    cursor = await db.db.execute(sql, params)
    return [tuple(row) for row in await cursor.fetchall()]


async def _acknowledged(store, project):
    async with store.transaction() as conn:
        async with conn.execute("SELECT event_id FROM acknowledgements WHERE project_id=?", (project,)) as cur:
            row = await cur.fetchone()
    return row[0] if row else None


async def test_the_project_dm_consumer_stores_pushed_events_at_once(gateway, db, monkeypatch):
    # Only the push can deliver within the test: the safety-net poll is a minute away.
    monkeypatch.setattr(dm_consumer, "_EVENT_POLL_SEC", 60.0)
    consumer = GatewayDmConsumer(None, db, TelegramGatewayClient(await gateway.connect(TOKEN_A)))
    idle = asyncio.Event()
    consume = consumer.consume

    async def consume_then_idle():
        await consume()
        idle.set()

    consumer.consume = consume_then_idle
    await consumer.start()
    try:
        async with asyncio.timeout(5):
            await idle.wait()
        payload = {"kind": "dm", "dialog_id": 5, "message_id": 9, "text": "hi", "out": False,
                   "date": "2026-11-01T10:00:00+00:00"}
        await gateway.store.record_event(gateway.account, "5:9:0", payload, now=time.time())
        await gateway.access.on_event(gateway.account)

        async with asyncio.timeout(5):
            while not await _rows(db, "SELECT 1 FROM incoming_dms"):
                await asyncio.sleep(0.02)
    finally:
        await consumer.stop()

    assert await _rows(db, "SELECT phone,chat_id,message_id,text,out FROM dm_messages") == [(PHONE, 5, 9, "hi", 0)]
    assert await _rows(db, "SELECT message_id,processed FROM incoming_dms") == [(9, 0)]
    assert await db.get_setting("gateway_event_cursor") == "1"
    assert await _acknowledged(gateway.store, PROJECT_A) == 1


async def test_an_expired_event_cursor_recovers_dm_history_on_the_relay(gateway, db, monkeypatch):
    old = {"kind": "dm", "dialog_id": 5, "message_id": 1, "text": "expired", "out": False, "date": None}
    await gateway.store.record_event(gateway.account, "5:1:0", old, now=time.time() - 8 * 86400)
    missed = SimpleNamespace(id=3, date=datetime.now(timezone.utc) - timedelta(minutes=5), text="missed", out=False)
    pages = [[missed], []]
    reads = []

    async def history(raw_client, *, api_id, api_hash, peer, min_id, limit):
        reads.append((raw_client, peer, min_id))
        return pages.pop(0)

    class Pool:
        released: list[str] = []

        async def get_dialogs_for_phone(self, phone, *, include_dm, refresh):
            return [{"channel_type": "dm", "dialog_id": 5, "channel_id": 5},
                    {"channel_type": "channel", "channel_id": 123}]

        async def get_client_by_phone(self, phone):
            return SimpleNamespace(raw_client="relayed-client"), phone

        async def release_client(self, phone):
            self.released.append(phone)

    monkeypatch.setattr(dm_consumer, "read_dialog_history_since", history)
    pool = Pool()
    consumer = GatewayDmConsumer(pool, db, TelegramGatewayClient(await gateway.connect(TOKEN_A)))

    await consumer.consume()

    assert reads == [("relayed-client", 5, 0), ("relayed-client", 5, 3)]
    assert pool.released == [PHONE]
    # The default catch-up mode journals recovered messages without replying to them.
    assert await _rows(db, "SELECT message_id,text,processed FROM incoming_dms") == [(3, "missed", 1)]
    assert await db.get_setting("gateway_event_cursor") == "1"
    assert await _acknowledged(gateway.store, PROJECT_A) == 1
    assert consumer.status()["resync_required"] is False


class _Login:
    """A Telegram login that hands out secrets the gateway must never relay."""

    def __init__(self, *args, **kwargs):
        pass

    async def send_code(self, phone):
        return {"phone_code_hash": "hash", "session_str": "pending-secret", "code_type": "Telegram"}

    async def verify_code(self, phone, code, code_hash, password):
        assert (code, code_hash, password) == ("12345", "hash", None)
        return "authorized-session-secret"

    async def cleanup(self):
        pass


async def test_logins_run_in_the_gateway_and_never_return_sessions(gateway, monkeypatch):
    gateway.access.auth = SimpleNamespace(api_id=1, api_hash="hash", proxy=None)
    monkeypatch.setattr("tg_agent.gateway.auth.TelegramAuth", _Login)
    project_a = TelegramGatewayClient(await gateway.connect(TOKEN_A))
    project_b = TelegramGatewayClient(await gateway.connect(TOKEN_B))

    sent = await project_a.auth("send_code", {"phone": "+200"})
    verified = await project_a.auth("verify_code", {"phone": "+200", "code": "12345", "phone_code_hash": "hash"})

    assert "secret" not in repr(sent) and "secret" not in repr(verified)
    assert "+200" in {row["phone"] for row in await project_a.accounts()}
    assert "+200" not in {row["phone"] for row in await project_b.accounts()}
    account = await gateway.store.account_for_phone("+200")
    assert await gateway.store.session(PROJECT_A, account) == "authorized-session-secret"


async def test_a_two_factor_challenge_reaches_the_project_and_secrets_are_cleared(gateway, monkeypatch):
    from tg_agent.services.telegram_command_dispatcher import TelegramCommandDispatcher
    from tg_shared.auth import TwoFactorRequiredError

    class TwoFactorLogin(_Login):
        async def verify_code(self, phone, code, code_hash, password):
            raise TwoFactorRequiredError()

    gateway.access.auth = SimpleNamespace(api_id=1, api_hash="hash", proxy=None)
    monkeypatch.setattr("tg_agent.gateway.auth.TelegramAuth", TwoFactorLogin)
    client = TelegramGatewayClient(await gateway.connect(TOKEN_A))
    with pytest.raises(TwoFactorRequiredError):
        await client.auth("verify_code", {"phone": "+200", "code": "12345", "phone_code_hash": "hash"})

    dispatcher = object.__new__(TelegramCommandDispatcher)
    dispatcher._gateway = client
    payload = {"phone": "+200", "code": "12345", "phone_code_hash": "hash", "password_2fa": "test-secret"}
    with pytest.raises(TwoFactorRequiredError):
        await dispatcher._dispatch("auth.verify_code", payload)
    assert payload["password_2fa"] == payload["code"] == ""


async def test_connecting_a_project_imports_older_bots_once(gateway, db, monkeypatch):
    from tg_agent.gateway import catalog
    from tg_agent.models import NotificationBot

    bots = [
        NotificationBot(tg_user_id=10, bot_username="legacy_bot", bot_token="123456789:moved-token"),
        NotificationBot(tg_user_id=20, bot_username="older_bot", bot_token="123456789:older-token"),
    ]
    async with gateway.store.transaction() as conn:
        await conn.executemany(
            "INSERT INTO gateway_project_bots VALUES (?,?,?)",
            [(PROJECT_A, bot.tg_user_id, gateway.store.cipher.encrypt(bot.model_dump_json())) for bot in bots],
        )
    await db.repos.notification_bots.save_bot(NotificationBot(tg_user_id=10, bot_username="legacy_bot",
                                                              bot_token=""))
    await db.repos.notification_bots.save_bot(
        NotificationBot(tg_user_id=20, bot_username="current_bot", bot_token="123456789:current-token"))

    async def from_config(cls, config):
        return TelegramGatewayClient(await gateway.connect(TOKEN_A))

    monkeypatch.setattr(TelegramGatewayClient, "from_config", classmethod(from_config))
    config = SimpleNamespace(gateway={"project_id": PROJECT_A})
    await asyncio.gather(catalog.connect_project(config, db), catalog.connect_project(config, db))

    assert (await db.repos.notification_bots.get_bot(10)).bot_token == "123456789:moved-token"
    assert (await db.repos.notification_bots.get_bot(20)).bot_token == "123456789:current-token"
    assert [row.phone for row in await db.get_account_summaries(active_only=False)] == [PHONE]
    await db.repos.notification_bots.delete_bot(10)
    client = await catalog.connect_project(config, db)

    assert await db.repos.notification_bots.get_bot(10) is None
    assert len(await client.export_bots()) == 2


async def test_failed_bot_import_rolls_back_and_retries(db):
    from tg_agent.gateway.catalog import restore_gateway_bots
    from tg_agent.models import NotificationBot

    bots = [
        NotificationBot(tg_user_id=user_id, bot_username="offline_bot", bot_token="123456789:offline-token")
        for user_id in (10, 20)
    ]
    async def export_bots():
        return [bot.model_dump(mode="json") for bot in bots]

    client = SimpleNamespace(export_bots=export_bots)
    await db.execute_write(
        "CREATE TRIGGER fail_second_bot BEFORE INSERT ON notification_bots WHEN NEW.tg_user_id=20 "
        "BEGIN SELECT RAISE(ABORT, 'offline test write failure'); END"
    )
    with pytest.raises(sqlite3.IntegrityError, match="offline test write failure"):
        await restore_gateway_bots(db, client, PROJECT_A)
    assert await db.repos.notification_bots.count() == 0
    await db.execute_write("DROP TRIGGER fail_second_bot")
    assert await restore_gateway_bots(db, client, PROJECT_A) == 2
    assert await db.repos.notification_bots.count() == 2


async def test_botfather_runs_on_the_gateway_connection_of_a_granted_account(gateway, monkeypatch):
    from tg_shared import botfather

    seen = []

    async def create_bot(client, name, username):
        seen.append(client)
        return "123456789:token"

    monkeypatch.setattr(botfather, "create_bot", create_bot)
    connection = await gateway.connect(TOKEN_A)

    assert await (await _telegram(connection, PHONE)).owner_create_bot("Name", "name_bot") == "123456789:token"
    assert seen == [gateway.access.clients[gateway.account]]
    with pytest.raises(RelayAccountUnavailableError):
        await (await _telegram(connection, OTHER)).owner_create_bot("Name", "name_bot")
