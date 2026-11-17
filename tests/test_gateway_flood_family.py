"""Read-side flood scoping in gateway mode (#1844) — offline.

#1834 scopes a read-side FloodWait to its family so that publishing from the
same account stays available. The scoping needs the pool's storage to keep
per-family deadlines; the gateway's ``GatewayPoolStorage`` had no such slice,
so in gateway mode every read flood still set the account-wide
``flood_until`` and blocked publishing for the whole wait.

The executed tests drive the real gateway entry point
``LiveTelegramAccess.execute`` over a REAL ``TelegramClient`` (never connected)
whose MTProto sender is a recording stub; FloodWait is injected at that sender.
"""

import secrets
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession
from telethon.tl.functions.channels import GetChannelsRequest
from telethon.tl.functions.messages import GetHistoryRequest, SendMessageRequest
from telethon.tl.types import Channel, UpdateShortSentMessage
from telethon.tl.types import messages as tl_messages
from telethon_floodgate import HandledFloodWaitError, TelegramRateLimitedError

from tests.helpers import sender_responder_to_send
from tg_agent.gateway.pool_storage import GatewayPoolStorage
from tg_agent.gateway.protocol import ACTION_MODELS, History, Send
from tg_agent.gateway.store import GatewayStore
from tg_agent.gateway.telegram import LiveTelegramAccess
from tg_agent.telegram.rpc_guard import dispose_rpc_guard
from tg_agent.telegram.flood_scope import family_floods_repo
from tg_agent.telegram.session_lock import release_all_session_locks

BARE = 1111111111


def _channel():
    return Channel(id=BARE, title="known", access_hash=5, photo=None, date=None)


def make_recording_client():
    """A real TelegramClient whose sender records requests and can answer FLOOD_WAIT."""
    from telethon import TelegramClient

    client = TelegramClient(StringSession(), 1, "x")
    dispatched = []
    flood_on = set()

    def send(request, ordered=False):
        dispatched.append(request)
        if type(request) in flood_on:
            raise FloodWaitError(request=request, capture=3600)
        if isinstance(request, GetChannelsRequest):
            return tl_messages.Chats(chats=[_channel()])
        if isinstance(request, GetHistoryRequest):
            return tl_messages.ChannelMessages(
                pts=1, count=0, messages=[], topics=[], chats=[], users=[]
            )
        if isinstance(request, SendMessageRequest):
            return UpdateShortSentMessage(
                id=77, date=datetime.now(timezone.utc), out=True, pts=1, pts_count=1
            )
        return None

    # The dispatch gate inspects sender.send's Future (MTProto contract).
    client._sender.send = sender_responder_to_send(send)
    # Offline: never attempt a real connect() from the pool's liveness check.
    client.is_connected = lambda: True
    client.session.process_entities([_channel()])
    return client, dispatched, flood_on


@pytest.fixture
async def gateway(tmp_path, monkeypatch):
    """A real gateway store + LiveTelegramAccess over one recording client."""
    monkeypatch.setenv("TGCF_SESSION_LOCK_DIR", str(tmp_path / "locks"))
    client, dispatched, flood_on = make_recording_client()

    class RecordingAuth:
        async def create_client_from_session(self, session, **kwargs):
            return client

        async def cleanup(self):
            pass

    auth = RecordingAuth()
    store = GatewayStore(tmp_path / "gateway")
    await store.open()
    opened = []

    def open_access():
        access = LiveTelegramAccess(store, auth)
        opened.append(access)
        return access

    access = open_access()
    account = await store.import_account("+100", "local-transport-session")
    project = str(uuid4())
    await store.register_project(project, tmp_path / project, secrets.token_urlsafe(32))
    await store.grant(project, account)
    try:
        yield SimpleNamespace(
            store=store, access=access, open_access=open_access, account=account, project=project,
            client=client, dispatched=dispatched, flood_on=flood_on,
        )
    finally:
        for item in opened:
            await item.close()
        await store.close()
        release_all_session_locks()


def _names(dispatched):
    return [type(request).__name__ for request in dispatched]


def _history(gw):
    return History(kind="history", account_id=gw.account, peer=BARE, limit=10)


async def _account_flood(storage):
    accounts = await storage.get_accounts()
    assert len(accounts) == 1
    return accounts[0].flood_wait_until


# ---------------------------------------------------------------------------
# The storage slice
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_gateway_storage_implements_the_family_flood_slice(tmp_path):
    """Same contract as AccountsRepository: set / list active / clear."""
    store = GatewayStore(tmp_path / "gateway")
    await store.open()
    try:
        storage = GatewayPoolStorage(store)
        await storage.initialize()
        now = datetime.now(timezone.utc)
        later = now + timedelta(hours=1)

        assert family_floods_repo(storage) is storage

        await storage.set_family_flood("acc-1", "history", later)
        await storage.set_family_flood("acc-1", "resolve", now - timedelta(seconds=5))
        await storage.set_family_flood("acc-2", "dialogs", later)

        floods = await storage.list_active_family_floods()
        assert set(floods) == {"acc-1", "acc-2"}
        # Expired deadlines are filtered out by the read.
        assert set(floods["acc-1"]) == {"history"}
        assert abs((floods["acc-1"]["history"] - later).total_seconds()) < 1
        assert floods["acc-1"]["history"].tzinfo is not None
        assert set(await storage.list_active_family_floods(phone="acc-2")) == {"acc-2"}
        # ``now`` moves the cut-off.
        assert await storage.list_active_family_floods(now=later + timedelta(seconds=1)) == {}

        # A later deadline for the same family replaces the earlier one.
        await storage.set_family_flood("acc-1", "history", later + timedelta(hours=1))
        floods = await storage.list_active_family_floods(phone="acc-1")
        assert floods["acc-1"]["history"] > later

        await storage.clear_family_floods("acc-2", family="history")
        assert "acc-2" in await storage.list_active_family_floods()
        await storage.clear_family_floods("acc-2", family="dialogs")
        await storage.clear_family_floods("acc-1")
        assert await storage.list_active_family_floods() == {}
    finally:
        await store.close()


# ---------------------------------------------------------------------------
# Executed through the gateway
# ---------------------------------------------------------------------------


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
async def test_history_flood_is_scoped_to_its_family_in_the_gateway(gateway):
    """A FloodWait on a history read leaves the account writable (#1832, gateway mode).

    Red before the fix: the account-wide ``flood_until`` is set, the send is
    refused with ``client unavailable`` and no family deadline exists.
    """
    gw = gateway
    gw.flood_on.add(GetHistoryRequest)
    with pytest.raises(HandledFloodWaitError):
        await gw.access.execute(gw.project, _history(gw))
    gw.flood_on.clear()
    storage = gw.access._pool._db

    assert await _account_flood(storage) is None
    floods = await storage.list_active_family_floods()
    assert set(floods[str(gw.account)]) == {"history"}

    # Publishing from the same account is still available...
    gw.dispatched.clear()
    send = ACTION_MODELS["send_message"](
        account_id=gw.account, arguments={"recipient": BARE, "text": "hi"}
    )
    result = await gw.access.execute(gw.project, send)
    assert result["message_id"] == 77
    assert _names(gw.dispatched) == ["SendMessageRequest"]

    # ...while a history read is refused before dispatch.
    gw.dispatched.clear()
    with pytest.raises(TelegramRateLimitedError) as refused:
        await gw.access.execute(gw.project, _history(gw))
    assert refused.value.category == "history"
    assert "GetHistoryRequest" not in _names(gw.dispatched)


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
async def test_send_flood_stays_account_wide_in_the_gateway(gateway):
    """Guard: Telegram refusing a write still blocks the whole account."""
    gw = gateway
    gw.flood_on.add(SendMessageRequest)
    send = Send(kind="send", account_id=gw.account, peer=BARE, peer_type="channel", text="hi")
    with pytest.raises(HandledFloodWaitError):
        await gw.access.execute(gw.project, send)
    storage = gw.access._pool._db

    assert await _account_flood(storage) is not None
    lister = getattr(storage, "list_active_family_floods", None)
    assert lister is None or await lister() == {}


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
async def test_family_flood_survives_a_gateway_restart_and_clears_with_the_account(gateway):
    """The deadline lives in the gateway store; clearing the account's flood clears it."""
    gw = gateway
    gw.flood_on.add(GetHistoryRequest)
    with pytest.raises(HandledFloodWaitError):
        await gw.access.execute(gw.project, _history(gw))
    gw.flood_on.clear()
    await gw.access.close()
    # A real restart discards the old client (and its guard) entirely; the
    # recording client is reused, so release its guard explicitly.
    await dispose_rpc_guard(gw.client)

    restarted = gw.open_access()
    gw.dispatched.clear()
    with pytest.raises(TelegramRateLimitedError):
        await restarted.execute(gw.project, _history(gw))
    assert "GetHistoryRequest" not in _names(gw.dispatched)

    await restarted._pool.clear_flood(str(gw.account))
    assert await restarted._pool._db.list_active_family_floods() == {}
    # The recording client is reused across the "restart"; a real one starts
    # without Telethon's own per-request flood memory.
    gw.client._flood_waited_requests.clear()
    result = await restarted.execute(gw.project, _history(gw))
    assert result["messages"] == []


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
async def test_resolve_family_flood_leaves_typed_sends_to_known_peers_available(gateway):
    """A typed send to a peer in the session store needs no resolve, flooded or not (#1842)."""
    gw = gateway
    await gw.access.connection(gw.project, str(gw.account))
    await gw.access._pool.report_family_flood(
        str(gw.account), 3600, "resolve", operation="telegram_resolve_input_entity"
    )
    gw.dispatched.clear()

    send = Send(kind="send", account_id=gw.account, peer=BARE, peer_type="channel", text="hi")
    result = await gw.access.execute(gw.project, send)

    assert result == {"message_id": 77}
    assert "SendMessageRequest" in _names(gw.dispatched)
    assert "GetChannelsRequest" not in _names(gw.dispatched)
