"""Resolver id-form family (#1829/#1830/#1831) — offline, real request resolution.

Every test drives a REAL ``TelegramClient`` (StringSession, never connected)
whose MTProto sender is replaced by a recording stub: the TL requests are built
by real Telethon code and entity knowledge reaches the client the way real RPC
responses seed it — ``session.process_entities``. ``get_entity`` /
``get_input_entity`` are never stubbed.
"""

import secrets
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from telethon.errors.rpcerrorlist import ChannelInvalidError
from telethon.sessions import StringSession
from telethon.tl.functions.channels import GetChannelsRequest
from telethon.tl.functions.messages import GetDialogsRequest, SendMessageRequest
from telethon.tl.types import (
    Channel,
    UpdateShortSentMessage,
)
from telethon.tl.types import (
    messages as tl_messages,
)

from tests.helpers import sender_responder_to_send
from tg_agent.gateway.protocol import Send
from tg_agent.gateway.store import GatewayStore
from tg_agent.gateway.telegram import LiveTelegramAccess
from tg_agent.services.telegram_actions import TelegramActionService
from tg_agent.telegram.session_lock import release_all_session_locks

PHONE = "+70000000282"
SESSION = "session-id-forms"
BARE = 1111111111
# Telegram's marked supergroup/channel form: -100<bare>.
MARKED = -1_001_111_111_111


def make_recording_client(*, seeded=()):
    """A real TelegramClient whose sender records every dispatched request.

    The stub answers the way Telegram would for this synthetic world: the
    channel with the bare id exists (and is seeded into the session store like
    any real RPC response would), any other channel id is CHANNEL_INVALID.
    """
    from telethon import TelegramClient

    client = TelegramClient(StringSession(), 1, "x")
    dispatched = []

    def send(request, ordered=False):
        dispatched.append(request)
        if isinstance(request, GetChannelsRequest):
            wanted = request.id[0].channel_id
            if wanted == BARE:
                channel = Channel(id=BARE, title="known", access_hash=5, photo=None, date=None)
                client.session.process_entities([channel])
                return tl_messages.Chats(chats=[channel])
            raise ChannelInvalidError(request=None)
        if isinstance(request, GetDialogsRequest):
            # A real sweep response seeds the entities it carries — the sender
            # processes them into the session store before returning.
            client.session.process_entities([_channel_entity()])
            return tl_messages.Dialogs(dialogs=[], users=[], chats=[], messages=[])
        if isinstance(request, SendMessageRequest):
            return UpdateShortSentMessage(
                id=77,
                date=datetime.now(timezone.utc),
                out=True,
                pts=1,
                pts_count=1,
            )
        return None

    # The dispatch gate inspects sender.send's Future (MTProto contract).
    client._sender.send = sender_responder_to_send(send)
    # The client is never connected (offline test); report it as connected so
    # the pool's liveness check never attempts a REAL network connect() —
    # that is the only non-Telethon-parsing behavior replaced here.
    client.is_connected = lambda: True
    for entity in seeded:
        client.session.process_entities([entity])
    return client, dispatched


def _channel_entity(channel_id=BARE):
    return Channel(id=channel_id, title="known", access_hash=5, photo=None, date=None)


async def _connect_native(harness, client):
    harness.queue_cli_client(phone=PHONE)
    harness.queue_native_client(session_string=SESSION, client=client)
    await harness.add_account(PHONE, session_string=SESSION, is_primary=True)
    await harness.initialize_connected_accounts()
    session, phone = await harness.pool.get_native_client_by_phone(PHONE)
    assert session is not None
    return session, phone


def _request_names(dispatched):
    return [type(request).__name__ for request in dispatched]


# ---------------------------------------------------------------------------
# #1829 — the marked -100<bare> form must resolve like its bare id
# ---------------------------------------------------------------------------


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
async def test_resolve_dialog_entity_marked_form_warm_matches_bare(real_pool_harness_factory):
    """Right after a dialog warm all three id forms resolve with zero RPC."""
    harness = real_pool_harness_factory()
    traces = {}
    entities = {}
    for label, dialog_id in (("marked", MARKED), ("bare", BARE), ("negative", -BARE)):
        client, dispatched = make_recording_client(seeded=[_channel_entity()])
        session, phone = await _connect_native(harness, client)
        entities[label] = await harness.pool.resolve_dialog_entity(session, phone, dialog_id)
        traces[label] = _request_names(dispatched)

    assert entities["marked"].channel_id == BARE
    assert entities["bare"].channel_id == BARE
    assert entities["negative"].channel_id == BARE
    # A known chat addressed right after a warm: no GetDialogs sweep, no
    # GetChannels at all — the session cache answers every form.
    assert traces["marked"] == traces["bare"] == traces["negative"] == []


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
async def test_resolve_dialog_entity_marked_form_cold_matches_bare(real_pool_harness_factory):
    """Cold cache: all three forms take the identical request trace."""
    harness = real_pool_harness_factory()
    traces = {}
    errors = {}
    for label, dialog_id in (("marked", MARKED), ("bare", BARE), ("negative", -BARE)):
        client, dispatched = make_recording_client()
        session, phone = await _connect_native(harness, client)
        try:
            entity = await harness.pool.resolve_dialog_entity(session, phone, dialog_id)
            assert entity.channel_id == BARE
        except Exception as exc:  # recorded: a correct fix resolves without one
            errors[label] = exc
        traces[label] = _request_names(dispatched)

    assert errors == {}
    assert traces["marked"] == traces["bare"] == traces["negative"]
    assert "GetDialogsRequest" not in traces["marked"]


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
async def test_get_cached_dialog_finds_row_for_marked_form(real_pool_harness_factory):
    """The dialog_cache lookup answers both the bare and the marked form."""
    harness = real_pool_harness_factory()
    await harness.db.repos.dialog_cache.upsert_dialogs(
        PHONE,
        [
            {
                "channel_id": BARE,
                "title": "known",
                "username": None,
                "channel_type": "channel",
                "deactivate": False,
                "is_own": False,
            }
        ],
    )
    bare_row = await harness.pool._get_cached_dialog(PHONE, BARE)
    marked_row = await harness.pool._get_cached_dialog(PHONE, MARKED)
    negative_row = await harness.pool._get_cached_dialog(PHONE, -BARE)

    assert bare_row is not None and int(bare_row["channel_id"]) == BARE
    assert marked_row is not None and int(marked_row["channel_id"]) == BARE
    assert negative_row is not None and int(negative_row["channel_id"]) == BARE


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
async def test_send_message_accepts_marked_form_after_warm(real_pool_harness_factory):
    """The audit's failing flow: send to -100<bare> right after a warm."""
    harness = real_pool_harness_factory()
    client, dispatched = make_recording_client(seeded=[_channel_entity()])
    session, phone = await _connect_native(harness, client)
    # Warm the way the pool does: a completed sweep seeds the session store and
    # the dialog cache — the marked id must then be a plain cache hit.
    await harness.pool.warm_all_dialogs()
    dispatched.clear()

    actions = TelegramActionService(harness.pool)
    result = await actions.send_message(phone=phone, recipient=str(MARKED), text="hi")

    assert result.message_id == 77
    # Zero GetDialogs and zero resolve RPCs — the action's own send is the
    # only request the whole write dispatches.
    assert _request_names(dispatched) == ["SendMessageRequest"]


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
async def test_gateway_typed_channel_peer_normalizes_marked_form(tmp_path, monkeypatch):
    """A typed integer channel peer in the marked form resolves by bare id."""
    monkeypatch.setenv("TGCF_SESSION_LOCK_DIR", str(tmp_path / "locks"))
    client, dispatched = make_recording_client()

    class RecordingAuth:
        async def create_client_from_session(self, session, **kwargs):
            return client

        async def cleanup(self):
            pass

    store = GatewayStore(tmp_path / "gateway")
    await store.open()
    access = LiveTelegramAccess(store, RecordingAuth())
    try:
        account = await store.import_account("+100", "local-transport-session")
        project = str(uuid4())
        await store.register_project(project, tmp_path / project, secrets.token_urlsafe(32))
        await store.grant(project, account)
        operation = Send(
            kind="send", account_id=account, peer=MARKED, peer_type="channel", text="hi"
        )
        result = await access.execute(project, operation)

        assert result == {"message_id": 77}
        names = _request_names(dispatched)
        assert "GetDialogsRequest" not in names
        get_channels = [r for r in dispatched if isinstance(r, GetChannelsRequest)]
        # Either a pure cache miss answered by the bare id, or no RPC at all —
        # never a request for the marked-channel id 100<bare>.
        for request in get_channels:
            assert request.id[0].channel_id == BARE
    finally:
        await access.close()
        await store.close()
        release_all_session_locks()
