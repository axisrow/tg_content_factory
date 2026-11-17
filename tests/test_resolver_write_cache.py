"""Write actions resolve cache-first (#1831) — offline, real request resolution.

A write RPC only needs an InputPeer; Telethon's ``get_input_entity`` answers
from the session store without a request, while ``get_entity`` goes to the
network every call (``contacts.ResolveUsername`` for strings). Same discipline
as the other resolver-family files: a REAL ``TelegramClient`` whose MTProto
sender is a recording stub; ``get_entity`` / ``get_input_entity`` are never
stubbed; the only replaced behavior is ``is_connected()`` (offline test).
"""

from datetime import datetime, timezone

import pytest
from telethon.sessions import StringSession
from telethon.tl.functions.channels import (
    GetChannelsRequest,
    GetParticipantsRequest,
)
from telethon.tl.functions.contacts import ResolveUsernameRequest
from telethon.tl.functions.messages import (
    DeleteMessagesRequest,
    EditMessageRequest,
    ForwardMessagesRequest,
    GetHistoryRequest,
    ReadHistoryRequest,
    SendMessageRequest,
    SendReactionRequest,
    UpdatePinnedMessageRequest,
)
from telethon.tl.types import (
    InputPeerChannel,
    InputPeerUser,
    Message,
    PeerChannel,
    PeerUser,
    UpdateMessageID,
    UpdateNewMessage,
    Updates,
    UpdateShortSentMessage,
    User,
)
from telethon_floodgate.peer import peer_key

from tests.helpers import sender_responder_to_send
from tg_agent.services.telegram_actions import TelegramActionService

PHONE = "+70000000284"
SESSION = "session-write-cache"
USER_ID = 42
USERNAME = "penpal"


class ChannelInvalidLikeError(Exception):
    """Stub-stand-in for CHANNEL_INVALID: this world has no channel peers."""


def make_recording_client(*, seeded=(), resolve_username_to=None):
    from telethon import TelegramClient
    from telethon.tl.types import (
        channels as tl_channels,
    )
    from telethon.tl.types import (
        contacts as tl_contacts,
    )
    from telethon.tl.types import (
        messages as tl_messages,
    )

    client = TelegramClient(StringSession(), 1, "x")
    dispatched = []

    def _message(msg_id):
        return Message(
            id=msg_id,
            peer_id=PeerUser(user_id=USER_ID),
            message="x",
            date=datetime.now(timezone.utc),
            out=True,
        )

    def _updates(msg_id=None):
        entries = []
        if msg_id is not None:
            entries.append(UpdateMessageID(id=msg_id, random_id=1))
            entries.append(
                UpdateNewMessage(message=_message(msg_id), pts=1, pts_count=1)
            )
        return Updates(
            updates=entries, users=[], chats=[], seq=0, date=datetime.now(timezone.utc)
        )

    def send(request, ordered=False):
        dispatched.append(request)
        if isinstance(request, ResolveUsernameRequest):
            if resolve_username_to is None:
                raise AssertionError("ResolveUsername must not be dispatched on a warm cache")
            user = User(id=resolve_username_to, first_name="r", username=USERNAME, access_hash=1)
            client.session.process_entities([user])
            return tl_contacts.ResolvedPeer(
                peer=PeerUser(user_id=resolve_username_to), users=[user], chats=[]
            )
        if isinstance(request, GetChannelsRequest):
            raise ChannelInvalidLikeError()
        if isinstance(request, SendMessageRequest):
            return UpdateShortSentMessage(
                id=77, date=datetime.now(timezone.utc), out=True, pts=1, pts_count=1
            )
        if isinstance(request, (EditMessageRequest, ForwardMessagesRequest)):
            return _updates(88 if isinstance(request, ForwardMessagesRequest) else 9)
        if isinstance(request, (DeleteMessagesRequest, ReadHistoryRequest)):
            return tl_messages.AffectedMessages(pts=1, pts_count=1)
        if isinstance(request, (UpdatePinnedMessageRequest, SendReactionRequest)):
            return _updates()
        if isinstance(request, GetParticipantsRequest):
            return tl_channels.ChannelParticipants(
                count=0, participants=[], users=[], chats=[], topics=[]
            )
        if type(request).__name__ == "GetUsersRequest":
            # Follow-up full-entity fetches cascade here; the response type is
            # a plain user list (TL Vector<User>), not a Users constructor.
            # The seeded user answers with its entity; anything else is a miss.
            return [
                User(id=USER_ID, first_name="pen", username=USERNAME, access_hash=1)
                for item in request.id
                if getattr(item, "user_id", None) == USER_ID
            ]
        if type(request).__name__ == "GetChatsRequest":
            return tl_messages.Chats(chats=[])
        if isinstance(request, GetHistoryRequest):
            return tl_messages.Messages(
                messages=[_message(5)], chats=[], users=[], topics=[]
            )
        return None

    # The dispatch gate inspects sender.send's Future (MTProto contract);
    # batch calls map to a list of Futures like the real sender does.
    client._sender.send = sender_responder_to_send(send)
    client.is_connected = lambda: True
    for entity in seeded:
        client.session.process_entities([entity])
    return client, dispatched


def _seeded_user():
    return User(id=USER_ID, first_name="pen", username=USERNAME, access_hash=1)


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


def _recipient(form: str) -> str:
    if form == "username":
        return f"@{USERNAME}"
    if form == "link":
        return f"https://t.me/{USERNAME}"
    raise ValueError(form)


# (label, expected own request, call) — each write action addressed by a
# username form must dispatch exactly its own RPC on a warm cache (#1831);
# forward resolves two peers from the store and still dispatches one request.
ACTIONS = {
    "send_message": (
        "SendMessageRequest",
        lambda a, p, r: a.send_message(phone=p, recipient=r, text="hi"),
    ),
    "edit_message": (
        "EditMessageRequest",
        lambda a, p, r: a.edit_message(phone=p, chat_id=r, message_id=5, text="edited"),
    ),
    "delete_messages": (
        "DeleteMessagesRequest",
        lambda a, p, r: a.delete_messages(phone=p, chat_id=r, message_ids=[5]),
    ),
    "forward_messages": (
        "ForwardMessagesRequest",
        lambda a, p, r: a.forward_messages(phone=p, from_chat=r, to_chat=r, message_ids=[5]),
    ),
    "pin_message": (
        "UpdatePinnedMessageRequest",
        lambda a, p, r: a.pin_message(phone=p, chat_id=r, message_id=5),
    ),
    "unpin_message": (
        "UpdatePinnedMessageRequest",
        lambda a, p, r: a.unpin_message(phone=p, chat_id=r, message_id=5),
    ),
    "mark_read": (
        "ReadHistoryRequest",
        lambda a, p, r: a.mark_read(phone=p, chat_id=r, max_id=5),
    ),
    "send_reaction": (
        "SendReactionRequest",
        lambda a, p, r: a.send_reaction(phone=p, chat_id=r, message_id=5, emoji="👍"),
    ),
    "get_participants": (
        # The synthetic world's peer is a user, and Telethon answers
        # get_participants on a user peer through the full-entity GetUsers
        # path — still exactly one own-RPC and zero resolve RPCs.
        "GetUsersRequest",
        lambda a, p, r: a.get_participants(phone=p, chat_id=r, limit=1),
    ),
}


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
@pytest.mark.parametrize("form", ["username", "link"])
@pytest.mark.parametrize("label", sorted(ACTIONS))
async def test_write_action_by_username_is_cache_first(real_pool_harness_factory, label, form):
    """Warm cache: the action dispatches only its own RPC (#1831)."""
    harness = real_pool_harness_factory()
    client, dispatched = make_recording_client(seeded=[_seeded_user()])
    session, phone = await _connect_native(harness, client)

    actions = TelegramActionService(harness.pool)
    expected_name, call = ACTIONS[label]
    await call(actions, phone, _recipient(form))

    names = _request_names(dispatched)
    # The write needs no network resolve at all — the session store answers.
    assert "ResolveUsernameRequest" not in names
    assert "GetDialogsRequest" not in names
    assert "GetChannelsRequest" not in names
    assert names == [expected_name]


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
async def test_unknown_username_costs_one_resolve(real_pool_harness_factory):
    """Cold cache: one network resolve for the unknown username, then the write."""
    harness = real_pool_harness_factory()
    client, dispatched = make_recording_client(resolve_username_to=USER_ID)
    session, phone = await _connect_native(harness, client)

    actions = TelegramActionService(harness.pool)
    result = await actions.send_message(phone=phone, recipient=f"@{USERNAME}", text="hi")

    assert result.message_id == 77
    assert _request_names(dispatched) == ["ResolveUsernameRequest", "SendMessageRequest"]


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
async def test_cached_username_is_used_without_rechecking(real_pool_harness_factory):
    """Stale-username semantics (#1831, decided): a write by cached username
    uses the session store without re-resolving, even though the real server
    might now point that username elsewhere. Cache-first is the deliberate
    trade: username budget is spent only on unknown targets, and every warm
    (collection/scheduler) refreshes the store."""
    harness = real_pool_harness_factory()
    # ResolveUsername would answer with a DIFFERENT user id if it ran — the
    # write must never ask.
    client, dispatched = make_recording_client(seeded=[_seeded_user()], resolve_username_to=999)
    session, phone = await _connect_native(harness, client)

    actions = TelegramActionService(harness.pool)
    await actions.send_message(phone=phone, recipient=f"@{USERNAME}", text="hi")

    sent = [r for r in dispatched if isinstance(r, SendMessageRequest)]
    assert len(sent) == 1
    # Addressed to the cached peer (id 42), not to whatever a fresh resolve
    # would have returned.
    assert sent[0].peer.user_id == USER_ID


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
async def test_peer_key_accepts_input_peer():
    """Per-peer send gating keys are stable across InputPeer/full-entity (#1831)."""
    assert peer_key(InputPeerChannel(1111111111, 5)) == "channel:1111111111"
    assert peer_key(InputPeerChannel(1111111111, 5)) == peer_key(PeerChannel(1111111111))
    assert peer_key(InputPeerUser(USER_ID, 1)) == peer_key(PeerUser(user_id=USER_ID))


@pytest.mark.anyio
@pytest.mark.native_backend_allowed
async def test_gateway_history_keeps_full_fetch_and_channel_id(tmp_path, monkeypatch):
    """Gateway History explicitly keeps the full-entity resolve (it needs the
    channel id) — one network resolve, working history page (#1831)."""
    import secrets
    from uuid import uuid4

    monkeypatch.setenv("TGCF_SESSION_LOCK_DIR", str(tmp_path / "locks"))
    client, dispatched = make_recording_client(resolve_username_to=USER_ID)

    class RecordingAuth:
        async def create_client_from_session(self, session, **kwargs):
            return client

        async def cleanup(self):
            pass

    from tg_agent.gateway.protocol import History
    from tg_agent.gateway.store import GatewayStore
    from tg_agent.gateway.telegram import LiveTelegramAccess
    from tg_agent.telegram.session_lock import release_all_session_locks

    store = GatewayStore(tmp_path / "gateway")
    await store.open()
    access = LiveTelegramAccess(store, RecordingAuth())
    try:
        account = await store.import_account("+100", "local-transport-session")
        project = str(uuid4())
        await store.register_project(project, tmp_path / project, secrets.token_urlsafe(32))
        await store.grant(project, account)
        operation = History(
            kind="history", account_id=account, peer=f"@{USERNAME}", limit=1
        )
        result = await access.execute(project, operation)

        assert result["messages"], "history page must not be empty"
        assert result["messages"][0]["channel_id"] == USER_ID
        names = _request_names(dispatched)
        # History keeps the full-entity resolve — exactly ONE network resolve.
        assert names.count("ResolveUsernameRequest") == 1
        assert "GetDialogsRequest" not in names
    finally:
        await access.close()
        await store.close()
        release_all_session_locks()
