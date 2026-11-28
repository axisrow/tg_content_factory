"""Session-owner relay (#1868): a client without an auth key, the owner's connection does the I/O.

The owner side runs against a fake Telethon client that answers like the real one
does once the relay has swapped ``read_result`` for the raw reader: with the
serialized TL bytes of the result. The client side is the real
``RelayTelegramClient`` and parses those bytes with the request's own reader.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
import stat
import struct
import sys
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import patch

import pytest
from telethon import errors, functions, types
from telethon.extensions import BinaryReader
from telethon.sessions import MemorySession, SQLiteSession
from telethon_floodgate import TelegramRateLimitedError, TelegramRpcDeferredError, UsernameResolveRateLimitedError

from tg_agent.telegram import relay
from tg_agent.telegram.relay import (
    RELAY_PROTOCOL,
    RelayAccountUnavailableError,
    RelayConnection,
    RelayDeliveryUnconfirmedError,
    RelayEndpoint,
    RelayForbiddenError,
    RelayPeer,
    RelayProtocolError,
    RelayServer,
    RelayTelegramClient,
    RelayUnauthorizedError,
    entity_rows,
    read_endpoint,
    serve_pool_relay,
)
from tg_agent.telegram.resolve_guard import ResolveGuardMixin

PHONE = "+10000000001"
VECTOR = 0x1CB5C415


def _tl_bytes(result) -> bytes:
    if isinstance(result, list):
        return struct.pack("<Ii", VECTOR, len(result)) + b"".join(bytes(item) for item in result)
    if result is True:
        return bytes(cast(Any, types).BoolTrue())
    if result is False:
        return bytes(cast(Any, types).BoolFalse())
    return bytes(result)


def _state(pts: int = 1) -> types.updates.State:
    return types.updates.State(pts=pts, qts=0, date=None, seq=0, unread_count=0)


class FakeOwnerClient:
    """Answers like a relayed real client: raw bytes, via the request's ``read_result``."""

    def __init__(self, handler=None, *, dc_id: int = 2, entities=None, session: Any = None):
        if session is None:
            session = type("S", (), {"dc_id": dc_id, "_entities": set(entities or ())})()
        self.session = session
        self.handler = handler or (lambda request: _state())
        self.calls: list[tuple[object, object]] = []
        self.borrowed: list[int] = []
        self.returned: list[object] = []

    async def _answer(self, sender, request):
        self.calls.append((sender, request))
        outcome = self.handler(request)
        if asyncio.iscoroutine(outcome):
            outcome = await outcome
        raw = _tl_bytes(outcome)
        # The relay must have replaced the reader: what Telethon would hand back.
        with BinaryReader(raw) as reader:
            return request.read_result(reader)

    async def __call__(self, request):
        return await self._answer(None, request)

    async def _call(self, sender, request):
        return await self._answer(sender, request)

    async def _borrow_exported_sender(self, dc_id):
        self.borrowed.append(dc_id)
        return f"exported-{dc_id}"

    async def _return_exported_sender(self, sender):
        self.returned.append(sender)


class Owner:
    def __init__(self, clients: dict, token: str = "secret-token"):
        self.clients = clients
        self.token = token

    async def authorize(self, token):
        return "grant" if token == self.token else None

    async def phones(self, grant):
        return sorted(self.clients)

    async def client(self, grant, phone):
        if phone not in self.clients:
            raise RelayAccountUnavailableError(f"no live connection for {phone}")
        return self.clients[phone]


@contextlib.asynccontextmanager
async def running(clients: dict, *, token: str = "secret-token", client_token: str | None = None):
    server = RelayServer(Owner(clients, token))
    port = await server.start()
    endpoint = RelayEndpoint(port=port, token=client_token or token, pid=os.getpid(), protocol=RELAY_PROTOCOL)
    connection = None
    try:
        connection = await RelayConnection.open(endpoint)
        yield server, connection
    finally:
        if connection is not None:
            await connection.close()
        await server.stop()


async def _client(connection, phone: str = PHONE) -> RelayTelegramClient:
    client = RelayTelegramClient(connection, phone, flood_sleep_threshold=0)
    await client.connect()
    return client


async def test_request_round_trips_through_the_owner_connection():
    owner = FakeOwnerClient(lambda request: _state(pts=77))
    async with running({PHONE: owner}) as (_, connection):
        client = await _client(connection)
        state = await client(functions.updates.GetStateRequest())
        assert isinstance(state, types.updates.State)
        assert state.pts == 77
        assert client.is_connected()
        await cast(Any, client).disconnect()
    assert isinstance(owner.calls[0][1], functions.updates.GetStateRequest)


async def test_vector_result_is_parsed_by_the_client_reader():
    me = types.User(id=4242, access_hash=99, first_name="Me", is_self=True)
    owner = FakeOwnerClient(lambda request: [me])
    async with running({PHONE: owner}) as (_, connection):
        client = await _client(connection)
        user = await client.get_me()
        assert cast(Any, user).id == 4242


async def test_telegram_errors_keep_their_type_and_capture():
    def flood(request):
        raise errors.FloodWaitError(request, capture=42)

    async with running({PHONE: FakeOwnerClient(flood)}) as (_, connection):
        client = await _client(connection)
        with pytest.raises(errors.FloodWaitError) as raised:
            await client(functions.updates.GetStateRequest())
    assert raised.value.seconds == 42


async def test_owner_local_refusals_reach_the_client_as_rate_limits():
    def refuse(request):
        raise TelegramRateLimitedError(PHONE, "resolve", 3.5)

    async with running({PHONE: FakeOwnerClient(refuse)}) as (_, connection):
        client = await _client(connection)
        with pytest.raises(TelegramRateLimitedError) as raised:
            await client(functions.contacts.ResolveUsernameRequest(username="somebody"))
    assert raised.value.retry_after_sec == pytest.approx(3.5)
    assert raised.value.category == "resolve"


def _init_connection(query):
    return functions.InitConnectionRequest(api_id=1, device_model="d", system_version="s", app_version="a",
                                           system_lang_code="en", lang_pack="", lang_code="en", query=query)


@pytest.mark.parametrize(
    "request_",
    [
        functions.auth.LogOutRequest(),
        functions.account.ResetAuthorizationRequest(hash=1),
        functions.account.DeleteAccountRequest(reason="x"),
        functions.account.ChangePhoneRequest(phone_number="+1", phone_code_hash="h", phone_code="1"),
        functions.account.SetAccountTTLRequest(ttl=types.AccountDaysTTL(days=30)),
        functions.account.UpdatePasswordSettingsRequest(
            password=types.InputCheckPasswordEmpty(),
            new_settings=types.account.PasswordInputSettings(),
        ),
        functions.InvokeWithoutUpdatesRequest(functions.auth.LogOutRequest()),
        functions.InvokeWithTakeoutRequest(1, functions.account.DeleteAccountRequest(reason="x")),
        functions.InvokeWithoutUpdatesRequest(functions.InvokeAfterMsgRequest(1, functions.auth.LogOutRequest())),
        functions.InvokeWithLayerRequest(1, functions.updates.GetStateRequest()),
        _init_connection(functions.updates.GetStateRequest()),
    ],
    ids=["log-out", "reset-authorization", "delete-account", "change-phone", "account-ttl", "password",
         "wrapped-log-out", "takeout-wrapped-delete", "nested-wrapped-log-out", "invoke-with-layer",
         "init-connection"],
)
async def test_requests_that_would_end_the_owner_session_are_refused(request_):
    owner = FakeOwnerClient()
    async with running({PHONE: owner}) as (_, connection):
        client = await _client(connection)
        with pytest.raises(RelayForbiddenError):
            await client(request_)
    assert owner.calls == []


async def test_a_wrapped_harmless_request_still_runs():
    owner = FakeOwnerClient()
    async with running({PHONE: owner}) as (_, connection):
        client = await _client(connection)
        state = await client(functions.InvokeWithoutUpdatesRequest(functions.updates.GetStateRequest()))
    assert isinstance(state, types.updates.State)
    assert len(owner.calls) == 1


async def test_botfather_flows_run_on_the_owner_client(monkeypatch):
    from tg_agent.telegram import botfather_flows
    from tg_shared import botfather

    owner = FakeOwnerClient()
    seen = []

    async def create_bot(client, name, username):
        seen.append((client, name, username))
        return "123456789:token"

    async def delete_bot(client, bot_username):
        seen.append((client, bot_username))
        raise botfather.BotNotFoundError(bot_username)

    monkeypatch.setattr(botfather, "create_bot", create_bot)
    monkeypatch.setattr(botfather, "delete_bot", delete_bot)
    async with running({PHONE: owner}) as (_, connection):
        client = await _client(connection)
        token = await botfather_flows.create_bot(client, "Name", "name_bot")
        with pytest.raises(botfather.BotNotFoundError):
            await botfather_flows.delete_bot(client, "@name_bot")

    assert token == "123456789:token"
    # Both conversations ran on the owner's client, which receives the updates.
    assert seen == [(owner, "Name", "name_bot"), (owner, "@name_bot")]


async def test_botfather_flows_run_locally_on_an_owned_client(monkeypatch):
    from tg_agent.telegram import botfather_flows
    from tg_shared import botfather

    local = object()

    async def create_bot(client, name, username):
        return f"local:{client is local}"

    monkeypatch.setattr(botfather, "create_bot", create_bot)
    assert await botfather_flows.create_bot(local, "Name", "name_bot") == "local:True"


async def test_a_conversation_on_a_relayed_client_fails_at_once():
    async with running({PHONE: FakeOwnerClient()}) as (_, connection):
        client = await _client(connection)
        with pytest.raises(relay.RelayError, match="only the session owner receives"):
            client.conversation("BotFather")


async def test_media_downloads_reuse_one_relay_sender_per_dc():
    async with running({PHONE: FakeOwnerClient()}) as (_, connection):
        client = await _client(connection)
        first = await client._borrow_exported_sender(4)
        await client._return_exported_sender(first)
        assert await client._borrow_exported_sender(4) is first
        assert await client._borrow_exported_sender(5) is not first


async def test_wrong_token_is_refused_at_handshake():
    with pytest.raises(RelayUnauthorizedError):
        async with running({PHONE: FakeOwnerClient()}, client_token="wrong"):
            pass


async def _raw_hello(port: int, *, protocol: int, layer: int) -> BaseException:
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    hello = {"id": 0, "op": "hello", "token": "secret-token", "protocol": protocol, "layer": layer}
    writer.write(json.dumps(hello).encode() + b"\n")
    await writer.drain()
    reply = await reader.readline()
    writer.close()
    await writer.wait_closed()
    return relay.rebuild_error(json.loads(reply)["error"])


async def test_protocol_mismatch_is_refused_with_a_restart_hint():
    server = RelayServer(Owner({PHONE: FakeOwnerClient()}))
    port = await server.start()
    try:
        error = await _raw_hello(port, protocol=0, layer=relay.LAYER)
        stale = RelayEndpoint(port=port, token="secret-token", pid=1, protocol=RELAY_PROTOCOL + 1)
        with pytest.raises(RelayProtocolError):
            await RelayConnection.open(stale)
    finally:
        await server.stop()
    assert type(error) is RelayProtocolError
    assert "restart the owner" in str(error)


async def test_a_telethon_layer_mismatch_names_the_owner_install():
    server = RelayServer(Owner({PHONE: FakeOwnerClient()}))
    port = await server.start()
    try:
        error = await _raw_hello(port, protocol=RELAY_PROTOCOL, layer=1)
    finally:
        await server.stop()
    assert isinstance(error, relay.RelayLayerMismatchError)
    assert "same Telethon" in str(error)
    assert sys.executable in str(error)


async def test_unknown_account_is_unavailable():
    async with running({PHONE: FakeOwnerClient()}) as (_, connection):
        assert connection.phones == [PHONE]
        client = await _client(connection, phone="+19999999999")
        with pytest.raises(RelayAccountUnavailableError):
            await client(functions.updates.GetStateRequest())


async def test_media_dc_requests_use_the_owner_exported_sender():
    owner = FakeOwnerClient(dc_id=2)
    async with running({PHONE: owner}) as (_, connection):
        client = await _client(connection)
        foreign = await client._borrow_exported_sender(4)
        await client._call(foreign, functions.updates.GetStateRequest())
        home = await client._borrow_exported_sender(2)
        await client._call(home, functions.updates.GetStateRequest())
        await client._return_exported_sender(foreign)
    assert owner.borrowed == [4]
    assert owner.returned == ["exported-4"]
    assert [sender for sender, _ in owner.calls] == ["exported-4", None]


async def test_owner_entity_cache_preloads_the_client_session():
    owner = FakeOwnerClient(entities={(123, 456, "friend", None, "Friend")})
    async with running({PHONE: owner}) as (_, connection):
        client = await _client(connection)
        assert await client.load_owner_entities() == 1
        peer = cast(Any, client.session).get_input_entity(123)
    assert peer == types.InputPeerUser(user_id=123, access_hash=456)


async def test_peers_resolved_through_the_relay_join_the_owner_cache():
    """The owner's client saw only raw bytes, so Telethon cached nothing there; every
    later lease seeds from the owner's cache, so the owner parses the answer itself."""
    user = types.User(id=77, access_hash=88, username="friend")
    resolved = types.contacts.ResolvedPeer(peer=types.PeerUser(77), chats=[], users=[user])
    session = MemorySession()
    async with running({PHONE: FakeOwnerClient(lambda request: resolved, session=session)}) as (_, connection):
        client = await _client(connection)
        await client(functions.contacts.ResolveUsernameRequest(username="friend"))
    assert session.get_input_entity(77) == types.InputPeerUser(user_id=77, access_hash=88)


@pytest.mark.parametrize(
    ("answer", "expected"),
    [
        (types.contacts.ResolvedPeer(peer=types.PeerChannel(1000), chats=[], users=[]), "peer 1000"),
        (TelegramRpcDeferredError(PHONE, "rpc_resolve", 42.0, "ResolveUsernameRequest",
                                  category="resolve", budget="resolve_username"),
         "refused by resolve_username, retry 42s"),
        (errors.FloodWaitError(None, capture=42), "FloodWait 42s"),
        (errors.UsernameNotOccupiedError(None), "error UsernameNotOccupiedError"),
    ],
)
async def test_the_owner_logs_every_username_lookup_it_runs(caplog, answer, expected):
    """#1877: the owner writes one line per ResolveUsername it runs for a relayed
    client — the account, the name, the outcome and the time."""
    def handler(request):
        if isinstance(answer, BaseException):
            raise answer
        return answer

    caplog.set_level(logging.INFO, logger="tg_agent.telegram.relay")
    async with running({PHONE: FakeOwnerClient(handler)}) as (_, connection):
        client = await _client(connection)
        with contextlib.suppress(Exception):
            await client(functions.contacts.ResolveUsernameRequest(username="somechan"))
    lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("relay: ResolveUsername")]
    assert len(lines) == 1, lines
    assert re.fullmatch(
        rf"relay: ResolveUsername @somechan account=\+10\.\.\.0001: {re.escape(expected)} in \d+ms", lines[0]
    ), lines[0]


async def test_an_owner_username_refusal_rotates_the_relayed_collector():
    """#1877, the whole chain: the owner's username budget refuses, and the relayed
    pool gets a rotatable refusal for its own phone, which stays cache-only meanwhile."""
    def refuse(request):
        raise TelegramRpcDeferredError("owner-key", "rpc_resolve", 42.0, "ResolveUsernameRequest",
                                       category="resolve", budget="resolve_username")

    class Pool(ResolveGuardMixin):
        _rpc_guard_enabled = True
        clients = {PHONE: object()}

    pool = Pool()
    async with running({PHONE: FakeOwnerClient(refuse)}) as (_, connection):
        client = await _client(connection)
        with pytest.raises(UsernameResolveRateLimitedError) as raised:
            await pool.run_live_username_resolve(
                lambda: client(functions.contacts.ResolveUsernameRequest(username="somechan")),
                phone=PHONE, username="somechan", operation="resolve_username",
            )
    assert raised.value.phone == PHONE
    assert 40 <= pool.get_resolve_username_backoff_remaining_sec(PHONE) <= 42


async def test_a_nested_resolve_flood_keeps_its_rpc_attribution_across_the_relay():
    """#1828: the owner's rpc gate names the constructor a server flood refused;
    the rebuilt client-side FloodWaitError carries the same curated facts, and
    an old owner without them still rebuilds a plain flood."""
    def flood_on_send(request):
        if isinstance(request, functions.messages.SendMessageRequest):
            flood = errors.FloodWaitError(request=request, capture=3600)
            flood.rpc = "SendMessageRequest"
            flood.source = "telegram"
            raise flood
        return _state()

    async with running({PHONE: FakeOwnerClient(flood_on_send)}) as (_, connection):
        client = await _client(connection)
        with pytest.raises(errors.FloodWaitError) as raised:
            await client(functions.messages.SendMessageRequest(
                peer=types.InputPeerUser(42, 42), message="hi", random_id=1,
            ))
    assert raised.value.seconds == 3600
    assert getattr(raised.value, "rpc", None) == "SendMessageRequest"
    assert getattr(raised.value, "source", None) == "telegram"

    resolve_flood = errors.FloodWaitError(None, capture=60)
    resolve_flood.rpc = "ResolveUsernameRequest"
    resolve_flood.source = "telegram"
    rebuilt = relay.rebuild_error(
        relay.error_payload(resolve_flood),
        functions.contacts.ResolveUsernameRequest(username="somechan"),
    )
    assert isinstance(rebuilt, errors.FloodWaitError)
    assert getattr(rebuilt, "rpc", None) == "ResolveUsernameRequest"
    assert getattr(relay.rebuild_error(relay.error_payload(
        errors.FloodWaitError(None, capture=5))), "rpc", None) is None
    # the wider flood family (slow mode) carries the facts the same way
    slow_mode = errors.SlowModeWaitError(request=None, capture=30)
    slow_mode.rpc = "SendMessageRequest"
    slow_mode.source = "telegram"
    assert getattr(relay.rebuild_error(relay.error_payload(slow_mode), None),
                   "rpc", None) == "SendMessageRequest"


def test_entity_rows_read_sqlite_sessions_with_text_phones(tmp_path):
    session = SQLiteSession(str(tmp_path / "owner"))
    try:
        user = types.User(id=5, access_hash=7, username="u", phone="15550001", first_name="A")
        session.process_entities(types.contacts.ResolvedPeer(peer=types.PeerUser(5), chats=[], users=[user]))
        assert entity_rows(session) == [[5, 7, "u", "15550001", "A"]]
    finally:
        session.close()


@pytest.mark.parametrize(
    ("request_type", "error"),
    [
        (functions.updates.GetStateRequest, ConnectionError),
        (lambda: functions.upload.SaveFilePartRequest(1, 0, b"offline"), ConnectionError),
        (
            lambda: functions.messages.UploadMediaRequest(
                types.InputPeerUser(42, 1), types.InputMediaUploadedPhoto(types.InputFile(1, 1, "p.jpg", ""))
            ),
            ConnectionError,
        ),
        (
            lambda: functions.InvokeWithoutUpdatesRequest(
                functions.messages.SendMediaRequest(types.InputPeerUser(42, 1), types.InputMediaDice("🎲"), "")
            ),
            RelayDeliveryUnconfirmedError,
        ),
        (
            lambda: functions.messages.ForwardMessagesRequest(
                types.InputPeerUser(42, 1), [7], types.InputPeerUser(43, 1)
            ),
            RelayDeliveryUnconfirmedError,
        ),
        (
            lambda: functions.messages.GetBotCallbackAnswerRequest(types.InputPeerUser(42, 1), 7, data=b"buy"),
            RelayDeliveryUnconfirmedError,
        ),
    ],
    ids=["read", "upload-part", "album-media-upload", "wrapped-send-media", "forward", "inline-button-press"],
)
async def test_pending_requests_fail_when_the_owner_goes_away(request_type, error):
    accepted = asyncio.Event()

    async def blocked(request):
        accepted.set()
        await asyncio.Future()

    async with running({PHONE: FakeOwnerClient(blocked)}) as (server, connection):
        client = await _client(connection)
        pending = asyncio.create_task(client(request_type()))
        async with asyncio.timeout(5):
            await accepted.wait()
        await server.stop()
        with pytest.raises(error) as caught:
            await asyncio.wait_for(pending, timeout=5)
        assert type(caught.value) is error


class StuckWriter:
    """The owner's end of a client that stopped reading: its socket buffer is full."""

    def __init__(self):
        self.aborted = False
        self.transport = SimpleNamespace(abort=lambda: setattr(self, "aborted", True))

    def is_closing(self):
        return self.aborted

    def write(self, _data):
        pass

    async def drain(self):
        await asyncio.Future()


async def test_a_client_that_stops_reading_is_dropped_instead_of_blocking(monkeypatch):
    monkeypatch.setattr(relay, "_DRAIN_TIMEOUT_SEC", 0.05)
    peer = RelayPeer(cast(asyncio.StreamWriter, StuckWriter()))

    async with asyncio.timeout(5):
        with pytest.raises(ConnectionError):
            await peer.push("events")
        assert peer.closed
        await peer.push("events")  # a dropped client costs the owner nothing more


async def test_concurrent_requests_are_matched_to_their_callers():
    async def resolve(request):
        user_id = int(request.username[1:])
        await asyncio.sleep(0.001 * (user_id % 5))
        user = types.User(id=user_id, access_hash=user_id, username=request.username)
        return types.contacts.ResolvedPeer(peer=types.PeerUser(user_id), chats=[], users=[user])

    async with running({PHONE: FakeOwnerClient(resolve)}) as (_, connection):
        client = await _client(connection)
        results = await asyncio.gather(*(
            client(functions.contacts.ResolveUsernameRequest(username=f"u{index}")) for index in range(1, 21)
        ))
    assert [result.peer.user_id for result in results] == list(range(1, 21))


async def test_ordered_batches_run_in_order():
    seen = []

    def record(request):
        seen.append(request.username)
        user = types.User(id=len(seen), access_hash=0, username=request.username)
        return types.contacts.ResolvedPeer(peer=types.PeerUser(len(seen)), chats=[], users=[user])

    async with running({PHONE: FakeOwnerClient(record)}) as (_, connection):
        client = await _client(connection)
        await client(
            [functions.contacts.ResolveUsernameRequest(username=name) for name in ("first", "second", "third")],
            ordered=True,
        )
    assert seen == ["first", "second", "third"]


async def test_pool_relay_publishes_a_private_endpoint_and_retracts_it(tmp_path):
    session = type("Session", (), {"raw_client": FakeOwnerClient(lambda request: _state(pts=5))})()
    pool = type("Pool", (), {"clients": {PHONE: session}})()
    path = tmp_path / "project.relay.json"
    handle = await serve_pool_relay(pool, path)
    try:
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        endpoint = read_endpoint(path)
        assert endpoint is not None and endpoint.pid == os.getpid()
        connection = await RelayConnection.open(endpoint)
        try:
            client = await _client(connection)
            assert (await client(functions.updates.GetStateRequest())).pts == 5
        finally:
            await connection.close()
    finally:
        await handle.stop()
    assert not path.exists()


# --------------------------------------------------------------------------- #
# wait.messages: `messages wait --chat` long-poll over the relay (#1871)
# --------------------------------------------------------------------------- #


class ListeningOwnerClient(FakeOwnerClient):
    """Records event-handler attach/detach like a live Telethon client."""

    def __init__(self):
        super().__init__()
        self.added: list = []
        self.removed: list = []

    def add_event_handler(self, callback, event=None):
        self.added.append(callback)

    def remove_event_handler(self, callback, event=None):
        self.removed.append(callback)


def _wait_event(chat_id: int, message_id: int, text: str, *, out: bool = False):
    return SimpleNamespace(
        chat_id=chat_id,
        chat=SimpleNamespace(title="Чат"),
        message=SimpleNamespace(out=out, id=message_id, text=text, action=None,
                                date=None, sender=None),
    )


async def _attached_handler(owner_client: ListeningOwnerClient) -> Any:
    for _ in range(200):
        if owner_client.added:
            return owner_client.added[0]
        await asyncio.sleep(0.01)
    raise AssertionError("the wait.messages op never attached its handler")


async def test_wait_messages_pushes_matches_and_detaches_on_cancel():
    from tg_agent.telegram.incoming_wait import PUSH_KIND

    owner_client = ListeningOwnerClient()
    async with running({PHONE: owner_client}) as (_server, connection):
        pushes: list[dict] = []
        first_push = asyncio.Event()

        def on_push(message: dict) -> None:
            pushes.append(message)
            first_push.set()

        connection.on_push(on_push)
        op = asyncio.create_task(
            connection.call("wait.messages", phone=PHONE, chat_id=-100123, timeout_sec=5)
        )
        handler = await _attached_handler(owner_client)

        await handler(_wait_event(-100123, 7, "привет"))
        # asyncio.timeout form: the #1840/#1850 unreleased-waiter hang class.
        async with asyncio.timeout(2):
            await first_push.wait()
        # A foreign chat and an own outgoing message must not push.
        await handler(_wait_event(-100999, 8, "чужой"))
        await handler(_wait_event(-100123, 9, "своё", out=True))
        await asyncio.sleep(0.05)
        assert len(pushes) == 1
        assert pushes[0]["push"] == PUSH_KIND
        assert pushes[0]["phone"] == PHONE
        assert pushes[0]["chat_id"] == -100123
        assert pushes[0]["message_id"] == 7
        assert pushes[0]["text"] == "привет"

        # The CLI is done (burst complete): the process stops reading; the
        # owner-side op is cancelled when the connection drops (or window ends).
        op.cancel()
        await asyncio.gather(op, return_exceptions=True)
    # The context closed the connection: the server cancelled the op, which
    # detaches its handler in the finally.
    assert owner_client.removed == [owner_client.added[0]]


async def test_wait_messages_op_ends_by_its_window_and_detaches():
    owner_client = ListeningOwnerClient()
    async with running({PHONE: owner_client}) as (_server, connection):
        result = await asyncio.wait_for(
            connection.call("wait.messages", phone=PHONE, chat_id=None, timeout_sec=0.1),
            timeout=5,
        )
        assert result == {"listened": True}
        assert owner_client.removed == [owner_client.added[0]]


async def test_wait_messages_keeps_the_handler_while_the_burst_runs():
    """The window follows the burst, not the clock of the subscription (#1981).

    A newer CLI names the two deadlines of the burst contract — the wait for the
    first match and the settle tail every match re-arms — and the owner holds the
    handler exactly that long. One fixed ``timeout + settle`` window counted from
    the subscription cut a long burst and lost its tail.
    """
    owner_client = ListeningOwnerClient()
    pushes: list[dict] = []
    async with running({PHONE: owner_client}) as (_server, connection):
        connection.on_push(pushes.append)
        op = asyncio.create_task(
            connection.call(
                "wait.messages", phone=PHONE, chat_id=-100123, timeout_sec=0.1,
                first_wait_sec=0.3, settle_sec=0.2,
            )
        )
        handler = await _attached_handler(owner_client)
        # Each arrival lands after the fixed window an older CLI is cut off at,
        # inside the settle tail of the previous one. A removed handler is not
        # called, as on a live client.
        for message_id in (7, 8, 9):
            await asyncio.sleep(0.12)
            if handler not in owner_client.removed:
                await handler(_wait_event(-100123, message_id, "в серии"))
        # The burst outran every fixed window: 3 × 0.12 s > 0.3 + 0.2 s.
        assert handler not in owner_client.removed
        op.cancel()
        await asyncio.gather(op, return_exceptions=True)

    assert [push["message_id"] for push in pushes] == [7, 8, 9]


async def test_wait_messages_ends_once_the_burst_goes_quiet():
    """After the first match only the settle tail holds the handler (#1981)."""
    from tg_agent.telegram import incoming_wait

    owner_client = ListeningOwnerClient()
    pushes: list[dict] = []
    async with running({PHONE: owner_client}) as (_server, connection):
        connection.on_push(pushes.append)
        with patch.object(incoming_wait, "RELAY_GRACE_SEC", 0.05):
            op = asyncio.create_task(
                connection.call(
                    "wait.messages", phone=PHONE, chat_id=-100123, timeout_sec=5.0,
                    first_wait_sec=5.0, settle_sec=0.2,
                )
            )
            handler = await _attached_handler(owner_client)
            await handler(_wait_event(-100123, 7, "одно"))
            result = await asyncio.wait_for(op, timeout=5)
    assert result == {"listened": True}
    assert len(pushes) == 1
    assert owner_client.removed == [owner_client.added[0]]


async def test_wait_messages_hold_survives_a_push_in_flight():
    """The settle tail is re-armed at the match, not after its push (#1981).

    ``peer.push`` holds the peer lock and a ``writer.drain()`` (up to 30 s); a
    tail counted only after the push returns expires while it is still in
    flight, and the next match finds the handler already detached.
    """
    from tg_agent.telegram import incoming_wait
    from tg_agent.telegram import relay as relay_module

    real_push = relay_module.RelayPeer.push

    async def slow_push(self: Any, kind: str, **fields: Any) -> None:
        await asyncio.sleep(0.1)
        await real_push(self, kind, **fields)

    owner_client = ListeningOwnerClient()
    pushes: list[dict] = []
    async with running({PHONE: owner_client}) as (_server, connection):
        connection.on_push(pushes.append)
        with patch.object(incoming_wait, "RELAY_GRACE_SEC", 0.05), \
                patch.object(relay_module.RelayPeer, "push", slow_push):
            op = asyncio.create_task(
                connection.call(
                    "wait.messages", phone=PHONE, chat_id=-100123, timeout_sec=5.0,
                    first_wait_sec=0.5, settle_sec=0.2,
                )
            )
            handler = await _attached_handler(owner_client)
            # A match at the edge of the first wait: its 0.1 s push outlives the
            # hold an after-the-push re-arm would have left (0.5 s window).
            await asyncio.sleep(0.45)
            await handler(_wait_event(-100123, 7, "первое"))
            # The next match lands near the old window's death: only a hold
            # re-armed at the match keeps the handler. A removed handler is not
            # called, as on a live client.
            await asyncio.sleep(0.07)
            if handler not in owner_client.removed:
                await handler(_wait_event(-100123, 8, "второе"))
            result = await asyncio.wait_for(op, timeout=5)
    assert result == {"listened": True}
    assert [push["message_id"] for push in pushes] == [7, 8]
    assert owner_client.removed == [owner_client.added[0]]


async def test_wait_messages_unknown_account_is_a_clean_error():
    owner_client = ListeningOwnerClient()
    async with running({PHONE: owner_client}) as (_server, connection):
        with pytest.raises(Exception) as excinfo:  # noqa: PT011 - RelayRemoteError wrapper
            await asyncio.wait_for(
                connection.call("wait.messages", phone="+70000000000", chat_id=None, timeout_sec=1),
                timeout=5,
            )
        assert "no live connection" in str(excinfo.value)
