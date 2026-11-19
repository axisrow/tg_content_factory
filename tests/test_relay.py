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
import os
import stat
import struct
import sys

import pytest
from telethon import errors, functions, types
from telethon.extensions import BinaryReader
from telethon.sessions import SQLiteSession
from telethon_floodgate import TelegramRateLimitedError

from tg_agent.telegram import relay
from tg_agent.telegram.relay import (
    RELAY_PROTOCOL,
    RelayAccountUnavailableError,
    RelayConnection,
    RelayEndpoint,
    RelayForbiddenError,
    RelayProtocolError,
    RelayServer,
    RelayTelegramClient,
    RelayUnauthorizedError,
    entity_rows,
    read_endpoint,
    serve_pool_relay,
)

PHONE = "+10000000001"
VECTOR = 0x1CB5C415


def _tl_bytes(result) -> bytes:
    if isinstance(result, list):
        return struct.pack("<Ii", VECTOR, len(result)) + b"".join(bytes(item) for item in result)
    if result is True:
        return bytes(types.BoolTrue())
    if result is False:
        return bytes(types.BoolFalse())
    return bytes(result)


def _state(pts: int = 1) -> types.updates.State:
    return types.updates.State(pts=pts, qts=0, date=None, seq=0, unread_count=0)


class FakeOwnerClient:
    """Answers like a relayed real client: raw bytes, via the request's ``read_result``."""

    def __init__(self, handler=None, *, dc_id: int = 2, entities=None):
        self.session = type("S", (), {"dc_id": dc_id, "_entities": set(entities or ())})()
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
        await client.disconnect()
    assert isinstance(owner.calls[0][1], functions.updates.GetStateRequest)


async def test_vector_result_is_parsed_by_the_client_reader():
    me = types.User(id=4242, access_hash=99, first_name="Me", is_self=True)
    owner = FakeOwnerClient(lambda request: [me])
    async with running({PHONE: owner}) as (_, connection):
        client = await _client(connection)
        user = await client.get_me()
        assert user.id == 4242


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
        peer = client.session.get_input_entity(123)
    assert peer == types.InputPeerUser(user_id=123, access_hash=456)


def test_entity_rows_read_sqlite_sessions_with_text_phones(tmp_path):
    session = SQLiteSession(str(tmp_path / "owner"))
    try:
        user = types.User(id=5, access_hash=7, username="u", phone="15550001", first_name="A")
        session.process_entities(types.contacts.ResolvedPeer(peer=types.PeerUser(5), chats=[], users=[user]))
        assert entity_rows(session) == [[5, 7, "u", "15550001", "A"]]
    finally:
        session.close()


async def test_pending_requests_fail_when_the_owner_goes_away():
    gate = asyncio.Event()

    async def blocked(request):
        await gate.wait()
        return _state()

    server = RelayServer(Owner({PHONE: FakeOwnerClient(blocked)}))
    port = await server.start()
    connection = await RelayConnection.open(
        RelayEndpoint(port=port, token="secret-token", pid=os.getpid(), protocol=RELAY_PROTOCOL)
    )
    try:
        client = await _client(connection)
        pending = asyncio.create_task(client(functions.updates.GetStateRequest()))
        await asyncio.sleep(0.05)
        await server.stop()
        with pytest.raises(ConnectionError):
            await pending
    finally:
        gate.set()
        await connection.close()


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
