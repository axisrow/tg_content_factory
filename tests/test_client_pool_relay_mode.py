"""A ``ClientPool`` built on the session owner's relay (#1868).

Such a pool never touches the auth key: no session lock, no materialized session
file, no native client. Its requests run on the owner's connection, the owner's
limiter state stays authoritative, and teardown closes the relay connection.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from telethon import types

from tests.test_relay import FakeOwnerClient
from tg_agent.models import Account
from tg_agent.telegram import backends
from tg_agent.telegram.client_pool import ClientPool
from tg_agent.telegram.relay import RelayConnection, RelayTelegramClient, serve_pool_relay
from tg_agent.telegram.resolve_guard import RESOLVE_LIMITER_SNAPSHOT_SETTING
from tg_shared.auth import TelegramAuth

PHONE = "+10000000001"


@pytest.fixture
async def relayed_pool(db, monkeypatch):
    me = types.User(id=4242, access_hash=1, first_name="Owner", is_self=True)
    owner_client = FakeOwnerClient(lambda request: [me], entities={(7, 8, "known", None, "Known")})
    owner_pool = SimpleNamespace(clients={PHONE: SimpleNamespace(raw_client=owner_client)})
    handle = await serve_pool_relay(owner_pool, None)
    connection = await RelayConnection.open(handle.endpoint)

    async def no_lock(*_args, **_kwargs):
        raise AssertionError("a relayed pool must not take the session lock")

    monkeypatch.setattr(backends, "acquire_session_lock", no_lock)
    await db.add_account(Account(phone=PHONE, session_string="owner-only-session", is_active=True))
    pool = ClientPool(TelegramAuth(1, "hash"), db, relay=connection)
    pool._materializer = SimpleNamespace(
        materialize=lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("no session file in relay mode")),
        ensure_empty_env_file=lambda: "",
    )
    try:
        yield pool, connection, owner_client
    finally:
        await pool.disconnect_all()
        await handle.stop()


async def test_relayed_pool_runs_requests_on_the_owner_connection(relayed_pool):
    pool, _connection, owner_client = relayed_pool

    await pool.initialize()

    raw = pool.clients[PHONE].raw_client
    assert isinstance(raw, RelayTelegramClient)
    me = await raw.get_me()
    assert me.id == 4242
    assert owner_client.calls
    # The owner's entity cache came along: cached peers resolve offline.
    assert raw.session.get_input_entity(7) == types.InputPeerUser(user_id=7, access_hash=8)


async def test_native_leases_resolve_cached_peers_offline_too(relayed_pool):
    """Sends, publishing and live reads take native leases: without the owner's
    cache each of them would spend the owner's resolve budget on known peers."""
    pool, _connection, _owner = relayed_pool
    await pool.initialize()

    result = await pool.get_native_client_by_phone(PHONE)
    assert result is not None
    session, phone = result
    try:
        assert session.raw_client.session.get_input_entity(7) == types.InputPeerUser(user_id=7, access_hash=8)
    finally:
        await pool.release_client(phone, session=session)


async def test_relayed_pool_leaves_the_shared_limiter_state_to_the_owner(relayed_pool, db):
    pool, _connection, _owner = relayed_pool
    await pool.initialize()

    await pool.persist_resolve_limiter_snapshot()
    await pool.persist_resolve_username_backoff()

    assert await db.get_setting(RESOLVE_LIMITER_SNAPSHOT_SETTING) is None


async def test_teardown_closes_the_relay_connection(relayed_pool):
    pool, connection, _owner = relayed_pool
    await pool.initialize()

    await pool.disconnect_all()

    assert not connection.is_open
    assert pool.clients == {}
