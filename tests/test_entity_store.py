"""Channel access keys survive a restart (#1877).

Reading a channel the account has not joined takes its access hash, and only a
username lookup returns one: 20 a minute and 60 an hour per account. The pool
sessions kept the keys in memory only, so every restart of the connection's owner
looked the channels up again. These tests pin the round trip through
``session_entities`` on the project database and on the gateway's.
"""

from __future__ import annotations

import logging
import time
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from telethon.crypto import AuthKey
from telethon.sessions import SQLiteSession

from tg_agent.gateway.pool_storage import GatewayPoolStorage
from tg_agent.gateway.store import GatewayStore
from tg_agent.models import Account
from tg_agent.telegram.account_lease_pool import AccountLease
from tg_agent.telegram.backends import BackendClientLease, TelegramTransportSession
from tg_agent.telegram.client_pool import ClientPool
from tg_agent.telegram.entity_store import USERNAME_TTL_SEC
from tg_shared.auth import TelegramAuth
from tg_shared.sessions import FreshEntityStringSession

PHONE = "+10000000001"
CHANNEL = (-1001111111111, 4242, "somechan", None, "Some channel")
GROUP = (-2222222, 0, None, None, "Some group")
USER = (3333333, 77, "someone", "10000000003", "Someone")
LOGIN = b"\x01" * 256


def _pool(store) -> ClientPool:
    return ClientPool(TelegramAuth(1, "hash"), store)


def _session(*rows, key: bytes = LOGIN) -> FreshEntityStringSession:
    session = FreshEntityStringSession()
    session.auth_key = AuthKey(key)
    session.add_entity_rows(rows)
    return session


def _connected(pool: ClientPool, session) -> None:
    """Publish a client the way the pool holds one, without a Telegram connection."""
    pool.clients[PHONE] = cast(Any, SimpleNamespace(raw_client=SimpleNamespace(session=session)))


async def _restored(store, session) -> FreshEntityStringSession:
    await _pool(store).restore_session_entities(PHONE, SimpleNamespace(session=session))
    return session


async def _rows(store, columns: str = "account, id, hash, username, name") -> list[tuple]:
    async with store.transaction() as conn:
        cursor = await conn.execute(f"SELECT {columns} FROM session_entities ORDER BY id")
        return [tuple(row) for row in await cursor.fetchall()]


@pytest.fixture(params=["project", "gateway"])
async def store(request, db, tmp_path):
    if request.param == "project":
        yield db
        return
    gateway = GatewayStore(tmp_path / "gateway")
    await gateway.open()
    try:
        storage = GatewayPoolStorage(gateway)
        await storage.initialize()
        yield storage
    finally:
        await gateway.close()


async def test_a_restart_finds_the_channel_keys_of_the_last_run(store, caplog):
    first = _pool(store)
    _connected(first, _session(CHANNEL, GROUP, USER))
    await first.persist_session_entities()

    with caplog.at_level(logging.INFO, logger="tg_agent.telegram.entity_store"):
        fresh = await _restored(store, _session())

    assert fresh._entities == {CHANNEL, GROUP}  # users stay in memory: they would grow without bound
    assert "2 channel key(s) loaded" in caplog.text


async def test_a_new_login_drops_the_keys_of_the_old_one(store):
    first = _pool(store)
    _connected(first, _session(CHANNEL))
    await first.persist_session_entities()

    fresh = await _restored(store, _session(key=b"\x02" * 256))

    assert fresh._entities == set()
    assert await _rows(store) == []


async def test_a_username_unconfirmed_for_a_day_loads_without_it(store):
    first = _pool(store)
    _connected(first, _session(CHANNEL))
    await first.persist_session_entities()
    await store.execute_write("UPDATE session_entities SET date = ?", (int(time.time()) - USERNAME_TTL_SEC - 60,))

    second = _pool(store)
    fresh = _session()
    await second.restore_session_entities(PHONE, SimpleNamespace(session=fresh))

    assert fresh._entities == {(CHANNEL[0], CHANNEL[1], None, None, CHANNEL[4])}  # the id and key still work

    # The next answer that names the channel confirms the username again.
    _connected(second, fresh)
    fresh.add_entity_rows([CHANNEL])
    await second.persist_session_entities()

    assert await _rows(store, "username, date >= strftime('%s', 'now') - 60") == [("somechan", 1)]


async def test_a_live_row_wins_over_the_stored_one(store):
    first = _pool(store)
    _connected(first, _session(CHANNEL))
    await first.persist_session_entities()
    live = (CHANNEL[0], 9999, "somechan", None, "Renamed")

    fresh = await _restored(store, _session(live))

    assert fresh._entities == {live}


async def test_only_changed_channel_rows_are_written(store):
    pool = _pool(store)
    session = _session(CHANNEL, GROUP, USER)
    _connected(pool, session)
    await pool.persist_session_entities()
    await store.execute_write("UPDATE session_entities SET name = 'written before'")

    await pool.persist_session_entities()  # nothing changed: nothing written
    moved = (CHANNEL[0], 5555, "somechan", None, "Some channel")
    session.add_entity_rows([moved])
    await pool.persist_session_entities()

    assert await _rows(store) == [
        (PHONE, CHANNEL[0], 5555, "somechan", "Some channel"),
        (PHONE, GROUP[0], 0, None, "written before"),
    ]


async def test_removing_or_closing_a_client_saves_its_keys(db):
    removed = _pool(db)
    _connected(removed, _session(CHANNEL))
    await removed.remove_client(PHONE)
    assert [row[1] for row in await _rows(db)] == [CHANNEL[0]]

    closed = _pool(db)
    _connected(closed, _session(GROUP))
    await closed.disconnect_all()
    assert [row[1] for row in await _rows(db)] == [CHANNEL[0], GROUP[0]]


async def test_file_sessions_and_relayed_pools_keep_out(db, tmp_path):
    relayed = _pool(db)
    relayed._relay = cast(Any, SimpleNamespace())  # it takes the owner's keys over the relay
    _connected(relayed, _session(CHANNEL))
    await relayed.persist_session_entities()
    assert await _rows(db) == []

    owner = _pool(db)
    _connected(owner, _session(CHANNEL))
    await owner.persist_session_entities()
    fresh = _session()
    await relayed.restore_session_entities(PHONE, SimpleNamespace(session=fresh))
    assert fresh._entities == set()

    file_session = SQLiteSession(str(tmp_path / "account"))  # keeps its own keys in its file
    try:
        file_session.auth_key = AuthKey(LOGIN)
        _connected(owner, file_session)
        await owner.persist_session_entities()
        await owner.restore_session_entities(PHONE, SimpleNamespace(session=file_session))
    finally:
        file_session.close()
    assert [row[1] for row in await _rows(db)] == [CHANNEL[0]]


@pytest.mark.parametrize("path", ["connect", "replace"])
async def test_a_client_gets_its_keys_before_the_pool_hands_it_out(db, monkeypatch, path):
    pool = _pool(db)
    raw = SimpleNamespace(session=_session())
    seen = []

    async def restore(phone, raw_client):
        seen.append((phone, raw_client, phone in pool.clients))

    monkeypatch.setattr(pool, "restore_session_entities", restore)
    lease = BackendClientLease(
        phone=PHONE, session=TelegramTransportSession(raw), backend_name="native", disconnect_on_release=True
    )
    monkeypatch.setattr(pool._backend_router, "acquire_client", AsyncMock(return_value=lease))
    account = Account(phone=PHONE, session_string="session", is_active=True)

    if path == "connect":
        await pool._connect_account(account)
    else:  # a fresh client replaces a missing or broken one
        assert await pool._acquire_from_lease(AccountLease(account=account, shared=True)) is not None

    assert seen == [(PHONE, raw, False)]
    assert pool.clients[PHONE].raw_client is raw
