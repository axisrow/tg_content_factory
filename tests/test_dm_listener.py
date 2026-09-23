"""Tests for the real-time incoming-DM listener (#1426, epic #1416 stage 2.1)."""

from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from types import SimpleNamespace

from src.database import Database
from src.models import Account
from src.telegram.auth import TelegramAuth
from src.telegram.client_pool import ClientPool
from src.telegram.dm_listener import DmListener, IncomingDmEvent


class _FakeRawClient:
    def __init__(self, name: str):
        self.name = name
        self.added: list[tuple] = []
        self.removed: list[tuple] = []

    def add_event_handler(self, callback, event=None):
        self.added.append((callback, event))

    def remove_event_handler(self, callback, event=None):
        self.removed.append((callback, event))
        return True


class _FakePool:
    """Only what the listener is allowed to use: the connected-client map.

    Deliberately has NO lease API — the listener must work directly on
    ``pool.clients`` without ever reserving an account (#1426).
    """

    def __init__(self):
        self.clients: dict[str, object] = {}


def _session(client: _FakeRawClient):
    return SimpleNamespace(raw_client=client)


class _FakeEvent:
    def __init__(self, *, is_private=True, chat_id=42, message_id=7, text="hi"):
        self.is_private = is_private
        self.chat_id = chat_id
        self.message = SimpleNamespace(id=message_id, text=text, date=None)


async def _wait_until(condition, timeout: float = 2.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met within timeout")


async def _make_db(tmp_path, *, inactive_phone: str | None = None) -> Database:
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    await db.repos.accounts.add_account(Account(phone="+111", session_string="s"))
    if inactive_phone is not None:
        inactive_id = await db.repos.accounts.add_account(
            Account(phone=inactive_phone, session_string="s", is_active=False)
        )
        assert inactive_id is not None
    return db


async def test_attaches_only_enabled_connected_accounts(tmp_path):
    """+111 active+connected → attached; +222 disabled and +333 (no account
    row) must NOT get a handler."""
    db = await _make_db(tmp_path, inactive_phone="+222")
    try:
        pool = _FakePool()
        c111 = _FakeRawClient("c111")
        c222 = _FakeRawClient("c222")
        c333 = _FakeRawClient("c333")
        pool.clients = {
            "+111": _session(c111),
            "+222": _session(c222),
            "+333": _session(c333),
        }
        listener = DmListener(pool, db)

        await listener._reconcile()

        assert set(listener._attached) == {"+111"}
        assert len(c111.added) == 1
        assert c222.added == []
        assert c333.added == []
    finally:
        await db.close()


async def test_dm_enqueued_group_events_ignored(tmp_path):
    db = await _make_db(tmp_path)
    try:
        pool = _FakePool()
        client = _FakeRawClient("c")
        pool.clients = {"+111": _session(client)}
        listener = DmListener(pool, db)
        await listener._reconcile()
        callback = client.added[0][0]

        callback(_FakeEvent(is_private=False))
        assert listener._queue.empty()

        callback(_FakeEvent(chat_id=77, message_id=9, text="привет"))

        assert listener._queue.qsize() == 1
        dm = listener._queue.get_nowait()
        assert isinstance(dm, IncomingDmEvent)
        assert dm.phone == "+111"
        assert dm.chat_id == 77
        assert dm.message_id == 9
        assert dm.text == "привет"

        status = listener.status()
        account = status["accounts"]["+111"]
        assert account["updates_received"] == 1
        assert account["last_update_at"] is not None
        assert 0 <= account["seconds_since_update"] < 5
    finally:
        await db.close()


async def test_liveness_visible_before_any_update(tmp_path):
    """An attached account that never received an update reports None —
    'updates stopped/never arrived' must be observable (#1426)."""
    db = await _make_db(tmp_path)
    try:
        pool = _FakePool()
        pool.clients = {"+111": _session(_FakeRawClient("c"))}
        listener = DmListener(pool, db)
        await listener._reconcile()

        account = listener.status()["accounts"]["+111"]
        assert account["attached"] is True
        assert account["updates_received"] == 0
        assert account["last_update_at"] is None
        assert account["seconds_since_update"] is None
    finally:
        await db.close()


async def test_consumer_delivers_event_to_callback(tmp_path):
    db = await _make_db(tmp_path)
    received: list[IncomingDmEvent] = []

    async def on_dm(dm: IncomingDmEvent) -> None:
        received.append(dm)

    try:
        pool = _FakePool()
        client = _FakeRawClient("c")
        pool.clients = {"+111": _session(client)}
        listener = DmListener(pool, db, event_callback=on_dm)
        await listener.start()
        try:
            await _wait_until(lambda: bool(client.added))
            client.added[0][0](_FakeEvent(chat_id=5, message_id=6, text="тест"))
            await _wait_until(lambda: len(received) == 1)
            assert received[0].chat_id == 5
            assert received[0].text == "тест"
        finally:
            await listener.stop()
    finally:
        await db.close()


async def test_reconcile_reattaches_after_client_replacement(tmp_path):
    """The pool silently swaps clients[phone] for a fresh object; the handler
    must move to the new raw client (regression guard, #1426)."""
    db = await _make_db(tmp_path)
    try:
        pool = _FakePool()
        old_client = _FakeRawClient("old")
        pool.clients = {"+111": _session(old_client)}
        listener = DmListener(pool, db)
        await listener._reconcile()

        new_client = _FakeRawClient("new")
        pool.clients["+111"] = _session(new_client)
        await listener._reconcile()

        assert len(old_client.removed) == 1
        assert old_client.removed[0][0] is old_client.added[0][0]
        assert len(new_client.added) == 1
        assert listener._attached["+111"].client is new_client
    finally:
        await db.close()


async def test_reconcile_detaches_disabled_account(tmp_path):
    db = await _make_db(tmp_path)
    try:
        pool = _FakePool()
        client = _FakeRawClient("c")
        pool.clients = {"+111": _session(client)}
        listener = DmListener(pool, db)
        await listener._reconcile()
        assert "+111" in listener._attached

        account_id = await db.repos.accounts.add_account(Account(phone="+111", session_string="s2"))
        assert account_id is not None
        await db.repos.accounts.set_account_active(account_id, active=False)
        await listener._reconcile()

        assert "+111" not in listener._attached
        assert len(client.removed) == 1
    finally:
        await db.close()


async def test_reconcile_detaches_disconnected_client(tmp_path):
    db = await _make_db(tmp_path)
    try:
        pool = _FakePool()
        client = _FakeRawClient("c")
        pool.clients = {"+111": _session(client)}
        listener = DmListener(pool, db)
        await listener._reconcile()

        del pool.clients["+111"]
        await listener._reconcile()

        assert listener._attached == {}
        assert len(client.removed) == 1
    finally:
        await db.close()


async def test_stop_detaches_all_handlers(tmp_path):
    db = await _make_db(tmp_path)
    try:
        pool = _FakePool()
        client = _FakeRawClient("c")
        pool.clients = {"+111": _session(client)}
        listener = DmListener(pool, db)
        await listener._reconcile()

        await listener.stop()
        await listener.stop()  # idempotent

        assert listener._attached == {}
        assert len(client.removed) == 1
        assert listener.status()["running"] is False
    finally:
        await db.close()


async def test_double_start_does_not_duplicate_handlers(tmp_path):
    """No double listener: a second start() must be a no-op (#1426)."""
    db = await _make_db(tmp_path)
    try:
        pool = _FakePool()
        client = _FakeRawClient("c")
        pool.clients = {"+111": _session(client)}
        listener = DmListener(pool, db, reconcile_interval=0.05)

        await listener.start()
        await listener.start()
        try:
            await _wait_until(lambda: bool(listener._attached))
            await asyncio.sleep(0.1)
            assert len(client.added) == 1
            assert listener.status()["running"] is True
        finally:
            await listener.stop()
        assert listener.status()["running"] is False
    finally:
        await db.close()


async def test_queue_does_not_lose_events_beyond_100(tmp_path):
    """The tg_messenger reference bus drops old items at 100 per subscriber —
    here nothing may be lost (regression guard, #1426)."""
    db = await _make_db(tmp_path)
    received: list[IncomingDmEvent] = []

    async def on_dm(dm: IncomingDmEvent) -> None:
        received.append(dm)

    try:
        pool = _FakePool()
        client = _FakeRawClient("c")
        pool.clients = {"+111": _session(client)}
        listener = DmListener(pool, db, event_callback=on_dm)
        await listener._reconcile()
        callback = client.added[0][0]

        consumer = asyncio.create_task(listener._process_loop())
        try:
            for i in range(150):
                callback(_FakeEvent(chat_id=1, message_id=i, text=f"m{i}"))
            await _wait_until(lambda: len(received) == 150)
            assert [dm.message_id for dm in received] == list(range(150))
        finally:
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)
    finally:
        await db.close()


async def test_listener_does_not_hold_account_lease(tmp_path):
    """The listener works directly on pool.clients — the collection rotation on
    the same accounts must stay untouched (#1426)."""
    db = await _make_db(tmp_path)
    try:
        pool = ClientPool(TelegramAuth(0, ""), db)
        client = _FakeRawClient("c")
        pool.clients["+111"] = _session(client)
        listener = DmListener(pool, db)

        await listener._reconcile()

        assert "+111" in listener._attached
        assert pool._in_use == set()
        assert not pool._active_leases
    finally:
        await db.close()


async def test_callback_failure_does_not_kill_consumer(tmp_path):
    db = await _make_db(tmp_path)
    received: list[IncomingDmEvent] = []

    async def flaky(dm: IncomingDmEvent) -> None:
        if dm.message_id == 1:
            raise RuntimeError("boom")
        received.append(dm)

    try:
        pool = _FakePool()
        client = _FakeRawClient("c")
        pool.clients = {"+111": _session(client)}
        listener = DmListener(pool, db, event_callback=flaky)
        await listener._reconcile()
        callback = client.added[0][0]

        consumer = asyncio.create_task(listener._process_loop())
        try:
            callback(_FakeEvent(message_id=1))
            callback(_FakeEvent(message_id=2))
            await _wait_until(lambda: len(received) == 1)
            assert received[0].message_id == 2
        finally:
            consumer.cancel()
            await asyncio.gather(consumer, return_exceptions=True)
    finally:
        await db.close()


async def test_incoming_event_dataclass_shape():
    dm = IncomingDmEvent(
        phone="+1",
        chat_id=2,
        message_id=3,
        text="t",
        message_date=None,
        received_at=datetime.now(timezone.utc),
    )
    assert dm.phone == "+1"
