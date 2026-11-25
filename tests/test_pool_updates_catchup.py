"""Catch-up of updates missed while a connection was down (#1852).

Port of chigwell/telegram-mcp#409: Telegram does not re-push updates that
arrived while a client was disconnected, and Telethon's automatic reconnect
does not fetch a difference by itself (its own no-updates timeout fires only
after ~15 minutes). So every app-visible reconnect path in the pool issues
one ``catch_up()`` (a single ``updates.getDifference``), and an optional
periodic loop (``TG_CATCH_UP_INTERVAL`` / ``catch_up_interval_sec``) bounds
how late an update can arrive when a drop happens without our code noticing.

Contracts pinned here:
  * ``reconnect_phone`` / ``force_reconnect_phone`` / the acquisition
    auto-reconnect each catch up exactly once, only after a real reconnect;
  * a failed or unauthorized reconnect does NOT catch up;
  * a catch-up failure never turns a successful reconnect into a failure
    (the periodic loop, when enabled, retries);
  * the periodic loop skips disconnected clients and survives per-client
    errors; ``disconnect_all`` cancels it (no leaked task keeps the pool
    alive past teardown).
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from tg_agent.config import parse_catch_up_interval
from tg_agent.telegram.backends import TelegramTransportSession
from tg_agent.telegram.client_pool import ClientPool


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.get_accounts = AsyncMock(return_value=[])
    return db


@pytest.fixture
def mock_auth():
    auth = MagicMock()
    auth.api_id = 12345
    auth.api_hash = "hash"
    auth.create_client_from_session = AsyncMock()
    return auth


@pytest.fixture
def pool(mock_auth, mock_db):
    return ClientPool(mock_auth, mock_db)


# ---------------------------------------------------------------------------
# Config: TG_CATCH_UP_INTERVAL parsing (min 30 s clamp, 0 = off)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("", 0.0),
        ("0", 0.0),
        ("120", 120.0),
        ("2.5", 30.0),
        ("-5", 0.0),
        ("soon", 0.0),
    ],
)
def test_parse_catch_up_interval(raw, expected):
    assert parse_catch_up_interval(raw) == expected


def test_parse_catch_up_interval_takes_config_value(monkeypatch):
    from tg_agent.config import load_config

    monkeypatch.setenv("TG_CATCH_UP_INTERVAL", "120")
    config = load_config("no-such-config-file.yaml")
    assert config.telegram_runtime.catch_up_interval_sec == 120.0


# ---------------------------------------------------------------------------
# Post-reconnect catch-up on the explicit reconnect paths
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_reconnect_phone_catches_up_after_reconnect(pool):
    """A real reconnect (was disconnected → connected) issues one catch_up."""
    client = AsyncMock()
    client.is_connected = MagicMock(side_effect=[False, True])
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    result = await pool.reconnect_phone("+7001")

    assert result is True
    client.catch_up.assert_awaited_once()


@pytest.mark.anyio
async def test_reconnect_phone_skips_catch_up_when_already_connected(pool):
    """No reconnect happened → no catch_up (nothing was missed)."""
    client = AsyncMock()
    client.is_connected = MagicMock(return_value=True)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    result = await pool.reconnect_phone("+7001")

    assert result is True
    client.connect.assert_not_awaited()
    client.catch_up.assert_not_awaited()


@pytest.mark.anyio
async def test_reconnect_phone_catch_up_failure_keeps_success(pool):
    """A failing catch_up must not turn a successful reconnect into False."""
    client = AsyncMock()
    client.is_connected = MagicMock(side_effect=[False, True])
    client.catch_up = AsyncMock(side_effect=ConnectionError("gone"))
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    result = await pool.reconnect_phone("+7001")

    assert result is True
    client.catch_up.assert_awaited_once()


@pytest.mark.anyio
async def test_force_reconnect_phone_catches_up(pool):
    """force_reconnect (#556 brick recovery) rebuilds the transport → catch up."""
    client = AsyncMock()
    client.disconnect = AsyncMock()
    client.connect = AsyncMock()
    client.is_user_authorized = AsyncMock(return_value=True)
    client.is_connected = MagicMock(return_value=True)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    result = await pool.force_reconnect_phone("+7001")

    assert result is True
    client.catch_up.assert_awaited_once()


@pytest.mark.anyio
async def test_force_reconnect_phone_unauthorized_skips_catch_up(pool):
    """Burned session: reconnect fails → no catch_up on the half-open client."""
    client = AsyncMock()
    client.disconnect = AsyncMock()
    client.connect = AsyncMock()
    # Real contract: TelegramClient.is_connected is sync; a bare AsyncMock
    # would hand the pool an un-awaited coroutine per check.
    client.is_connected = MagicMock(return_value=True)
    client.is_user_authorized = AsyncMock(return_value=False)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)

    result = await pool.force_reconnect_phone("+7001")

    assert result is False
    client.catch_up.assert_not_awaited()


@pytest.mark.anyio
async def test_acquire_auto_reconnect_catches_up(pool):
    """The acquisition path reconnecting a dropped session also catches up.

    Upstream could not hook Telethon's *internal* auto-reconnect (hence their
    periodic loop); ours is app code in ``_acquire_from_lease`` — so it gets
    the catch-up directly.
    """
    client = AsyncMock()
    client.is_connected = MagicMock(return_value=False)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)
    pool._lease_pool.get_account = AsyncMock(return_value=None)
    pool._session_overrides["+7001"] = "sess"

    result = await pool.get_client_by_phone("+7001")

    assert result is not None
    client.catch_up.assert_awaited_once()
    await pool.release_client("+7001")


# ---------------------------------------------------------------------------
# Periodic catch-up loop
# ---------------------------------------------------------------------------


class _RoundClient:
    """Raw-client double counting catch_ups; optionally failing each call."""

    def __init__(self, connected: bool = True, error: Exception | None = None):
        self.connected = connected
        self.error = error
        self.catch_ups = 0

    def is_connected(self) -> bool:
        return self.connected

    async def catch_up(self) -> None:
        self.catch_ups += 1
        if self.error is not None:
            raise self.error


@pytest.mark.anyio
async def test_periodic_catch_up_skips_disconnected_and_survives_errors(pool):
    up = _RoundClient()
    down = _RoundClient(connected=False)
    failing = _RoundClient(error=ConnectionError("gone"))
    pool.clients = {
        "+7001": TelegramTransportSession(up, disconnect_on_close=False),
        "+7002": TelegramTransportSession(down, disconnect_on_close=False),
        "+7003": TelegramTransportSession(failing, disconnect_on_close=False),
    }

    task = asyncio.create_task(pool._periodic_catch_up(0.01))
    try:
        while up.catch_ups < 2:
            await asyncio.sleep(0.01)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    assert down.catch_ups == 0
    assert failing.catch_ups >= 2, "a per-client error must not stop the loop"


@pytest.mark.anyio
async def test_start_periodic_catch_up_respects_interval(pool):
    pool._catch_up_task = None

    pool._runtime_config.catch_up_interval_sec = 0.0
    pool._start_periodic_catch_up()
    assert pool._catch_up_task is None

    pool._runtime_config.catch_up_interval_sec = 30.0
    pool._start_periodic_catch_up()
    task = pool._catch_up_task
    assert task is not None

    # A repeated start must not stack a second loop.
    pool._start_periodic_catch_up()
    assert pool._catch_up_task is task

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.anyio
async def test_initialize_starts_periodic_catch_up(real_pool_harness_factory):
    """initialize() on a configured pool actually spawns the loop (e2e wiring)."""
    harness = real_pool_harness_factory()
    harness.queue_cli_client(phone="+70000000001", client=AsyncMock())
    await harness.add_account("+70000000001", session_string="session-a")
    harness.pool._runtime_config.catch_up_interval_sec = 30.0

    try:
        await harness.initialize_connected_accounts()
        task = getattr(harness.pool, "_catch_up_task", None)
        assert task is not None
    finally:
        task = getattr(harness.pool, "_catch_up_task", None)
        if task is not None:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        await harness.pool.disconnect_all()


@pytest.mark.anyio
async def test_start_periodic_catch_up_clamps_interval(pool):
    """A too-tight interval is lifted to the 30 s floor regardless of where it
    came from — a config.yaml value bypasses the env parser, so the pool must
    enforce the floor itself (#1859 review)."""
    captured: dict[str, float] = {}

    async def _fake_loop(interval: float) -> None:
        captured["interval"] = interval
        await asyncio.sleep(3600)

    pool._periodic_catch_up = _fake_loop
    pool._runtime_config.catch_up_interval_sec = 1.0
    pool._start_periodic_catch_up()
    # Let the spawned task run to its first await point (interval recorded there).
    for _ in range(3):
        await asyncio.sleep(0)
    assert captured["interval"] == 30.0
    task = pool._catch_up_task
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.anyio
async def test_periodic_catch_up_failure_log_carries_traceback(pool, caplog):
    """The per-client catch-all warning carries exc_info — an empty str(exc)
    must not be the only failure evidence (#1859 review)."""
    import logging

    failing = _RoundClient(error=RuntimeError("boom"))
    pool.clients = {
        "+7001": TelegramTransportSession(failing, disconnect_on_close=False)
    }

    task = asyncio.create_task(pool._periodic_catch_up(0.01))
    try:
        while failing.catch_ups < 1:
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.02)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert warnings, "expected a warning for the failing catch-up"
    assert all(r.exc_info for r in warnings), "catch-all warnings must carry the traceback"


@pytest.mark.anyio
async def test_disconnect_all_cancels_catch_up_task():
    """Teardown cancels the loop — no leaked task keeps operating on dead clients."""
    pool = ClientPool.__new__(ClientPool)
    pool.clients = {}
    pool._in_use = set()
    pool._active_leases = {}
    pool._dialogs_fetched = set()
    pool._mtproto_watchdog = MagicMock()
    pool._catch_up_task = asyncio.create_task(asyncio.sleep(3600))

    await pool.disconnect_all()

    assert pool._catch_up_task.cancelled() or pool._catch_up_task.done()
