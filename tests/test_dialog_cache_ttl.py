"""TTL + re-auth invalidation for the per-phone dialog-warm flag (#1043).

For numeric ``PeerChannel`` collection, ``_acquire_collection_client`` warms the
Telethon entity cache once per phone per process and records that via
``mark_dialogs_fetched(phone)`` (gate ``is_dialogs_fetched(phone)``). The
StringSession entity cache lives in the *session object* and is rebuilt on a
fresh ``get_dialogs()`` call, so a warm done once is normally enough.

The gap this suite locks down: on a long-lived worker the warm flag was treated
as "warm forever" — there was **no TTL** and **no invalidation when the session
object is replaced**. After a re-auth that swaps in a fresh StringSession (empty
entity cache), ``is_dialogs_fetched`` still returned True → the warm round-trip
was skipped → numeric-channel resolves started missing on the long-running
worker.

Telethon detail (verified against telethon 1.42, see section 3 below): the
entity cache that resolves a numeric ``PeerChannel`` lives in
``StringSession._entities`` on the *session object*, and ``get_input_entity``
falls back to it. So it survives a bare ``disconnect()`` + ``connect()`` on the
same client, and is only lost when the session object itself is replaced.

Fix surface (cache flags only, no ClientPool refactor — #1023 boundary):
  * a monotonic TTL on the warm flag (``_dialogs_warm_ttl_sec``) — the robust
    catch-all for long-lived-worker drift;
  * invalidation of the warm flag where the session object is genuinely
    replaced (``add_client`` re-auth), mirroring what ``remove_client`` already
    does on teardown. A bare ``reconnect_phone`` / ``force_reconnect_phone`` of
    the same session deliberately does NOT invalidate (cache survives → a
    re-warm would be a needless round-trip and a FloodWait risk).
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import SchedulerConfig
from src.models import Channel
from src.telegram.backends import TelegramTransportSession
from src.telegram.client_pool import ClientPool
from src.telegram.collector import _ACQUIRE_RETRY, Collector


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.get_accounts = AsyncMock(return_value=[])
    db.repos.dialog_cache.list_dialogs = AsyncMock(return_value=[])
    db.repos.dialog_cache.get_cached_at = AsyncMock(return_value=None)
    db.repos.dialog_cache.replace_dialogs = AsyncMock()
    db.repos.dialog_cache.clear_dialogs = AsyncMock()
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
# 1. Fresh warm flag stays warm within the TTL (perf: still "warm once").
# ---------------------------------------------------------------------------


def test_fresh_warm_flag_is_considered_fetched(pool):
    """A just-warmed phone must read as fetched — no needless re-warm (#1043)."""
    pool.mark_dialogs_fetched("+7001")
    assert pool.is_dialogs_fetched("+7001") is True


def test_warm_flag_within_ttl_does_not_expire(pool):
    """Inside the TTL window the flag stays warm — we do not regress the
    "warm at most once per phone" behaviour the hot path relies on."""
    clock = {"now": 1_000.0}
    pool._monotonic = lambda: clock["now"]
    pool._dialogs_warm_ttl_sec = 100.0

    pool.mark_dialogs_fetched("+7001")
    clock["now"] += 99.0  # still inside the 100s window

    assert pool.is_dialogs_fetched("+7001") is True


# ---------------------------------------------------------------------------
# 2. TTL expiry → the next collection re-warms.
# ---------------------------------------------------------------------------


def test_warm_flag_expires_after_ttl(pool):
    """Past the TTL the flag reads as cold → the next pass warms again."""
    clock = {"now": 1_000.0}
    pool._monotonic = lambda: clock["now"]
    pool._dialogs_warm_ttl_sec = 100.0

    pool.mark_dialogs_fetched("+7001")
    clock["now"] += 101.0  # just past the window

    assert pool.is_dialogs_fetched("+7001") is False


def test_expired_warm_flag_can_be_rewarmed(pool):
    """After TTL expiry a fresh mark restores the warm state for a new window."""
    clock = {"now": 1_000.0}
    pool._monotonic = lambda: clock["now"]
    pool._dialogs_warm_ttl_sec = 100.0

    pool.mark_dialogs_fetched("+7001")
    clock["now"] += 200.0
    assert pool.is_dialogs_fetched("+7001") is False

    pool.mark_dialogs_fetched("+7001")  # re-warm
    assert pool.is_dialogs_fetched("+7001") is True


# ---------------------------------------------------------------------------
# 3. Re-auth (new StringSession) invalidates the warm flag; a bare reconnect
#    of the same session keeps it (the entity cache survives on the same
#    session object — re-warming there would be a needless round-trip).
#
# Telethon fact this rests on (verified against telethon 1.42): the entity
# cache that resolves a numeric ``PeerChannel`` lives in
# ``StringSession._entities`` on the *session object*. ``get_input_entity``
# falls back to ``session.get_input_entity(peer)``, so the cache survives
# ``client.disconnect()`` + ``client.connect()`` as long as the same client /
# session object is reused. Only swapping in a brand-new session (re-auth)
# starts from an empty cache.
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_add_client_resets_warm_flag(pool):
    """Swapping a StringSession (re-auth) drops the warm flag → re-warm next pass.

    The new session object has an empty entity cache, so a stale warm flag
    would make ``_acquire_collection_client`` skip the warm round-trip and let
    numeric-channel resolves miss on the long-running worker (#1043).
    """
    pool.mark_dialogs_fetched("+7001")
    assert pool.is_dialogs_fetched("+7001") is True

    # add_client connects the account; make that a no-op for the unit test.
    lease = MagicMock()
    lease.disconnect_on_release = False
    pool._connect_account = AsyncMock(return_value=lease)
    pool._backend_router = MagicMock()
    pool._backend_router.release = AsyncMock()

    await pool.add_client("+7001", "new-session-string")

    assert pool.is_dialogs_fetched("+7001") is False


@pytest.mark.anyio
async def test_reconnect_phone_keeps_warm_flag(pool):
    """A reconnect reuses the same session object → entity cache survives →
    the warm flag must be kept (re-warming would be a needless round-trip)."""
    client = AsyncMock()
    client.is_connected = MagicMock(side_effect=[False, True])
    client.connect = AsyncMock()
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)
    pool.mark_dialogs_fetched("+7001")

    result = await pool.reconnect_phone("+7001")

    assert result is True
    assert pool.is_dialogs_fetched("+7001") is True


@pytest.mark.anyio
async def test_force_reconnect_phone_keeps_warm_flag(pool):
    """force_reconnect (#556 MTProto brick recovery) tears down and re-opens the
    transport on the *same* session object → ``session._entities`` survives → the
    warm flag stays warm; no re-warm / FloodWait risk on the hot path (#1043)."""
    client = AsyncMock()
    client.disconnect = AsyncMock()
    client.connect = AsyncMock()
    client.is_user_authorized = AsyncMock(return_value=True)
    client.is_connected = MagicMock(return_value=True)
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)
    pool.mark_dialogs_fetched("+7001")

    result = await pool.force_reconnect_phone("+7001")

    assert result is True
    assert pool.is_dialogs_fetched("+7001") is True


@pytest.mark.anyio
async def test_acquire_from_lease_resets_warm_flag_on_session_replacement(pool):
    """Auto-reconnect fallback swaps in a fresh backend session → warm flag drops.

    When a cached direct session is disconnected and ``connect()`` fails,
    ``_acquire_from_lease`` falls back to the backend, which builds a brand-new
    client / StringSession (empty entity cache) and replaces ``clients[phone]``.
    The warm flag must be invalidated there too — otherwise numeric PeerChannel
    collection would skip the warm against the fresh empty session (#1043,
    cycle-review HIGH finding). A bare reconnect that *succeeds* is covered
    separately and keeps the flag.
    """
    # A cached direct session that looks disconnected → triggers the fallback.
    stale_client = AsyncMock()
    stale_client.is_connected = MagicMock(return_value=False)
    stale_client.connect = AsyncMock(side_effect=RuntimeError("stream closed"))
    pool.clients["+7001"] = TelegramTransportSession(stale_client, disconnect_on_close=False)
    pool.mark_dialogs_fetched("+7001")
    assert pool.is_dialogs_fetched("+7001") is True

    # Backend hands back a fresh session (new object → cold entity cache).
    fresh_client = AsyncMock()
    fresh_session = TelegramTransportSession(fresh_client, disconnect_on_close=False)
    lease = MagicMock()
    lease.session = fresh_session
    lease.disconnect_on_release = True
    pool._backend_router = MagicMock()
    pool._backend_router.acquire_client = AsyncMock(return_value=lease)

    account_lease = SimpleNamespace(
        account=SimpleNamespace(phone="+7001"), shared=False
    )
    result = await pool._acquire_from_lease(account_lease)

    assert result is not None
    assert pool.is_dialogs_fetched("+7001") is False


@pytest.mark.anyio
async def test_acquire_from_lease_keeps_warm_flag_on_successful_inplace_reconnect(pool):
    """A successful in-place auto-reconnect reuses the same session object →
    entity cache survives → the warm flag must be kept (no needless re-warm).

    Complements the replacement case above: the reset is precise — it fires only
    when the backend swaps in a fresh session, not on every reconnect.
    """
    client = AsyncMock()
    client.is_connected = MagicMock(return_value=False)  # cached session looks down
    client.connect = AsyncMock()  # reconnect SUCCEEDS in place
    pool.clients["+7001"] = TelegramTransportSession(client, disconnect_on_close=False)
    pool.mark_dialogs_fetched("+7001")
    pool._active_leases["+7001"] = []
    pool._backend_router = MagicMock()
    pool._backend_router.acquire_client = AsyncMock()  # must NOT be used

    account_lease = SimpleNamespace(
        account=SimpleNamespace(phone="+7001"), shared=False
    )
    result = await pool._acquire_from_lease(account_lease)

    assert result is not None
    pool._backend_router.acquire_client.assert_not_awaited()  # same session reused
    assert pool.is_dialogs_fetched("+7001") is True


# ---------------------------------------------------------------------------
# 4. End-to-end on the collector's hot path: an expired warm flag drives
#    ``_acquire_collection_client`` to re-warm a no-username channel (so numeric
#    PeerChannel resolves recover on a long-lived worker), while a fresh flag
#    still skips the warm round-trip.
# ---------------------------------------------------------------------------


def _identity_adapt(session, **_kwargs):
    return session


def _warmable_session():
    session = MagicMock()
    session.warm_dialog_cache = AsyncMock()
    return session


def _real_pool_for_collector(mock_auth, mock_db, clock, ttl, *, session, phone):
    """A *real* ``ClientPool`` (so the genuine TTL warm-flag logic runs) with
    only the acquisition surface the collector touches stubbed out.

    Using a real pool — not a rebound ``make_mock_pool`` — keeps the TTL path
    honest: ``is_dialogs_fetched`` / ``mark_dialogs_fetched`` are the production
    methods reading the production state, with the clock injected via
    ``_monotonic``.
    """
    pool = ClientPool(mock_auth, mock_db)
    pool._monotonic = lambda: clock["now"]
    pool._dialogs_warm_ttl_sec = ttl
    pool.get_available_client = AsyncMock(return_value=(session, phone))
    pool.get_phone_for_channel = lambda channel_id: None
    pool.is_warming = lambda: False
    pool.wait_for_warm = AsyncMock()
    pool.release_client = AsyncMock()
    pool.remember_channel_phone = AsyncMock()
    return pool


@pytest.mark.anyio
async def test_acquire_rewarms_after_ttl_expiry(mock_auth, mock_db):
    """After the warm flag ages past its TTL, the next collection re-warms the
    phone — the core #1043 fix: numeric-channel resolves recover on a long-lived
    worker instead of silently skipping the warm forever."""
    channel = Channel(channel_id=556001, title="Private")  # no username
    session = _warmable_session()
    clock = {"now": 1_000.0}
    pool = _real_pool_for_collector(
        mock_auth, mock_db, clock, ttl=100.0, session=session, phone="+7000"
    )
    pool.mark_dialogs_fetched("+7000")  # warmed at t=1000
    clock["now"] += 200.0  # past the 100s TTL → stale

    collector = Collector(pool, mock_db, SchedulerConfig())
    with patch("src.telegram.collector.adapt_transport_session", _identity_adapt):
        result = await collector._acquire_collection_client(channel, set())

    assert result is not _ACQUIRE_RETRY
    session.warm_dialog_cache.assert_awaited_once()  # re-warmed
    assert pool.is_dialogs_fetched("+7000") is True  # freshly re-marked


@pytest.mark.anyio
async def test_acquire_skips_rewarm_within_ttl(mock_auth, mock_db):
    """Within the TTL the collector must NOT re-warm — the warm-once-per-window
    invariant (no perf regression / FloodWait risk on the hot path)."""
    channel = Channel(channel_id=556002, title="Private")  # no username
    session = _warmable_session()
    clock = {"now": 1_000.0}
    pool = _real_pool_for_collector(
        mock_auth, mock_db, clock, ttl=100.0, session=session, phone="+7000"
    )
    pool.mark_dialogs_fetched("+7000")  # warmed at t=1000
    clock["now"] += 50.0  # still inside the 100s TTL → fresh

    collector = Collector(pool, mock_db, SchedulerConfig())
    with patch("src.telegram.collector.adapt_transport_session", _identity_adapt):
        result = await collector._acquire_collection_client(channel, set())

    assert result is not _ACQUIRE_RETRY
    session.warm_dialog_cache.assert_not_awaited()  # no needless re-warm
