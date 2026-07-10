"""Regression guards for Collector hot-zone invariants (issue #1028, epic #1024).

Tier-1 hot zone: ``Collector`` is a large module whose historically high-complexity
``_collect_channel`` drives the incremental read path. The happy read path is proven
in production, but the thin internal mechanics — dedup on retry, ``last_collected_id``
progress, cancellation, entity-cache warming, and back-to-back FloodWaits — were not
covered. Each test here pins one such invariant so a future refactor that breaks it
fails loudly instead of silently corrupting data or losing progress.

These complement (do **not** duplicate) the existing suites:
``test_collector.py``, ``test_collector_runtime.py``, ``test_collector_internals.py``,
``test_collector_extended.py``. Fake/harness-first — no real Telegram.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from telethon.errors import FloodWaitError

from src.config import SchedulerConfig
from src.models import Channel
from src.telegram.collector import (
    _ACQUIRE_RETRY,
    Collector,
    NoActiveCollectionClientsError,
)
from src.telegram.flood_wait import FloodWaitInfo, HandledFloodWaitError
from tests.helpers import (
    AsyncIterMessages,
    FakeTelethonClient,
    make_mock_message,
    make_mock_pool,
)


def _identity_adapt(session, **_kwargs):
    """Stand-in for adapt_transport_session: return the session unchanged so the
    fake client's own warm_dialog_cache / iter_messages are exercised directly."""
    return session


def _msg(msg_id: int, text: str | None = None):
    """A minimal Telethon-like message; mirrors test_collector._make_mock_message."""
    return make_mock_message(msg_id, text=text or f"msg {msg_id}")


async def _add_channel(db, channel_id: int, *, last_collected_id: int = 0, username: str = "guard"):
    """Insert a channel and seed its durable cursor, returning the stored row."""
    ch = Channel(
        channel_id=channel_id,
        title="Guard",
        username=username,
        last_collected_id=last_collected_id,
    )
    ch_id = await db.add_channel(ch)
    if last_collected_id:
        await db.update_channel_last_id(channel_id, last_collected_id)
    stored = await db.get_channel_by_pk(ch_id)
    assert stored is not None
    return stored


# ---------------------------------------------------------------------------
# Zone 1 — Dedup on retry / flood
#
# A FloodWait mid-stream rotates the channel to another account. The retry must
# resume from the *advanced* cursor (incremental: min_id = last_collected_id,
# already moved by the pre-flood flush) — it must NOT re-scan from the original
# min_id. And if an overlapping id is re-delivered anyway, INSERT OR IGNORE +
# UNIQUE(channel_id, message_id) must absorb it: no crash, no duplicate rows.
#
# (DB-level INSERT OR IGNORE semantics are covered in
# tests/repositories/test_messages_repository.py and test_database.py; here we
# pin the *collector* behaviour around the flood→retry boundary.)
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_retry_after_flood_resumes_from_advanced_cursor(db):
    """flood→retry must resume from the cursor the pre-flood flush advanced.

    Pass 1 persists msg 6 (cursor 5→6) then floods. Pass 2 (different account)
    must re-stream with min_id=6, not the original min_id=5 — so it never
    re-collects msg 6. A Telethon-faithful fake that honours min_id is used, so
    the retry stream yields only the genuinely-new msg 7.
    """
    stored = await _add_channel(db, -100501, last_collected_id=5)

    client1 = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    client2 = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())

    async def _flooding_first_pass():
        # Persisted by the finalize flush before the flood aborts.
        yield _msg(6, "original")
        raise FloodWaitError(request=None, capture=3)

    client1.iter_messages = MagicMock(side_effect=lambda *a, **kw: _flooding_first_pass())

    retry_min_ids: list[int] = []

    def _retry_stream(*_args, **kwargs):
        # A faithful Telethon fake: min_id is exclusive, so only ids > min_id
        # are delivered. After the pre-flood flush the cursor is 6.
        min_id = kwargs.get("min_id", 0)
        retry_min_ids.append(min_id)
        candidates = [_msg(6, "redelivered"), _msg(7, "new")]
        return AsyncIterMessages([m for m in candidates if m.id > min_id])

    client2.iter_messages = MagicMock(side_effect=_retry_stream)

    pool = make_mock_pool(
        get_available_client=AsyncMock(side_effect=[(client1, "+7001"), (client2, "+7002")])
    )
    collector = Collector(
        pool, db, SchedulerConfig(delay_between_requests_sec=0, max_flood_wait_sec=10)
    )

    await collector._collect_channel(stored)

    # The retry resumed from the advanced cursor, never re-scanning from 5.
    assert retry_min_ids == [6]
    messages, total = await db.search_messages(channel_id=-100501, limit=100)
    persisted_ids = sorted(m.message_id for m in messages)
    assert persisted_ids == [6, 7], f"expected no duplicate rows, got {persisted_ids}"
    assert total == 2


@pytest.mark.anyio
async def test_retry_redelivering_same_id_does_not_duplicate(db):
    """If a retry re-delivers an already-persisted id (e.g. a fake/Telethon that
    ignores min_id, or an off-by-one), the collector pass must end with exactly
    one row per id and no error surfaced.

    Dedup here is enforced by UNIQUE(channel_id, message_id) (schema.py) — the
    persistence layer absorbs the re-collected id. (The INSERT OR IGNORE clause
    itself is pinned at the repository level in
    tests/repositories/test_messages_repository.py and test_database.py; this
    test guards the *collector*'s end-state across the flood→retry boundary.)
    """
    stored = await _add_channel(db, -100506, last_collected_id=5)

    client1 = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    client2 = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())

    async def _flooding_first_pass():
        yield _msg(6, "original")
        raise FloodWaitError(request=None, capture=3)

    client1.iter_messages = MagicMock(side_effect=lambda *a, **kw: _flooding_first_pass())
    # Deliberately ignore min_id and re-deliver id 6 alongside the new id 7.
    client2.iter_messages = MagicMock(
        side_effect=lambda *a, **kw: AsyncIterMessages([_msg(6, "redelivered"), _msg(7, "new")])
    )

    pool = make_mock_pool(
        get_available_client=AsyncMock(side_effect=[(client1, "+7001"), (client2, "+7002")])
    )
    collector = Collector(
        pool, db, SchedulerConfig(delay_between_requests_sec=0, max_flood_wait_sec=10)
    )

    # Must not raise (a UNIQUE violation would surface as an error otherwise).
    await collector._collect_channel(stored)

    messages, total = await db.search_messages(channel_id=-100506, limit=100)
    persisted_ids = sorted(m.message_id for m in messages)
    assert persisted_ids == [6, 7], f"expected no duplicate rows, got {persisted_ids}"
    assert total == 2


@pytest.mark.anyio
async def test_reflushing_the_same_batch_object_is_idempotent(db):
    """Re-running collection over an overlapping window is a no-op for shared ids.

    Two back-to-back passes over ranges that overlap on ids {3,4,5} must leave a
    single row per id — proving INSERT OR IGNORE absorbs the re-collected window
    (the cursor is monotonic, but a forced re-scan still overlaps).
    """
    stored = await _add_channel(db, -100502, last_collected_id=0)

    first = [_msg(i) for i in range(1, 6)]  # ids 1..5
    second = [_msg(i) for i in range(3, 9)]  # ids 3..8 (3,4,5 overlap)

    client = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    client.iter_messages = MagicMock(side_effect=lambda *a, **kw: AsyncIterMessages(list(first)))
    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig(delay_between_requests_sec=0))

    await collector._collect_channel(stored)
    # Force a re-scan of an overlapping window with force=True (ignores cursor pre-filters).
    client.iter_messages = MagicMock(side_effect=lambda *a, **kw: AsyncIterMessages(list(second)))
    reread = await db.get_channel_by_channel_id(-100502)
    assert reread is not None
    await collector.collect_single_channel(reread, full=True, force=True)

    messages, total = await db.search_messages(channel_id=-100502, limit=100)
    persisted_ids = sorted(m.message_id for m in messages)
    assert persisted_ids == [1, 2, 3, 4, 5, 6, 7, 8]
    assert total == 8


# ---------------------------------------------------------------------------
# Zone 2 — last_collected_id progress integrity
#
# Existing coverage (test_collector.py): flush-failure keeps cursor,
# DB-busy keeps cursor, full backfill does not rewind. NOT covered below:
#   * an empty pass (no new messages, persisted_max == min_id) must not touch
#     the cursor at all;
#   * a channel deleted between the final flush and the cursor update must not
#     resurrect the row via update_channel_last_id.
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_empty_pass_leaves_cursor_untouched(db):
    """No new messages (persisted_max == min_id) must not write the cursor.

    Guards the ``persisted_max_msg_id > min_id`` gate in _finalize_collection_pass:
    an empty stream must leave last_collected_id exactly as it was, and must not
    call update_channel_last_id at all.
    """
    stored = await _add_channel(db, -100503, last_collected_id=42)

    client = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    client.iter_messages = MagicMock(side_effect=lambda *a, **kw: AsyncIterMessages([]))
    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig(delay_between_requests_sec=0))

    update_spy = AsyncMock(side_effect=db.update_channel_last_id)
    db.update_channel_last_id = update_spy  # type: ignore[method-assign]

    count = await collector._collect_channel(stored)

    assert count == 0
    update_spy.assert_not_awaited()
    updated = await db.get_channel_by_channel_id(-100503)
    assert updated is not None
    assert updated.last_collected_id == 42


@pytest.mark.anyio
async def test_channel_deleted_between_flush_and_update_does_not_resurrect(db):
    """A channel removed between the final flush and the cursor update must not
    be re-created. _finalize_collection_pass re-checks _channel_still_exists
    before update_channel_last_id; if the row is gone the cursor is not written.
    """
    stored = await _add_channel(db, -100504, last_collected_id=5)

    client = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    client.iter_messages = MagicMock(
        side_effect=lambda *a, **kw: AsyncIterMessages([_msg(6), _msg(7)])
    )
    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig(delay_between_requests_sec=0))

    real_still_exists = collector._channel_still_exists
    delete_after = {"done": False}

    async def _delete_then_check(channel_id: int) -> bool:
        # Simulate the channel being deleted by the operator/web after the flush
        # but before the cursor update: first call (leftover-flush guard) sees it
        # present, the cursor-update guard sees it gone.
        present = await real_still_exists(channel_id)
        if present and not delete_after["done"]:
            existing = await db.get_channel_by_channel_id(channel_id)
            if existing is not None and existing.id:
                await db.delete_channel(existing.id)
            delete_after["done"] = True
            return True
        return await real_still_exists(channel_id)

    collector._channel_still_exists = _delete_then_check  # type: ignore[method-assign]
    update_spy = AsyncMock(side_effect=db.update_channel_last_id)
    db.update_channel_last_id = update_spy  # type: ignore[method-assign]

    await collector._collect_channel(stored)

    # Channel is gone; the cursor update must have been skipped (no resurrection).
    update_spy.assert_not_awaited()
    assert await db.get_channel_by_channel_id(-100504) is None


# ---------------------------------------------------------------------------
# Zone 3 — Cancellation
#
# Existing test_cancel_stats_does_not_cancel_channel_collection checks only the
# flags. Here we drive a real stream and trip cancellation at a `% 10 == 0`
# boundary, then assert progress already persisted before the abort survives.
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_cancellation_mid_stream_breaks_at_ten_boundary_and_keeps_progress(db):
    """Cancel tripped mid-stream STOPS the stream at the next `% 10 == 0` check
    AND keeps the partial batch that finalize flushes.

    The flush boundary is left at the default 500 (larger than the 25-message
    stream) so no batch flush happens mid-stream — the ONLY thing that can break
    the loop early is the `% 10 == 0` cancellation check in
    _stream_channel_messages. Cancellation is set as the 10th message is yielded,
    so the loop must break right at len==10 and finalize flushes those 10. The
    exact `total == 10` upper bound pins that mid-stream break: a regression that
    ignores the `% 10` cancel check would drain all 25 and fail here
    (cycle-review #1080 — the earlier flush-boundary variant did not pin this).
    """
    stored = await _add_channel(db, -100505, last_collected_id=0)
    cancel_event = asyncio.Event()

    def _stream_cancelling_at_ten(*_args, **_kwargs):
        # 25 messages; arm cancellation exactly as the 10th is delivered so the
        # next `% 10 == 0` check (at len == 10) breaks the stream.
        async def _gen():
            for i in range(1, 26):
                if i == 10:
                    cancel_event.set()
                yield _msg(i)

        return _gen()

    client = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    client.iter_messages = MagicMock(side_effect=_stream_cancelling_at_ten)
    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig(delay_between_requests_sec=0))

    # Default MESSAGE_FLUSH_BATCH_SIZE (500) > 25, so no mid-stream flush fires;
    # the `% 10` cancel check is the sole early-exit path.
    await collector.collect_single_channel(stored, cancel_event=cancel_event)

    updated = await db.get_channel_by_channel_id(-100505)
    assert updated is not None
    messages, total = await db.search_messages(channel_id=-100505, limit=100)
    # Stream broke at the 10th message: exactly 10 persisted, none of the
    # remaining 15. The upper bound is what pins the cancellation break — `>= 10`
    # alone would pass even if the `% 10` check were a no-op and all 25 streamed.
    assert total == 10
    assert updated.last_collected_id == 10
    # The partial batch was flushed by finalize — contiguous 1..10, no loss.
    persisted_ids = sorted(m.message_id for m in messages)
    assert persisted_ids == list(range(1, 11))


# ---------------------------------------------------------------------------
# Zone 4 — Entity-cache / warm dialogs (_acquire_collection_client)
#
# For no-username (private) channels the client picker must:
#   * when no preferred phone is known and the pool is warming, wait up to 30s
#     for warming before re-reading the channel→phone map;
#   * fall back to get_available_client if the preferred phone is still unknown
#     after warming;
#   * warm the PeerChannel dialog cache at most once per phone (is_dialogs_fetched
#     gate) — StringSession loses the entity cache between restarts, so a needless
#     re-warm is a wasted round-trip and a FloodWait risk.
# ---------------------------------------------------------------------------


def _warmable_session():
    """A session double exposing the warm_dialog_cache coroutine the picker calls."""
    session = MagicMock()
    session.warm_dialog_cache = AsyncMock()
    return session


@pytest.mark.anyio
async def test_acquire_waits_for_warming_then_uses_discovered_phone(db):
    """No preferred phone + pool warming → wait_for_warm(30s), then re-read the map.

    The phone only becomes known *after* warming completes, so the picker must
    consult get_phone_for_channel a second time (post-warm) and use it, instead
    of falling straight through to get_available_client.
    """
    channel = Channel(channel_id=555001, title="Private")  # no username
    session = _warmable_session()

    pool = make_mock_pool(get_client_by_phone=AsyncMock(return_value=(session, "+7001")))
    pool.is_warming = lambda: True

    warmed = {"done": False}

    async def _wait_for_warm(timeout=None):
        # Warming "completes" here: the channel→phone map is now populated.
        assert timeout == 30.0
        pool._channel_phone_map[555001] = "+7001"
        warmed["done"] = True

    pool.wait_for_warm = AsyncMock(side_effect=_wait_for_warm)
    pool.mark_dialogs_fetched("+7001")  # cache already warm for this phone

    collector = Collector(pool, db, SchedulerConfig())
    with patch("src.telegram.collector_mixins.collection.adapt_transport_session", _identity_adapt):
        result = await collector._acquire_collection_client(channel, set())

    assert warmed["done"] is True
    pool.wait_for_warm.assert_awaited_once()
    assert result is not _ACQUIRE_RETRY
    acquired_session, phone, _resolve_cache_only = result
    assert phone == "+7001"
    pool.get_client_by_phone.assert_awaited_once_with("+7001")
    pool.get_available_client.assert_not_awaited()


@pytest.mark.anyio
async def test_acquire_falls_back_to_available_when_phone_unknown_after_warm(db):
    """Preferred phone still unknown after warming → fall back to get_available_client.

    Guards the final branch of the no-username picker: warming finished but the
    map is still empty (brand-new channel, never seen on any account), so the
    picker must not deadlock waiting — it grabs any available client.
    """
    channel = Channel(channel_id=555002, title="Private")  # no username
    session = _warmable_session()

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(session, "+7009")))
    pool.is_warming = lambda: True
    pool.wait_for_warm = AsyncMock()  # warming completes but discovers nothing
    pool.mark_dialogs_fetched("+7009")

    collector = Collector(pool, db, SchedulerConfig())
    with patch("src.telegram.collector_mixins.collection.adapt_transport_session", _identity_adapt):
        result = await collector._acquire_collection_client(channel, set())

    pool.wait_for_warm.assert_awaited_once()
    assert result is not _ACQUIRE_RETRY
    _session, phone, _cache_only = result
    assert phone == "+7009"
    pool.get_available_client.assert_awaited_once()
    pool.get_client_by_phone.assert_not_awaited()


@pytest.mark.anyio
async def test_acquire_does_not_rewarm_already_warmed_phone(db):
    """A phone whose dialog cache is already warm must not be re-warmed.

    is_dialogs_fetched(phone) gates the warm_dialog_cache() call; if it returns
    True the picker must skip warming entirely (no duplicate round-trip).
    """
    channel = Channel(channel_id=555003, title="Private")  # no username
    session = _warmable_session()

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(session, "+7000")))
    pool.mark_dialogs_fetched("+7000")  # already warm

    collector = Collector(pool, db, SchedulerConfig())
    with patch("src.telegram.collector_mixins.collection.adapt_transport_session", _identity_adapt):
        result = await collector._acquire_collection_client(channel, set())

    assert result is not _ACQUIRE_RETRY
    session.warm_dialog_cache.assert_not_awaited()


@pytest.mark.anyio
async def test_acquire_warms_cold_phone_exactly_once_and_marks_fetched(db):
    """A cold phone is warmed once and then marked fetched so the next pass skips it."""
    channel = Channel(channel_id=555004, title="Private")  # no username
    session = _warmable_session()

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(session, "+7000")))
    # "+7000" starts cold (not in _dialogs_fetched).

    collector = Collector(pool, db, SchedulerConfig())
    with patch("src.telegram.collector_mixins.collection.adapt_transport_session", _identity_adapt):
        result = await collector._acquire_collection_client(channel, set())

    assert result is not _ACQUIRE_RETRY
    session.warm_dialog_cache.assert_awaited_once()
    assert pool.is_dialogs_fetched("+7000") is True


@pytest.mark.anyio
async def test_acquire_flood_during_warm_retries_without_marking_fetched(db):
    """A FloodWait while warming must release the client and signal a retry, and
    must NOT mark the phone as warmed (so the next pass tries warming again).
    """
    channel = Channel(channel_id=555005, title="Private")  # no username
    session = _warmable_session()
    # A long (non-transient, > 60s) flood propagates instead of being retried
    # inside run_with_flood_wait_retry, so it reaches the picker's handler.
    info = FloodWaitInfo(
        operation="collect_channel_warm_dialog_cache",
        phone="+7000",
        wait_seconds=600,
        next_available_at_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
        detail="flood 600s",
    )
    session.warm_dialog_cache = AsyncMock(side_effect=HandledFloodWaitError(info))

    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(session, "+7000")))
    pool.release_client = AsyncMock()

    collector = Collector(pool, db, SchedulerConfig())
    with patch("src.telegram.collector_mixins.collection.adapt_transport_session", _identity_adapt):
        result = await collector._acquire_collection_client(channel, set())

    assert result is _ACQUIRE_RETRY
    pool.release_client.assert_awaited_once_with("+7000")
    # Cache must remain cold so the retry re-warms.
    assert pool.is_dialogs_fetched("+7000") is False


@pytest.mark.anyio
async def test_acquire_falls_back_when_preferred_phone_unavailable(db):
    """#1245: an unavailable *preferred* phone must rotate to another account, not
    raise a global unavailability error.

    For a private channel with a known preferred phone the picker calls
    get_client_by_phone(preferred). On the old code, if that returned None (the
    preferred phone alone is flood-waited / in-use / gone) the picker fell
    straight through to _raise_collection_unavailability →
    NoActiveCollectionClientsError, which drains the ENTIRE in-memory collection
    queue in collection_queue.py. One busy preferred phone must not wipe every
    other channel's pending task: the picker must fall back to
    get_available_client and use whatever account is free.
    """
    channel = Channel(channel_id=555006, title="Private", preferred_phone="+7001")
    session = _warmable_session()

    pool = make_mock_pool(
        get_client_by_phone=AsyncMock(return_value=None),  # preferred unavailable
        get_available_client=AsyncMock(return_value=(session, "+7002")),  # rotate here
    )
    pool.mark_dialogs_fetched("+7002")  # skip warming; isolate the acquisition branch

    collector = Collector(pool, db, SchedulerConfig())
    with patch("src.telegram.collector_mixins.collection.adapt_transport_session", _identity_adapt):
        result = await collector._acquire_collection_client(channel, set())

    assert result is not _ACQUIRE_RETRY
    _session, phone, _cache_only = result
    assert phone == "+7002", "picker must rotate to the available account, not raise"
    pool.get_client_by_phone.assert_awaited_once_with("+7001")
    pool.get_available_client.assert_awaited_once()


@pytest.mark.anyio
async def test_acquire_raises_when_preferred_unavailable_and_no_fallback(db):
    """Companion to #1245: when the preferred phone is unavailable AND no other
    account is free, the picker must still raise the unavailability error.

    Guards the #1245 fix from over-reaching: the fallback to get_available_client
    is added, but a genuine "no clients at all" outage must still surface
    NoActiveCollectionClientsError (the requeue-and-defer path) rather than being
    silently swallowed.
    """
    channel = Channel(channel_id=555007, title="Private", preferred_phone="+7001")

    availability = SimpleNamespace(
        state="no_connected_active",
        retry_after_sec=None,
        next_available_at_utc=None,
    )
    pool = make_mock_pool(
        get_client_by_phone=AsyncMock(return_value=None),  # preferred unavailable
        get_available_client=AsyncMock(return_value=None),  # nothing else free either
        get_stats_availability=AsyncMock(return_value=availability),
    )

    collector = Collector(pool, db, SchedulerConfig())
    with patch("src.telegram.collector_mixins.collection.adapt_transport_session", _identity_adapt):
        with pytest.raises(NoActiveCollectionClientsError):
            await collector._acquire_collection_client(channel, set())

    pool.get_client_by_phone.assert_awaited_once_with("+7001")
    pool.get_available_client.assert_awaited_once()


# ---------------------------------------------------------------------------
# Zone 5 — Multiple back-to-back FloodWaits inside collection
#
# Existing coverage handles a single stream FloodWait (retry / rotate / all
# flooded). NOT covered below:
#   * two stream FloodWaits in a row, rotating across three accounts, with
#     progress carried through every rotation;
#   * a transient FloodWait raised while warming the dialog cache (a distinct
#     path from a stream/resolve flood) that is retried and then collects.
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_two_consecutive_stream_floods_rotate_and_keep_progress(db):
    """Two stream FloodWaits in a row rotate across three accounts without losing
    progress: each flooded pass persists its partial batch, advances the cursor,
    and the NEXT rotation resumes from that advanced cursor (not the original
    min_id). The durable cursor ends at the last id seen by the account that
    finished.

    Each rotated client is Telethon-faithful — it records the min_id it was
    handed and yields only ids > min_id. A stale-cursor regression (a rotation
    reusing the original min_id=5) is therefore caught by the recorded min_id
    sequence, not merely inferred from the final DB state (cycle-review #1080).
    """
    stored = await _add_channel(db, -100601, last_collected_id=5)

    client1 = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    client2 = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    client3 = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    seen_min_ids: list[int] = []

    def _faithful_flood(new_id: int, *, flood: bool):
        """Yield `new_id` only if it is past the handed-in min_id, then flood."""

        async def _gen(min_id: int):
            seen_min_ids.append(min_id)
            if new_id > min_id:
                yield _msg(new_id)
            if flood:
                raise FloodWaitError(request=None, capture=3)

        def _factory(*_args, **kwargs):
            return _gen(kwargs.get("min_id", 0))

        return _factory

    client1.iter_messages = MagicMock(side_effect=_faithful_flood(6, flood=True))
    client2.iter_messages = MagicMock(side_effect=_faithful_flood(7, flood=True))
    client3.iter_messages = MagicMock(side_effect=_faithful_flood(8, flood=False))

    pool = make_mock_pool(
        get_available_client=AsyncMock(
            side_effect=[
                (client1, "+70001"),
                (client2, "+70002"),
                (client3, "+70003"),
            ]
        )
    )
    collector = Collector(
        pool, db, SchedulerConfig(delay_between_requests_sec=0, max_flood_wait_sec=10)
    )

    count = await collector._collect_channel(stored)

    # Each rotation resumed from the cursor the previous pass advanced — never
    # re-scanning from the original min_id=5.
    assert seen_min_ids == [5, 6, 7]
    assert count == 3  # msgs 6, 7, 8 each persisted across the rotations
    updated = await db.get_channel_by_channel_id(-100601)
    assert updated is not None
    assert updated.last_collected_id == 8
    # Each of the two flooded passes reported its flood to the pool.
    assert pool.report_flood.await_count == 2
    messages, total = await db.search_messages(channel_id=-100601, limit=100)
    assert sorted(m.message_id for m in messages) == [6, 7, 8]
    assert total == 3


@pytest.mark.anyio
async def test_all_accounts_flooded_after_multiple_rotations_preserves_cursor(db):
    """After exhausting every account to consecutive floods, the cursor still
    reflects the furthest persisted id — surfacing unavailability must not rewind
    progress already written by the flooded passes.
    """
    stored = await _add_channel(db, -100602, last_collected_id=5)

    client1 = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    client2 = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    seen_min_ids: list[int] = []

    def _faithful_long_flood(new_id: int):
        """Yield `new_id` only if past the handed-in min_id, then long-flood."""

        async def _gen(min_id: int):
            seen_min_ids.append(min_id)
            if new_id > min_id:
                yield _msg(new_id)
            raise FloodWaitError(request=None, capture=600)  # long → rotate, not retry

        def _factory(*_args, **kwargs):
            return _gen(kwargs.get("min_id", 0))

        return _factory

    client1.iter_messages = MagicMock(side_effect=_faithful_long_flood(6))
    client2.iter_messages = MagicMock(side_effect=_faithful_long_flood(7))

    pool = make_mock_pool(
        get_available_client=AsyncMock(
            side_effect=[
                (client1, "+70001"),
                (client2, "+70002"),
                None,  # everyone flooded now
            ]
        ),
        get_stats_availability=AsyncMock(
            return_value=SimpleNamespace(
                state="all_flooded",
                retry_after_sec=600,
                next_available_at_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
        ),
    )
    collector = Collector(
        pool, db, SchedulerConfig(delay_between_requests_sec=0, max_flood_wait_sec=10)
    )

    from src.telegram.collector import AllCollectionClientsFloodedError

    with pytest.raises(AllCollectionClientsFloodedError):
        await collector._collect_channel(stored)

    # The second rotation resumed from the cursor the first flooded pass
    # advanced (6), not the original min_id=5 — progress carried through.
    assert seen_min_ids == [5, 6]

    updated = await db.get_channel_by_channel_id(-100602)
    assert updated is not None
    # msg 7 was persisted by the second flooded pass before the rotation hit a
    # dead end; the cursor must not have rewound below it.
    assert updated.last_collected_id == 7
    assert pool.report_flood.await_count == 2


@pytest.mark.anyio
async def test_transient_warm_dialog_flood_is_retried_in_place(db):
    """A transient (<60s) FloodWait while warming the dialog cache is retried in
    place by run_with_flood_wait_retry — a distinct path from a stream/resolve
    flood. The second warm succeeds, the phone is marked fetched, and the picker
    returns a usable client (no _ACQUIRE_RETRY, no client release).
    """
    channel = Channel(channel_id=606001, title="Private")  # no username
    session = _warmable_session()
    sleeps: list[float] = []

    async def _fake_sleep(seconds):
        sleeps.append(seconds)

    warm_calls = {"n": 0}

    async def _warm():
        warm_calls["n"] += 1
        if warm_calls["n"] == 1:
            info = FloodWaitInfo(
                operation="collect_channel_warm_dialog_cache",
                phone="+7000",
                wait_seconds=3,  # transient → retried in place
                next_available_at_utc=datetime(2026, 1, 1, tzinfo=timezone.utc),
                detail="flood 3s",
            )
            raise HandledFloodWaitError(info)

    session.warm_dialog_cache = AsyncMock(side_effect=_warm)
    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(session, "+7000")))
    pool.release_client = AsyncMock()

    collector = Collector(pool, db, SchedulerConfig(delay_between_requests_sec=0))
    with (
        patch("src.telegram.collector_mixins.collection.adapt_transport_session", _identity_adapt),
        patch("src.telegram.flood_wait.asyncio.sleep", _fake_sleep),
    ):
        result = await collector._acquire_collection_client(channel, set())

    assert warm_calls["n"] == 2  # first warm flooded, retry succeeded
    assert result is not _ACQUIRE_RETRY
    assert pool.is_dialogs_fetched("+7000") is True
    pool.release_client.assert_not_awaited()
    # The transient flood was slept off exactly once before the retry (the
    # flood-wait helper adds a small safety buffer on top of the 3s wait).
    assert len(sleeps) == 1
    assert sleeps[0] >= 3
