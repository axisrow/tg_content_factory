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
from src.telegram.collector import _ACQUIRE_RETRY, Collector
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
    ignores min_id, or an off-by-one), INSERT OR IGNORE must absorb it — exactly
    one row survives and the pass does not raise a UNIQUE violation.
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
async def test_cancellation_mid_stream_persists_flushed_progress(db):
    """Cancel tripped mid-stream must keep whatever was already flushed.

    With a flush boundary smaller than the stream, the first batch is persisted
    and last_collected_id advanced before cancellation breaks the loop. The
    leftover (sub-batch) tail is still flushed by finalize, so no collected
    message is silently dropped.
    """
    stored = await _add_channel(db, -100505, last_collected_id=0)
    cancel_event = asyncio.Event()

    client = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    # 25 messages; with the flush boundary shrunk to 10 the loop flushes at 10
    # and 20. We trip cancellation right after the first flush, so the loop must
    # break at the next `% 10 == 0` check without losing the persisted batch.
    msgs = [_msg(i) for i in range(1, 26)]
    client.iter_messages = MagicMock(side_effect=lambda *a, **kw: AsyncIterMessages(list(msgs)))
    pool = make_mock_pool(get_available_client=AsyncMock(return_value=(client, "+7000")))
    collector = Collector(pool, db, SchedulerConfig(delay_between_requests_sec=0))

    real_flush = db.insert_messages_batch

    async def _flush_then_cancel(batch):
        result = await real_flush(batch)
        # After the first real flush, request cancellation so the next
        # `% 10 == 0` check inside the stream breaks the loop.
        cancel_event.set()
        return result

    db.insert_messages_batch = AsyncMock(side_effect=_flush_then_cancel)  # type: ignore[method-assign]

    import src.telegram.collector as collector_mod

    original_batch_size = collector_mod.MESSAGE_FLUSH_BATCH_SIZE
    collector_mod.MESSAGE_FLUSH_BATCH_SIZE = 10
    try:
        await collector.collect_single_channel(stored, cancel_event=cancel_event)
    finally:
        collector_mod.MESSAGE_FLUSH_BATCH_SIZE = original_batch_size

    updated = await db.get_channel_by_channel_id(-100505)
    assert updated is not None
    # Progress persisted up to at least the first flushed batch — never lost.
    assert updated.last_collected_id >= 10
    messages, total = await db.search_messages(channel_id=-100505, limit=100)
    # Every persisted id must be contiguous from 1 (no gaps, no dupes).
    persisted_ids = sorted(m.message_id for m in messages)
    assert persisted_ids == list(range(1, total + 1))
    assert total >= 10


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
    with patch("src.telegram.collector.adapt_transport_session", _identity_adapt):
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
    with patch("src.telegram.collector.adapt_transport_session", _identity_adapt):
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
    with patch("src.telegram.collector.adapt_transport_session", _identity_adapt):
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
    with patch("src.telegram.collector.adapt_transport_session", _identity_adapt):
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
    with patch("src.telegram.collector.adapt_transport_session", _identity_adapt):
        result = await collector._acquire_collection_client(channel, set())

    assert result is _ACQUIRE_RETRY
    pool.release_client.assert_awaited_once_with("+7000")
    # Cache must remain cold so the retry re-warms.
    assert pool.is_dialogs_fetched("+7000") is False


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
    progress: each flooded pass persists its partial batch before rotating, and
    the durable cursor ends at the last id seen by the account that finished.
    """
    stored = await _add_channel(db, -100601, last_collected_id=5)

    client1 = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    client2 = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())
    client3 = FakeTelethonClient(entity_resolver=lambda _arg: SimpleNamespace())

    async def _flood_after(msg_id: int):
        yield _msg(msg_id)
        raise FloodWaitError(request=None, capture=3)

    client1.iter_messages = MagicMock(side_effect=lambda *a, **kw: _flood_after(6))
    client2.iter_messages = MagicMock(side_effect=lambda *a, **kw: _flood_after(7))
    client3.iter_messages = MagicMock(
        side_effect=lambda *a, **kw: AsyncIterMessages([_msg(8)])
    )

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

    async def _flood_after(msg_id: int):
        yield _msg(msg_id)
        raise FloodWaitError(request=None, capture=600)  # long → rotate, not retry

    client1.iter_messages = MagicMock(side_effect=lambda *a, **kw: _flood_after(6))
    client2.iter_messages = MagicMock(side_effect=lambda *a, **kw: _flood_after(7))

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
        patch("src.telegram.collector.adapt_transport_session", _identity_adapt),
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
