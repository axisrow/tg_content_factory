"""Tests for parallel collection workers in CollectionQueue.

Validates that the queue runs multiple channel tasks concurrently when more
than one Telegram account is available, and that concurrency is capped by
the configured collection_worker_count.
"""
from __future__ import annotations

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.collection_queue import CollectionQueue
from src.config import SchedulerConfig
from src.database import Database
from src.models import Channel, CollectionTask, CollectionTaskStatus
from src.telegram.collector import Collector


def _make_pool(
    *,
    clients: dict[str, object] | None = None,
    get_available_client=None,
):
    pool = MagicMock()
    pool.clients = clients if clients is not None else {"+7001": MagicMock()}
    pool._active_leases = {}
    pool._premium_flood_wait_until = {}
    pool._session_overrides = {}
    pool._dialogs_cache = {}
    pool._dialogs_cache_ttl_sec = 60.0
    pool._dialogs_db_cache_ttl_sec = 3600.0
    pool._dialog_refresh_tasks = {}
    pool._channel_phone_map = {}
    pool._warming_task = None
    pool.is_dialogs_fetched = MagicMock(return_value=False)
    pool.mark_dialogs_fetched = MagicMock()
    pool.connected_phones = MagicMock(return_value=set(pool.clients.keys()))
    pool.get_phone_for_channel = MagicMock(return_value=None)
    pool.register_channel_phone = MagicMock()
    pool.clear_channel_phone = MagicMock()
    pool.is_warming = MagicMock(return_value=False)
    pool.release_client = AsyncMock()
    pool.report_flood = AsyncMock()
    pool.get_available_client = get_available_client or AsyncMock(return_value=None)
    pool.get_client_by_phone = AsyncMock(return_value=None)
    pool.reconnect_phone = AsyncMock(return_value=False)
    pool.get_stats_availability = AsyncMock()
    del pool.get_stats_availability
    pool.available_stats_client_count = AsyncMock(return_value=len(pool.clients))
    pool.available_collection_client_count = AsyncMock(return_value=len(pool.clients))
    pool._connected_phones = lambda: set(pool.clients.keys())
    pool._in_use = set()
    return pool


class _ParallelCollector:
    def __init__(self, target_workers: int = 1):
        self.calls: list[int] = []
        self.start_events: list[asyncio.Event] = []
        self.cancelled_calls: list[int] = []
        self._target_workers = target_workers
        self._active = 0
        self.is_cancelled = False

    def collection_worker_count(self) -> int:
        return self._target_workers

    async def collect_single_channel(
        self,
        channel,
        *,
        full=False,
        progress_callback=None,
        force=False,
        cancel_event=None,
    ):
        self._active += 1
        evt = asyncio.Event()
        self.start_events.append(evt)
        self.calls.append(channel.channel_id)
        release_wait = asyncio.create_task(evt.wait())
        wait_tasks = {release_wait}
        cancel_wait = None
        if cancel_event is not None:
            cancel_wait = asyncio.create_task(cancel_event.wait())
            wait_tasks.add(cancel_wait)
        try:
            done, _ = await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_COMPLETED)
            if cancel_wait is not None and cancel_wait in done:
                self.cancelled_calls.append(channel.channel_id)
                return 0
        finally:
            for task in wait_tasks:
                task.cancel()
            await asyncio.gather(*wait_tasks, return_exceptions=True)
            self._active -= 1
        return 10

    async def cancel(self):
        self.is_cancelled = True
        for evt in self.start_events:
            evt.set()

    def get_collection_availability(self):
        return type("Avail", (), {"state": "available", "retry_after_sec": None, "next_available_at_utc": None})()


async def _seed_channel(db: Database, channel_id: int) -> Channel:
    ch = Channel(channel_id=channel_id, title=f"ch_{abs(channel_id)}")
    await db.add_channel(ch)
    channels = await db.get_channels()
    return next(c for c in channels if c.channel_id == channel_id)


@pytest.mark.anyio
async def test_parallel_workers_collect_multiple_channels(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        ch1 = await _seed_channel(db, -1001)
        ch2 = await _seed_channel(db, -1002)

        collector = _ParallelCollector(target_workers=2)
        queue = CollectionQueue(collector, db)

        await queue.enqueue(ch1)
        await queue.enqueue(ch2)

        deadline = asyncio.get_event_loop().time() + 3.0
        while asyncio.get_event_loop().time() < deadline:
            if len(collector.start_events) >= 2:
                break
            await asyncio.sleep(0.05)

        assert len(collector.start_events) == 2, (
            f"Expected 2 concurrent collections, got {len(collector.start_events)}"
        )
        assert set(collector.calls) == {-1001, -1002}

        for evt in collector.start_events:
            evt.set()
        await asyncio.sleep(0.3)
        await queue.shutdown()
    finally:
        await db.close()


@pytest.mark.anyio
async def test_single_worker_when_configured(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        ch1 = await _seed_channel(db, -2001)
        ch2 = await _seed_channel(db, -2002)

        collector = _ParallelCollector(target_workers=1)
        queue = CollectionQueue(collector, db)

        await queue.enqueue(ch1)
        await queue.enqueue(ch2)

        deadline = asyncio.get_event_loop().time() + 3.0
        while asyncio.get_event_loop().time() < deadline:
            if len(collector.start_events) >= 1:
                break
            await asyncio.sleep(0.05)

        assert len(collector.start_events) == 1, "Only one worker should be running"
        assert len(collector.calls) == 1

        collector.start_events[0].set()
        await asyncio.sleep(0.5)

        assert len(collector.calls) == 2, "Second channel should be processed after first completes"

        for evt in collector.start_events:
            evt.set()
        await asyncio.sleep(0.3)
        await queue.shutdown()
    finally:
        await db.close()


@pytest.mark.anyio
async def test_active_task_ids_tracks_running_tasks(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        ch1 = await _seed_channel(db, -3001)
        ch2 = await _seed_channel(db, -3002)

        collector = _ParallelCollector(target_workers=2)
        queue = CollectionQueue(collector, db)

        task_id_1 = await queue.enqueue(ch1)
        task_id_2 = await queue.enqueue(ch2)

        deadline = asyncio.get_event_loop().time() + 3.0
        while asyncio.get_event_loop().time() < deadline:
            if len(queue._active_task_ids) >= 2:
                break
            await asyncio.sleep(0.05)

        assert task_id_1 in queue._active_task_ids
        assert task_id_2 in queue._active_task_ids

        for evt in collector.start_events:
            evt.set()
        await asyncio.sleep(0.3)
        await queue.shutdown()
    finally:
        await db.close()


@pytest.mark.anyio
async def test_cancel_specific_task_does_not_cancel_others(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        ch1 = await _seed_channel(db, -4001)
        ch2 = await _seed_channel(db, -4002)

        collector = _ParallelCollector(target_workers=2)
        queue = CollectionQueue(collector, db)

        task_id_1 = await queue.enqueue(ch1)
        task_id_2 = await queue.enqueue(ch2)

        deadline = asyncio.get_event_loop().time() + 3.0
        while asyncio.get_event_loop().time() < deadline:
            if len(queue._active_task_ids) >= 2:
                break
            await asyncio.sleep(0.05)

        await queue.cancel_task(task_id_1)

        deadline = asyncio.get_event_loop().time() + 3.0
        while asyncio.get_event_loop().time() < deadline:
            if -4001 in collector.cancelled_calls:
                break
            await asyncio.sleep(0.05)

        assert collector.cancelled_calls == [-4001]
        assert collector.is_cancelled is False
        assert task_id_2 in queue._active_task_ids

        for evt in collector.start_events:
            evt.set()
        await asyncio.sleep(0.3)
        await queue.shutdown()

        task_1 = await db.get_collection_task(task_id_1)
        task_2 = await db.get_collection_task(task_id_2)
        assert task_1 is not None
        assert task_2 is not None
        assert task_1.status == "cancelled"
        assert task_2.status == "completed"
    finally:
        await db.close()


@pytest.mark.anyio
async def test_shutdown_requeues_active_tasks(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        ch1 = await _seed_channel(db, -5001)

        collector = _ParallelCollector(target_workers=1)
        queue = CollectionQueue(collector, db)

        task_id = await queue.enqueue(ch1)

        deadline = asyncio.get_event_loop().time() + 3.0
        while asyncio.get_event_loop().time() < deadline:
            if queue._active_task_ids:
                break
            await asyncio.sleep(0.05)

        assert task_id in queue._active_task_ids

        await queue.shutdown(grace_timeout=0.05)

        task = await db.get_collection_task(task_id)
        assert task is not None
        assert task.status == "pending"
    finally:
        await db.close()


@pytest.mark.anyio
async def test_collector_active_count_reflects_parallel_runs():
    pool = _make_pool()
    db = MagicMock()
    collector = Collector(pool, db, SchedulerConfig())
    assert collector._active_collection_count == 0
    assert collector.is_running is False

    collector._active_collection_count = 3
    assert collector.is_running is True

    collector._active_collection_count = 0
    assert collector.is_running is False


@pytest.mark.anyio
async def test_collect_single_channel_task_cancel_event_clears_idle_global_cancel():
    pool = _make_pool()
    db = MagicMock()
    collector = Collector(pool, db, SchedulerConfig())
    collector._load_min_subscribers_filter = AsyncMock(return_value=0)
    collector._collect_channel = AsyncMock(return_value=0)
    task_cancel_event = asyncio.Event()

    collector._cancel_event.set()
    await collector.collect_single_channel(
        Channel(channel_id=-6001, title="test"),
        cancel_event=task_cancel_event,
    )

    assert not collector._cancel_event.is_set()
    assert collector._is_collection_cancelled(task_cancel_event) is False
    collector._collect_channel.assert_awaited_once()
    assert collector._collect_channel.await_args.kwargs["cancel_event"] is task_cancel_event


@pytest.mark.anyio
async def test_collect_single_channel_task_cancel_event_preserves_active_global_cancel():
    pool = _make_pool()
    db = MagicMock()
    collector = Collector(pool, db, SchedulerConfig())
    collector._load_min_subscribers_filter = AsyncMock(return_value=0)
    collector._collect_channel = AsyncMock(return_value=0)
    task_cancel_event = asyncio.Event()

    collector._active_collection_count = 1
    collector._cancel_event.set()
    await collector.collect_single_channel(
        Channel(channel_id=-6002, title="test"),
        cancel_event=task_cancel_event,
    )

    assert collector._cancel_event.is_set()
    assert collector._is_collection_cancelled(task_cancel_event) is True
    assert collector._active_collection_count == 1
    collector._collect_channel.assert_awaited_once()
    assert collector._collect_channel.await_args.kwargs["cancel_event"] is task_cancel_event


@pytest.mark.anyio
async def test_queue_ignores_stale_collector_cancel_for_idle_channel_task(tmp_path):
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        channel = await _seed_channel(db, -6003)
        pool = _make_pool()
        collector = Collector(pool, db, SchedulerConfig())
        collector._load_min_subscribers_filter = AsyncMock(return_value=0)
        collector._collect_channel = AsyncMock(return_value=4)
        collector._cancel_event.set()
        queue = CollectionQueue(collector, db)

        task_id = await queue.enqueue(channel)
        await asyncio.wait_for(queue._queue.join(), timeout=2.0)

        task = await db.get_collection_task(task_id)
        assert task is not None
        assert task.status == CollectionTaskStatus.COMPLETED
        assert task.messages_collected == 4
        assert not collector.is_cancelled
    finally:
        await queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_supervisor_logs_worker_crash(caplog):
    collector = _ParallelCollector(target_workers=1)
    queue = CollectionQueue(collector, MagicMock())
    # A malformed queue item crashes the worker on tuple-unpack — an unexpected
    # error the worker does not handle — so the supervisor must log the crash.
    # (A transient pre-dispatch validation error is now *recovered*, not crashed —
    # see test_collection_queue_strand.py — so a different, genuinely-uncaught
    # fault is needed to exercise the supervisor's crash-logging path.)
    queue._queue.put_nowait(("malformed-item",))

    caplog.set_level(logging.ERROR)
    queue._ensure_supervisor()

    deadline = asyncio.get_event_loop().time() + 3.0
    while asyncio.get_event_loop().time() < deadline:
        if queue._supervisor and queue._supervisor.done():
            break
        await asyncio.sleep(0.05)

    assert "Collection queue worker crashed" in caplog.text
    assert not queue._active_task_ids
    await queue.shutdown()


@pytest.mark.anyio
async def test_collection_worker_count_auto_mode():
    pool = _make_pool(clients={"+1": MagicMock(), "+2": MagicMock(), "+3": MagicMock()})
    db = MagicMock()
    collector = Collector(pool, db, SchedulerConfig(collection_worker_count=0))
    assert collector.collection_worker_count() == 3
    assert await collector.available_collection_worker_count() == 3


@pytest.mark.anyio
async def test_collection_worker_count_auto_caps_at_10():
    clients = {f"+{i}": MagicMock() for i in range(15)}
    pool = _make_pool(clients=clients)
    db = MagicMock()
    collector = Collector(pool, db, SchedulerConfig(collection_worker_count=0))
    assert collector.collection_worker_count() == 10
    pool.available_collection_client_count.return_value = 12
    assert await collector.available_collection_worker_count() == 10


@pytest.mark.anyio
async def test_collection_worker_count_explicit_limit():
    pool = _make_pool(clients={"+1": MagicMock(), "+2": MagicMock(), "+3": MagicMock()})
    db = MagicMock()
    collector = Collector(pool, db, SchedulerConfig(collection_worker_count=2))
    assert collector.collection_worker_count() == 2
    assert await collector.available_collection_worker_count() == 2


@pytest.mark.anyio
async def test_available_collection_worker_count_uses_exclusive_available_clients():
    pool = _make_pool(clients={"+1": MagicMock(), "+2": MagicMock(), "+3": MagicMock()})
    pool.available_collection_client_count.return_value = 1
    db = MagicMock()
    collector = Collector(pool, db, SchedulerConfig(collection_worker_count=0))

    assert collector.collection_worker_count() == 3
    assert await collector.available_collection_worker_count() == 1
    pool.available_collection_client_count.assert_awaited_once()


@pytest.mark.anyio
async def test_available_collection_slot_count_returns_raw_exclusive_slots():
    pool = _make_pool(clients={"+1": MagicMock(), "+2": MagicMock()})
    pool.available_collection_client_count.return_value = 0
    db = MagicMock()
    collector = Collector(pool, db, SchedulerConfig(collection_worker_count=0))

    assert await collector.available_collection_slot_count() == 0
    assert await collector.available_collection_worker_count() == 1


@pytest.mark.anyio
async def test_available_collection_worker_count_respects_explicit_limit_and_availability():
    pool = _make_pool(clients={"+1": MagicMock(), "+2": MagicMock(), "+3": MagicMock()})
    pool.available_collection_client_count.return_value = 1
    db = MagicMock()
    collector = Collector(pool, db, SchedulerConfig(collection_worker_count=3))

    assert await collector.available_collection_worker_count() == 1


@pytest.mark.anyio
async def test_available_collection_worker_count_keeps_single_fallback_when_none_available():
    pool = _make_pool(clients={"+1": MagicMock(), "+2": MagicMock(), "+3": MagicMock()})
    pool.available_collection_client_count.return_value = 0
    db = MagicMock()
    collector = Collector(pool, db, SchedulerConfig(collection_worker_count=0))

    assert await collector.available_collection_worker_count() == 1


@pytest.mark.anyio
async def test_collection_worker_count_no_clients():
    pool = _make_pool(clients={})
    db = MagicMock()
    collector = Collector(pool, db, SchedulerConfig(collection_worker_count=0))
    assert collector.collection_worker_count() == 1
    assert await collector.available_collection_worker_count() == 1


@pytest.mark.anyio
async def test_target_worker_count_delegates_to_collector():
    collector = MagicMock()
    del collector.available_collection_slot_count
    del collector.available_collection_worker_count
    collector.collection_worker_count = MagicMock(return_value=3)
    queue = CollectionQueue(collector, MagicMock())
    assert queue._target_worker_count() == 3
    assert await queue._available_target_worker_count() == 3


@pytest.mark.anyio
async def test_target_worker_count_prefers_available_collection_count():
    collector = MagicMock()
    del collector.available_collection_slot_count
    collector.available_collection_worker_count = AsyncMock(return_value=2)
    collector.collection_worker_count = MagicMock(return_value=3)
    queue = CollectionQueue(collector, MagicMock())

    assert queue._target_worker_count() == 3
    collector.collection_worker_count.reset_mock()
    assert await queue._available_target_worker_count() == 2
    collector.collection_worker_count.assert_not_called()


@pytest.mark.anyio
async def test_available_target_worker_count_adds_free_slots_to_active_tasks():
    collector = MagicMock()
    collector.available_collection_slot_count = AsyncMock(return_value=1)
    collector.collection_worker_count = MagicMock(return_value=3)
    queue = CollectionQueue(collector, MagicMock())
    queue._active_task_ids[99] = asyncio.Event()

    assert await queue._available_target_worker_count() == 2


@pytest.mark.anyio
async def test_worker_parks_when_no_free_slot_is_available_for_active_task():
    collector = MagicMock()
    collector.available_collection_slot_count = AsyncMock(return_value=0)
    collector.collection_worker_count = MagicMock(return_value=3)
    collector.collect_single_channel = AsyncMock(return_value=0)
    channels = MagicMock()
    queue = CollectionQueue(collector, channels)
    queue._active_task_ids[99] = asyncio.Event()
    queue._queue.put_nowait((1, Channel(channel_id=-8001, title="queued"), False, False))

    await queue._run_single_worker()

    assert queue._queue.qsize() == 1
    collector.collect_single_channel.assert_not_awaited()


@pytest.mark.anyio
async def test_worker_starts_replacement_when_free_slot_exists_for_active_task():
    collector = MagicMock()
    collector.available_collection_slot_count = AsyncMock(return_value=1)
    collector.collection_worker_count = MagicMock(return_value=3)
    collector.collect_single_channel = AsyncMock(return_value=0)
    collector.is_cancelled = False

    task = CollectionTask(
        id=1,
        channel_id=-8002,
        channel_title="queued",
        status=CollectionTaskStatus.PENDING,
    )
    channels = MagicMock()
    channels.get_collection_task = AsyncMock(return_value=task)
    channels.update_collection_task = AsyncMock()
    channels.update_collection_task_progress = AsyncMock()

    queue = CollectionQueue(collector, channels)
    queue._active_task_ids[99] = asyncio.Event()
    queued = Channel(channel_id=-8002, title="queued")
    queue._queue.put_nowait((1, queued, False, False))

    await queue._run_single_worker()

    collector.collect_single_channel.assert_awaited_once()
    assert collector.collect_single_channel.await_args.args[0] == queued
    assert queue._queue.empty()
