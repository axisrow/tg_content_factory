"""Tests for the periodic DB-pull loop in CollectionQueue.

Regression for: clicking "Собрать все каналы" on /scheduler/ wrote PENDING rows
into `collection_tasks` from the web side, but the worker's CollectionQueue
only ran `requeue_startup_tasks` once at boot — so freshly enqueued tasks were
ignored until the next worker restart.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import pytest

from src.collection_queue import CollectionQueue
from src.database import Database
from src.models import Channel
from src.telegram.collector import UsernameResolveFloodWaitDeferredError


class _FakeCollector:
    def __init__(self):
        self.calls: list[int] = []
        self.is_cancelled = False

    async def collect_single_channel(self, channel, *, full=False, progress_callback=None, force=False):
        self.calls.append(channel.channel_id)
        return 0

    async def cancel(self):
        return None


class _UsernameResolveFloodCollector(_FakeCollector):
    def __init__(self, next_available_at: datetime):
        super().__init__()
        self.next_available_at = next_available_at

    async def collect_single_channel(self, channel, *, full=False, progress_callback=None, force=False):
        self.calls.append(channel.channel_id)
        raise UsernameResolveFloodWaitDeferredError(
            wait_seconds=120,
            next_available_at=self.next_available_at,
        )


class _BlockingCollector:
    def __init__(self):
        self.calls: list[int] = []
        self.started = asyncio.Event()
        self.finish = asyncio.Event()
        self.is_cancelled = False

    async def collect_single_channel(self, channel, *, full=False, progress_callback=None, force=False):
        self.calls.append(channel.channel_id)
        self.started.set()
        await self.finish.wait()
        return 7

    async def cancel(self):
        self.is_cancelled = True
        self.finish.set()


async def _seed_channel(db: Database, channel_id: int = -1001) -> None:
    await db.add_channel(Channel(channel_id=channel_id, title="t", is_active=True))


async def _create_pending_task(db: Database, channel_id: int = -1001) -> int:
    """Insert a PENDING channel_collect task the same way CollectionService does in web mode."""
    return await db.repos.tasks.create_collection_task_if_not_active(
        channel_id, "t", channel_username=None, payload=None
    )


@pytest.mark.anyio
async def test_db_pull_picks_up_pending_task_added_after_startup(tmp_path):
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        await _seed_channel(db)

        collector = _FakeCollector()
        queue = CollectionQueue(collector, db)
        # Startup ingest sees nothing yet.
        assert await queue.requeue_startup_tasks() == 0

        # Web side writes a PENDING row directly (no in-memory queue).
        task_id = await _create_pending_task(db)
        assert task_id is not None

        # Without the pull loop the worker would never see it.
        queue.start_db_pull(interval=0.05)
        try:
            deadline = asyncio.get_event_loop().time() + 2.0
            while asyncio.get_event_loop().time() < deadline:
                if collector.calls:
                    break
                await asyncio.sleep(0.05)
            assert collector.calls == [-1001]
        finally:
            await queue.stop_db_pull()
            await queue.shutdown()
    finally:
        await db.close()


@pytest.mark.anyio
async def test_username_resolve_flood_defer_keeps_task_pending(tmp_path):
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        await _seed_channel(db)
        next_available_at = datetime.now(timezone.utc) + timedelta(minutes=2)

        collector = _UsernameResolveFloodCollector(next_available_at)
        queue = CollectionQueue(collector, db)
        channel = (await db.get_channels(active_only=True))[0]
        task_id = await queue.enqueue(channel)

        deadline = asyncio.get_event_loop().time() + 2.0
        while asyncio.get_event_loop().time() < deadline:
            task = await db.get_collection_task(task_id)
            if task.status == "pending" and task.run_after is not None:
                break
            await asyncio.sleep(0.05)

        task = await db.get_collection_task(task_id)
        assert task.status == "pending"
        assert task.error is None
        assert task.run_after is not None
        assert task.run_after > next_available_at
        assert "Flood Wait на resolve_username" in (task.note or "")
    finally:
        await queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_db_pull_does_not_double_ingest(tmp_path):
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        await _seed_channel(db)
        collector = _FakeCollector()
        queue = CollectionQueue(collector, db)

        # Pre-enqueue via the regular API — the task is in `_known_task_ids`
        # and in the in-memory queue.
        ch = (await db.get_channels(active_only=True))[0]
        task_id = await queue.enqueue(ch)
        assert task_id is not None

        # Tight pull interval so we'd see double-ingest immediately.
        queue.start_db_pull(interval=0.02)
        try:
            await asyncio.sleep(0.3)
        finally:
            await queue.stop_db_pull()
            await queue.shutdown()

        # Exactly one collection call, not multiple.
        assert collector.calls.count(-1001) == 1
    finally:
        await db.close()


@pytest.mark.anyio
async def test_db_pull_swallows_errors(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        await _seed_channel(db)
        collector = _FakeCollector()
        queue = CollectionQueue(collector, db)

        calls = {"n": 0}
        original = queue._ingest_pending_tasks

        async def flaky():
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("transient")
            return await original()

        monkeypatch.setattr(queue, "_ingest_pending_tasks", flaky)

        queue.start_db_pull(interval=0.02)
        try:
            await _create_pending_task(db)
            deadline = asyncio.get_event_loop().time() + 2.0
            while asyncio.get_event_loop().time() < deadline:
                if collector.calls:
                    break
                await asyncio.sleep(0.05)
            # First tick raised, but the loop survived; subsequent ticks
            # ingested the pending task.
            assert calls["n"] >= 2
            assert collector.calls == [-1001]
        finally:
            await queue.stop_db_pull()
            await queue.shutdown()
    finally:
        await db.close()


@pytest.mark.anyio
async def test_stop_db_pull_is_idempotent(tmp_path):
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        collector = _FakeCollector()
        queue = CollectionQueue(collector, db)

        # Stop without start — no-op.
        await queue.stop_db_pull()

        queue.start_db_pull(interval=0.05)
        await queue.stop_db_pull()
        # Second stop also no-op.
        await queue.stop_db_pull()

        # Re-start works after stop.
        queue.start_db_pull(interval=0.05)
        await queue.stop_db_pull()
    finally:
        await db.close()


@pytest.mark.anyio
async def test_delayed_requeue_queue_full_releases_known_task_id(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        await _seed_channel(db)
        collector = _FakeCollector()
        queue = CollectionQueue(collector, db)
        task_id = await _create_pending_task(db)
        channel = (await db.get_channels(active_only=True))[0]

        queue._queue = asyncio.Queue(maxsize=1)
        queue._queue.put_nowait((999999, channel, False, True))
        monkeypatch.setattr(queue, "_ensure_worker", lambda: None)
        queue._schedule_requeue_after_delay(
            task_id=task_id,
            channel=channel,
            force=False,
            full=True,
            run_after=datetime.fromtimestamp(0, tz=timezone.utc),
        )
        await asyncio.sleep(0)

        assert task_id not in queue._known_task_ids
        queue._queue.get_nowait()
        queue._queue.task_done()
        assert await queue._ingest_pending_tasks() == 1
    finally:
        await queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_clear_pending_tasks_clears_known_task_ids(tmp_path):
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        await _seed_channel(db)
        collector = _FakeCollector()
        queue = CollectionQueue(collector, db)
        channel = (await db.get_channels(active_only=True))[0]
        task_id = await queue.enqueue(channel)
        assert task_id in queue._known_task_ids

        deleted = await queue.clear_pending_tasks()

        assert deleted == 1
        assert queue._known_task_ids == set()
    finally:
        await queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_shutdown_waits_for_active_collection_and_leaves_queued_pending(tmp_path, caplog):
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        await _seed_channel(db, -1001)
        await _seed_channel(db, -1002)
        channels = await db.get_channels(active_only=True)
        by_id = {channel.channel_id: channel for channel in channels}

        collector = _BlockingCollector()
        queue = CollectionQueue(collector, db)
        first_id = await queue.enqueue(by_id[-1001])
        second_id = await queue.enqueue(by_id[-1002])
        await asyncio.wait_for(collector.started.wait(), timeout=1.0)

        caplog.set_level(logging.WARNING, logger="src.collection_queue")
        shutdown_task = asyncio.create_task(queue.shutdown(grace_timeout=2.0))
        await asyncio.sleep(0.05)
        assert not shutdown_task.done()

        collector.finish.set()
        await asyncio.wait_for(shutdown_task, timeout=2.0)

        first = await db.get_collection_task(first_id)
        second = await db.get_collection_task(second_id)
        assert first.status == "completed"
        assert first.messages_collected == 7
        assert second.status == "pending"
        assert collector.calls == [-1001]
        assert "ждём завершения активной задачи сбора" in caplog.text
        assert "Новые задачи останутся pending в БД" in caplog.text
    finally:
        await queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_shutdown_timeout_requeues_active_collection_task(tmp_path):
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        await _seed_channel(db, -1001)
        channel = (await db.get_channels(active_only=True))[0]

        collector = _BlockingCollector()
        queue = CollectionQueue(collector, db)
        task_id = await queue.enqueue(channel)
        await asyncio.wait_for(collector.started.wait(), timeout=1.0)

        await queue.shutdown(grace_timeout=0.01)

        task = await db.get_collection_task(task_id)
        assert task.status == "pending"
        assert task.started_at is None
        assert task.error is None
        assert "Остановка сервиса" in (task.note or "")
        assert collector.is_cancelled is True
    finally:
        await queue.shutdown()
        await db.close()
