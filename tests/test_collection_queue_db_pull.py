"""Tests for the periodic DB-pull loop in CollectionQueue.

Regression for: clicking "Собрать все каналы" on /scheduler/ wrote PENDING rows
into `collection_tasks` from the web side, but the worker's CollectionQueue
only ran `requeue_startup_tasks` once at boot — so freshly enqueued tasks were
ignored until the next worker restart.
"""
from __future__ import annotations

import asyncio

import pytest

from src.collection_queue import CollectionQueue
from src.database import Database
from src.models import Channel


class _FakeCollector:
    def __init__(self):
        self.calls: list[int] = []
        self.is_cancelled = False

    async def collect_single_channel(self, channel, *, full=False, progress_callback=None, force=False):
        self.calls.append(channel.channel_id)
        return 0

    async def cancel(self):
        return None


async def _seed_channel(db: Database, channel_id: int = -1001) -> None:
    await db.add_channel(Channel(channel_id=channel_id, title="t", is_active=True))


async def _create_pending_task(db: Database, channel_id: int = -1001) -> int:
    """Insert a PENDING channel_collect task the same way CollectionService does in web mode."""
    return await db.repos.tasks.create_collection_task_if_not_active(
        channel_id, "t", channel_username=None, payload=None
    )


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
