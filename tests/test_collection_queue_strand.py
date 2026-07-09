"""Regression #971: a transient read error in pre-dispatch validation must not
strand a PENDING task forever.

`_validate_task_pre_dispatch` runs *before* the worker's try/finally block. If it
raised (e.g. "database is locked"), the task stayed in `_known_task_ids` while its
DB row stayed PENDING — and `_ingest_pending_tasks` skips ids already in
`_known_task_ids`, so the task could never be re-picked. The fix wraps the call so
the id is discarded and the queue slot released, letting a later ingest re-pick it.
"""
from __future__ import annotations

import asyncio
import sqlite3

import pytest

from src.collection_queue import CollectionQueue
from src.database import Database, DatabaseBusyError
from src.models import Channel


class _FakeCollector:
    def __init__(self):
        self.calls: list[int] = []

    async def collect_single_channel(self, channel, **kwargs):
        self.calls.append(channel.channel_id)
        return 0

    async def cancel(self):
        return None


async def _seed_channel(db: Database, channel_id: int = -1001) -> None:
    await db.add_channel(Channel(channel_id=channel_id, title="t", is_active=True))


@pytest.mark.anyio
async def test_validation_read_error_does_not_strand_pending_task(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        await _seed_channel(db)
        task_id = await db.repos.tasks.create_collection_task_if_not_active(
            -1001, "t", channel_username=None, payload=None
        )
        assert task_id is not None

        collector = _FakeCollector()
        queue = CollectionQueue(collector, db)

        # Put the task on the in-memory queue exactly as enqueue() would, without
        # auto-starting workers.
        channel = Channel(channel_id=-1001, title="t", is_active=True)
        queue._queue.put_nowait((task_id, channel, False, False))
        queue._known_task_ids.add(task_id)

        async def _boom(*args, **kwargs):
            raise sqlite3.OperationalError("database is locked")

        monkeypatch.setattr(queue, "_validate_task_pre_dispatch", _boom)

        # One worker pass: it should swallow the error, release the slot, and exit
        # cleanly when the queue drains (instead of crashing the worker).
        await queue._run_single_worker()

        assert task_id not in queue._known_task_ids
        assert queue._queue.empty()
        assert collector.calls == []

        # The PENDING row survived and is now re-pickable by the DB-pull loop.
        task = await db.get_collection_task(task_id)
        assert task.status == "pending"
        assert await queue._ingest_pending_tasks() == 1
        assert task_id in queue._known_task_ids
    finally:
        await queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_busy_post_read_of_task_does_not_fail_completed_collection(tmp_path, monkeypatch):
    """Regression #1249: after a successful collect, a transient DatabaseBusyError
    on the post-completion read of the task must NOT drag the task into FAILED —
    the messages are already saved. The task must still be marked COMPLETED."""
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        await _seed_channel(db)
        channel = (await db.get_channels(active_only=True))[0]
        task_id = await db.repos.tasks.create_collection_task_if_not_active(
            channel.channel_id, "t", channel_username=None, payload=None
        )
        assert task_id is not None
        await db.repos.tasks.update_collection_task(task_id, "running")

        collector = _FakeCollector()
        queue = CollectionQueue(collector, db)

        async def _busy(*args, **kwargs):
            raise DatabaseBusyError("database is locked")

        # The post-completion guard read of the task row raises busy.
        # ChannelBundle is a frozen dataclass, so patch the underlying repo method
        # that ChannelBundle.get_collection_task delegates to.
        monkeypatch.setattr(queue._channels.tasks, "get_collection_task", _busy)

        # collect_single_channel already returned count=5 (messages persisted).
        await queue._handle_collection_completion(
            task_id, channel, 5, cancel_event=asyncio.Event(), force=False
        )

        # queue._channels.tasks is the same repo instance as db.repos.tasks, so
        # undo the busy patch before the verification read.
        monkeypatch.undo()
        task = await db.get_collection_task(task_id)
        assert task.status == "completed"
        assert task.messages_collected == 5
        assert task.error is None
    finally:
        await queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_busy_post_read_of_channel_does_not_fail_completed_collection(tmp_path, monkeypatch):
    """Regression #1249: the count==0 skip-note lookup (get_by_pk) may also hit a
    transient DatabaseBusyError — that must only cost the cosmetic note, never
    fail the completed task."""
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        await _seed_channel(db)
        channel = (await db.get_channels(active_only=True))[0]
        assert channel.id is not None
        task_id = await db.repos.tasks.create_collection_task_if_not_active(
            channel.channel_id, "t", channel_username=None, payload=None
        )
        assert task_id is not None
        await db.repos.tasks.update_collection_task(task_id, "running")

        collector = _FakeCollector()
        queue = CollectionQueue(collector, db)

        async def _busy(*args, **kwargs):
            raise DatabaseBusyError("database is locked")

        # count==0 path takes the get_by_pk skip-note lookup, which raises busy.
        # ChannelBundle.get_by_pk delegates to channels.get_channel_by_pk; patch
        # that (the bundle itself is a frozen dataclass).
        monkeypatch.setattr(queue._channels.channels, "get_channel_by_pk", _busy)

        await queue._handle_collection_completion(
            task_id, channel, 0, cancel_event=asyncio.Event(), force=False
        )

        task = await db.get_collection_task(task_id)
        assert task.status == "completed"
        assert task.messages_collected == 0
        assert task.error is None
    finally:
        await queue.shutdown()
        await db.close()
