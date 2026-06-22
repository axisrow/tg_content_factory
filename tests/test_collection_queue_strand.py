"""Regression #971: a transient read error in pre-dispatch validation must not
strand a PENDING task forever.

`_validate_task_pre_dispatch` runs *before* the worker's try/finally block. If it
raised (e.g. "database is locked"), the task stayed in `_known_task_ids` while its
DB row stayed PENDING — and `_ingest_pending_tasks` skips ids already in
`_known_task_ids`, so the task could never be re-picked. The fix wraps the call so
the id is discarded and the queue slot released, letting a later ingest re-pick it.
"""
from __future__ import annotations

import sqlite3

import pytest

from src.collection_queue import CollectionQueue
from src.database import Database
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
