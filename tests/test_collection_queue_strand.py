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


def _inject_read_error(db: Database, table: str, message: str = "database is locked"):
    """Make every read-pool connection raise a RAW sqlite3.OperationalError for a
    ``SELECT ... FROM <table> WHERE id = ?`` query — reproducing exactly what the
    production read path does under lock: ReadPoolProxy.execute calls conn.execute()
    directly, so a busy lock surfaces UN-normalised (not DatabaseBusyError). Patching
    the repo method with a fake DatabaseBusyError (the old, wrong approach) never
    exercised this path. ``message`` lets a test inject a NON-busy OperationalError
    to prove the guard does not swallow real errors. Returns an undo() callable.
    """
    pool = db._read_pool
    assert pool is not None, "file-backed Database must have a read pool"
    conns = list(pool._all_conns)
    originals = [c.execute for c in conns]
    needle = f"from {table} where id = ?"

    def _make(orig):
        async def _boom(sql, params=()):
            if needle in " ".join(sql.lower().split()):
                raise sqlite3.OperationalError(message)
            return await orig(sql, params)

        return _boom

    for conn, orig in zip(conns, originals, strict=True):
        conn.execute = _make(orig)

    def _undo():
        for conn, orig in zip(conns, originals, strict=True):
            conn.execute = orig

    return _undo


@pytest.mark.anyio
async def test_busy_post_read_of_task_does_not_fail_completed_collection(tmp_path):
    """Regression #1249: after a successful collect, a transient RAW
    sqlite3.OperationalError('database is locked') on the post-completion read of
    the task (the real read-pool path — NOT a normalised DatabaseBusyError) must
    NOT drag the task into FAILED. Messages are already saved; it stays COMPLETED."""
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

        # The post-completion guard read of the task row raises a raw busy error
        # through the real ReadPoolProxy.
        undo = _inject_read_error(db, "collection_tasks")

        # collect_single_channel already returned count=5 (messages persisted).
        await queue._handle_collection_completion(
            task_id, channel, 5, cancel_event=asyncio.Event(), force=False
        )

        # Restore reads before the verification query (the write of COMPLETED went
        # through the write connection and is unaffected by the read-pool patch).
        undo()
        task = await db.get_collection_task(task_id)
        assert task.status == "completed"
        assert task.messages_collected == 5
        assert task.error is None
    finally:
        await queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_busy_post_read_of_channel_does_not_fail_completed_collection(tmp_path):
    """Regression #1249: the count==0 skip-note lookup (get_by_pk) may also hit a
    transient RAW sqlite3.OperationalError on the read pool — that must only cost
    the cosmetic note, never fail the completed task."""
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

        # count==0 path takes the get_by_pk skip-note lookup (SELECT ... FROM
        # channels WHERE id = ?), which raises a raw busy error via the read pool.
        undo = _inject_read_error(db, "channels")

        await queue._handle_collection_completion(
            task_id, channel, 0, cancel_event=asyncio.Event(), force=False
        )

        undo()
        task = await db.get_collection_task(task_id)
        assert task.status == "completed"
        assert task.messages_collected == 0
        assert task.error is None
    finally:
        await queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_non_busy_operational_error_is_not_swallowed(tmp_path):
    """The #1249 guard is narrow on purpose: a NON-busy sqlite3.OperationalError
    (a real bug — e.g. a bad column) must propagate, not be silently turned into a
    COMPLETED task. Guards against a future widening to a bare ``except Exception``."""
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

        undo = _inject_read_error(db, "collection_tasks", message="no such column: bogus")
        try:
            with pytest.raises(sqlite3.OperationalError, match="no such column"):
                await queue._handle_collection_completion(
                    task_id, channel, 5, cancel_event=asyncio.Event(), force=False
                )
        finally:
            undo()
    finally:
        await queue.shutdown()
        await db.close()
