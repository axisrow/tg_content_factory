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
from src.telegram.collector import (
    UsernameResolveFloodWaitDeferredError,
    UsernameResolveRateLimitedError,
)
from tests.helpers import wait_until


class _FakeCollector:
    def __init__(self):
        self.calls: list[int] = []
        self.full_calls: list[bool] = []
        self.is_cancelled = False

    async def collect_single_channel(
        self, channel, *, full=False, progress_callback=None, force=False, cancel_event=None
    ):
        self.calls.append(channel.channel_id)
        self.full_calls.append(full)
        return 0

    async def cancel(self):
        return None


class _NoClientsCollector(_FakeCollector):
    async def get_collection_availability(self):
        return type(
            "Availability",
            (),
            {
                "state": "no_connected_active",
                "retry_after_sec": None,
                "next_available_at_utc": None,
            },
        )()


class _UsernameResolveFloodCollector(_FakeCollector):
    def __init__(self, next_available_at: datetime):
        super().__init__()
        self.next_available_at = next_available_at

    async def collect_single_channel(
        self, channel, *, full=False, progress_callback=None, force=False, cancel_event=None
    ):
        self.calls.append(channel.channel_id)
        raise UsernameResolveFloodWaitDeferredError(
            wait_seconds=120,
            next_available_at=self.next_available_at,
        )


class _UsernameResolveRateLimitedCollector(_FakeCollector):
    async def collect_single_channel(
        self, channel, *, full=False, progress_callback=None, force=False, cancel_event=None
    ):
        self.calls.append(channel.channel_id)
        self.full_calls.append(full)
        raise UsernameResolveRateLimitedError("+7001", 28.2)


class _BlockingCollector:
    def __init__(self):
        self.calls: list[int] = []
        self.started = asyncio.Event()
        self.finish = asyncio.Event()
        self.is_cancelled = False

    async def collect_single_channel(
        self, channel, *, full=False, progress_callback=None, force=False, cancel_event=None
    ):
        self.calls.append(channel.channel_id)
        self.started.set()
        await self.finish.wait()
        return 7

    async def cancel(self):
        self.is_cancelled = True
        self.finish.set()


class _ResolveBackoffPool:
    def __init__(self, remaining_sec: int):
        self.remaining_sec = remaining_sec

    def get_resolve_username_backoff_remaining_sec(self) -> int:
        return self.remaining_sec


class _ReconnectPool:
    """Pool stub whose reconnect_phone always succeeds, so the queue treats a
    ConnectionError as recoverable and takes the reconnect-requeue path."""

    def __init__(self):
        self.clients = {"+1": object()}

    async def reconnect_phone(self, phone: str) -> bool:
        return True

    def get_resolve_username_backoff_remaining_sec(self) -> int:
        return 0


class _ConnectionErrorCollector(_FakeCollector):
    """Collector that raises ConnectionError on collect, driving the
    reconnect-and-requeue recovery path (#1248)."""

    def __init__(self):
        super().__init__()
        self._pool = _ReconnectPool()

    async def collect_single_channel(
        self, channel, *, full=False, progress_callback=None, force=False, cancel_event=None
    ):
        self.calls.append(channel.channel_id)
        raise ConnectionError("connection reset")


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
async def test_db_pull_does_not_ingest_when_no_clients(tmp_path, caplog):
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        await _seed_channel(db)

        collector = _NoClientsCollector()
        queue = CollectionQueue(collector, db)
        task_id = await _create_pending_task(db)

        caplog.set_level(logging.WARNING, logger="src.collection_queue")
        assert await queue._ingest_pending_tasks() == 0

        task = await db.get_collection_task(task_id)
        assert task.status == "pending"
        assert collector.calls == []
        assert queue._known_task_ids == set()
        assert "Pending-task ingest throttled" in caplog.text
    finally:
        await queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_pending_task_without_payload_defaults_to_incremental(tmp_path):
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        await _seed_channel(db)
        collector = _FakeCollector()
        queue = CollectionQueue(collector, db)
        await _create_pending_task(db)

        assert await queue._ingest_pending_tasks() == 1
        await queue._run_worker()

        assert collector.calls == [-1001]
        assert collector.full_calls == [False]
    finally:
        await queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_resolve_backoff_delayed_requeue_preserves_payload_flags(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        await db.add_channel(
            Channel(channel_id=-1001, title="t", username="named_channel", is_active=True)
        )
        task_id = await db.repos.tasks.create_collection_task_if_not_active(
            -1001,
            "t",
            channel_username="named_channel",
            payload={"force": True, "full": True},
        )
        collector = _FakeCollector()
        collector._pool = _ResolveBackoffPool(remaining_sec=60)
        queue = CollectionQueue(collector, db)
        queue._ensure_worker = lambda: None

        scheduled: list[dict] = []

        def capture_requeue(**kwargs):
            scheduled.append(kwargs)

        monkeypatch.setattr(queue, "_schedule_requeue_after_delay", capture_requeue)

        assert await queue._ingest_pending_tasks() == 1

        assert queue._queue.empty()
        assert scheduled
        assert scheduled[0]["task_id"] == task_id
        assert scheduled[0]["force"] is True
        assert scheduled[0]["full"] is True
        assert scheduled[0]["run_after"] > datetime.now(timezone.utc)
    finally:
        await queue.shutdown()
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
async def test_username_resolve_rate_limit_keeps_task_pending(tmp_path):
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        await _seed_channel(db)

        collector = _UsernameResolveRateLimitedCollector()
        queue = CollectionQueue(collector, db)
        channel = (await db.get_channels(active_only=True))[0]
        before = datetime.now(timezone.utc)
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
        assert task.run_after >= before + timedelta(seconds=33)
        assert "resolve_username rate-limited" in (task.note or "")

        queue.start_db_pull(interval=0.02)
        try:
            await asyncio.sleep(0.12)
        finally:
            await queue.stop_db_pull()

        assert task_id in queue._known_task_ids
        assert len(queue._delayed_requeues) == 1
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
        # The requeue task runs immediately (run_after in the past), finds the
        # queue full, and drops the task id so it can be re-ingested later.
        await wait_until(lambda: task_id not in queue._known_task_ids)

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
        assert "ждём завершения" in caplog.text and "pending" in caplog.text.lower()
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


@pytest.mark.anyio
async def test_is_paused_property(tmp_path):
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        queue = CollectionQueue(_FakeCollector(), db)
        assert queue.is_paused is False
        queue.pause()
        assert queue.is_paused is True
        queue.resume()
        assert queue.is_paused is False
    finally:
        await queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_ingest_noop_while_paused(tmp_path):
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        await _seed_channel(db)
        collector = _FakeCollector()
        queue = CollectionQueue(collector, db)
        queue.pause()

        task_id = await _create_pending_task(db)
        # Paused: PENDING rows are not buffered into memory.
        assert await queue._ingest_pending_tasks() == 0

        task = await db.get_collection_task(task_id)
        assert task.status == "pending"
        assert collector.calls == []
        assert queue._known_task_ids == set()
    finally:
        await queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_worker_stays_alive_while_paused(tmp_path):
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        await _seed_channel(db)
        channel = (await db.get_channels(active_only=True))[0]
        collector = _FakeCollector()
        queue = CollectionQueue(collector, db)
        queue.pause()

        task_id = await queue.enqueue(channel)
        await asyncio.sleep(0.3)

        # Supervisor is alive but holds the task without processing it while paused.
        assert queue._supervisor is not None and not queue._supervisor.done()
        assert collector.calls == []
        task = await db.get_collection_task(task_id)
        assert task.status == "pending"
    finally:
        await queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_pause_lets_running_task_finish_and_holds_next(tmp_path):
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        await _seed_channel(db, -1001)
        await _seed_channel(db, -1002)
        by_id = {c.channel_id: c for c in await db.get_channels(active_only=True)}

        collector = _BlockingCollector()
        queue = CollectionQueue(collector, db)
        first_id = await queue.enqueue(by_id[-1001])
        second_id = await queue.enqueue(by_id[-1002])
        await asyncio.wait_for(collector.started.wait(), timeout=1.0)

        # Pause while the first task is mid-collection.
        queue.pause()
        collector.finish.set()  # let the running task finish

        # First completes; the second stays PENDING and is not picked up.
        deadline = asyncio.get_event_loop().time() + 2.0
        while asyncio.get_event_loop().time() < deadline:
            first = await db.get_collection_task(first_id)
            if first.status == "completed":
                break
            await asyncio.sleep(0.02)
        assert first.status == "completed"
        assert first.messages_collected == 7
        await asyncio.sleep(0.2)
        second = await db.get_collection_task(second_id)
        assert second.status == "pending"
        assert collector.calls == [-1001]

        # Resume: the held task now runs.
        queue.resume()
        deadline = asyncio.get_event_loop().time() + 2.0
        while asyncio.get_event_loop().time() < deadline:
            second = await db.get_collection_task(second_id)
            if second.status == "completed":
                break
            await asyncio.sleep(0.02)
        assert second.status == "completed"
        assert collector.calls == [-1001, -1002]
    finally:
        await queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_resume_reingests_pending_via_db_pull(tmp_path):
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        await _seed_channel(db)
        collector = _FakeCollector()
        queue = CollectionQueue(collector, db)
        queue.pause()

        task_id = await _create_pending_task(db)
        queue.start_db_pull(interval=0.05)
        try:
            # Paused: db pull does not ingest, task stays pending.
            await asyncio.sleep(0.3)
            assert collector.calls == []
            assert (await db.get_collection_task(task_id)).status == "pending"

            # Resume: the periodic db pull re-ingests and the worker runs it.
            queue.resume()
            deadline = asyncio.get_event_loop().time() + 2.0
            while asyncio.get_event_loop().time() < deadline:
                if collector.calls:
                    break
                await asyncio.sleep(0.05)
            assert collector.calls == [-1001]
        finally:
            await queue.stop_db_pull()
    finally:
        await queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_shutdown_while_paused_completes_cleanly(tmp_path):
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        await _seed_channel(db)
        channel = (await db.get_channels(active_only=True))[0]
        collector = _FakeCollector()
        queue = CollectionQueue(collector, db)
        queue.pause()
        task_id = await queue.enqueue(channel)
        await asyncio.sleep(0.1)

        # Shutdown must return even though the queue is paused.
        await asyncio.wait_for(queue.shutdown(grace_timeout=1.0), timeout=2.0)

        # The queued task was never processed; it stays pending in the DB.
        assert collector.calls == []
        assert (await db.get_collection_task(task_id)).status == "pending"
    finally:
        await db.close()


@pytest.mark.anyio
async def test_connection_error_queue_full_keeps_task_pending_for_pull_loop(tmp_path):
    """Regression #1248: on ConnectionError the queue reconnects and tries to
    re-enqueue; if the in-memory queue is FULL the task must stay PENDING (not be
    overwritten to FAILED) so the DB pull loop re-picks it. Before the fix the
    reconnect-requeue returned False on QueueFull and the caller wrote FAILED,
    losing the retry forever."""
    db = Database(str(tmp_path / "queue.db"))
    await db.initialize()
    try:
        await _seed_channel(db)
        channel = (await db.get_channels(active_only=True))[0]
        collector = _ConnectionErrorCollector()
        queue = CollectionQueue(collector, db)

        # The task is RUNNING (as it would be when collect raised).
        task_id = await _create_pending_task(db)
        await db.repos.tasks.update_collection_task(task_id, "running")
        queue._known_task_ids.add(task_id)

        # Saturate the in-memory queue so the reconnect requeue hits QueueFull.
        queue._queue = asyncio.Queue(maxsize=1)
        queue._queue.put_nowait((999999, channel, False, False))

        keep_known, stop_after = await queue._handle_collection_exception(
            ConnectionError("connection reset"),
            task_id=task_id,
            channel=channel,
            force=False,
            full=False,
        )

        # Caller must NOT overwrite to FAILED and must release the id so the pull
        # loop can re-pick it.
        assert keep_known is False
        assert stop_after is False
        # Mirror the worker's finally block for keep_known_task_id=False.
        queue._known_task_ids.discard(task_id)

        task = await db.get_collection_task(task_id)
        assert task.status == "pending"
        assert task.error is None
        assert task_id not in queue._known_task_ids

        # Drain the saturating item; the DB pull loop now re-picks the task.
        queue._queue.get_nowait()
        queue._queue.task_done()
        assert await queue._ingest_pending_tasks() == 1
        assert task_id in queue._known_task_ids
    finally:
        await queue.shutdown()
        await db.close()
