from __future__ import annotations

import asyncio
import logging
import time
from datetime import timedelta, timezone

from src.database import Database
from src.database.bundles import ChannelBundle
from src.models import Channel, CollectionTaskStatus
from src.telegram.collector import (
    AllCollectionClientsFloodedError,
    Collector,
    NoActiveCollectionClientsError,
)

logger = logging.getLogger(__name__)


class CollectionQueue:
    # How often the worker re-checks the DB for new PENDING channel-collect tasks
    # written by the web container in split / embedded-worker setups (#491 follow-up).
    DB_PULL_INTERVAL_SEC = 3.0

    def __init__(self, collector: Collector, channels: ChannelBundle | Database):
        self._collector = collector
        if isinstance(channels, Database):
            channels = ChannelBundle.from_database(channels)
        self._channels = channels
        self._queue: asyncio.Queue[tuple[int, Channel, bool, bool]] = asyncio.Queue(maxsize=500)
        self._worker: asyncio.Task | None = None
        self._current_task_id: int | None = None
        self._retried_tasks: set[int] = set()
        self._delayed_requeues: set[asyncio.Task] = set()
        # Task IDs already pushed into the in-memory queue or scheduled via
        # delayed-requeue. Prevents double-ingestion when the periodic DB pull
        # sees a task that's still waiting in `self._queue`.
        self._known_task_ids: set[int] = set()
        self._pull_task: asyncio.Task | None = None
        self._pull_stop = asyncio.Event()

    async def enqueue(self, channel: Channel, force: bool = False, full: bool = True) -> int | None:
        """Enqueue a channel for collection, atomically skipping duplicates.

        Returns the new task ID, or ``None`` if an active task already exists.
        """
        payload = {}
        if force:
            payload["force"] = True
        if not full:
            payload["full"] = False
        task_id = await self._channels.create_collection_task_if_not_active(
            channel.channel_id,
            channel.title,
            channel_username=channel.username,
            payload=payload or None,
        )
        if task_id is None:
            return None
        try:
            self._queue.put_nowait((task_id, channel, force, full))
            self._known_task_ids.add(task_id)
        except asyncio.QueueFull:
            logger.warning(
                "Collection queue full (maxsize=%d); task %d stays PENDING in DB "
                "and will be picked up on the next restart/requeue cycle",
                self._queue.maxsize,
                task_id,
            )
        self._ensure_worker()
        return task_id

    async def cancel_task(self, task_id: int, note: str | None = None) -> bool:
        if task_id == self._current_task_id:
            await self._collector.cancel()
        return await self._channels.cancel_collection_task(task_id, note=note)

    async def clear_pending_tasks(self) -> int:
        # Delete from DB first, then drain in-memory queue.
        # Items removed from DB but still in the asyncio.Queue are safe:
        # _run_worker re-fetches the task from DB and skips if it's gone (task is None check).
        deleted = await self._channels.delete_pending_channel_tasks()
        removed_from_memory = 0
        while True:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            else:
                self._queue.task_done()
                removed_from_memory += 1
        # Cancel any delayed requeues (flood-wait timers) so they don't
        # re-inject deleted task IDs back into the queue.
        for task in list(self._delayed_requeues):
            task.cancel()
        self._delayed_requeues.clear()
        self._known_task_ids.clear()
        logger.info(
            "Cleared %d pending collection tasks from DB and %d queued items from memory",
            deleted,
            removed_from_memory,
        )
        return deleted

    def _ensure_worker(self) -> None:
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(self._run_worker())

    def _schedule_requeue_after_delay(
        self,
        *,
        task_id: int,
        channel: Channel,
        force: bool,
        full: bool,
        run_after,
    ) -> None:
        async def _requeue_later() -> None:
            remaining = max(0.0, run_after.timestamp() - time.time())
            if remaining > 0:
                await asyncio.sleep(remaining)
            try:
                self._queue.put_nowait((task_id, channel, force, full))
                self._known_task_ids.add(task_id)
            except asyncio.QueueFull:
                logger.warning(
                    "Collection queue full on delayed requeue; task %d stays PENDING "
                    "in DB and will be picked up by the DB pull loop",
                    task_id,
                )
                self._known_task_ids.discard(task_id)
            self._ensure_worker()

        # Reserve the slot up front so the periodic DB pull does not double-ingest
        # a task whose delayed requeue is still sleeping.
        self._known_task_ids.add(task_id)

        task = asyncio.create_task(_requeue_later())
        self._delayed_requeues.add(task)
        task.add_done_callback(self._delayed_requeues.discard)

    async def _run_worker(self) -> None:
        while True:
            try:
                task_id, channel, force, full = await asyncio.wait_for(
                    self._queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                if self._queue.empty():
                    break
                continue
            except asyncio.CancelledError:
                break

            # Check if task was cancelled while waiting in queue
            task = await self._channels.get_collection_task(task_id)
            if task is None:
                logger.info("Task %d skipped: task was deleted before collection", task_id)
                self._queue.task_done()
                continue
            if task and task.status == CollectionTaskStatus.CANCELLED:
                self._queue.task_done()
                continue
            if task.run_after is not None:
                remaining = task.run_after.timestamp() - time.time()
                if remaining > 0:
                    self._schedule_requeue_after_delay(
                        task_id=task_id,
                        channel=channel,
                        force=force,
                        full=full,
                        run_after=task.run_after,
                    )
                    self._queue.task_done()
                    continue

            # Channel may become filtered after being queued.
            fresh_channel = None
            if channel.id is not None:
                fresh_channel = await self._channels.get_by_pk(channel.id)
                if fresh_channel is None:
                    await self._channels.cancel_collection_task(
                        task_id,
                        note="Канал удалён до начала сбора.",
                    )
                    logger.info(
                        "Task %d skipped: channel %d was deleted before collection",
                        task_id,
                        channel.channel_id,
                    )
                    self._queue.task_done()
                    continue
            if fresh_channel is not None:
                channel = fresh_channel
            if channel.is_filtered and not force:
                await self._channels.cancel_collection_task(
                    task_id,
                    note="Канал отфильтрован до начала сбора.",
                )
                logger.info(
                    "Task %d skipped: channel %d is filtered",
                    task_id,
                    channel.channel_id,
                )
                self._queue.task_done()
                continue

            try:
                self._current_task_id = task_id
                await self._channels.update_collection_task(task_id, CollectionTaskStatus.RUNNING)

                async def _progress(count: int) -> None:
                    await self._channels.update_collection_task_progress(task_id, count)

                count = await self._collector.collect_single_channel(
                    channel, full=full, progress_callback=_progress, force=force
                )
                if self._collector.is_cancelled:
                    await self._channels.cancel_collection_task(
                        task_id,
                        note="Задача отменена во время сбора.",
                    )
                    logger.info("Task %d cancelled during collection", task_id)
                else:
                    note = None
                    if count == 0 and not force and channel.id is not None:
                        after_ch = await self._channels.get_by_pk(channel.id)
                        if after_ch and after_ch.is_filtered and not channel.is_filtered:
                            before_flags = set((channel.filter_flags or "").split(",")) - {""}
                            after_flags = set((after_ch.filter_flags or "").split(",")) - {""}
                            new_flags = after_flags - before_flags
                            reason = next(iter(new_flags), "low_subscriber_ratio")
                            note = f"Пропущен: {reason}"
                    await self._channels.update_collection_task(
                        task_id,
                        CollectionTaskStatus.COMPLETED,
                        messages_collected=count,
                        note=note,
                    )
                    logger.info("Collected %d messages from channel %d", count, channel.channel_id)
            except AllCollectionClientsFloodedError as exc:
                run_after = exc.next_available_at + timedelta(seconds=5)
                note = (
                    "Отложено: все аккаунты во Flood Wait "
                    f"до {exc.next_available_at.astimezone(timezone.utc).isoformat()}"
                )
                self._retried_tasks.discard(task_id)
                await self._channels.reschedule_collection_task(
                    task_id,
                    run_after=run_after,
                    note=note,
                )
                self._schedule_requeue_after_delay(
                    task_id=task_id,
                    channel=channel,
                    force=force,
                    full=full,
                    run_after=run_after,
                )
                logger.warning(
                    "Rescheduled collection task %d for channel %d until %s: all clients flooded",
                    task_id,
                    channel.channel_id,
                    run_after.isoformat(),
                )
            except NoActiveCollectionClientsError as exc:
                self._retried_tasks.discard(task_id)
                await self._channels.update_collection_task(
                    task_id,
                    CollectionTaskStatus.FAILED,
                    error=str(exc)[:500],
                    note="Нет подключённых активных аккаунтов для сбора.",
                )
                logger.error(
                    "Collection failed for channel %d: no active connected clients",
                    channel.channel_id,
                )
            except ConnectionError as exc:
                requeued = await self._try_reconnect_and_requeue(task_id, channel, full, force, exc)
                if not requeued:
                    self._retried_tasks.discard(task_id)
                    await self._channels.update_collection_task(
                        task_id,
                        CollectionTaskStatus.FAILED,
                        error=str(exc)[:500],
                    )
                    logger.exception("Collection failed for channel %d (reconnect failed)", channel.channel_id)
            except Exception as exc:
                self._retried_tasks.discard(task_id)
                await self._channels.update_collection_task(
                    task_id,
                    CollectionTaskStatus.FAILED,
                    error=str(exc)[:500],
                )
                logger.exception("Collection failed for channel %d", channel.channel_id)
            else:
                self._retried_tasks.discard(task_id)
            finally:
                self._current_task_id = None
                self._known_task_ids.discard(task_id)
                self._queue.task_done()

    async def _try_reconnect_and_requeue(
        self, task_id: int, channel: Channel, full: bool, force: bool, exc: Exception
    ) -> bool:
        """Try to reconnect the Telegram client and re-enqueue the failed task. Returns True if requeued."""
        if task_id in self._retried_tasks:
            return False
        pool = getattr(self._collector, "_pool", None)
        if pool is None or not hasattr(pool, "reconnect_phone"):
            return False
        reconnected = False
        for phone in list(pool.clients):
            result = await pool.reconnect_phone(phone)
            reconnected = reconnected or result
        if not reconnected:
            return False
        self._retried_tasks.add(task_id)
        await self._channels.update_collection_task(task_id, CollectionTaskStatus.PENDING, note="Reconnect retry")
        try:
            self._queue.put_nowait((task_id, channel, force, full))
            self._known_task_ids.add(task_id)
        except asyncio.QueueFull:
            logger.warning(
                "Collection queue full on reconnect requeue; task %d stays PENDING "
                "and will be picked up by requeue_startup_tasks",
                task_id,
            )
            return False
        logger.warning(
            "ConnectionError for channel %d, reconnected and re-queued task %d: %s",
            channel.channel_id, task_id, exc,
        )
        return True

    async def _ingest_pending_tasks(self) -> int:
        """Pull all currently-PENDING channel-collect tasks from the DB into the
        in-memory queue. Used both at startup (after resetting orphaned RUNNING
        rows) and from the periodic DB-pull loop. De-duplicates against
        `_known_task_ids` so tasks already sitting in the queue or scheduled
        for delayed requeue are not pushed twice.
        """
        pending = await self._channels.get_pending_channel_tasks()
        count = 0
        for task in pending:
            if task.id is None or task.id in self._known_task_ids:
                continue
            if task.channel_id is None:
                logger.warning("Skipping task %d: channel_id is None", task.id)
                continue
            channel = await self._channels.get_by_channel_id(task.channel_id)
            if channel is None:
                await self._channels.cancel_collection_task(task.id)
                logger.warning(
                    "Cancelled orphaned task %d: channel %d not found",
                    task.id,
                    task.channel_id,
                )
                continue
            force = bool((task.payload or {}).get("force", False))
            full = bool((task.payload or {}).get("full", True))
            if task.run_after is not None and task.run_after.timestamp() > time.time():
                self._schedule_requeue_after_delay(
                    task_id=task.id,
                    channel=channel,
                    force=force,
                    full=full,
                    run_after=task.run_after,
                )
            else:
                try:
                    self._queue.put_nowait((task.id, channel, force, full))
                    self._known_task_ids.add(task.id)
                except asyncio.QueueFull:
                    logger.warning(
                        "Collection queue full during pending-task ingest; task %d stays PENDING "
                        "in DB and will be picked up after the queue drains",
                        task.id,
                    )
                    break
            count += 1
        if count:
            self._ensure_worker()
        return count

    async def requeue_startup_tasks(self) -> int:
        """Re-enqueue pending collection tasks that survived a server restart.

        Also resets orphaned RUNNING tasks (left from ungraceful shutdown) to PENDING.
        """
        reset_count = await self._channels.reset_orphaned_running_tasks()
        if reset_count:
            logger.info("Reset %d orphaned RUNNING tasks to PENDING", reset_count)

        count = await self._ingest_pending_tasks()
        if count:
            logger.info("Re-enqueued %d pending collection tasks on startup", count)
        return count

    def start_db_pull(self, *, interval: float | None = None) -> None:
        """Start the background loop that periodically ingests new PENDING
        channel-collect tasks from the DB.

        Without this, tasks created via `CollectionService._enqueue_channel`
        (the web-mode fallback that writes a PENDING row when no in-memory
        queue is available) sit in the DB forever — only `requeue_startup_tasks`
        picks them up, and that runs only at worker startup.
        """
        if self._pull_task is not None and not self._pull_task.done():
            return
        self._pull_stop.clear()
        self._pull_task = asyncio.create_task(
            self._db_pull_loop(interval or self.DB_PULL_INTERVAL_SEC),
            name="collection-queue-db-pull",
        )

    async def stop_db_pull(self, timeout: float = 5.0) -> None:
        if self._pull_task is None:
            return
        self._pull_stop.set()
        try:
            await asyncio.wait_for(self._pull_task, timeout=timeout)
        except asyncio.TimeoutError:
            self._pull_task.cancel()
            try:
                await self._pull_task
            except (asyncio.CancelledError, Exception):
                pass
        self._pull_task = None

    async def _db_pull_loop(self, interval: float) -> None:
        while not self._pull_stop.is_set():
            try:
                await self._ingest_pending_tasks()
            except Exception:
                logger.exception("[collection-queue] DB pull failed")
            try:
                await asyncio.wait_for(self._pull_stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                continue

    async def shutdown(self) -> None:
        await self.stop_db_pull()
        if self._worker and not self._worker.done():
            self._worker.cancel()
            try:
                await self._worker
            except asyncio.CancelledError:
                pass
        # Snapshot tasks before cancelling — the done_callback (discard)
        # removes them from the set as soon as each resolves, so iterating
        # the live set in a second loop would see an empty collection.
        pending = list(self._delayed_requeues)
        for task in pending:
            task.cancel()
        for task in pending:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._delayed_requeues.clear()
        self._retried_tasks.clear()
