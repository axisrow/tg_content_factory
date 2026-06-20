from __future__ import annotations

import asyncio
import inspect
import logging
import time
from datetime import datetime, timedelta, timezone

from src.database import Database
from src.database.bundles import ChannelBundle
from src.live_runtime_pause import LiveRuntimePauseGate
from src.models import Channel, CollectionTaskStatus
from src.telegram.collector import (
    RESOLVE_USERNAME_BACKOFF_BUFFER_SEC,
    AllCollectionClientsFloodedError,
    Collector,
    NoActiveCollectionClientsError,
    UsernameResolveFloodWaitDeferredError,
    UsernameResolveRateLimitedError,
)

logger = logging.getLogger(__name__)


class CollectionQueue:
    DB_PULL_INTERVAL_SEC = 3.0
    NO_CLIENTS_RETRY_DELAY_SEC = 120
    GRACEFUL_SHUTDOWN_TIMEOUT_SEC = 120.0
    FORCE_CANCEL_TIMEOUT_SEC = 10.0
    SHUTDOWN_REQUEUE_NOTE = "Остановка сервиса во время сбора; задача будет продолжена после запуска."
    NO_CLIENTS_REQUEUE_NOTE = "Отложено: нет подключённых активных аккаунтов для сбора."

    def __init__(
        self,
        collector: Collector,
        channels: ChannelBundle | Database,
        *,
        live_runtime_pause_gate: LiveRuntimePauseGate | None = None,
    ):
        self._collector = collector
        if isinstance(channels, Database):
            channels = ChannelBundle.from_database(channels)
        self._channels = channels
        self._queue: asyncio.Queue[tuple[int, Channel, bool, bool]] = asyncio.Queue(maxsize=500)
        self._supervisor: asyncio.Task | None = None
        self._workers: list[asyncio.Task] = []
        self._active_task_ids: dict[int, asyncio.Event] = {}
        self._retried_tasks: set[int] = set()
        self._delayed_requeues: set[asyncio.Task] = set()
        self._known_task_ids: set[int] = set()
        self._pull_task: asyncio.Task | None = None
        self._pull_stop = asyncio.Event()
        self._shutdown_requested = False
        self._shutdown_event = asyncio.Event()
        self._live_runtime_pause_gate = live_runtime_pause_gate
        self._resume_gate = asyncio.Event()
        self._resume_gate.set()
        self._stop_workers = False

    async def enqueue(self, channel: Channel, force: bool = False, full: bool = False) -> int | None:
        payload = {"full": full}
        if force:
            payload["force"] = True
        task_id = await self._channels.create_collection_task_if_not_active(
            channel.channel_id,
            channel.title,
            channel_username=channel.username,
            payload=payload,
        )
        if task_id is None:
            return None
        if self._shutdown_requested:
            logger.info(
                "Service is shutting down; collection task %d stays PENDING in DB",
                task_id,
            )
            return task_id
        if self._is_live_runtime_paused():
            logger.info(
                "Live runtime paused for agent request; collection task %d stays PENDING in DB",
                task_id,
            )
            return task_id
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
        cancel_event = self._active_task_ids.get(task_id)
        if cancel_event is not None:
            cancel_event.set()
        return await self._channels.cancel_collection_task(task_id, note=note)

    async def clear_pending_tasks(self) -> int:
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

    @property
    def is_paused(self) -> bool:
        return not self._resume_gate.is_set()

    def pause(self) -> None:
        if self._resume_gate.is_set():
            self._resume_gate.clear()
            logger.info(
                "Collection queue paused (active tasks %s allowed to finish)",
                list(self._active_task_ids.keys()),
            )

    def resume(self) -> None:
        if not self._resume_gate.is_set():
            self._resume_gate.set()
            self._ensure_worker()
            logger.info("Collection queue resumed")

    def _target_worker_count(self) -> int:
        getter = getattr(self._collector, "collection_worker_count", None)
        if callable(getter):
            return max(1, int(getter()))
        return 1

    async def _available_target_worker_count(self) -> int:
        slot_getter = getattr(self._collector, "available_collection_slot_count", None)
        if callable(slot_getter):
            slots = slot_getter()
            if asyncio.iscoroutine(slots):
                slots = await slots
            active_count = len(self._active_task_ids)
            configured = self._target_worker_count()
            desired = active_count + max(0, int(slots))
            if desired <= 0:
                return 1
            return max(1, active_count, min(configured, desired))

        getter = getattr(self._collector, "available_collection_worker_count", None)
        if callable(getter):
            count = getter()
            if asyncio.iscoroutine(count):
                count = await count
            return max(1, int(count))
        return self._target_worker_count()

    def _ensure_supervisor(self) -> None:
        if self._supervisor is None or self._supervisor.done():
            self._supervisor = asyncio.create_task(self._run_supervisor())

    def _ensure_worker(self) -> None:
        self._ensure_supervisor()

    def _is_live_runtime_paused(self) -> bool:
        return (
            self._live_runtime_pause_gate is not None
            and self._live_runtime_pause_gate.is_paused
        )

    async def _wait_if_live_runtime_paused(self) -> bool:
        if self._live_runtime_pause_gate is None:
            return True
        return await self._live_runtime_pause_gate.wait_if_paused(stop_event=self._shutdown_event)

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
            if self._shutdown_requested:
                self._known_task_ids.discard(task_id)
                return
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

        self._known_task_ids.add(task_id)
        task = asyncio.create_task(_requeue_later())
        self._delayed_requeues.add(task)
        task.add_done_callback(self._delayed_requeues.discard)

    async def _run_supervisor(self) -> None:
        self._stop_workers = False
        while not self._shutdown_requested and not self._stop_workers:
            if not self._resume_gate.is_set():
                try:
                    await asyncio.wait_for(self._resume_gate.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                if self._shutdown_requested:
                    break

            self._workers = [w for w in self._workers if not w.done()]
            target = await self._available_target_worker_count()
            while len(self._workers) < target:
                w = asyncio.create_task(self._run_single_worker())
                self._workers.append(w)

            if not self._workers:
                break
            done, _ = await asyncio.wait(
                self._workers,
                timeout=1.0,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if done:
                for w in done:
                    if w in self._workers:
                        self._workers.remove(w)
                    try:
                        exc = w.exception()
                    except asyncio.CancelledError:
                        continue
                    if exc is not None:
                        logger.exception(
                            "Collection queue worker crashed",
                            exc_info=(type(exc), exc, exc.__traceback__),
                        )
            if not self._queue.empty() and len(self._workers) < target:
                continue
            if (
                self._queue.empty()
                and not self._active_task_ids
                and not any(not worker.done() for worker in self._workers)
            ):
                break

    async def _run_single_worker(self) -> None:
        while True:
            if self._shutdown_requested or self._stop_workers:
                break
            if not self._resume_gate.is_set():
                try:
                    await asyncio.wait_for(self._resume_gate.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                if self._shutdown_requested or self._stop_workers:
                    break
            if not await self._wait_if_live_runtime_paused():
                break
            target = await self._available_target_worker_count()
            if len(self._active_task_ids) >= target:
                break
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

            validated = await self._validate_task_pre_dispatch(task_id, channel, force, full)
            if validated is None:
                continue
            channel = validated

            cancel_event = asyncio.Event()
            self._active_task_ids[task_id] = cancel_event
            stop_after_no_clients = False
            keep_known_task_id = False
            try:
                await self._channels.update_collection_task(task_id, CollectionTaskStatus.RUNNING)
                collect_kwargs = self._build_collect_kwargs(
                    task_id, full=full, force=force, cancel_event=cancel_event
                )
                count = await self._collector.collect_single_channel(channel, **collect_kwargs)
                await self._handle_collection_completion(
                    task_id, channel, count, cancel_event=cancel_event, force=force
                )
                self._retried_tasks.discard(task_id)
            except Exception as exc:
                keep_known_task_id, stop_after_no_clients = await self._handle_collection_exception(
                    exc, task_id=task_id, channel=channel, force=force, full=full
                )
            finally:
                self._active_task_ids.pop(task_id, None)
                if not keep_known_task_id:
                    self._known_task_ids.discard(task_id)
                self._queue.task_done()
                if stop_after_no_clients:
                    self._stop_workers = True
                    break

    def _build_collect_kwargs(
        self, task_id: int, *, full: bool, force: bool, cancel_event: asyncio.Event
    ) -> dict:
        """Build the kwargs for ``collect_single_channel``, including a progress
        callback bound to ``task_id`` and a ``cancel_event`` only when the
        collector's signature accepts one. Split out of ``_run_single_worker`` (#922).
        """
        async def _progress(count: int) -> None:
            await self._channels.update_collection_task_progress(task_id, count)

        collect_kwargs = {
            "full": full,
            "progress_callback": _progress,
            "force": force,
        }
        try:
            signature = inspect.signature(self._collector.collect_single_channel)
            accepts_cancel_event = (
                "cancel_event" in signature.parameters
                or any(
                    parameter.kind == inspect.Parameter.VAR_KEYWORD
                    for parameter in signature.parameters.values()
                )
            )
        except (TypeError, ValueError):
            accepts_cancel_event = True
        if accepts_cancel_event:
            collect_kwargs["cancel_event"] = cancel_event
        return collect_kwargs

    async def _validate_task_pre_dispatch(
        self, task_id: int, channel: Channel, force: bool, full: bool
    ) -> Channel | None:
        """Run every pre-dispatch guard for a dequeued task.

        Returns the channel to collect (refreshed from DB when applicable), or
        ``None`` when the task must be skipped — in which case this method has
        already marked the queue item done and cancelled/requeued as needed.
        Split out of ``_run_single_worker`` (#922).
        """
        task = await self._channels.get_collection_task(task_id)
        if task is None:
            logger.info("Task %d skipped: task was deleted before collection", task_id)
            self._queue.task_done()
            return None
        if task and task.status == CollectionTaskStatus.CANCELLED:
            self._queue.task_done()
            return None
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
                return None

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
                return None
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
            return None

        if self._shutdown_requested:
            self._known_task_ids.discard(task_id)
            self._queue.task_done()
            return None
        return channel

    async def _handle_collection_completion(
        self, task_id: int, channel: Channel, count: int, *, cancel_event: asyncio.Event, force: bool
    ) -> None:
        """Persist the terminal status of a finished collection.

        Requeues (on shutdown) or cancels when the run was cancelled, else marks
        it COMPLETED with an optional "skipped" note when the channel became
        filtered mid-run. Split out of ``_run_single_worker`` (#922).
        """
        persisted = await self._channels.get_collection_task(task_id)
        persisted_cancelled = (
            persisted is not None
            and persisted.status == CollectionTaskStatus.CANCELLED
        )
        collector_cancelled = bool(getattr(self._collector, "is_cancelled", False))
        cancelled = cancel_event.is_set() or persisted_cancelled or collector_cancelled
        if cancelled:
            if self._shutdown_requested and not persisted_cancelled:
                try:
                    await self._reset_task_to_pending_after_shutdown(task_id)
                    logger.info("Task %d requeued after service shutdown interrupted collection", task_id)
                except (ValueError, RuntimeError):
                    logger.debug("Could not reset task %d during shutdown", task_id)
            else:
                await self._channels.cancel_collection_task(
                    task_id,
                    note="Задача отменена во время сбора.",
                )
                logger.info("Task %d cancelled during collection", task_id)
            return
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

    async def _handle_collection_exception(
        self, exc: Exception, *, task_id: int, channel: Channel, force: bool, full: bool
    ) -> tuple[bool, bool]:
        """Handle a failure raised while collecting a single channel.

        Returns ``(keep_known_task_id, stop_after_no_clients)`` for the worker
        loop's finally block. The ``isinstance`` chain mirrors the original
        except-clause precedence exactly. Split out of ``_run_single_worker`` (#922).
        """
        if isinstance(exc, AllCollectionClientsFloodedError):
            run_after = exc.next_available_at + timedelta(seconds=5)
            note = (
                "Отложено: все аккаунты во Flood Wait "
                f"до {exc.next_available_at.astimezone(timezone.utc).isoformat()}"
            )
            self._retried_tasks.discard(task_id)
            await self._channels.reschedule_collection_task(task_id, run_after=run_after, note=note)
            self._schedule_requeue_after_delay(
                task_id=task_id, channel=channel, force=force, full=full, run_after=run_after
            )
            logger.warning(
                "Rescheduled collection task %d for channel %d until %s: all clients flooded",
                task_id,
                channel.channel_id,
                run_after.isoformat(),
            )
            return True, False
        if isinstance(exc, UsernameResolveFloodWaitDeferredError):
            run_after = exc.next_available_at + timedelta(
                seconds=RESOLVE_USERNAME_BACKOFF_BUFFER_SEC
            )
            note = (
                "Отложено: Flood Wait на resolve_username до "
                f"{run_after.astimezone(timezone.utc).isoformat()}"
            )
            self._retried_tasks.discard(task_id)
            await self._channels.reschedule_collection_task(task_id, run_after=run_after, note=note)
            self._schedule_requeue_after_delay(
                task_id=task_id, channel=channel, force=force, full=full, run_after=run_after
            )
            logger.warning(
                "Rescheduled collection task %d for channel %d until %s: username resolve flood wait",
                task_id,
                channel.channel_id,
                run_after.isoformat(),
            )
            return True, False
        if isinstance(exc, UsernameResolveRateLimitedError):
            run_after = exc.run_after_with_buffer()
            note = (
                "Отложено: resolve_username rate-limited до "
                f"{run_after.astimezone(timezone.utc).isoformat()}"
            )
            self._retried_tasks.discard(task_id)
            await self._channels.reschedule_collection_task(task_id, run_after=run_after, note=note)
            self._schedule_requeue_after_delay(
                task_id=task_id, channel=channel, force=force, full=full, run_after=run_after
            )
            logger.warning(
                "Rescheduled collection task %d for channel %d until %s: "
                "username resolve rate-limited on %s",
                task_id,
                channel.channel_id,
                run_after.isoformat(),
                exc.phone,
            )
            return True, False
        if isinstance(exc, NoActiveCollectionClientsError):
            run_after = datetime.now(timezone.utc) + timedelta(
                seconds=self.NO_CLIENTS_RETRY_DELAY_SEC
            )
            self._retried_tasks.discard(task_id)
            await self._channels.reschedule_collection_task(
                task_id,
                run_after=run_after,
                note=self.NO_CLIENTS_REQUEUE_NOTE,
            )
            drained_task_ids = self._drain_memory_queue()
            for drained_task_id in drained_task_ids:
                self._known_task_ids.discard(drained_task_id)
            logger.warning(
                "Deferred collection task %d for channel %d until %s: no active connected clients; "
                "left %d queued task(s) pending in DB",
                task_id,
                channel.channel_id,
                run_after.isoformat(),
                len(drained_task_ids),
            )
            return False, True
        if isinstance(exc, ConnectionError):
            requeued = await self._try_reconnect_and_requeue(task_id, channel, full, force, exc)
            if not requeued:
                self._retried_tasks.discard(task_id)
                await self._update_task_status_shutdown_safe(
                    task_id, CollectionTaskStatus.FAILED, error=str(exc)[:500],
                )
                logger.exception("Collection failed for channel %d (reconnect failed)", channel.channel_id)
            return requeued, False
        self._retried_tasks.discard(task_id)
        await self._update_task_status_shutdown_safe(
            task_id, CollectionTaskStatus.FAILED, error=str(exc)[:500],
        )
        logger.exception("Collection failed for channel %d", channel.channel_id)
        return False, False

    async def _run_worker(self) -> None:
        await self._run_single_worker()

    async def _update_task_status_shutdown_safe(
        self, task_id: int, status: CollectionTaskStatus, **kwargs
    ) -> None:
        """Update collection task status, suppressing DB errors during shutdown."""
        try:
            await self._channels.update_collection_task(task_id, status, **kwargs)
        except (ValueError, RuntimeError):
            if not self._shutdown_requested:
                raise
            logger.debug("Could not update task %d status during shutdown", task_id)

    async def _reset_task_to_pending_after_shutdown(self, task_id: int) -> None:
        reset = getattr(self._channels, "reset_collection_task_to_pending", None)
        if callable(reset):
            await reset(task_id, note=self.SHUTDOWN_REQUEUE_NOTE)
            return
        await self._channels.update_collection_task(
            task_id,
            CollectionTaskStatus.PENDING,
            note=self.SHUTDOWN_REQUEUE_NOTE,
        )

    async def _try_reconnect_and_requeue(
        self, task_id: int, channel: Channel, full: bool, force: bool, exc: Exception
    ) -> bool:
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
                "and will be picked up by the DB pull loop",
                task_id,
            )
            return False
        logger.warning(
            "ConnectionError for channel %d, reconnected and re-queued task %d: %s",
            channel.channel_id, task_id, exc,
        )
        return True

    async def _ingest_pending_tasks(self) -> int:
        if not self._resume_gate.is_set() or self._is_live_runtime_paused():
            return 0
        availability_getter = getattr(self._collector, "get_collection_availability", None)
        if callable(availability_getter):
            availability = availability_getter()
            if asyncio.iscoroutine(availability):
                availability = await availability
            if getattr(availability, "state", None) == "no_connected_active":
                logger.warning(
                    "[collection-queue] Pending-task ingest throttled: no active connected clients"
                )
                return 0

        resolve_backoff_remaining = 0
        pool = getattr(self._collector, "_pool", None)
        if pool is not None:
            resolve_backoff_remaining = pool.get_resolve_username_backoff_remaining_sec()

        pending = await self._channels.get_pending_channel_tasks()
        count = 0
        for task in pending:
            if self._shutdown_requested:
                break
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
            full = bool((task.payload or {}).get("full", False))
            if resolve_backoff_remaining > 0 and channel.username:
                run_after = datetime.now(timezone.utc) + timedelta(
                    seconds=resolve_backoff_remaining + RESOLVE_USERNAME_BACKOFF_BUFFER_SEC
                )
                self._schedule_requeue_after_delay(
                    task_id=task.id,
                    channel=channel,
                    force=force,
                    full=full,
                    run_after=run_after,
                )
                count += 1
                continue
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
        reset_count = await self._channels.reset_orphaned_running_tasks()
        if reset_count:
            logger.info("Reset %d orphaned RUNNING tasks to PENDING", reset_count)

        count = await self._ingest_pending_tasks()
        if count:
            logger.info("Re-enqueued %d pending collection tasks on startup", count)
        return count

    def start_db_pull(self, *, interval: float | None = None) -> None:
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

    def _drain_memory_queue(self) -> set[int]:
        drained: set[int] = set()
        while True:
            try:
                queued_task_id, *_ = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            self._queue.task_done()
            drained.add(queued_task_id)
        return drained

    async def _cancel_delayed_requeues(self) -> None:
        pending = list(self._delayed_requeues)
        for task in pending:
            task.cancel()
        for task in pending:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._delayed_requeues.clear()

    async def shutdown(self, *, grace_timeout: float | None = None) -> None:
        self._shutdown_requested = True
        self._shutdown_event.set()
        await self.stop_db_pull()
        await self._cancel_delayed_requeues()

        if self._supervisor and not self._supervisor.done():
            timeout = self.GRACEFUL_SHUTDOWN_TIMEOUT_SEC if grace_timeout is None else grace_timeout
            active_ids = list(self._active_task_ids.keys())
            if active_ids:
                logger.warning(
                    "Останавливаем сервис: ждём завершения %d активной(ых) задачи(ч) сбора "
                    "(до %.0f сек). Новые задачи останутся pending в БД.",
                    len(active_ids),
                    timeout,
                )
            try:
                await asyncio.wait_for(asyncio.shield(self._supervisor), timeout=timeout)
            except asyncio.TimeoutError:
                remaining_ids = list(self._active_task_ids.keys())
                if remaining_ids:
                    logger.warning(
                        "%d активная(ых) задачи(ч) сбора не завершилась за %.0f сек; "
                        "останавливаем сбор и возвращаем в pending.",
                        len(remaining_ids),
                        timeout,
                    )
                await self._collector.cancel()
                for cancel_evt in self._active_task_ids.values():
                    cancel_evt.set()
                self._stop_workers = True
                try:
                    await asyncio.wait_for(
                        asyncio.shield(self._supervisor),
                        timeout=self.FORCE_CANCEL_TIMEOUT_SEC,
                    )
                except asyncio.TimeoutError:
                    for tid in list(self._active_task_ids.keys()):
                        await self._reset_task_to_pending_after_shutdown(tid)
                    self._supervisor.cancel()
                    try:
                        await self._supervisor
                    except asyncio.CancelledError:
                        pass
                    for w in self._workers:
                        if not w.done():
                            w.cancel()
                    await asyncio.gather(*self._workers, return_exceptions=True)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Collection queue supervisor failed during shutdown")

        drained = self._drain_memory_queue()
        if drained:
            logger.info(
                "Left %d queued collection task(s) pending in DB for the next worker start",
                len(drained),
            )
        for tid in list(self._active_task_ids.keys()):
            try:
                await self._reset_task_to_pending_after_shutdown(tid)
            except Exception:
                logger.exception("Failed to reset active collection task during shutdown")
        self._active_task_ids.clear()
        self._retried_tasks.clear()
        self._known_task_ids.clear()
