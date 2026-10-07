from __future__ import annotations

import asyncio
import inspect
import logging
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from telethon.errors import (
    ChannelPrivateError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)
from telethon_floodgate import TelegramRateLimitedError

from src.database import Database, DatabaseBusyError
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
from src.telegram.collector_types import unknown_tl_type_note

logger = logging.getLogger(__name__)

# Read-path lock errors reach us as a RAW sqlite3.OperationalError, not
# DatabaseBusyError: repositories read through ReadPoolProxy.execute (src/database/
# pool.py), which calls conn.execute() directly and never runs _with_busy_retry —
# the only place that normalises a busy lock into DatabaseBusyError. So a busy read
# must be matched by BOTH type and message. Messages mirror facade._SQLITE_BUSY_MESSAGES.
_SQLITE_BUSY_MESSAGES = ("database is locked", "database table is locked", "database is busy")

# Slack added on top of the gate's exact retry_after so the rescheduled run
# does not land a millisecond before the sliding window actually reopens.
GATE_RATE_LIMIT_RETRY_BUFFER_SEC = 5.0

# Consecutive ChannelPrivateError failures (no intervening success or other
# error) before a channel is auto-deactivated (#1451). Owner decision: 3 — a
# single failure can be account-rotation noise on a live channel.
CHANNEL_PRIVATE_DEACTIVATE_THRESHOLD = 3


def _is_transient_busy_error(exc: BaseException) -> bool:
    """True for a transient SQLite lock from either DB path (#1249).

    - ``DatabaseBusyError`` — the normalised write-path busy error.
    - a raw ``sqlite3.OperationalError`` whose message names a lock — the read path
      (``ReadPoolProxy``) surfaces this un-normalised.
    Any other ``OperationalError`` (bad SQL, schema errors) is NOT swallowed.
    """
    if isinstance(exc, DatabaseBusyError):
        return True
    if isinstance(exc, sqlite3.OperationalError):
        message = str(exc).lower()
        return any(part in message for part in _SQLITE_BUSY_MESSAGES)
    return False


def _is_permanent_collection_error(exc: BaseException) -> bool:
    """Username vacant/invalid = channel deleted or renamed; retrying is pointless.

    ChannelPrivateError is deliberately absent: it never reaches the retry pass
    (its own branch earlier in _handle_collection_exception handles the
    deleted/kicked/made-private ambiguity via the streak-of-3 auto-deactivate).
    ChannelInvalidError is deliberately retryable: in this deployment it is
    usually the StringSession entity-cache loss after a restart (CLAUDE.md
    "Entity cache") — the channel collects fine once dialogs are warmed; a truly
    dead channel just exhausts its retry-pass attempts and stays FAILED.
    """
    return isinstance(exc, (UsernameNotOccupiedError, UsernameInvalidError))


class CollectionQueue:
    DB_PULL_INTERVAL_SEC = 3.0
    NO_CLIENTS_RETRY_DELAY_SEC = 120
    # Retry pass: total collection attempts a task gets (including the original)
    # and the delay before a retry attempt starts — the delayed requeue rides the
    # same reschedule path as the flood/gate retries.
    RETRY_PASS_MAX_ATTEMPTS = 5
    RETRY_PASS_DELAY_SEC = 30.0
    GRACEFUL_SHUTDOWN_TIMEOUT_SEC = 120.0
    FORCE_CANCEL_TIMEOUT_SEC = 10.0
    SHUTDOWN_REQUEUE_NOTE = "Остановка сервиса во время сбора; задача будет продолжена после запуска."
    NO_CLIENTS_REQUEUE_NOTE = "Отложено: нет подключённых активных аккаунтов для сбора."
    _CANCEL_SWEEP_INTERVAL_SEC = 1.0
    _CANCEL_SWEEP_READ_TIMEOUT_SEC = 5.0

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
        # Retry pass: task_id -> failed attempts in this run. In-memory on purpose:
        # retry scope is one queue run; FAILED rows must not resurrect after a
        # restart. Kept strictly separate from _channel_private_error_counts.
        self._retry_pass: dict[int, int] = {}
        self._channel_private_error_counts: dict[int, int] = {}
        self._last_cancel_sweep = 0.0
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
        self._retry_pass.pop(task_id, None)
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
        self._retry_pass.clear()
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
        getter: Any = getattr(self._collector, "collection_worker_count", None)
        if callable(getter):
            count: Any = getter()
            return max(1, int(count))
        return 1

    async def _available_target_worker_count(self) -> int:
        slot_getter: Any = getattr(self._collector, "available_collection_slot_count", None)
        if callable(slot_getter):
            slots: Any = slot_getter()
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
            count: Any = getter()
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
            await self._maybe_arm_cancel_events_from_db()
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
        # _stop_workers (нет клиентов) заканчивает надзор, пока чужие сборы ещё
        # живы с удерживаемым клиентом — гасить своп отмены в этом окне нельзя:
        # инцидент 07.10.26 не должен повторяться внутри него.
        while self._active_task_ids and not self._shutdown_requested:
            await self._maybe_arm_cancel_events_from_db()
            await asyncio.sleep(self._CANCEL_SWEEP_INTERVAL_SEC)

    async def _maybe_arm_cancel_events_from_db(self) -> None:
        """Взвести cancel_events задач, отменённых другим процессом (инцидент
        07.10.26: CLI/БД переворачивают строку в CANCELLED, а in-memory
        cancel_event живёт только здесь — сбор качал историю до конца).

        Не чаще раза в секунду (monotonic-гейт), на каждой итерации супервизора
        включая тики паузы — отмена из БД обязана останавливать активный сбор
        и на паузе (pause() = «активные задачи дожидаются», и их отменяют как
        раз в этот момент). При простое чтений нет: супервизор завершается.
        Один пакетный ids+status SELECT — без материализации строки и
        enum-каста; удалённая строка трактуется как отмена (зомби-сбор без
        записи в БД хуже остановки). Чтение с таймаутом (не блокируем
        супервизор на исчерпанном read-pool), сбой — предупреждением без
        трейсбека и повтор на следующем тике: опрос не должен убивать
        супервизор и топить лог.
        """
        now = time.monotonic()
        if now - self._last_cancel_sweep < self._CANCEL_SWEEP_INTERVAL_SEC:
            return
        self._last_cancel_sweep = now
        # Снимок ДО await: задача, зарегистрированная в _active_task_ids, пока
        # выполняется SELECT, в выборку не попала — «нет в результате» для неё
        # означало бы ложную отмену только что начавшегося сбора.
        queried = {
            task_id: cancel_event
            for task_id, cancel_event in self._active_task_ids.items()
            if not cancel_event.is_set()
        }
        if not queried:
            return
        try:
            status_pairs = await asyncio.wait_for(
                self._channels.tasks.fetch_task_status_pairs(list(queried)),
                timeout=self._CANCEL_SWEEP_READ_TIMEOUT_SEC,
            )
        except Exception as exc:
            logger.warning("Cancel sweep DB read failed: %s", exc)
            return
        status_by_id = dict(status_pairs)
        for task_id, cancel_event in queried.items():
            if task_id not in status_by_id or (
                status_by_id[task_id] == CollectionTaskStatus.CANCELLED.value
            ):
                cancel_event.set()

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

            # task_done() bookkeeping has two call sites by design: the skip
            # paths inside _validate_task_pre_dispatch already call it before
            # returning None (and we `continue`, skipping the try/finally below),
            # while the dispatch path below calls it once in the finally block.
            # Exactly one task_done() per dequeued item, never two.
            try:
                validated = await self._validate_task_pre_dispatch(task_id, channel, force, full)
            except Exception:
                # A transient read error (e.g. "database is locked") here must not
                # strand the PENDING row: drop it from _known_task_ids so a later
                # _ingest_pending_tasks can re-pick it, and release the queue slot.
                logger.warning("Pre-dispatch validation failed for task %d; will requeue", task_id, exc_info=True)
                self._known_task_ids.discard(task_id)
                self._queue.task_done()
                continue
            if validated is None:
                continue
            channel = validated

            cancel_event = asyncio.Event()
            self._active_task_ids[task_id] = cancel_event
            stop_after_no_clients = False
            keep_known_task_id = False
            should_stop_workers = False
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
                self._retry_pass.pop(task_id, None)
            except Exception as exc:
                keep_known_task_id, stop_after_no_clients = await self._handle_collection_exception(
                    exc, task_id=task_id, channel=channel, force=force, full=full
                )
            finally:
                self._active_task_ids.pop(task_id, None)
                if not keep_known_task_id:
                    self._known_task_ids.discard(task_id)
                self._queue.task_done()
                await self._log_progress()
                if stop_after_no_clients:
                    self._stop_workers = True
                    should_stop_workers = True
            if should_stop_workers:
                break

    async def _log_progress(self) -> None:
        """«Сколько ещё собирать»: остаток незавершённых задач после каждой задачи."""
        try:
            left = await self._channels.tasks.count_active_channel_tasks()
        except Exception:
            return  # ponytail: прогресс-строка косметическая — не роняем воркер
        logger.info("Collection queue progress: %d task(s) left to collect", left)

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
        if not channel.is_active:
            await self._channels.cancel_collection_task(
                task_id,
                note="Канал деактивирован до начала сбора.",
            )
            logger.info(
                "Task %d skipped: channel %d is inactive",
                task_id,
                channel.channel_id,
            )
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
        # The collect run already succeeded here — messages are saved and
        # last_collected_id is advanced. A transient DatabaseBusyError on these
        # post-read guards must NOT drag the successful task into FAILED (#1249),
        # so treat a busy read as "not cancelled / no skip note" and still mark
        # the task COMPLETED. Mirrors the pre-dispatch busy guard at the top of
        # the worker loop.
        try:
            persisted = await self._channels.get_collection_task(task_id)
        except (DatabaseBusyError, sqlite3.OperationalError) as exc:
            if not _is_transient_busy_error(exc):
                raise
            logger.warning(
                "DB busy reading task %d after successful collect; assuming not cancelled", task_id
            )
            persisted = None
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
            try:
                after_ch = await self._channels.get_by_pk(channel.id)
            except (DatabaseBusyError, sqlite3.OperationalError) as exc:
                if not _is_transient_busy_error(exc):
                    raise
                # Busy on the skip-note lookup only costs us the cosmetic
                # "Пропущен: <reason>" note — never fail the completed task (#1249).
                logger.warning(
                    "DB busy reading channel %d for skip note on task %d; completing without note",
                    channel.channel_id,
                    task_id,
                )
                after_ch = None
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
        if channel.id is not None:
            self._channel_private_error_counts.pop(channel.id, None)
        logger.info("Collected %d messages from channel %d", count, channel.channel_id)

    async def _handle_collection_exception(
        self, exc: Exception, *, task_id: int, channel: Channel, force: bool, full: bool
    ) -> tuple[bool, bool]:
        """Handle a failure raised while collecting a single channel.

        Returns ``(keep_known_task_id, stop_after_no_clients)`` for the worker
        loop's finally block. The ``isinstance`` chain mirrors the original
        except-clause precedence exactly. Split out of ``_run_single_worker`` (#922).
        """
        # Any non-ChannelPrivateError outcome breaks the deactivation streak —
        # including infra outages (flooded accounts, rate limits), so a dead
        # channel must earn its own streak, not ride a busy network's.
        if not isinstance(exc, ChannelPrivateError) and channel.id is not None:
            self._channel_private_error_counts.pop(channel.id, None)
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
        if isinstance(exc, TelegramRateLimitedError):
            # The calibrated gate (#1418) legitimately binds on peak collector
            # minutes (media 50/min vs the 48/min history cap): deferring the
            # task is the designed outcome, not a failure — mirror the
            # resolve-rate-limited branch above.
            run_after = datetime.now(timezone.utc) + timedelta(
                seconds=exc.retry_after_sec + GATE_RATE_LIMIT_RETRY_BUFFER_SEC
            )
            note = (
                "Отложено: gate "
                f"{exc.category} rate-limited до {run_after.astimezone(timezone.utc).isoformat()}"
            )
            self._retried_tasks.discard(task_id)
            await self._channels.reschedule_collection_task(task_id, run_after=run_after, note=note)
            self._schedule_requeue_after_delay(
                task_id=task_id, channel=channel, force=force, full=full, run_after=run_after
            )
            logger.warning(
                "Rescheduled collection task %d for channel %d until %s: "
                "gate %s rate-limited on %s",
                task_id,
                channel.channel_id,
                run_after.isoformat(),
                exc.category,
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
        if isinstance(exc, ChannelPrivateError):
            await self._handle_channel_private_error(task_id, channel, exc)
            return False, False
        if isinstance(exc, ConnectionError):
            outcome = await self._try_reconnect_and_requeue(task_id, channel, full, force, exc)
            if outcome == "requeued":
                # Task is PENDING and back in the in-memory queue; keep it owned.
                return True, False
            if outcome == "pending":
                # Queue was full: the task is PENDING in the DB. Do NOT overwrite
                # to FAILED — that would strand the retry (the pull loop only
                # re-picks PENDING). keep_known_task_id=False so the worker's
                # finally drops it from _known_task_ids and the pull loop re-picks
                # it (#1248).
                logger.warning(
                    "ConnectionError for channel %d: reconnected but queue full; task %d stays "
                    "PENDING for the DB pull loop",
                    channel.channel_id,
                    task_id,
                )
                return False, False
            self._retried_tasks.discard(task_id)
            await self._update_task_status_shutdown_safe(
                task_id, CollectionTaskStatus.FAILED, error=str(exc)[:500],
            )
            logger.exception("Collection failed for channel %d (reconnect failed)", channel.channel_id)
            return False, False
        self._retried_tasks.discard(task_id)
        tl_note = unknown_tl_type_note(exc)
        await self._update_task_status_shutdown_safe(
            task_id, CollectionTaskStatus.FAILED, error=tl_note or str(exc)[:500]
        )
        if tl_note:
            # Retry pass still applies: Telegram serves new types unevenly across its servers.
            logger.warning(
                "Skipping channel %d: Telegram schema is newer than Telethon — %s", channel.channel_id, tl_note
            )
        else:
            logger.exception("Collection failed for channel %d", channel.channel_id)
        if _is_permanent_collection_error(exc):
            # Deleted/renamed channel: no point re-attempting this run.
            self._retry_pass.pop(task_id, None)
            return False, False
        attempts = self._retry_pass.get(task_id, 0) + 1
        if attempts >= self.RETRY_PASS_MAX_ATTEMPTS:
            self._retry_pass.pop(task_id, None)  # exhausted: terminal FAILED
            return False, False
        self._retry_pass[task_id] = attempts
        # Same delayed-requeue path as the flood/gate branches: the delay keeps the
        # retry out of the fresh channels' way; restart-survival and single-pickup
        # semantics are inherited from that machinery.
        run_after = datetime.now(timezone.utc) + timedelta(seconds=self.RETRY_PASS_DELAY_SEC)
        await self._channels.reschedule_collection_task(
            task_id,
            run_after=run_after,
            note=f"Retry pass {attempts + 1}/{self.RETRY_PASS_MAX_ATTEMPTS}",
        )
        self._schedule_requeue_after_delay(
            task_id=task_id, channel=channel, force=force, full=full, run_after=run_after
        )
        logger.warning(
            "Retry pass: task %d re-enqueued for channel %d (attempt %d/%d)",
            task_id,
            channel.channel_id,
            attempts + 1,
            self.RETRY_PASS_MAX_ATTEMPTS,
        )
        return True, False

    async def _handle_channel_private_error(
        self, task_id: int, channel: Channel, exc: ChannelPrivateError
    ) -> None:
        """Count consecutive ChannelPrivateError failures per channel pk (#1451).

        At ``CHANNEL_PRIVATE_DEACTIVATE_THRESHOLD`` auto-deactivate — the same
        ``set_active(False, origin="auto")`` + ``type='unavailable'`` pair the
        resolve-gone path uses (dispatcher channels_mixin). A human active
        decision suppresses the deactivation (rowcount 0): log once and stop
        counting.
        """
        self._retried_tasks.discard(task_id)
        error = f"ChannelPrivateError: {exc}"[:500]
        if channel.id is None:
            await self._update_task_status_shutdown_safe(
                task_id, CollectionTaskStatus.FAILED, error=error
            )
            return
        streak = self._channel_private_error_counts.get(channel.id, 0) + 1
        self._channel_private_error_counts[channel.id] = streak
        # FAILED first: a busy-DB failure below must not strand the task RUNNING.
        await self._update_task_status_shutdown_safe(
            task_id, CollectionTaskStatus.FAILED, error=error
        )
        if streak < CHANNEL_PRIVATE_DEACTIVATE_THRESHOLD:
            return
        self._channel_private_error_counts.pop(channel.id, None)
        if await self._channels.set_active(
            channel.id,
            False,
            reason=f"{CHANNEL_PRIVATE_DEACTIVATE_THRESHOLD} consecutive ChannelPrivateError",
        ) == 0:
            logger.info(
                "Suppressed auto-deactivation of channel %d (pk=%d): rowcount 0 "
                "(operator-kept active or channel row removed)",
                channel.channel_id,
                channel.id,
            )
            return
        await self._channels.set_type(channel.channel_id, "unavailable")
        logger.warning(
            "Channel %d (pk=%d) deactivated after %d consecutive ChannelPrivateError",
            channel.channel_id,
            channel.id,
            CHANNEL_PRIVATE_DEACTIVATE_THRESHOLD,
        )

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
            await cast(Any, reset(task_id, note=self.SHUTDOWN_REQUEUE_NOTE))
            return
        await self._channels.update_collection_task(
            task_id,
            CollectionTaskStatus.PENDING,
            note=self.SHUTDOWN_REQUEUE_NOTE,
        )

    async def _try_reconnect_and_requeue(
        self, task_id: int, channel: Channel, full: bool, force: bool, exc: Exception
    ) -> str:
        """Try to recover a ConnectionError by reconnecting and re-queueing.

        Returns one of three outcomes so the caller can react correctly (#1248):
        - ``"requeued"`` — task is PENDING and back in the in-memory queue; the
          caller must NOT overwrite it to FAILED and must keep it in
          ``_known_task_ids`` (the in-memory item owns it).
        - ``"pending"`` — the in-memory queue was full, so the task is PENDING in
          the DB only. The caller must NOT overwrite it to FAILED (that would lose
          the retry: the DB pull loop only re-picks PENDING rows). It must also
          drop the task from ``_known_task_ids`` so the pull loop can re-pick it.
        - ``"failed"`` — reconnect was impossible or already attempted; the caller
          should mark the task FAILED.
        """
        if task_id in self._retried_tasks:
            return "failed"
        pool = getattr(self._collector, "_pool", None)
        if pool is None or not hasattr(pool, "reconnect_phone"):
            return "failed"
        reconnected = False
        for phone in list(pool.clients):
            result = await pool.reconnect_phone(phone)
            reconnected = reconnected or result
        if not reconnected:
            return "failed"
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
            return "pending"
        logger.warning(
            "ConnectionError for channel %d, reconnected and re-queued task %d: %s",
            channel.channel_id, task_id, exc,
        )
        return "requeued"

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
            payload = cast(dict[str, Any], task.payload or {})
            force = bool(payload.get("force", False))
            full = bool(payload.get("full", False))
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
        self._retry_pass.clear()
        self._known_task_ids.clear()
