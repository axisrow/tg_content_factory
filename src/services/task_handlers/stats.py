from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import date, datetime, timedelta, timezone

from src.database import DatabaseBusyError
from src.models import (
    CollectionTask,
    CollectionTaskStatus,
    CollectionTaskType,
    SqStatsTaskPayload,
    StatsAllTaskPayload,
)
from src.services.task_handlers.base import TaskHandlerContext
from src.telegram.collector import AllStatsClientsFloodedError, NoActiveStatsClientsError

logger = logging.getLogger(__name__)


class StatsTaskHandler:
    task_types = (CollectionTaskType.STATS_ALL, CollectionTaskType.SQ_STATS)

    def __init__(self, context: TaskHandlerContext):
        self._context = context

    async def handle(self, task: CollectionTask) -> None:
        if task.task_type == CollectionTaskType.STATS_ALL:
            await self.handle_stats_all(task)
            return
        if task.task_type == CollectionTaskType.SQ_STATS:
            await self.handle_sq_stats(task)
            return
        raise ValueError(f"Unsupported stats task type: {task.task_type}")

    async def handle_stats_all(self, task: CollectionTask) -> None:
        ctx = self._context
        if task.id is None:
            return

        payload = task.payload
        if not isinstance(payload, StatsAllTaskPayload):
            await ctx.tasks.update_collection_task(
                task.id, CollectionTaskStatus.FAILED, error="Unsupported stats task payload"
            )
            return

        channel_ids = payload.channel_ids
        channels_ok = payload.channels_ok or 0
        channels_err = payload.channels_err or 0
        remaining_ids = deque(
            payload.remaining_channel_ids
            if payload.remaining_channel_ids is not None
            else channel_ids[payload.next_index:]
        )
        next_index = len(channel_ids) - len(remaining_ids)

        logger.info(
            "Running stats task #%s: next_index=%d total=%d",
            task.id,
            next_index,
            len(channel_ids),
        )

        if not remaining_ids:
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                messages_collected=len(channel_ids),
            )
            return

        collector_wait_sec = 0.0
        while not ctx.stop_event.is_set():
            if ctx.collector.is_running:
                await asyncio.sleep(ctx.poll_interval_sec)
                collector_wait_sec += ctx.poll_interval_sec
                if collector_wait_sec >= ctx.channel_timeout_sec:
                    await ctx.tasks.update_collection_task(
                        task.id,
                        CollectionTaskStatus.FAILED,
                        messages_collected=channels_ok + channels_err,
                        error="Timed out waiting for collector to finish",
                    )
                    return
                continue
            break

        if ctx.stop_event.is_set():
            reschedule_payload = StatsAllTaskPayload(
                channel_ids=channel_ids,
                next_index=len(channel_ids) - len(remaining_ids),
                channels_ok=channels_ok,
                channels_err=channels_err,
                remaining_channel_ids=list(remaining_ids),
            )
            await ctx.tasks.reschedule_stats_task(
                task.id,
                payload=reschedule_payload,
                run_after=datetime.now(timezone.utc),
                messages_collected=channels_ok + channels_err,
            )
            return

        batch_limit = self._stats_batch_limit()
        batch_queue: deque[int] = deque()
        while remaining_ids and len(batch_queue) < batch_limit:
            batch_queue.append(remaining_ids.popleft())

        state_lock = asyncio.Lock()
        in_flight: list[int] = []
        stop_workers = False
        cancelled = False
        reschedule_at: datetime | None = None
        fail_error: str | None = None
        deferred_front: deque[int] = deque()

        type_collect_unlocked = getattr(type(ctx.collector), "collect_channel_stats_unlocked", None)
        collect_stats = (
            ctx.collector.collect_channel_stats_unlocked
            if callable(type_collect_unlocked)
            else ctx.collector.collect_channel_stats
        )

        async def _snapshot_payload() -> StatsAllTaskPayload:
            async with state_lock:
                remaining_snapshot = list(
                    dict.fromkeys([*deferred_front, *in_flight, *batch_queue, *remaining_ids])
                )
                processed = len(channel_ids) - len(remaining_snapshot)
                return StatsAllTaskPayload(
                    channel_ids=channel_ids,
                    next_index=processed,
                    channels_ok=channels_ok,
                    channels_err=channels_err,
                    remaining_channel_ids=remaining_snapshot,
                )

        async def _persist_progress() -> None:
            progress_payload = await _snapshot_payload()
            await ctx.tasks.persist_stats_progress(
                task.id,
                payload=progress_payload,
                messages_collected=progress_payload.channels_ok + progress_payload.channels_err,
            )

        async def _defer_remaining(channel_id: int, run_after: datetime | None, error: str | None = None) -> None:
            nonlocal stop_workers, reschedule_at, fail_error
            async with state_lock:
                stop_workers = True
                if channel_id in in_flight:
                    in_flight.remove(channel_id)
                deferred_front.appendleft(channel_id)
                if run_after is not None:
                    reschedule_at = run_after
                if error is not None:
                    fail_error = error

        async def _record_processed(channel_id: int, ok: bool) -> None:
            nonlocal channels_ok, channels_err
            async with state_lock:
                if channel_id in in_flight:
                    in_flight.remove(channel_id)
                if ok:
                    channels_ok += 1
                else:
                    channels_err += 1
            await _persist_progress()

        async def _worker() -> None:
            nonlocal cancelled, stop_workers
            while not ctx.stop_event.is_set():
                fresh = await ctx.tasks.get_collection_task(task.id)
                if fresh and fresh.status == CollectionTaskStatus.CANCELLED:
                    async with state_lock:
                        cancelled = True
                        stop_workers = True
                    return

                async with state_lock:
                    if stop_workers or not batch_queue:
                        return
                    channel_id = batch_queue.popleft()
                    in_flight.append(channel_id)

                channel = await ctx.channel_bundle.get_by_channel_id(channel_id)
                if channel is None:
                    await _record_processed(channel_id, ok=False)
                    continue

                try:
                    result = await asyncio.wait_for(
                        collect_stats(channel),
                        timeout=ctx.channel_timeout_sec,
                    )
                except asyncio.TimeoutError:
                    await _record_processed(channel_id, ok=False)
                except AllStatsClientsFloodedError as exc:
                    await _defer_remaining(channel_id, exc.next_available_at)
                    return
                except NoActiveStatsClientsError:
                    await _defer_remaining(
                        channel_id,
                        None,
                        error="No active connected Telegram accounts",
                    )
                    return
                except Exception as exc:
                    logger.error("Stats error for channel %s: %s", channel.channel_id, exc)
                    await _record_processed(channel_id, ok=False)
                else:
                    fresh = await ctx.tasks.get_collection_task(task.id)
                    if fresh and fresh.status == CollectionTaskStatus.CANCELLED:
                        async with state_lock:
                            cancelled = True
                            stop_workers = True
                        return
                    if result is None:
                        availability = await ctx.collector.get_stats_availability()
                        if (
                            availability.state == "all_flooded"
                            and availability.next_available_at_utc is not None
                        ):
                            await _defer_remaining(channel_id, availability.next_available_at_utc)
                            return
                        if availability.state != "available":
                            await _defer_remaining(
                                channel_id,
                                None,
                                error="No active connected Telegram accounts",
                            )
                            return
                        await _record_processed(channel_id, ok=False)
                    else:
                        await _record_processed(channel_id, ok=True)

                async with state_lock:
                    has_more = bool(batch_queue) and not stop_workers
                if has_more:
                    await asyncio.sleep(ctx.collector.delay_between_channels_sec)

        self._set_stats_all_running(True)
        try:
            worker_count = await self._stats_worker_count(len(batch_queue))
            # TaskGroup so a worker exception cancels its siblings and surfaces as
            # an ExceptionGroup, instead of gather() leaving detached workers
            # running ("Task exception was never retrieved") (audit #836/10).
            try:
                async with asyncio.TaskGroup() as tg:
                    for _ in range(worker_count):
                        tg.create_task(_worker())
            except* DatabaseBusyError as eg:
                # A worker can raise DatabaseBusyError from a stats write (it sits outside
                # the per-channel try/except). gather() re-raised it un-wrapped so the
                # dispatcher's `except DatabaseBusyError` requeued STATS_ALL; TaskGroup
                # instead wraps it in an ExceptionGroup that bypasses that handler and marks
                # the task permanently FAILED on a transient lock. Re-raise a plain
                # DatabaseBusyError so the requeue path still fires (#868 review).
                raise DatabaseBusyError(str(eg.exceptions[0])) from eg.exceptions[0]
        finally:
            self._set_stats_all_running(False)

        if cancelled:
            return

        if fail_error is not None:
            progress_payload = await _snapshot_payload()
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                messages_collected=progress_payload.channels_ok + progress_payload.channels_err,
                error=fail_error,
            )
            return

        progress_payload = await _snapshot_payload()
        if reschedule_at is not None or progress_payload.remaining_channel_ids:
            cooldown_at = datetime.now(timezone.utc) + timedelta(seconds=self._stats_cooldown_sec())
            await ctx.tasks.reschedule_stats_task(
                task.id,
                payload=progress_payload,
                run_after=reschedule_at or cooldown_at,
                messages_collected=progress_payload.channels_ok + progress_payload.channels_err,
            )
            return

        await ctx.tasks.update_collection_task(
            task.id,
            CollectionTaskStatus.COMPLETED,
            messages_collected=channels_ok + channels_err,
        )

    def _set_stats_all_running(self, running: bool) -> None:
        setter = getattr(self._context.collector, "set_stats_all_running", None)
        if callable(setter):
            setter(running)
        elif hasattr(self._context.collector, "_stats_all_running"):
            setattr(self._context.collector, "_stats_all_running", running)

    def _stats_batch_limit(self) -> int:
        config = self._context.config
        scheduler_config = getattr(config, "scheduler", config)
        configured = 10
        if scheduler_config is not None:
            configured = int(getattr(scheduler_config, "stats_all_max_channels_per_run", configured) or configured)
        return max(1, configured)

    def _stats_cooldown_sec(self) -> int:
        config = self._context.config
        scheduler_config = getattr(config, "scheduler", config)
        configured = 600
        if scheduler_config is not None:
            configured = int(getattr(scheduler_config, "stats_all_cooldown_sec", configured) or configured)
        return max(0, configured)

    async def _stats_worker_count(self, remaining_count: int) -> int:
        ctx = self._context
        configured = 1
        config = ctx.config
        scheduler_config = getattr(config, "scheduler", config)
        if scheduler_config is not None:
            configured = int(getattr(scheduler_config, "stats_all_worker_count", configured) or configured)
        collector_counter = getattr(ctx.collector, "available_stats_all_worker_count", None)
        type_counter = getattr(type(ctx.collector), "available_stats_all_worker_count", None)
        if callable(collector_counter) and callable(type_counter):
            try:
                counter_result = collector_counter()
                if asyncio.iscoroutine(counter_result):
                    counter_result = await counter_result
                configured = int(counter_result)
            except Exception:
                logger.debug("Failed to read collector stats-all worker count", exc_info=True)
        return max(1, min(configured, max(1, remaining_count)))

    async def handle_sq_stats(self, task: CollectionTask) -> None:
        ctx = self._context
        if task.id is None:
            return

        if not ctx.sq_bundle:
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                note="No search query bundle configured",
            )
            return

        payload = task.payload
        if not isinstance(payload, SqStatsTaskPayload):
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error="Invalid SQ_STATS payload",
            )
            return

        sq = await ctx.sq_bundle.get_by_id(payload.sq_id)
        if not sq:
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                note=f"Search query id={payload.sq_id} not found",
            )
            return

        try:
            today = date.today().isoformat()
            daily = await ctx.sq_bundle.get_fts_daily_stats_for_query(sq, days=1)
            today_count = 0
            for d in daily:
                if d.day == today:
                    today_count = d.count
                    break
            await ctx.sq_bundle.record_stat(payload.sq_id, today_count)
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                messages_collected=today_count,
                note=f"sq={sq.query}",
            )
        except Exception as exc:
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error=str(exc)[:500],
            )
