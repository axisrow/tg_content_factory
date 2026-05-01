from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timezone

from src.models import (
    CollectionTask,
    CollectionTaskStatus,
    CollectionTaskType,
    SqStatsTaskPayload,
    StatsAllTaskPayload,
)
from src.services.task_handlers.base import TaskHandlerContext

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
        next_index = payload.next_index
        channels_ok = payload.channels_ok or 0
        channels_err = payload.channels_err or 0

        logger.info(
            "Running stats task #%s: next_index=%d total=%d",
            task.id,
            next_index,
            len(channel_ids),
        )

        if next_index >= len(channel_ids):
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                messages_collected=len(channel_ids),
            )
            return

        cursor = next_index
        collector_wait_sec = 0.0
        while cursor < len(channel_ids):
            if ctx.stop_event.is_set():
                break

            fresh = await ctx.tasks.get_collection_task(task.id)
            if fresh and fresh.status == CollectionTaskStatus.CANCELLED:
                return

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
            collector_wait_sec = 0.0

            channel_id = channel_ids[cursor]
            channel = await ctx.channel_bundle.get_by_channel_id(channel_id)
            if channel is None:
                channels_err += 1
                cursor += 1
                progress_payload = StatsAllTaskPayload(
                    channel_ids=channel_ids,
                    next_index=cursor,
                    channels_ok=channels_ok,
                    channels_err=channels_err,
                )
                await ctx.tasks.persist_stats_progress(
                    task.id,
                    payload=progress_payload,
                    messages_collected=channels_ok + channels_err,
                )
                continue

            try:
                result = await asyncio.wait_for(
                    ctx.collector.collect_channel_stats(channel),
                    timeout=ctx.channel_timeout_sec,
                )
            except asyncio.TimeoutError:
                channels_err += 1
                cursor += 1
            except Exception as exc:
                logger.error("Stats error for channel %s: %s", channel.channel_id, exc)
                channels_err += 1
                cursor += 1
            else:
                fresh = await ctx.tasks.get_collection_task(task.id)
                if fresh and fresh.status == CollectionTaskStatus.CANCELLED:
                    return
                if result is None:
                    availability = await ctx.collector.get_stats_availability()
                    if (
                        availability.state == "all_flooded"
                        and availability.next_available_at_utc is not None
                    ):
                        reschedule_payload = StatsAllTaskPayload(
                            channel_ids=channel_ids,
                            next_index=cursor,
                            channels_ok=channels_ok,
                            channels_err=channels_err,
                        )
                        await ctx.tasks.reschedule_stats_task(
                            task.id,
                            payload=reschedule_payload,
                            run_after=availability.next_available_at_utc,
                            messages_collected=channels_ok + channels_err,
                        )
                        return

                    await ctx.tasks.update_collection_task(
                        task.id,
                        CollectionTaskStatus.FAILED,
                        messages_collected=channels_ok + channels_err,
                        error="No active connected Telegram accounts",
                    )
                    return

                channels_ok += 1
                cursor += 1

            progress_payload = StatsAllTaskPayload(
                channel_ids=channel_ids,
                next_index=cursor,
                channels_ok=channels_ok,
                channels_err=channels_err,
            )
            await ctx.tasks.persist_stats_progress(
                task.id,
                payload=progress_payload,
                messages_collected=channels_ok + channels_err,
            )

            if cursor < len(channel_ids):
                await asyncio.sleep(ctx.collector.delay_between_channels_sec)

        if cursor < len(channel_ids):
            reschedule_payload = StatsAllTaskPayload(
                channel_ids=channel_ids,
                next_index=cursor,
                channels_ok=channels_ok,
                channels_err=channels_err,
            )
            await ctx.tasks.reschedule_stats_task(
                task.id,
                payload=reschedule_payload,
                run_after=datetime.now(timezone.utc),
                messages_collected=channels_ok + channels_err,
            )
            return

        await ctx.tasks.update_collection_task(
            task.id,
            CollectionTaskStatus.COMPLETED,
            messages_collected=channels_ok + channels_err,
        )

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
