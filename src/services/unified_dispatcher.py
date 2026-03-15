from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING, Any

from src.database.bundles import ChannelBundle, SearchQueryBundle
from src.database.repositories.collection_tasks import CollectionTasksRepository
from src.models import (
    CollectionTask,
    CollectionTaskStatus,
    CollectionTaskType,
    SearchQuery,
    SqStatsTaskPayload,
    StatsAllTaskPayload,
)
from src.telegram.collector import Collector

if TYPE_CHECKING:
    from src.search.engine import SearchEngine
    from src.services.notification_matcher import NotificationMatcher
    from src.services.photo_auto_upload_service import PhotoAutoUploadService
    from src.services.photo_task_service import PhotoTaskService

logger = logging.getLogger(__name__)

HANDLED_TYPES = [
    CollectionTaskType.STATS_ALL.value,
    CollectionTaskType.NOTIFICATION_SEARCH.value,
    CollectionTaskType.SQ_STATS.value,
    CollectionTaskType.PHOTO_DUE.value,
    CollectionTaskType.PHOTO_AUTO.value,
]

NotificationQueryFn = Callable[..., Coroutine[Any, Any, list[SearchQuery]]]


class UnifiedDispatcher:
    """Polls DB for non-CHANNEL_COLLECT tasks and dispatches them to handlers."""

    def __init__(
        self,
        collector: Collector,
        channel_bundle: ChannelBundle,
        tasks_repo: CollectionTasksRepository,
        *,
        notification_query_fn: NotificationQueryFn | None = None,
        search_engine: SearchEngine | None = None,
        notification_matcher: NotificationMatcher | None = None,
        sq_bundle: SearchQueryBundle | None = None,
        photo_task_service: PhotoTaskService | None = None,
        photo_auto_upload_service: PhotoAutoUploadService | None = None,
        default_batch_size: int = 20,
        poll_interval_sec: float = 1.0,
        channel_timeout_sec: float = 120.0,
    ):
        self._collector = collector
        self._channel_bundle = channel_bundle
        self._tasks = tasks_repo
        self._notification_query_fn = notification_query_fn
        self._search_engine = search_engine
        self._notification_matcher = notification_matcher
        self._sq_bundle = sq_bundle
        self._photo_task_service = photo_task_service
        self._photo_auto_upload_service = photo_auto_upload_service
        self._default_batch_size = default_batch_size
        self._poll_interval_sec = poll_interval_sec
        self._channel_timeout_sec = channel_timeout_sec
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        recovered = await self._tasks.requeue_running_generic_tasks_on_startup(
            datetime.now(timezone.utc), HANDLED_TYPES
        )
        if recovered:
            logger.warning("Recovered %d interrupted generic tasks on startup", recovered)
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            task: CollectionTask | None = None
            try:
                task = await self._tasks.claim_next_due_generic_task(
                    datetime.now(timezone.utc), HANDLED_TYPES
                )
                if task is None:
                    await asyncio.sleep(self._poll_interval_sec)
                    continue

                await self._dispatch(task)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Unified dispatcher loop failure")
                if task and task.id is not None:
                    try:
                        fresh = await self._tasks.get_collection_task(task.id)
                        if fresh and fresh.status == CollectionTaskStatus.RUNNING:
                            await self._tasks.update_collection_task(
                                task.id,
                                CollectionTaskStatus.FAILED,
                                error="Task failed with unexpected dispatcher error",
                            )
                    except Exception:
                        logger.exception("Failed to mark broken task as failed")
                await asyncio.sleep(self._poll_interval_sec)

    async def _dispatch(self, task: CollectionTask) -> None:
        handler = {
            CollectionTaskType.STATS_ALL: self._handle_stats_all,
            CollectionTaskType.NOTIFICATION_SEARCH: self._handle_notification_search,
            CollectionTaskType.SQ_STATS: self._handle_sq_stats,
            CollectionTaskType.PHOTO_DUE: self._handle_photo_due,
            CollectionTaskType.PHOTO_AUTO: self._handle_photo_auto,
        }.get(task.task_type)
        if handler is None:
            await self._tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error=f"Unknown task type: {task.task_type}",
            )
            return
        await handler(task)

    # ── STATS_ALL ──

    async def _handle_stats_all(self, task: CollectionTask) -> None:
        if task.id is None:
            return

        payload = task.payload
        if not isinstance(payload, StatsAllTaskPayload):
            await self._tasks.update_collection_task(
                task.id, CollectionTaskStatus.FAILED, error="Unsupported stats task payload"
            )
            return

        channel_ids = payload.channel_ids
        next_index = payload.next_index
        batch_size = max(1, payload.batch_size or self._default_batch_size)
        channels_ok = payload.channels_ok or (task.messages_collected or 0)
        channels_err = payload.channels_err

        logger.info(
            "Running stats task #%s: next_index=%d batch_size=%d total=%d",
            task.id, next_index, batch_size, len(channel_ids),
        )

        if next_index >= len(channel_ids):
            await self._tasks.update_collection_task(
                task.id, CollectionTaskStatus.COMPLETED, messages_collected=channels_ok,
            )
            return

        batch_end = min(next_index + batch_size, len(channel_ids))
        cursor = next_index

        collector_wait_sec = 0.0
        while cursor < batch_end:
            if self._stop_event.is_set():
                break
            if self._collector.is_running:
                await asyncio.sleep(self._poll_interval_sec)
                collector_wait_sec += self._poll_interval_sec
                if collector_wait_sec >= self._channel_timeout_sec:
                    await self._tasks.update_collection_task(
                        task.id, CollectionTaskStatus.FAILED,
                        messages_collected=channels_ok,
                        error="Timed out waiting for collector to finish",
                    )
                    return
                continue
            collector_wait_sec = 0.0

            channel_id = channel_ids[cursor]
            channel = await self._channel_bundle.get_by_channel_id(channel_id)
            if channel is None:
                channels_err += 1
                cursor += 1
                continue

            try:
                result = await asyncio.wait_for(
                    self._collector.collect_channel_stats(channel),
                    timeout=self._channel_timeout_sec,
                )
            except asyncio.TimeoutError:
                channels_err += 1
                cursor += 1
            except Exception as exc:
                logger.error("Stats error for channel %s: %s", channel.channel_id, exc)
                channels_err += 1
                cursor += 1
            else:
                if result is None:
                    availability = await self._collector.get_stats_availability()
                    if (
                        availability.state == "all_flooded"
                        and availability.next_available_at_utc is not None
                    ):
                        continuation_payload = StatsAllTaskPayload(
                            channel_ids=channel_ids, next_index=cursor,
                            batch_size=batch_size, channels_ok=channels_ok,
                            channels_err=channels_err,
                        )
                        continuation_id = await self._tasks.create_stats_continuation_task(
                            payload=continuation_payload,
                            run_after=availability.next_available_at_utc,
                            parent_task_id=task.id,
                        )
                        await self._tasks.update_collection_task(
                            task.id, CollectionTaskStatus.FAILED,
                            messages_collected=channels_ok,
                            error=(
                                f"Deferred to task #{continuation_id} until "
                                f"{availability.next_available_at_utc.isoformat()} "
                                "(all clients flood-waited)"
                            ),
                        )
                        return

                    await self._tasks.update_collection_task(
                        task.id, CollectionTaskStatus.FAILED,
                        messages_collected=channels_ok,
                        error="No active connected Telegram accounts",
                    )
                    return

                channels_ok += 1
                cursor += 1
                await self._tasks.update_collection_task_progress(task.id, channels_ok)

            if cursor < batch_end:
                await asyncio.sleep(self._collector.delay_between_channels_sec)

        if cursor < len(channel_ids):
            continuation_payload = StatsAllTaskPayload(
                channel_ids=channel_ids, next_index=cursor,
                batch_size=batch_size, channels_ok=channels_ok,
                channels_err=channels_err,
            )
            await self._tasks.create_stats_continuation_task(
                payload=continuation_payload,
                run_after=datetime.now(timezone.utc),
                parent_task_id=task.id,
            )
            await self._tasks.update_collection_task(
                task.id, CollectionTaskStatus.COMPLETED, messages_collected=channels_ok,
            )
            return

        await self._tasks.update_collection_task(
            task.id, CollectionTaskStatus.COMPLETED, messages_collected=channels_ok,
        )

    # ── NOTIFICATION_SEARCH ──

    async def _handle_notification_search(self, task: CollectionTask) -> None:
        if task.id is None:
            return

        if not self._search_engine:
            await self._tasks.update_collection_task(
                task.id, CollectionTaskStatus.COMPLETED,
                note="No search engine configured",
            )
            return

        if not self._notification_query_fn:
            await self._tasks.update_collection_task(
                task.id, CollectionTaskStatus.COMPLETED,
                note="No notification query source",
            )
            return

        queries = await self._notification_query_fn(active_only=True)
        total_results = 0
        searched = 0
        errors = 0

        for sq in queries:
            try:
                quota = await self._search_engine.check_search_quota(sq.query)
                if quota and quota.get("remains") == 0 and not quota.get("query_is_free"):
                    logger.info("Search quota exhausted, stopping notification search")
                    break

                result = await self._search_engine.search_telegram(sq.query, limit=50)
                if result.error:
                    errors += 1
                else:
                    total_results += result.total
                    searched += 1

                    if self._notification_matcher and result.messages:
                        await self._notification_matcher.match_and_notify(result.messages, [sq])
            except Exception:
                logger.exception("Error searching query '%s'", sq.query)
                errors += 1

        await self._tasks.update_collection_task(
            task.id, CollectionTaskStatus.COMPLETED,
            messages_collected=total_results,
            note=f"queries={searched}, errors={errors}",
        )

    # ── SQ_STATS ──

    async def _handle_sq_stats(self, task: CollectionTask) -> None:
        if task.id is None:
            return

        if not self._sq_bundle:
            await self._tasks.update_collection_task(
                task.id, CollectionTaskStatus.COMPLETED,
                note="No search query bundle configured",
            )
            return

        payload = task.payload
        if not isinstance(payload, SqStatsTaskPayload):
            await self._tasks.update_collection_task(
                task.id, CollectionTaskStatus.FAILED, error="Invalid SQ_STATS payload",
            )
            return

        sq = await self._sq_bundle.get_by_id(payload.sq_id)
        if not sq:
            await self._tasks.update_collection_task(
                task.id, CollectionTaskStatus.COMPLETED,
                note=f"Search query id={payload.sq_id} not found",
            )
            return

        try:
            today = date.today().isoformat()
            daily = await self._sq_bundle.get_fts_daily_stats_for_query(sq, days=1)
            today_count = 0
            for d in daily:
                if d.day == today:
                    today_count = d.count
                    break
            await self._sq_bundle.record_stat(payload.sq_id, today_count)
            await self._tasks.update_collection_task(
                task.id, CollectionTaskStatus.COMPLETED,
                messages_collected=today_count,
                note=f"sq={sq.query}",
            )
        except Exception as exc:
            await self._tasks.update_collection_task(
                task.id, CollectionTaskStatus.FAILED,
                error=str(exc)[:500],
            )

    # ── PHOTO_DUE ──

    async def _handle_photo_due(self, task: CollectionTask) -> None:
        if task.id is None:
            return

        if not self._photo_task_service:
            await self._tasks.update_collection_task(
                task.id, CollectionTaskStatus.COMPLETED, note="No photo service",
            )
            return
        try:
            processed = await self._photo_task_service.run_due()
            await self._tasks.update_collection_task(
                task.id, CollectionTaskStatus.COMPLETED,
                messages_collected=processed,
            )
        except Exception as exc:
            await self._tasks.update_collection_task(
                task.id, CollectionTaskStatus.FAILED, error=str(exc)[:500],
            )

    # ── PHOTO_AUTO ──

    async def _handle_photo_auto(self, task: CollectionTask) -> None:
        if task.id is None:
            return

        if not self._photo_auto_upload_service:
            await self._tasks.update_collection_task(
                task.id, CollectionTaskStatus.COMPLETED, note="No photo auto service",
            )
            return
        try:
            jobs = await self._photo_auto_upload_service.run_due()
            await self._tasks.update_collection_task(
                task.id, CollectionTaskStatus.COMPLETED,
                messages_collected=jobs,
            )
        except Exception as exc:
            await self._tasks.update_collection_task(
                task.id, CollectionTaskStatus.FAILED, error=str(exc)[:500],
            )
