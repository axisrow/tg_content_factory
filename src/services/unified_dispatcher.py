from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timezone
from typing import TYPE_CHECKING

from src.database.bundles import ChannelBundle, PipelineBundle, SearchQueryBundle
from src.database.repositories.collection_tasks import CollectionTasksRepository
from src.models import (
    CollectionTask,
    CollectionTaskStatus,
    CollectionTaskType,
    PipelineRunTaskPayload,
    SqStatsTaskPayload,
    StatsAllTaskPayload,
)
from src.telegram.collector import Collector

if TYPE_CHECKING:
    from src.database import Database
    from src.search.engine import SearchEngine
    from src.services.photo_auto_upload_service import PhotoAutoUploadService
    from src.services.photo_task_service import PhotoTaskService

logger = logging.getLogger(__name__)

HANDLED_TYPES = [
    CollectionTaskType.STATS_ALL.value,
    CollectionTaskType.SQ_STATS.value,
    CollectionTaskType.PHOTO_DUE.value,
    CollectionTaskType.PHOTO_AUTO.value,
    CollectionTaskType.PIPELINE_RUN.value,
]


class UnifiedDispatcher:
    """Polls DB for non-CHANNEL_COLLECT tasks and dispatches them to handlers."""

    def __init__(
        self,
        collector: Collector,
        channel_bundle: ChannelBundle,
        tasks_repo: CollectionTasksRepository,
        *,
        sq_bundle: SearchQueryBundle | None = None,
        photo_task_service: PhotoTaskService | None = None,
        photo_auto_upload_service: PhotoAutoUploadService | None = None,
        default_batch_size: int = 20,
        poll_interval_sec: float = 1.0,
        channel_timeout_sec: float = 120.0,
        search_engine: "SearchEngine" | None = None,
        pipeline_bundle: PipelineBundle | None = None,
        db: "Database" | None = None,
    ):

        self._collector = collector
        self._channel_bundle = channel_bundle
        self._tasks = tasks_repo
        self._sq_bundle = sq_bundle
        self._photo_task_service = photo_task_service
        self._photo_auto_upload_service = photo_auto_upload_service
        self._default_batch_size = default_batch_size
        self._poll_interval_sec = poll_interval_sec
        self._channel_timeout_sec = channel_timeout_sec
        self._search_engine = search_engine
        self._pipeline_bundle = pipeline_bundle
        self._db = db
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
            CollectionTaskType.SQ_STATS: self._handle_sq_stats,
            CollectionTaskType.PHOTO_DUE: self._handle_photo_due,
            CollectionTaskType.PHOTO_AUTO: self._handle_photo_auto,
            CollectionTaskType.PIPELINE_RUN: self._handle_pipeline_run,
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
            task.id,
            next_index,
            batch_size,
            len(channel_ids),
        )

        if next_index >= len(channel_ids):
            await self._tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                messages_collected=channels_ok,
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
                        task.id,
                        CollectionTaskStatus.FAILED,
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
                            channel_ids=channel_ids,
                            next_index=cursor,
                            batch_size=batch_size,
                            channels_ok=channels_ok,
                            channels_err=channels_err,
                        )
                        continuation_id = await self._tasks.create_stats_continuation_task(
                            payload=continuation_payload,
                            run_after=availability.next_available_at_utc,
                            parent_task_id=task.id,
                        )
                        await self._tasks.update_collection_task(
                            task.id,
                            CollectionTaskStatus.FAILED,
                            messages_collected=channels_ok,
                            error=(
                                f"Deferred to task #{continuation_id} until "
                                f"{availability.next_available_at_utc.isoformat()} "
                                "(all clients flood-waited)"
                            ),
                        )
                        return

                    await self._tasks.update_collection_task(
                        task.id,
                        CollectionTaskStatus.FAILED,
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
                channel_ids=channel_ids,
                next_index=cursor,
                batch_size=batch_size,
                channels_ok=channels_ok,
                channels_err=channels_err,
            )
            await self._tasks.create_stats_continuation_task(
                payload=continuation_payload,
                run_after=datetime.now(timezone.utc),
                parent_task_id=task.id,
            )
            await self._tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                messages_collected=channels_ok,
            )
            return

        await self._tasks.update_collection_task(
            task.id,
            CollectionTaskStatus.COMPLETED,
            messages_collected=channels_ok,
        )

    # ── SQ_STATS ──

    async def _handle_sq_stats(self, task: CollectionTask) -> None:
        if task.id is None:
            return

        if not self._sq_bundle:
            await self._tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                note="No search query bundle configured",
            )
            return

        payload = task.payload
        if not isinstance(payload, SqStatsTaskPayload):
            await self._tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error="Invalid SQ_STATS payload",
            )
            return

        sq = await self._sq_bundle.get_by_id(payload.sq_id)
        if not sq:
            await self._tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
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
                task.id,
                CollectionTaskStatus.COMPLETED,
                messages_collected=today_count,
                note=f"sq={sq.query}",
            )
        except Exception as exc:
            await self._tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error=str(exc)[:500],
            )

    # ── PHOTO_DUE ──

    async def _handle_photo_due(self, task: CollectionTask) -> None:
        if task.id is None:
            return

        if not self._photo_task_service:
            await self._tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                note="No photo service",
            )
            return
        try:
            processed = await self._photo_task_service.run_due()
            await self._tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                messages_collected=processed,
            )
        except Exception as exc:
            await self._tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error=str(exc)[:500],
            )

    # ── PHOTO_AUTO ──

    async def _handle_photo_auto(self, task: CollectionTask) -> None:
        if task.id is None:
            return

        if not self._photo_auto_upload_service:
            await self._tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                note="No photo auto service",
            )
            return
        try:
            jobs = await self._photo_auto_upload_service.run_due()
            await self._tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                messages_collected=jobs,
            )
        except Exception as exc:
            await self._tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error=str(exc)[:500],
            )

    async def _handle_pipeline_run(self, task: CollectionTask) -> None:
        """Handle a PIPELINE_RUN generic task by executing the pipeline generation.

        This will create a generation_run record and run the pipeline generation
        using the configured SearchEngine and provider. The task is marked as
        completed on success or failed otherwise.
        """
        if task.id is None:
            return

        payload = task.payload
        if not isinstance(payload, PipelineRunTaskPayload):
            await self._tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error="Invalid PIPELINE_RUN payload",
            )
            return

        if not self._pipeline_bundle or not self._search_engine or not self._db:
            await self._tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error="Pipeline execution environment not configured",
            )
            return

        pipeline_id = payload.pipeline_id
        try:
            # Import lazily to avoid circular imports during module load
            from src.services.generation_service import GenerationService
            from src.services.pipeline_service import PipelineService
            from src.services.provider_service import AgentProviderService

            svc = PipelineService(self._pipeline_bundle)
            pipeline = await svc.get(pipeline_id)
            if pipeline is None:
                await self._tasks.update_collection_task(
                    task.id,
                    CollectionTaskStatus.COMPLETED,
                    note=f"Pipeline id={pipeline_id} not found",
                )
                return

            provider_service = AgentProviderService(self._db)
            provider_callable = provider_service.get_provider_callable(pipeline.llm_model)
            gen = GenerationService(self._search_engine, provider_callable=provider_callable)

            # create generation run
            db = self._db
            run_id = await db.repos.generation_runs.create_run(
                pipeline.id, pipeline.prompt_template
            )
            await db.repos.generation_runs.set_status(run_id, "running")
            retrieval_query = pipeline.prompt_template or pipeline.name or ""
            try:
                result = await gen.generate(
                    query=retrieval_query,
                    prompt_template=pipeline.prompt_template,
                    model=pipeline.llm_model,
                )
                await db.repos.generation_runs.save_result(
                    run_id,
                    result.get("generated_text", ""),
                    {"citations": result.get("citations", [])},
                )
                await db.repos.generation_runs.set_status(run_id, "completed")
                await self._tasks.update_collection_task(
                    task.id,
                    CollectionTaskStatus.COMPLETED,
                    messages_collected=1,
                    note=f"Pipeline run id={run_id}",
                )
            except Exception as exc:
                logger.exception("Pipeline run failed for pipeline_id=%d run_id=%d", pipeline_id, run_id)
                await db.repos.generation_runs.set_status(run_id, "failed")
                await self._tasks.update_collection_task(
                    task.id,
                    CollectionTaskStatus.FAILED,
                    error=str(exc)[:500],
                )
        except Exception as exc:
            logger.exception("Pipeline run handler failed for pipeline_id=%d", pipeline_id)
            await self._tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error=str(exc)[:500],
            )
