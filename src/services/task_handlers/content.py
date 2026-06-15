from __future__ import annotations

import logging

from src.models import (
    CollectionTask,
    CollectionTaskStatus,
    CollectionTaskType,
    ContentGenerateTaskPayload,
    ContentPublishTaskPayload,
)
from src.services.task_handlers.base import TaskHandlerContext, build_content_generation_service

logger = logging.getLogger(__name__)


class ContentTaskHandler:
    task_types = (CollectionTaskType.CONTENT_GENERATE, CollectionTaskType.CONTENT_PUBLISH)

    def __init__(self, context: TaskHandlerContext):
        self._context = context

    async def handle(self, task: CollectionTask) -> None:
        if task.task_type == CollectionTaskType.CONTENT_GENERATE:
            await self.handle_content_generate(task)
            return
        if task.task_type == CollectionTaskType.CONTENT_PUBLISH:
            await self.handle_content_publish(task)
            return
        raise ValueError(f"Unsupported content task type: {task.task_type}")

    async def handle_content_generate(self, task: CollectionTask) -> None:
        ctx = self._context
        if task.id is None:
            return

        payload = task.payload
        if not isinstance(payload, ContentGenerateTaskPayload):
            await ctx.tasks.update_collection_task(
                task.id, CollectionTaskStatus.FAILED, error="Invalid CONTENT_GENERATE payload"
            )
            return

        if not ctx.db or not ctx.search_engine or not ctx.pipeline_bundle:
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error="Pipeline execution environment not configured",
            )
            return

        pipeline_id = payload.pipeline_id
        try:
            from src.models import PipelinePublishMode
            from src.services.pipeline_service import PipelineService
            from src.services.publish_service import PublishService

            svc = PipelineService(ctx.pipeline_bundle)
            pipeline = await svc.get(pipeline_id)
            if pipeline is None:
                await ctx.tasks.update_collection_task(
                    task.id,
                    CollectionTaskStatus.COMPLETED,
                    note=f"Pipeline id={pipeline_id} not found",
                )
                return

            db = ctx.db
            gen = await build_content_generation_service(ctx)
            run = await gen.generate(pipeline=pipeline, model=pipeline.llm_model)

            effective_mode = (
                (run.metadata or {}).get("effective_publish_mode", pipeline.publish_mode.value)
                if run is not None
                else pipeline.publish_mode.value
            )
            if effective_mode == PipelinePublishMode.AUTO.value and run is not None:
                publish_svc = PublishService(db, ctx.client_pool)
                try:
                    results = await publish_svc.publish_run(run, pipeline)
                except Exception as pub_exc:
                    logger.exception(
                        "Auto-publish failed for run id=%s (pipeline_id=%d); generation already saved",
                        run.id,
                        pipeline_id,
                    )
                    await ctx.tasks.update_collection_task(
                        task.id,
                        CollectionTaskStatus.FAILED,
                        error=f"Generation ok but publish failed: {pub_exc!s:.400}",
                    )
                    return
                # publish_run() does not raise on delivery errors (no client,
                # unresolved dialog, write-forbidden, timeout) — it returns
                # PublishResult(success=False). Inspect the list, else a silent
                # delivery loss is reported as COMPLETED (audit #835/1).
                if not results or not all(r.success for r in results):
                    err = next(
                        (r.error for r in results if not r.success and r.error),
                        "publish returned no results" if not results else "publish failed",
                    )
                    logger.warning(
                        "Auto-publish delivery failed for run id=%s (pipeline_id=%d): %s",
                        run.id,
                        pipeline_id,
                        err,
                    )
                    await ctx.tasks.update_collection_task(
                        task.id,
                        CollectionTaskStatus.FAILED,
                        error=f"Generation ok but publish failed: {err}"[:500],
                    )
                    return

            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                messages_collected=1 if run is not None else 0,
                note=f"Generated run id={run.id}" if run is not None else "Generation returned no result",
            )
        except Exception as exc:
            logger.exception("Content generate handler failed for pipeline_id=%d", pipeline_id)
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error=str(exc)[:500],
            )

    async def handle_content_publish(self, task: CollectionTask) -> None:
        ctx = self._context
        if task.id is None:
            return

        payload = task.payload
        if not isinstance(payload, ContentPublishTaskPayload):
            await ctx.tasks.update_collection_task(
                task.id, CollectionTaskStatus.FAILED, error="Invalid CONTENT_PUBLISH payload"
            )
            return

        if not ctx.db or not ctx.pipeline_bundle:
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error="Pipeline execution environment not configured",
            )
            return

        if ctx.client_pool is None:
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error="client_pool not configured",
            )
            return

        pipeline_id = payload.pipeline_id
        try:
            from src.database.repositories.generation_runs import GenerationRunsRepository
            from src.services.pipeline_service import PipelineService
            from src.services.publish_service import PublishService

            db = ctx.db
            publish_svc = PublishService(db, ctx.client_pool)

            filter_sql = "AND pipeline_id = ?" if pipeline_id is not None else ""
            params: tuple = (pipeline_id,) if pipeline_id is not None else ()
            cur = await db.execute(
                f"SELECT * FROM generation_runs WHERE moderation_status = 'approved' {filter_sql} ORDER BY id ASC",
                params,
            )
            rows = await cur.fetchall()
            if not rows:
                await ctx.tasks.update_collection_task(
                    task.id, CollectionTaskStatus.COMPLETED, note="No approved runs to publish"
                )
                return

            runs = [GenerationRunsRepository._to_generation_run(row) for row in rows]

            pipeline_svc = PipelineService(ctx.pipeline_bundle)
            published = 0
            attempted = 0
            failures: list[str] = []
            for run in runs:
                if run.pipeline_id is None:
                    continue
                pipeline = await pipeline_svc.get(run.pipeline_id)
                if pipeline is None:
                    continue
                attempted += 1
                results = await publish_svc.publish_run(run, pipeline)
                # A run is published only when *every* target succeeded — mirrors
                # PublishService.set_published_at. Counting any(r.success) as a
                # publish hides partial delivery (audit #838/5).
                if results and all(r.success for r in results):
                    published += 1
                else:
                    err = next(
                        (r.error for r in results if not r.success and r.error),
                        "no results" if not results else "publish failed",
                    )
                    failures.append(f"run {run.id}: {err}")

            if failures:
                await ctx.tasks.update_collection_task(
                    task.id,
                    CollectionTaskStatus.FAILED,
                    messages_collected=published,
                    error=f"Published {published}/{attempted} runs; failures: "
                    + "; ".join(failures[:5]),
                )
                return

            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                messages_collected=published,
                note=f"Published {published}/{attempted} runs",
            )
        except Exception as exc:
            logger.exception("Content publish handler failed")
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error=str(exc)[:500],
            )
