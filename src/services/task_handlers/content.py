from __future__ import annotations

import logging

from src.models import (
    CollectionTask,
    CollectionTaskStatus,
    CollectionTaskType,
    ContentGenerateTaskPayload,
    ContentPublishTaskPayload,
)
from src.services.task_handlers.base import TaskHandlerContext, build_image_service, resolve_llm_provider_service

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
            from src.services.content_generation_service import ContentGenerationService
            from src.services.draft_notification_service import DraftNotificationService
            from src.services.pipeline_service import PipelineService
            from src.services.publish_service import PublishService
            from src.services.quality_scoring_service import QualityScoringService

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
            notification_service = DraftNotificationService(db, ctx.notifier)
            image_service = await build_image_service(ctx)
            provider_service = await resolve_llm_provider_service(ctx)
            quality_service = QualityScoringService(db, provider_service=provider_service)
            gen = ContentGenerationService(
                db,
                ctx.search_engine,
                config=ctx.config,
                image_service=image_service,
                notification_service=notification_service,
                quality_service=quality_service,
                client_pool=ctx.client_pool,
                provider_service=provider_service,
            )
            run = await gen.generate(pipeline=pipeline, model=pipeline.llm_model)

            effective_mode = (
                (run.metadata or {}).get("effective_publish_mode", pipeline.publish_mode.value)
                if run is not None
                else pipeline.publish_mode.value
            )
            if effective_mode == PipelinePublishMode.AUTO.value and run is not None:
                publish_svc = PublishService(db, ctx.client_pool)
                try:
                    await publish_svc.publish_run(run, pipeline)
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
            for run in runs:
                if run.pipeline_id is None:
                    continue
                pipeline = await pipeline_svc.get(run.pipeline_id)
                if pipeline is None:
                    continue
                results = await publish_svc.publish_run(run, pipeline)
                if any(r.success for r in results):
                    published += 1

            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                messages_collected=published,
                note=f"Published {published}/{len(runs)} runs",
            )
        except Exception as exc:
            logger.exception("Content publish handler failed")
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error=str(exc)[:500],
            )
