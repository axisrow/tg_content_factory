from __future__ import annotations

import logging

from src.models import CollectionTask, CollectionTaskStatus, CollectionTaskType, PipelineRunTaskPayload
from src.services.task_handlers.base import TaskHandlerContext, build_image_service, resolve_llm_provider_service

logger = logging.getLogger(__name__)


class PipelineTaskHandler:
    task_types = (CollectionTaskType.PIPELINE_RUN,)

    def __init__(self, context: TaskHandlerContext):
        self._context = context

    async def handle(self, task: CollectionTask) -> None:
        await self.handle_pipeline_run(task)

    async def handle_pipeline_run(self, task: CollectionTask) -> None:
        ctx = self._context
        if task.id is None:
            return

        payload = task.payload
        if not isinstance(payload, PipelineRunTaskPayload):
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error="Invalid PIPELINE_RUN payload",
            )
            return

        if not ctx.pipeline_bundle or not ctx.search_engine or not ctx.db:
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error="Pipeline execution environment not configured",
            )
            return

        pipeline_id = payload.pipeline_id
        run_id: int | None = None
        try:
            from src.services.content_generation_service import ContentGenerationService
            from src.services.draft_notification_service import DraftNotificationService
            from src.services.pipeline_service import PipelineService
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
            try:
                run = await gen.generate(
                    pipeline=pipeline,
                    model=pipeline.llm_model,
                    dry_run=payload.dry_run,
                    since_hours=payload.since_hours,
                )
                run_id = run.id
                await ctx.tasks.update_collection_task(
                    task.id,
                    CollectionTaskStatus.COMPLETED,
                    messages_collected=run.result_count,
                    note=f"Pipeline run id={run_id}",
                )
            except Exception as exc:
                logger.exception("Pipeline run failed for pipeline_id=%d run_id=%s", pipeline_id, run_id)
                if run_id is not None:
                    await db.repos.generation_runs.set_status(run_id, "failed")
                await ctx.tasks.update_collection_task(
                    task.id,
                    CollectionTaskStatus.FAILED,
                    error=str(exc)[:500],
                )
        except Exception as exc:
            logger.exception("Pipeline run handler failed for pipeline_id=%d", pipeline_id)
            if run_id is not None:
                await ctx.db.repos.generation_runs.set_status(run_id, "failed")
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error=str(exc)[:500],
            )
