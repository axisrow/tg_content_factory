from __future__ import annotations

import logging

from tg_agent.models import CollectionTask, CollectionTaskStatus, CollectionTaskType
from tg_agent.services.task_handlers.base import TaskHandlerContext
from tg_agent.utils.errors import error_text

logger = logging.getLogger(__name__)


class PhotoTaskHandler:
    task_types = (CollectionTaskType.PHOTO_DUE, CollectionTaskType.PHOTO_AUTO)

    def __init__(self, context: TaskHandlerContext):
        self._context = context

    async def handle(self, task: CollectionTask) -> None:
        if task.task_type == CollectionTaskType.PHOTO_DUE:
            await self.handle_photo_due(task)
            return
        if task.task_type == CollectionTaskType.PHOTO_AUTO:
            await self.handle_photo_auto(task)
            return
        raise ValueError(f"Unsupported photo task type: {task.task_type}")

    async def handle_photo_due(self, task: CollectionTask) -> None:
        ctx = self._context
        if task.id is None:
            return

        if not ctx.photo_task_service:
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error="PhotoTaskService not configured",
            )
            return
        try:
            processed = await ctx.photo_task_service.run_due()
            failed = ctx.photo_task_service.last_run_failed
            if failed:
                # A claimed item whose send failed is FAILED on its row; the task
                # that ran it must not say COMPLETED — the content handlers' rule
                # (audit #835/1). messages_collected counts what went out.
                reasons = "; ".join(ctx.photo_task_service.last_run_errors[:5])
                # The worker log carries it too — the rows alone are not a trace.
                logger.warning(
                    "PHOTO_DUE task id=%s: %d of %d due photo item(s) were not sent: %s",
                    task.id,
                    failed,
                    processed,
                    reasons,
                )
                await ctx.tasks.update_collection_task(
                    task.id,
                    CollectionTaskStatus.FAILED,
                    messages_collected=processed - failed,
                    error=f"{failed} of {processed} due photo item(s) were not sent: {reasons}",
                )
                return
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                messages_collected=processed,
            )
        except Exception as exc:
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error=error_text(exc),
            )

    async def handle_photo_auto(self, task: CollectionTask) -> None:
        ctx = self._context
        if task.id is None:
            return

        if not ctx.photo_auto_upload_service:
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error="PhotoAutoUploadService not configured",
            )
            return
        try:
            jobs = await ctx.photo_auto_upload_service.run_due()
            assert isinstance(jobs, int)
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                messages_collected=jobs,
            )
        except Exception as exc:
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error=error_text(exc),
            )
