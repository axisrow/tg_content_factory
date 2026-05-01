from __future__ import annotations

from src.models import CollectionTask, CollectionTaskStatus, CollectionTaskType
from src.services.task_handlers.base import TaskHandlerContext


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
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                messages_collected=processed,
            )
        except Exception as exc:
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error=str(exc)[:500],
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
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                messages_collected=jobs,
            )
        except Exception as exc:
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error=str(exc)[:500],
            )
