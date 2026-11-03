from __future__ import annotations

from tg_agent.models import CollectionTask, CollectionTaskStatus, CollectionTaskType
from tg_agent.services.task_handlers.base import TaskHandlerContext


class SeedingTaskHandler:
    """SEED_DUE (#1757): отправка due-реплик посева через SeedingPublisherService.

    Публик-ран ставится только явным «да» владельца (``seeding schedule`` /
    resume-цепочка апрувнутого расписания) — планировщик SEED_DUE не создаёт.
    """

    task_types = (CollectionTaskType.SEED_DUE,)

    def __init__(self, context: TaskHandlerContext):
        self._context = context

    async def handle(self, task: CollectionTask) -> None:
        ctx = self._context
        if task.id is None:
            return

        if not ctx.seeding_publisher:
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error="SeedingPublisherService not configured",
            )
            return
        try:
            sent = await ctx.seeding_publisher.run_due()
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                messages_collected=sent,
            )
        except Exception as exc:
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error=str(exc)[:500],
            )
