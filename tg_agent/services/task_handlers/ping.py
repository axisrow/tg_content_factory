from __future__ import annotations

from tg_agent.models import CollectionTask, CollectionTaskStatus, CollectionTaskType
from tg_agent.services.task_handlers.base import TaskHandlerContext
from tg_agent.utils.errors import error_text


class PingTaskHandler:
    task_types = (CollectionTaskType.PING_DUE,)

    def __init__(self, context: TaskHandlerContext):
        self._context = context

    async def handle(self, task: CollectionTask) -> None:
        ctx = self._context
        if task.id is None:
            return

        if not ctx.ping_monitor_service:
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error="PingMonitorService not configured",
            )
            return
        try:
            checked = await ctx.ping_monitor_service.run_due()
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                messages_collected=checked,
            )
        except Exception as exc:
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error=error_text(exc),
            )
