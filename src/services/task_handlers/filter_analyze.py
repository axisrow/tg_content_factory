from __future__ import annotations

import asyncio
import logging

from src.filters.analyzer import ChannelAnalyzer
from src.filters.models import FilterReport
from src.models import (
    CollectionTask,
    CollectionTaskStatus,
    CollectionTaskType,
    FilterAnalyzeTaskPayload,
)
from src.services.filter_deletion_service import FilterDeletionService
from src.services.task_handlers.base import TaskHandlerContext

logger = logging.getLogger(__name__)

DEFAULT_FILTER_ANALYZE_TIMEOUT_SEC = 600.0


class FilterAnalyzeTaskHandler:
    """Run the channel filter analysis as a background task (#793).

    The analysis scans the whole messages table (tens of seconds on large DBs),
    so the web handler only enqueues this task and the UI polls its status.
    """

    task_types = (CollectionTaskType.FILTER_ANALYZE,)

    def __init__(self, context: TaskHandlerContext):
        self._context = context

    def _timeout_sec(self) -> float | None:
        config = self._context.config
        scheduler_config = getattr(config, "scheduler", config)
        raw = getattr(scheduler_config, "filter_analyze_timeout_sec", DEFAULT_FILTER_ANALYZE_TIMEOUT_SEC)
        try:
            timeout = float(raw)
        except (TypeError, ValueError):
            timeout = DEFAULT_FILTER_ANALYZE_TIMEOUT_SEC
        return timeout if timeout > 0 else None

    async def handle(self, task: CollectionTask) -> None:
        ctx = self._context
        if task.id is None:
            return

        if not isinstance(task.payload, FilterAnalyzeTaskPayload):
            await ctx.tasks.update_collection_task(
                task.id, CollectionTaskStatus.FAILED, error="Unsupported filter-analyze payload"
            )
            return

        if ctx.db is None:
            await ctx.tasks.update_collection_task(
                task.id, CollectionTaskStatus.FAILED, error="Database is not available"
            )
            return

        analyzer = ChannelAnalyzer(ctx.db)
        timeout = self._timeout_sec()

        async def _analyze() -> FilterReport:
            # apply_filters writes under the same DB contention that can stall
            # analyze_all, so the timeout must cover both (review on #823).
            report = await analyzer.analyze_all()
            if await self._is_cancelled(task.id):
                return report
            await analyzer.apply_filters(report)
            return report

        try:
            if timeout is not None:
                report = await asyncio.wait_for(_analyze(), timeout=timeout)
            else:
                report = await _analyze()
        except asyncio.TimeoutError:
            await ctx.tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error=f"Analysis timed out after {timeout:.0f}s",
            )
            return
        except Exception as exc:
            logger.exception("filter/analyze task %s failed", task.id)
            await ctx.tasks.update_collection_task(
                task.id, CollectionTaskStatus.FAILED, error=str(exc)[:500]
            )
            return

        # An admin can cancel the task from the scheduler page mid-analysis;
        # honour that before any side effects (Codex review on #823).
        if await self._is_cancelled(task.id):
            logger.info("filter/analyze task %s cancelled, skipping apply/purge", task.id)
            return

        purge_note = ""
        purged_count = 0
        try:
            purged_count = await self._auto_purge(report)
        except Exception as exc:
            # Filters are already applied — the task did its main job, so it
            # completes; the purge failure stays visible in the note and the
            # log instead of raising a false "analysis failed" alarm (#676,
            # review on #823).
            logger.exception("filter/analyze task %s: auto-purge failed", task.id)
            purge_note = f" auto_purge_failed={str(exc)[:200]}"

        await ctx.tasks.update_collection_task(
            task.id,
            CollectionTaskStatus.COMPLETED,
            messages_collected=report.filtered_count,
            note=(
                f"analyzed={report.total_channels} filtered={report.filtered_count}"
                f" purged={purged_count}{purge_note}"
            ),
        )

    async def _is_cancelled(self, task_id: int) -> bool:
        fresh = await self._context.tasks.get_collection_task(task_id)
        return fresh is not None and fresh.status == CollectionTaskStatus.CANCELLED

    async def _auto_purge(self, report) -> int:
        """Mirror the legacy inline auto-purge from the web handler."""
        db = self._context.db
        assert db is not None
        auto_delete = await db.repos.settings.get_setting("auto_delete_filtered")
        if auto_delete != "1" or report.filtered_count <= 0:
            return 0

        channels = await db.get_channels_with_counts(active_only=False, include_filtered=True)
        pk_map = {ch.channel_id: ch.id for ch in channels if ch.id is not None}
        filtered_pks = [
            pk_map[r.channel_id] for r in report.results if r.is_filtered and r.channel_id in pk_map
        ]
        if not filtered_pks:
            return 0

        # purge_channels_by_pks never touches the optional channel_service.
        svc = FilterDeletionService(db)
        result = await svc.purge_channels_by_pks(filtered_pks)
        if result.errors:
            logger.warning(
                "filter/analyze auto-purge: %d channel(s) failed: %s",
                len(result.errors),
                "; ".join(result.errors),
            )
        else:
            logger.info("filter/analyze: auto-purged %d filtered channels", result.purged_count)
        return result.purged_count
