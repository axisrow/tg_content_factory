from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from src.config import SchedulerConfig
from src.database.bundles import PipelineBundle, SchedulerBundle, SearchQueryBundle
from src.settings_utils import parse_int_setting

if TYPE_CHECKING:
    from src.services.task_enqueuer import TaskEnqueuer

logger = logging.getLogger(__name__)


class SchedulerManager:
    def __init__(
        self,
        config: SchedulerConfig | None = None,
        *,
        scheduler_bundle: SchedulerBundle | None = None,
        search_query_bundle: SearchQueryBundle | None = None,
        task_enqueuer: TaskEnqueuer | None = None,
        pipeline_bundle: PipelineBundle | None = None,
    ):
        if config is None:
            config = SchedulerConfig()
        self._config = config
        self._task_enqueuer = task_enqueuer
        self._scheduler_bundle = scheduler_bundle
        self._sq_bundle = search_query_bundle
        self._pipeline_bundle = pipeline_bundle
        self._scheduler: AsyncIOScheduler | None = None
        self._job_id = "collect_all"
        self._photo_due_job_id = "photo_due"
        self._photo_auto_job_id = "photo_auto"
        self._current_interval_minutes: int = config.collect_interval_minutes
        self._bg_task: asyncio.Task | None = None
        self._jobs_cache: dict[str, object] = {}
        self._jobs_cache_ts: float = 0.0

    @property
    def is_running(self) -> bool:
        return self._scheduler is not None and self._scheduler.running

    @property
    def interval_minutes(self) -> int:
        return self._current_interval_minutes

    async def start(self) -> None:
        if self._scheduler is not None and self._scheduler.running:
            logger.warning("Scheduler already running")
            return

        if self._scheduler is not None:
            try:
                self._scheduler.shutdown(wait=False)
            except Exception:
                pass

        self._scheduler = AsyncIOScheduler()
        if self._scheduler_bundle:
            saved_interval = await self._scheduler_bundle.get_setting("collect_interval_minutes")
        else:
            saved_interval = None
        collect_interval = parse_int_setting(
            saved_interval,
            setting_name="collect_interval_minutes",
            default=self._config.collect_interval_minutes,
            logger=logger,
        )
        self._current_interval_minutes = collect_interval
        self._scheduler.add_job(
            self._run_collection,
            IntervalTrigger(minutes=collect_interval),
            id=self._job_id,
            replace_existing=True,
        )

        if self._sq_bundle:
            await self.sync_search_query_jobs()

        if self._pipeline_bundle:
            await self.sync_pipeline_jobs()

        if self._task_enqueuer is not None:
            self._scheduler.add_job(
                self._run_photo_due,
                IntervalTrigger(minutes=1),
                id=self._photo_due_job_id,
                replace_existing=True,
            )
            self._scheduler.add_job(
                self._run_photo_auto,
                IntervalTrigger(minutes=1),
                id=self._photo_auto_job_id,
                replace_existing=True,
            )

        self._scheduler.start()
        logger.info("Scheduler started: collecting every %d minutes", collect_interval)

    async def stop(self) -> None:
        if self._bg_task and not self._bg_task.done():
            self._bg_task.cancel()
            try:
                await self._bg_task
            except asyncio.CancelledError:
                pass
        self._bg_task = None

        if self._scheduler is None or not self._scheduler.running:
            return
        self._scheduler.shutdown(wait=False)
        self._scheduler = None
        logger.info("Scheduler stopped")

    def update_interval(self, minutes: int) -> None:
        if self._scheduler and self._scheduler.running:
            self._scheduler.reschedule_job(self._job_id, trigger=IntervalTrigger(minutes=minutes))
            self._current_interval_minutes = minutes
            logger.info("Collection interval updated to %d minutes", minutes)
        else:
            self._current_interval_minutes = minutes

    def get_job_next_run(self, job_id: str):
        """Return next_run_time for a scheduled job, or None if missing."""
        if self._scheduler is None:
            return None
        # Prefer scheduler.get_job if available
        try:
            job = self._scheduler.get_job(job_id)
            if job is None:
                return None
            return getattr(job, "next_run_time", None)
        except Exception:
            # Fallback: scan jobs list for compatibility with fake schedulers in tests
            try:
                for job in self._scheduler.get_jobs():
                    if getattr(job, "id", None) == job_id:
                        return getattr(job, "next_run_time", None)
            except Exception:
                return None
        return None

    def get_all_jobs_next_run(self) -> dict[str, object]:
        """Return dict of job_id -> next_run_time for all scheduled jobs (TTL-cached 5s)."""
        if self._scheduler is None:
            return {}
        now = time.monotonic()
        if now - self._jobs_cache_ts < 5.0:
            return self._jobs_cache
        try:
            jobs = self._scheduler.get_jobs()
            self._jobs_cache = {job.id: getattr(job, "next_run_time", None) for job in jobs}
            self._jobs_cache_ts = now
            return self._jobs_cache
        except Exception:
            return {}

    async def trigger_now(self) -> dict:
        return await self._run_collection()

    async def trigger_background(self) -> None:
        """Fire-and-forget collection run."""
        if self._bg_task and not self._bg_task.done():
            return
        self._bg_task = asyncio.create_task(self._run_collection())

    async def _run_collection(self) -> dict:
        """Enqueue all channels for collection."""
        logger.info("Starting scheduled collection")
        if not self._task_enqueuer:
            return {"enqueued": 0, "skipped": 0, "total": 0, "errors": 0}
        try:
            result = await self._task_enqueuer.enqueue_all_channels()
            stats = {
                "enqueued": result.queued_count,
                "skipped": result.skipped_existing_count,
                "total": result.total_candidates,
                "errors": 0,
            }
            logger.info("Scheduled collection enqueued: %s", stats)
            return stats
        except Exception:
            logger.exception("Collection enqueue failed")
            return {"enqueued": 0, "skipped": 0, "total": 0, "errors": 1}

    async def sync_search_query_jobs(self) -> None:
        if not self._sq_bundle or not self._scheduler:
            return

        all_active = await self._sq_bundle.get_all(active_only=True)
        active_queries = [sq for sq in all_active if sq.track_stats]
        active_ids = {f"sq_{sq.id}" for sq in active_queries}

        existing_jobs = self._scheduler.get_jobs()
        for job in existing_jobs:
            if job.id.startswith("sq_") and job.id not in active_ids:
                self._scheduler.remove_job(job.id)
                logger.info("Removed search query job %s", job.id)

        for sq in active_queries:
            job_id = f"sq_{sq.id}"
            self._scheduler.add_job(
                self._run_search_query,
                IntervalTrigger(minutes=sq.interval_minutes),
                id=job_id,
                replace_existing=True,
                args=[sq.id],
            )
        logger.info("Synced %d search query jobs", len(active_queries))

    async def sync_pipeline_jobs(self) -> None:
        """Sync scheduler jobs with active content pipelines.

        Ensures there is a periodic job for each enabled pipeline and removes jobs
        for pipelines that are no longer active.
        """
        if not self._pipeline_bundle or not self._scheduler:
            return

        all_active = await self._pipeline_bundle.get_all(active_only=True)
        active_pipelines = [p for p in all_active if p.is_active]
        active_ids = {f"pipeline_run_{p.id}" for p in active_pipelines if p.id is not None}

        existing_jobs = self._scheduler.get_jobs()
        for job in existing_jobs:
            if job.id.startswith("pipeline_run_") and job.id not in active_ids:
                self._scheduler.remove_job(job.id)
                logger.info("Removed pipeline job %s", job.id)

        for p in active_pipelines:
            if p.id is None:
                continue
            job_id = f"pipeline_run_{p.id}"
            self._scheduler.add_job(
                self._run_pipeline_job,
                IntervalTrigger(minutes=p.generate_interval_minutes),
                id=job_id,
                replace_existing=True,
                args=[p.id],
            )
        logger.info("Synced %d pipeline jobs", len(active_pipelines))

    async def _run_pipeline_job(self, pipeline_id: int) -> None:
        """Enqueue a pipeline run task for the given pipeline id."""
        if not self._task_enqueuer:
            return
        try:
            await self._task_enqueuer.enqueue_pipeline_run(pipeline_id)
        except Exception:
            logger.exception("Error enqueuing pipeline run for pipeline_id=%d", pipeline_id)

    async def _run_search_query(self, sq_id: int) -> None:
        """Enqueue SQ_STATS task for a search query."""
        if not self._task_enqueuer:
            return
        try:
            await self._task_enqueuer.enqueue_sq_stats(sq_id)
        except Exception:
            logger.exception("Error enqueuing SQ_STATS for sq_id=%d", sq_id)

    async def _run_photo_due(self) -> dict:
        if not self._task_enqueuer:
            return {"processed": 0}
        try:
            await self._task_enqueuer.enqueue_photo_due()
        except Exception:
            logger.exception("Error enqueuing photo_due")
        return {"enqueued": True}

    async def _run_photo_auto(self) -> dict:
        if not self._task_enqueuer:
            return {"jobs": 0}
        try:
            await self._task_enqueuer.enqueue_photo_auto()
        except Exception:
            logger.exception("Error enqueuing photo_auto")
        return {"enqueued": True}
