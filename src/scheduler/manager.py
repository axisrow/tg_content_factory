from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from src.config import SchedulerConfig
from src.database import Database
from src.database.bundles import SchedulerBundle, SearchQueryBundle
from src.settings_utils import parse_int_setting

if TYPE_CHECKING:
    from src.services.task_enqueuer import TaskEnqueuer

logger = logging.getLogger(__name__)


class _LegacySchedulerBundle:
    def __init__(self, store):
        self._store = store

    async def get_setting(self, key: str) -> str | None:
        return await self._store.get_setting(key)

    async def list_notification_queries(self, active_only: bool = True):
        return await self._store.get_notification_queries(active_only=active_only)


class SchedulerManager:
    def __init__(
        self,
        *args,
        config: SchedulerConfig | None = None,
        scheduler_bundle: SchedulerBundle | Database | None = None,
        search_query_bundle: SearchQueryBundle | None = None,
        task_enqueuer: TaskEnqueuer | None = None,
        collector=None,
        search_engine=None,
        photo_task_service=None,
        photo_auto_upload_service=None,
        db=None,
    ):
        # Support legacy: SchedulerManager(collector, config, bundle)
        # and new: SchedulerManager(config, ...) or all-keyword
        if args:
            if isinstance(args[0], SchedulerConfig):
                config = args[0]
            else:
                collector = args[0]
                if len(args) >= 2:
                    config = args[1]
                if len(args) >= 3 and scheduler_bundle is None:
                    scheduler_bundle = args[2]

        if config is None:
            config = SchedulerConfig()
        self._config = config
        self._task_enqueuer = task_enqueuer

        if scheduler_bundle is None:
            if db is not None:
                scheduler_bundle = db
            elif collector is not None:
                scheduler_bundle = getattr(collector, "_db", None)
        if isinstance(scheduler_bundle, Database):
            scheduler_bundle = SchedulerBundle.from_database(scheduler_bundle)
        elif not isinstance(scheduler_bundle, SchedulerBundle):
            if scheduler_bundle is not None:
                scheduler_bundle = _LegacySchedulerBundle(scheduler_bundle)
        self._scheduler_bundle = scheduler_bundle
        self._sq_bundle = search_query_bundle
        self._scheduler: AsyncIOScheduler | None = None
        self._job_id = "collect_all"
        self._search_job_id = "notification_search"
        self._photo_due_job_id = "photo_due"
        self._photo_auto_job_id = "photo_auto"
        self._current_interval_minutes: int = config.collect_interval_minutes

        # Legacy attributes for backward compat
        self._collector = collector
        self._search_engine = search_engine
        self._photo_task_service = photo_task_service
        self._photo_auto_upload_service = photo_auto_upload_service
        self._last_run: datetime | None = None
        self._last_stats: dict | None = None
        self._last_search_run: datetime | None = None
        self._last_search_stats: dict | None = None
        self._bg_task: asyncio.Task | None = None
        self._search_bg_task: asyncio.Task | None = None

    @property
    def is_running(self) -> bool:
        return self._scheduler is not None and self._scheduler.running

    @property
    def is_collecting(self) -> bool:
        if self._collector:
            return self._collector.is_running
        return False

    @property
    def interval_minutes(self) -> int:
        return self._current_interval_minutes

    @property
    def search_interval_minutes(self) -> int:
        return self._config.search_interval_minutes

    @property
    def last_run(self) -> datetime | None:
        return self._last_run

    @property
    def last_stats(self) -> dict | None:
        return self._last_stats

    @property
    def last_search_run(self) -> datetime | None:
        return self._last_search_run

    @property
    def last_search_stats(self) -> dict | None:
        return self._last_search_stats

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

        # Notification search job
        has_search = self._task_enqueuer is not None or self._search_engine is not None
        if has_search:
            self._scheduler.add_job(
                self._run_keyword_search,
                IntervalTrigger(minutes=self._config.search_interval_minutes),
                id=self._search_job_id,
                replace_existing=True,
            )
            logger.info(
                "Notification search job added: every %d minutes",
                self._config.search_interval_minutes,
            )

        if self._sq_bundle:
            await self.sync_search_query_jobs()

        # Photo jobs
        has_photo_due = self._task_enqueuer is not None or self._photo_task_service is not None
        has_photo_auto = (
            self._task_enqueuer is not None or self._photo_auto_upload_service is not None
        )
        if has_photo_due:
            self._scheduler.add_job(
                self._run_photo_due,
                IntervalTrigger(minutes=1),
                id=self._photo_due_job_id,
                replace_existing=True,
            )
        if has_photo_auto:
            self._scheduler.add_job(
                self._run_photo_auto,
                IntervalTrigger(minutes=1),
                id=self._photo_auto_job_id,
                replace_existing=True,
            )

        self._scheduler.start()
        logger.info("Scheduler started: collecting every %d minutes", collect_interval)

    async def stop(self) -> None:
        for task in (self._bg_task, self._search_bg_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._bg_task = None
        self._search_bg_task = None

        if self._scheduler is None or not self._scheduler.running:
            return
        self._scheduler.shutdown(wait=False)
        self._scheduler = None
        logger.info("Scheduler stopped")

    def update_interval(self, minutes: int) -> None:
        self._current_interval_minutes = minutes
        if self._scheduler and self._scheduler.running:
            self._scheduler.reschedule_job(self._job_id, trigger=IntervalTrigger(minutes=minutes))
            logger.info("Collection interval updated to %d minutes", minutes)

    async def trigger_now(self) -> dict:
        return await self._run_collection()

    async def trigger_background(self) -> None:
        """Fire-and-forget collection run (legacy compat)."""
        if self._collector and self._collector.is_running:
            return
        if self._bg_task and not self._bg_task.done():
            return
        self._bg_task = asyncio.create_task(self._run_collection())

    async def trigger_search_now(self) -> dict:
        return await self._run_keyword_search()

    async def trigger_search_background(self) -> None:
        """Fire-and-forget notification search run (legacy compat)."""
        if self._search_bg_task and not self._search_bg_task.done():
            return
        self._search_bg_task = asyncio.create_task(self._run_keyword_search())

    async def _run_collection(self) -> dict:
        """Enqueue all channels for collection."""
        logger.info("Starting scheduled collection")
        if self._task_enqueuer:
            try:
                result = await self._task_enqueuer.enqueue_all_channels()
                stats = {
                    "channels": result.queued_count,
                    "skipped": result.skipped_existing_count,
                    "total": result.total_candidates,
                }
                self._last_run = datetime.now(timezone.utc)
                self._last_stats = stats
                logger.info("Scheduled collection enqueued: %s", stats)
                return stats
            except Exception:
                logger.exception("Collection enqueue failed")
                return {"channels": 0, "messages": 0, "errors": 1}
        # Legacy fallback
        if self._collector:
            try:
                stats = await self._collector.collect_all_channels()
                self._last_run = datetime.now(timezone.utc)
                self._last_stats = stats
                return stats
            except Exception:
                logger.exception("Collection failed")
                return {"channels": 0, "messages": 0, "errors": 1}
        return {"channels": 0, "messages": 0, "errors": 0}

    async def _run_keyword_search(self) -> dict:
        """Enqueue a notification search task."""
        if self._task_enqueuer:
            try:
                task_id = await self._task_enqueuer.enqueue_notification_search()
                if task_id:
                    logger.info("Enqueued notification search task #%d", task_id)
                return {"enqueued": bool(task_id)}
            except Exception:
                logger.exception("Notification search enqueue failed")
                return {"queries": 0, "results": 0, "errors": 1}
        # Legacy fallback
        if self._search_engine and self._scheduler_bundle:
            stats = await self._run_keyword_search_legacy()
            self._last_search_run = datetime.now(timezone.utc)
            self._last_search_stats = stats
            return stats
        return {"queries": 0, "results": 0, "errors": 0}

    async def _run_keyword_search_legacy(self) -> dict:
        queries = await self._scheduler_bundle.list_notification_queries(active_only=True)
        total_results = 0
        searched = 0
        errors = 0
        for sq in queries:
            try:
                quota = await self._search_engine.check_search_quota(sq.query)
                if quota and quota.get("remains") == 0 and not quota.get("query_is_free"):
                    break
                result = await self._search_engine.search_telegram(sq.query, limit=50)
                if result.error:
                    errors += 1
                else:
                    total_results += result.total
                    searched += 1
            except Exception:
                logger.exception("Error searching query '%s'", sq.query)
                errors += 1
        return {"queries": searched, "results": total_results, "errors": errors}

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

    async def _run_search_query(self, sq_id: int) -> None:
        """Enqueue SQ_STATS task for a search query."""
        if self._task_enqueuer:
            try:
                await self._task_enqueuer.enqueue_sq_stats(sq_id)
            except Exception:
                logger.exception("Error enqueuing SQ_STATS for sq_id=%d", sq_id)
            return
        # Legacy fallback
        if not self._sq_bundle:
            return
        sq = await self._sq_bundle.get_by_id(sq_id)
        if not sq:
            return
        try:
            from datetime import date

            today = date.today().isoformat()
            daily = await self._sq_bundle.get_fts_daily_stats_for_query(sq, days=1)
            today_count = 0
            for d in daily:
                if d.day == today:
                    today_count = d.count
                    break
            await self._sq_bundle.record_stat(sq_id, today_count)
        except Exception:
            logger.exception("Error running search query id=%d", sq_id)

    async def _run_photo_due(self) -> dict:
        if self._task_enqueuer:
            try:
                await self._task_enqueuer.enqueue_photo_due()
            except Exception:
                logger.exception("Error enqueuing photo_due")
            return {"enqueued": True}
        # Legacy fallback
        if self._photo_task_service:
            processed = await self._photo_task_service.run_due()
            return {"processed": processed}
        return {"processed": 0}

    async def _run_photo_auto(self) -> dict:
        if self._task_enqueuer:
            try:
                await self._task_enqueuer.enqueue_photo_auto()
            except Exception:
                logger.exception("Error enqueuing photo_auto")
            return {"enqueued": True}
        # Legacy fallback
        if self._photo_auto_upload_service:
            jobs = await self._photo_auto_upload_service.run_due()
            return {"jobs": jobs}
        return {"jobs": 0}
