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
from src.telegram.collector import Collector

if TYPE_CHECKING:
    from src.search.engine import SearchEngine

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
        collector: Collector,
        config: SchedulerConfig,
        scheduler_bundle: SchedulerBundle | Database | None = None,
        search_engine: SearchEngine | None = None,
        search_query_bundle: SearchQueryBundle | None = None,
    ):
        self._collector = collector
        self._config = config
        if scheduler_bundle is None:
            scheduler_bundle = getattr(collector, "_db", None)
        if isinstance(scheduler_bundle, Database):
            scheduler_bundle = SchedulerBundle.from_database(scheduler_bundle)
        elif not isinstance(scheduler_bundle, SchedulerBundle):
            scheduler_bundle = _LegacySchedulerBundle(scheduler_bundle)
        self._scheduler_bundle = scheduler_bundle
        self._search_engine = search_engine
        self._sq_bundle = search_query_bundle
        self._scheduler: AsyncIOScheduler | None = None
        self._job_id = "collect_all"
        self._search_job_id = "notification_search"
        self._last_run: datetime | None = None
        self._last_stats: dict | None = None
        self._last_search_run: datetime | None = None
        self._last_search_stats: dict | None = None
        self._bg_task: asyncio.Task | None = None
        self._search_bg_task: asyncio.Task | None = None
        self._current_interval_minutes: int = config.collect_interval_minutes

    @property
    def is_running(self) -> bool:
        return self._scheduler is not None and self._scheduler.running

    @property
    def is_collecting(self) -> bool:
        return self._collector.is_running

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

    @property
    def interval_minutes(self) -> int:
        # Before start() is called, reflects config default (not yet loaded from DB).
        return self._current_interval_minutes

    @property
    def search_interval_minutes(self) -> int:
        return self._config.search_interval_minutes

    async def start(self) -> None:
        if self._scheduler is not None and self._scheduler.running:
            logger.warning("Scheduler already running")
            return

        self._scheduler = AsyncIOScheduler()
        saved_interval = await self._scheduler_bundle.get_setting("collect_interval_minutes")
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

        if self._search_engine:
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

        self._scheduler.start()
        logger.info(
            "Scheduler started: collecting every %d minutes",
            collect_interval,
        )

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
        """Reschedule the collection job with a new interval."""
        self._current_interval_minutes = minutes
        if self._scheduler and self._scheduler.running:
            self._scheduler.reschedule_job(self._job_id, trigger=IntervalTrigger(minutes=minutes))
            logger.info("Collection interval updated to %d minutes", minutes)
        else:
            logger.debug(
                "Scheduler not running; interval %d minutes will apply on next start", minutes
            )

    async def trigger_now(self) -> dict:
        """Trigger immediate collection run."""
        return await self._run_collection()

    async def trigger_background(self) -> None:
        """Fire-and-forget collection run."""
        if self._collector.is_running or (self._bg_task and not self._bg_task.done()):
            return
        self._bg_task = asyncio.create_task(self._run_collection())

    async def trigger_search_now(self) -> dict:
        """Trigger immediate notification query search."""
        return await self._run_keyword_search()

    async def trigger_search_background(self) -> None:
        """Fire-and-forget notification query search run."""
        if self._search_bg_task and not self._search_bg_task.done():
            return
        self._search_bg_task = asyncio.create_task(self._run_keyword_search())

    async def _run_collection(self) -> dict:
        logger.info("Starting scheduled collection")
        try:
            stats = await self._collector.collect_all_channels()
        except Exception:
            logger.exception("Collection failed with unhandled error")
            return {"channels": 0, "messages": 0, "errors": 1}
        self._last_run = datetime.now(timezone.utc)
        self._last_stats = stats
        return stats

    async def _run_keyword_search(self) -> dict:
        """Search Telegram API by notification queries (premium global search).

        This complements the local text/regex matching in Collector._check_notification_queries
        which runs at collection time. This method uses SearchEngine.search_telegram (Telegram's
        premium global search API) to discover messages across channels not yet collected.
        """
        if not self._search_engine:
            return {"queries": 0, "results": 0, "errors": 0}

        logger.info("Starting scheduled notification query search")
        queries = await self._scheduler_bundle.list_notification_queries(active_only=True)
        total_results = 0
        searched = 0
        errors = 0

        for sq in queries:
            query = sq.query
            try:
                quota = await self._search_engine.check_search_quota(query)
                if quota and quota.get("remains") == 0 and not quota.get("query_is_free"):
                    logger.info("Search quota exhausted, stopping notification search")
                    break

                result = await self._search_engine.search_telegram(query, limit=50)
                if result.error:
                    logger.warning("Search for '%s' returned error: %s", query, result.error)
                    errors += 1
                else:
                    total_results += result.total
                    searched += 1
                    logger.info(
                        "Query '%s': found %d messages", query, result.total
                    )
            except Exception:
                logger.exception("Error searching query '%s'", query)
                errors += 1

        stats = {"queries": searched, "results": total_results, "errors": errors}
        self._last_search_run = datetime.now(timezone.utc)
        self._last_search_stats = stats
        logger.info("Notification query search complete: %s", stats)
        return stats

    async def sync_search_query_jobs(self) -> None:
        if not self._sq_bundle or not self._scheduler:
            return

        all_active = await self._sq_bundle.get_all(active_only=True)
        active_queries = [sq for sq in all_active if sq.track_stats]
        active_ids = {f"sq_{sq.id}" for sq in active_queries}

        existing_jobs = self._scheduler.get_all_jobs()
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
        if not self._sq_bundle:
            return
        sq = await self._sq_bundle.get_by_id(sq_id)
        if not sq:
            logger.warning("Search query id=%d not found, skipping", sq_id)
            return
        try:
            from datetime import date as date_cls

            today = date_cls.today().isoformat()
            daily = await self._sq_bundle.get_fts_daily_stats_for_query(sq, days=1)
            today_count = 0
            for d in daily:
                if d.day == today:
                    today_count = d.count
                    break
            await self._sq_bundle.record_stat(sq_id, today_count)
            logger.info(
                "Search query '%s' (id=%d): %d matches today", sq.query, sq_id, today_count
            )
        except Exception:
            logger.exception("Error running search query id=%d", sq_id)
