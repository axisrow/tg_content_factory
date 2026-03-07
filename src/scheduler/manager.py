from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from src.config import SchedulerConfig
from src.telegram.collector import Collector

if TYPE_CHECKING:
    from src.database import Database
    from src.search.engine import SearchEngine

logger = logging.getLogger(__name__)


class SchedulerManager:
    def __init__(
        self,
        collector: Collector,
        config: SchedulerConfig,
        search_engine: SearchEngine | None = None,
        db: Database | None = None,
    ):
        self._collector = collector
        self._config = config
        self._search_engine = search_engine
        self._db = db
        self._scheduler: AsyncIOScheduler | None = None
        self._job_id = "collect_all"
        self._search_job_id = "keyword_search"
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
        saved_interval = (
            await self._db.get_setting("collect_interval_minutes") if self._db else None
        )
        collect_interval = (
            int(saved_interval) if saved_interval else self._config.collect_interval_minutes
        )
        self._current_interval_minutes = collect_interval
        self._scheduler.add_job(
            self._run_collection,
            IntervalTrigger(minutes=collect_interval),
            id=self._job_id,
            replace_existing=True,
        )

        if self._search_engine and self._db:
            self._scheduler.add_job(
                self._run_keyword_search,
                IntervalTrigger(minutes=self._config.search_interval_minutes),
                id=self._search_job_id,
                replace_existing=True,
            )
            logger.info(
                "Keyword search job added: every %d minutes",
                self._config.search_interval_minutes,
            )

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
        if self._collector.is_running:
            return
        self._bg_task = asyncio.create_task(self._run_collection())

    async def trigger_search_now(self) -> dict:
        """Trigger immediate keyword search."""
        return await self._run_keyword_search()

    async def trigger_search_background(self) -> None:
        """Fire-and-forget keyword search run."""
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
        """Search by active keywords using search_telegram, respecting quotas."""
        if not self._search_engine or not self._db:
            return {"keywords": 0, "results": 0, "errors": 0}

        logger.info("Starting scheduled keyword search")
        keywords = await self._db.get_keywords(active_only=True)
        total_results = 0
        searched = 0
        errors = 0

        for kw in keywords:
            pattern = kw.pattern
            try:
                quota = await self._search_engine.check_search_quota(pattern)
                if quota and quota.get("remains") == 0 and not quota.get("query_is_free"):
                    logger.info("Search quota exhausted, stopping keyword search")
                    break

                result = await self._search_engine.search_telegram(pattern, limit=50)
                if result.error:
                    logger.warning("Search for '%s' returned error: %s", pattern, result.error)
                    errors += 1
                else:
                    total_results += result.total
                    searched += 1
                    logger.info(
                        "Keyword '%s': found %d messages", pattern, result.total
                    )
            except Exception:
                logger.exception("Error searching keyword '%s'", pattern)
                errors += 1

        stats = {"keywords": searched, "results": total_results, "errors": errors}
        self._last_search_run = datetime.now(timezone.utc)
        self._last_search_stats = stats
        logger.info("Keyword search complete: %s", stats)
        return stats
