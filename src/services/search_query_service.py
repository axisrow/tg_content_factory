from __future__ import annotations

import logging
from datetime import date as date_cls

from src.database import Database
from src.database.bundles import SearchQueryBundle
from src.models import SearchQuery, SearchQueryDailyStat

logger = logging.getLogger(__name__)


class SearchQueryService:
    def __init__(self, bundle: SearchQueryBundle | Database):
        if isinstance(bundle, Database):
            bundle = SearchQueryBundle.from_database(bundle)
        self._bundle = bundle

    async def add(
        self,
        query: str,
        interval_minutes: int = 60,
        *,
        is_regex: bool = False,
        is_fts: bool = False,
        notify_on_collect: bool = False,
        track_stats: bool = True,
        exclude_patterns: str = "",
        max_length: int | None = None,
    ) -> int:
        sq = SearchQuery(
            query=query,
            interval_minutes=interval_minutes,
            is_regex=is_regex,
            is_fts=is_fts,
            notify_on_collect=notify_on_collect,
            track_stats=track_stats,
            exclude_patterns=exclude_patterns,
            max_length=max_length,
        )
        return await self._bundle.add(sq)

    async def list(self, active_only: bool = False) -> list[SearchQuery]:
        return await self._bundle.get_all(active_only)

    async def get(self, sq_id: int) -> SearchQuery | None:
        return await self._bundle.get_by_id(sq_id)

    async def toggle(self, sq_id: int) -> None:
        sq = await self._bundle.get_by_id(sq_id)
        if sq:
            await self._bundle.set_active(sq_id, not sq.is_active)

    async def update(
        self,
        sq_id: int,
        query: str,
        interval_minutes: int,
        *,
        is_regex: bool = False,
        is_fts: bool = False,
        notify_on_collect: bool = False,
        track_stats: bool = True,
        exclude_patterns: str = "",
        max_length: int | None = None,
    ) -> bool:
        existing = await self._bundle.get_by_id(sq_id)
        if not existing:
            return False
        sq = SearchQuery(
            query=query,
            interval_minutes=interval_minutes,
            is_regex=is_regex,
            is_fts=is_fts,
            is_active=existing.is_active,
            notify_on_collect=notify_on_collect,
            track_stats=track_stats,
            exclude_patterns=exclude_patterns,
            max_length=max_length,
        )
        await self._bundle.update(sq_id, sq)
        return True

    async def delete(self, sq_id: int) -> None:
        await self._bundle.delete(sq_id)

    async def run_once(self, sq_id: int) -> int:
        sq = await self._bundle.get_by_id(sq_id)
        if not sq:
            return 0
        if sq.is_regex:
            logger.info("Search query '%s' (id=%d): regex not counted via FTS", sq.query, sq_id)
            return 0
        daily = await self._bundle.get_fts_daily_stats_for_query(sq, days=1)
        today = date_cls.today().isoformat()
        count = next((d.count for d in daily if d.day == today), 0)
        if sq.track_stats:
            await self._bundle.record_stat(sq_id, count)
        logger.info("Search query '%s' (id=%d): %d matches today", sq.query, sq_id, count)
        return count

    async def get_daily_stats(
        self, sq_id: int, days: int = 30
    ) -> list[SearchQueryDailyStat]:
        return await self._bundle.get_daily_stats(sq_id, days)

    async def get_with_stats(
        self, days: int = 30
    ) -> list[dict]:
        queries = await self._bundle.get_all()
        last_runs = await self._bundle.get_last_recorded_at_all()
        # Regex queries can't be counted via FTS5; exclude them from FTS stats batch
        tracked = [sq for sq in queries if sq.track_stats and not sq.is_regex]
        # Only tracked non-regex queries have stats; others get empty daily_stats/total_30d=0
        stats_map = await self._bundle.get_fts_daily_stats_batch(tracked, days)
        result = []
        for sq in queries:
            daily = self._fill_missing_days(stats_map.get(sq.id, []), days)
            total = sum(s.count for s in daily)
            result.append({
                "query": sq,
                "total_30d": total,
                "last_run": last_runs.get(sq.id),
                "daily_stats": daily,
            })
        return result

    @staticmethod
    def _fill_missing_days(
        stats: list[SearchQueryDailyStat], days: int
    ) -> list[SearchQueryDailyStat]:
        if not stats:
            return []
        from datetime import date, timedelta

        today = date.today()
        existing = {s.day: s for s in stats}
        filled = []
        for i in range(days, 0, -1):
            d = (today - timedelta(days=i)).isoformat()
            filled.append(existing.get(d, SearchQueryDailyStat(day=d, count=0)))
        # include today
        d = today.isoformat()
        filled.append(existing.get(d, SearchQueryDailyStat(day=d, count=0)))
        return filled
