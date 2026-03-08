from __future__ import annotations

import logging

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
        name: str,
        query: str,
        interval_minutes: int = 60,
        *,
        is_regex: bool = False,
        notify_on_collect: bool = False,
        track_stats: bool = True,
    ) -> int:
        sq = SearchQuery(
            name=name,
            query=query,
            interval_minutes=interval_minutes,
            is_regex=is_regex,
            notify_on_collect=notify_on_collect,
            track_stats=track_stats,
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

    async def delete(self, sq_id: int) -> None:
        await self._bundle.delete(sq_id)

    async def run_once(self, sq_id: int) -> int:
        sq = await self._bundle.get_by_id(sq_id)
        if not sq:
            return 0
        count = await self._bundle.count_fts_matches(sq.query)
        if sq.track_stats:
            await self._bundle.record_stat(sq_id, count)
        logger.info("Search query '%s' (id=%d): %d matches", sq.name, sq_id, count)
        return count

    async def get_daily_stats(
        self, sq_id: int, days: int = 30
    ) -> list[SearchQueryDailyStat]:
        return await self._bundle.get_daily_stats(sq_id, days)

    async def get_with_stats(
        self, days: int = 30
    ) -> list[dict]:
        queries = await self._bundle.get_all()
        all_stats = await self._bundle.get_stats_for_all(days)
        last_runs = await self._bundle.get_last_recorded_at_all()
        result = []
        for sq in queries:
            total = sum(s.count for s in all_stats.get(sq.id, []))
            result.append({
                "query": sq,
                "total_30d": total,
                "last_run": last_runs.get(sq.id),
                "daily_stats": all_stats.get(sq.id, []),
            })
        return result
