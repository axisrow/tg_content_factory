from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from src.database import Database
from src.database.bundles import SearchQueryBundle
from src.models import SearchQuery, SearchQueryDailyStat
from src.utils.search_query_chat_filter import (
    ChatFilterValidation,
    single_resolved_channel_id,
    validate_chat_filter,
)

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
        chat_filter: str = "",
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
            chat_filter=chat_filter,
        )
        return await self._bundle.add(sq)

    async def list_queries(self, active_only: bool = False) -> list[SearchQuery]:
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
        chat_filter: str | None = None,
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
            chat_filter=chat_filter if chat_filter is not None else existing.chat_filter,
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
        today = datetime.now(timezone.utc).date().isoformat()
        count = next((stat.count for stat in daily if stat.day == today), 0)
        if sq.track_stats:
            await self._bundle.record_stat(sq_id, count)
        logger.info("Search query '%s' (id=%d): %d matches today", sq.query, sq_id, count)
        return count

    async def get_daily_stats(self, sq_id: int, days: int = 30) -> list[SearchQueryDailyStat]:
        return await self._bundle.get_daily_stats(sq_id, days)

    async def validate_chat_filter(self, chat_filter: str) -> ChatFilterValidation:
        channels = await self._get_channels()
        return validate_chat_filter(chat_filter, channels)

    async def get_with_stats(self, days: int = 30) -> list[dict]:
        queries = await self._bundle.get_all()
        last_runs = await self._bundle.get_last_recorded_at_all()
        channels = await self._get_channels()
        # Regex queries can't be counted via FTS5; exclude them from FTS stats batch
        tracked = [sq for sq in queries if sq.track_stats and not sq.is_regex]
        tracked_ids = {sq.id for sq in tracked if sq.id is not None}
        # Only tracked non-regex queries have stats; others get empty daily_stats/total_30d=0
        stats_map = await self._bundle.get_fts_daily_stats_batch(tracked, days)
        result = []
        for sq in queries:
            sq_id = sq.id
            raw = stats_map.get(sq_id, []) if sq_id is not None and sq_id in tracked_ids else None
            daily = self._fill_missing_days(raw, days)
            total = sum(s.count for s in daily)
            chat_validation = validate_chat_filter(sq.chat_filter, channels)
            result.append(
                {
                    "query": sq,
                    "total_30d": total,
                    "last_run": last_runs.get(sq_id) if sq_id is not None else None,
                    "daily_stats": daily,
                    "chat_filter_warnings": chat_validation.warning_text(),
                    "chat_filter_channel_id": single_resolved_channel_id(sq.chat_filter, channels),
                }
            )
        return result

    @staticmethod
    def _fill_missing_days(
        stats: list[SearchQueryDailyStat] | None, days: int
    ) -> list[SearchQueryDailyStat]:
        if stats is None:
            return []
        today = datetime.now(timezone.utc).date()
        if not stats:
            return [
                SearchQueryDailyStat(day=(today - timedelta(days=i)).isoformat(), count=0)
                for i in range(days, -1, -1)
            ]
        existing = {stat.day: stat for stat in stats}
        filled = []
        for i in range(days, 0, -1):
            day_str = (today - timedelta(days=i)).isoformat()
            filled.append(existing.get(day_str, SearchQueryDailyStat(day=day_str, count=0)))
        # include today
        day_str = today.isoformat()
        filled.append(existing.get(day_str, SearchQueryDailyStat(day=day_str, count=0)))
        return filled

    async def _get_channels(self):
        get_channels = getattr(self._bundle, "get_channels", None)
        if get_channels is None:
            return []
        return await get_channels()
