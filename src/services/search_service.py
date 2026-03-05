from __future__ import annotations

from src.models import SearchResult
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine


class SearchService:
    def __init__(self, engine: SearchEngine, ai_search: AISearchEngine | None = None):
        self._engine = engine
        self._ai_search = ai_search

    async def search(
        self,
        mode: str,
        query: str,
        limit: int,
        channel_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        offset: int = 0,
    ) -> SearchResult:
        if mode == "ai" and self._ai_search:
            return await self._ai_search.search(query)
        if mode == "telegram":
            return await self._engine.search_telegram(query, limit=limit)
        if mode == "my_chats":
            return await self._engine.search_my_chats(query, limit=limit)
        if mode == "channel":
            return await self._engine.search_in_channel(channel_id, query, limit=limit)
        return await self._engine.search_local(
            query=query,
            channel_id=channel_id,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            offset=offset,
        )

    async def check_quota(self, query: str = "") -> dict | None:
        return await self._engine.check_search_quota(query)
