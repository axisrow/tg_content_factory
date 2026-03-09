from __future__ import annotations

from src.database.bundles import SearchBundle
from src.models import SearchResult


class LocalSearch:
    def __init__(self, search: SearchBundle):
        self._search = search

    async def search(
        self,
        query: str,
        channel_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 50,
        offset: int = 0,
        is_fts: bool = False,
        min_length: int | None = None,
        max_length: int | None = None,
    ) -> SearchResult:
        messages, total = await self._search.search_messages(
            query=query,
            channel_id=channel_id,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            offset=offset,
            is_fts=is_fts,
            min_length=min_length,
            max_length=max_length,
        )
        return SearchResult(messages=messages, total=total, query=query)
