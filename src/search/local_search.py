from __future__ import annotations

from src.database import Database
from src.models import SearchResult


class LocalSearch:
    def __init__(self, db: Database):
        self._db = db

    async def search(
        self,
        query: str,
        channel_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> SearchResult:
        messages, total = await self._db.search_messages(
            query=query,
            channel_id=channel_id,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            offset=offset,
        )
        return SearchResult(messages=messages, total=total, query=query)
