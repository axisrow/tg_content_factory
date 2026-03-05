from __future__ import annotations

from src.database import Database
from src.models import SearchResult
from src.search.local_search import LocalSearch
from src.search.persistence import SearchPersistence
from src.search.telegram_search import TelegramSearch
from src.telegram.client_pool import ClientPool


class SearchEngine:
    """Facade for local and Telegram-based search strategies."""

    def __init__(self, db: Database, pool: ClientPool | None = None):
        self._local = LocalSearch(db)
        self._telegram = TelegramSearch(pool, SearchPersistence(db))

    async def search_local(
        self,
        query: str,
        channel_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> SearchResult:
        return await self._local.search(
            query=query,
            channel_id=channel_id,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            offset=offset,
        )

    async def check_search_quota(self, query: str = "") -> dict | None:
        return await self._telegram.check_search_quota(query)

    async def search_telegram(self, query: str, limit: int = 50) -> SearchResult:
        return await self._telegram.search_telegram(query, limit)

    async def search_my_chats(self, query: str, limit: int = 50) -> SearchResult:
        return await self._telegram.search_my_chats(query, limit)

    async def search_in_channel(
        self,
        channel_id: int | None,
        query: str,
        limit: int = 50,
    ) -> SearchResult:
        return await self._telegram.search_in_channel(channel_id, query, limit)
