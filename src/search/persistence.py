from __future__ import annotations

from src.database.bundles import SearchBundle
from src.models import Channel, Message


class SearchPersistence:
    def __init__(self, search: SearchBundle):
        self._search = search

    async def cache_search_results(
        self,
        channels: dict[int, Channel],
        messages: list[Message],
        phone: str,
        query: str,
    ) -> None:
        for ch in channels.values():
            await self._search.add_channel(ch)

        if messages:
            await self._search.insert_messages_batch(messages)

        await self._search.log_search(phone, query, len(messages))

    async def cache_messages_and_channels(
        self,
        channels: dict[int, Channel],
        messages: list[Message],
    ) -> None:
        for ch in channels.values():
            await self._search.add_channel(ch)
        if messages:
            await self._search.insert_messages_batch(messages)
