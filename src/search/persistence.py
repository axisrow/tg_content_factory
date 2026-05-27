from __future__ import annotations

import logging

from src.database.bundles import SearchBundle
from src.models import Channel, Message

logger = logging.getLogger(__name__)


class SearchPersistence:
    def __init__(self, search: SearchBundle):
        self._search = search

    async def cache_search_results(
        self,
        channels: dict[int, Channel],
        messages: list[Message],
        phone: str,
        query: str,
    ) -> list[Message]:
        for ch in channels.values():
            await self._search.add_channel(ch)

        if messages:
            await self._search.insert_messages_batch(messages)

        await self._search.log_search(phone, query, len(messages))
        return await self._load_persisted_messages(messages)

    async def cache_messages_and_channels(
        self,
        channels: dict[int, Channel],
        messages: list[Message],
    ) -> list[Message]:
        for ch in channels.values():
            await self._search.add_channel(ch)
        if messages:
            await self._search.insert_messages_batch(messages)
        return await self._load_persisted_messages(messages)

    async def _load_persisted_messages(self, messages: list[Message]) -> list[Message]:
        if not messages:
            return []
        keys = [(msg.channel_id, msg.message_id) for msg in messages]
        try:
            persisted = await self._search.messages.get_messages_by_channel_message_ids(keys)
        except Exception:
            logger.exception("Failed to load persisted messages; returning originals")
            return messages
        by_key = {(msg.channel_id, msg.message_id): msg for msg in persisted}
        if len(by_key) < len(keys):
            logger.warning(
                "Partial persistence load: %d of %d messages found; falling back to originals for the rest",
                len(by_key),
                len(keys),
            )
        return [by_key.get((msg.channel_id, msg.message_id), msg) for msg in messages]
