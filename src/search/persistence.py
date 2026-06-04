from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Iterable

from src.database.bundles import SearchBundle
from src.models import Channel, Message, StatsAllTaskPayload
from src.services.channel_onboarding import channel_with_meta, enqueue_stats_for_new_channels

logger = logging.getLogger(__name__)


class SearchPersistence:
    def __init__(
        self,
        search: SearchBundle,
        create_stats_task: Callable[[StatsAllTaskPayload], Awaitable[int]] | None = None,
        fetch_channel_meta: Callable[[int, str | None], Awaitable[dict | None]] | None = None,
    ):
        self._search = search
        self._create_stats_task = create_stats_task
        self._fetch_channel_meta = fetch_channel_meta

    async def cache_search_results(
        self,
        channels: dict[int, Channel],
        messages: list[Message],
        phone: str,
        query: str,
    ) -> list[Message]:
        new_channel_ids = await self._cache_channels(channels.values())
        await enqueue_stats_for_new_channels(
            self._create_stats_task,
            new_channel_ids,
            context="telegram search cache",
        )

        if messages:
            await self._search.insert_messages_batch(messages, premium_search_query=query)

        await self._search.log_search(phone, query, len(messages))
        return await self._load_persisted_messages(messages)

    async def cache_messages_and_channels(
        self,
        channels: dict[int, Channel],
        messages: list[Message],
    ) -> list[Message]:
        new_channel_ids = await self._cache_channels(channels.values())
        await enqueue_stats_for_new_channels(
            self._create_stats_task,
            new_channel_ids,
            context="telegram chat search cache",
        )
        if messages:
            await self._search.insert_messages_batch(messages)
        return await self._load_persisted_messages(messages)

    async def _cache_channels(self, channels: Iterable[Channel]) -> list[int]:
        new_channel_ids: list[int] = []
        for ch in channels:
            existing = await self._search.channels.get_channel_by_channel_id(ch.channel_id)
            channel = ch
            if existing is None and ch.is_active:
                channel = channel_with_meta(ch, await self._fetch_meta(ch))
            await self._search.add_channel(channel)
            if existing is None and channel.is_active:
                new_channel_ids.append(channel.channel_id)
        return new_channel_ids

    async def _fetch_meta(self, channel: Channel) -> dict | None:
        if self._fetch_channel_meta is None:
            return None
        try:
            return await self._fetch_channel_meta(channel.channel_id, channel.channel_type)
        except Exception as exc:
            logger.warning("Failed to fetch search channel metadata for %s: %s", channel.channel_id, exc)
            return None

    async def _load_persisted_messages(self, messages: list[Message]) -> list[Message]:
        if not messages:
            return []
        keys = [(msg.channel_id, msg.message_id) for msg in messages]
        unique_keys = set(keys)
        try:
            persisted = await self._search.messages.get_messages_by_channel_message_ids(keys)
        except Exception:
            logger.exception("Failed to load persisted messages; returning originals")
            return messages
        by_key = {(msg.channel_id, msg.message_id): msg for msg in persisted}
        if len(by_key) < len(unique_keys):
            logger.warning(
                "Partial persistence load: %d of %d messages found; falling back to originals for the rest",
                len(by_key),
                len(unique_keys),
            )
        return [by_key.get((msg.channel_id, msg.message_id), msg) for msg in messages]
