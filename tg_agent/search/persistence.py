from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Iterable

from tg_agent.database.bundles import SearchBundle
from tg_agent.models import Channel, Message
from tg_agent.services.channel_onboarding import channel_with_meta

logger = logging.getLogger(__name__)


class SearchPersistence:
    def __init__(
        self,
        search: SearchBundle,
        fetch_channel_meta: Callable[[int, str | None], Awaitable[dict | None]] | None = None,
    ):
        self._search = search
        self._fetch_channel_meta = fetch_channel_meta

    async def cache_search_results(
        self,
        channels: dict[int, Channel],
        messages: list[Message],
        phone: str,
        query: str,
    ) -> list[Message]:
        await self._cache_channels(channels.values())

        if messages:
            await self._search.insert_messages_batch(messages, premium_search_query=query)

        await self._search.log_search(phone, query, len(messages))
        return await self._load_persisted_messages(messages)

    async def cache_messages_and_channels(
        self,
        channels: dict[int, Channel],
        messages: list[Message],
    ) -> list[Message]:
        await self._cache_channels(channels.values())
        if messages:
            await self._search.insert_messages_batch(messages)
        return await self._load_persisted_messages(messages)

    async def _cache_channels(self, channels: Iterable[Channel]) -> None:
        for ch in channels:
            existing = await self._search.channels.get_channel_by_channel_id(ch.channel_id)
            channel = ch
            if existing is None:
                if ch.is_active:
                    channel = channel_with_meta(ch, await self._fetch_meta(ch))
                # Search discovers channels, but is not an operator approval
                # to collect them (#1313): new discoveries land in the approval
                # queue and stay invisible to the standard readers until the
                # owner decides (replaces the temporary is_active=0 from #1308).
                channel = channel.model_copy(update={"approval_state": "pending"})
            await self._search.add_channel(channel)

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
