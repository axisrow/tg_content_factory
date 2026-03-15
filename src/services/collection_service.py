from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from src.database import Database
from src.database.bundles import ChannelBundle
from src.models import Channel

if TYPE_CHECKING:
    from src.collection_queue import CollectionQueue
    from src.telegram.collector import Collector

EnqueueResult = Literal["not_found", "filtered", "queued"]


@dataclass(slots=True)
class BulkEnqueueResult:
    queued_count: int
    skipped_existing_count: int
    total_candidates: int


class CollectionService:
    def __init__(
        self,
        channels: ChannelBundle | Database,
        collector: Collector,
        collection_queue: CollectionQueue | None = None,
    ):
        if isinstance(channels, Database):
            channels = ChannelBundle.from_database(channels)
        self._channels = channels
        self._collector = collector
        self._queue = collection_queue

    async def _enqueue_channel(
        self, channel: Channel, force: bool = False, full: bool = True
    ) -> None:
        if self._queue is not None:
            await self._queue.enqueue(channel, force=force, full=full)
        else:
            payload = {}
            if force:
                payload["force"] = True
            if not full:
                payload["full"] = False
            await self._channels.create_collection_task(
                channel.channel_id, channel.title,
                channel_username=channel.username,
                payload=payload or None,
            )

    async def enqueue_channel_by_pk(self, pk: int, force: bool = False) -> EnqueueResult:
        channel = await self._channels.get_by_pk(pk)
        if not channel:
            return "not_found"
        if channel.is_filtered and not force:
            return "filtered"
        await self._enqueue_channel(channel, force=force)
        return "queued"

    async def enqueue_all_channels(self) -> BulkEnqueueResult:
        channels = await self._channels.list_channels(active_only=True, include_filtered=False)
        busy_channel_ids = await self._channels.get_channel_ids_with_active_tasks()
        queued_count = 0
        skipped_existing_count = 0

        for channel in channels:
            if channel.channel_id in busy_channel_ids:
                skipped_existing_count += 1
                continue
            # Bulk collection should continue from last_collected_id when the
            # channel already has history, instead of reloading the full archive.
            await self._enqueue_channel(channel, force=True, full=False)
            queued_count += 1

        return BulkEnqueueResult(
            queued_count=queued_count,
            skipped_existing_count=skipped_existing_count,
            total_candidates=len(channels),
        )

    async def collect_channel_stats(self, channel: Channel) -> None:
        await self._collector.collect_channel_stats(channel)

    async def collect_all_stats(self) -> None:
        await self._collector.collect_all_stats()

    async def collect_single_channel_full(self, channel: Channel) -> int:
        return await self._collector.collect_single_channel(channel, full=True)
