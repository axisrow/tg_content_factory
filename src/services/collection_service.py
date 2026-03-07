from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from src.database import Database
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
    def __init__(self, db: Database, collector: Collector, queue: CollectionQueue):
        self._db = db
        self._collector = collector
        self._queue = queue

    async def enqueue_channel_by_pk(self, pk: int, force: bool = False) -> EnqueueResult:
        channel = await self._db.get_channel_by_pk(pk)
        if not channel:
            return "not_found"
        if channel.is_filtered and not force:
            return "filtered"
        await self._queue.enqueue(channel, force=force)
        return "queued"

    async def enqueue_all_channels(self) -> BulkEnqueueResult:
        channels = await self._db.get_channels(active_only=True, include_filtered=False)
        busy_channel_ids = await self._db.get_channel_ids_with_active_tasks()
        queued_count = 0
        skipped_existing_count = 0

        for channel in channels:
            if channel.channel_id in busy_channel_ids:
                skipped_existing_count += 1
                continue
            await self._queue.enqueue(channel, force=True)
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
