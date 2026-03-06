from __future__ import annotations

from typing import Literal

from src.collection_queue import CollectionQueue
from src.database import Database
from src.models import Channel
from src.telegram.collector import Collector

EnqueueResult = Literal["not_found", "filtered", "queued"]


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

    async def collect_channel_stats(self, channel: Channel) -> None:
        await self._collector.collect_channel_stats(channel)

    async def collect_all_stats(self) -> None:
        await self._collector.collect_all_stats()

    async def collect_single_channel_full(self, channel: Channel) -> int:
        return await self._collector.collect_single_channel(channel, full=True)
