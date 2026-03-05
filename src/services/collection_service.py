from __future__ import annotations

from src.collection_queue import CollectionQueue
from src.database import Database
from src.models import Channel
from src.telegram.collector import Collector


class CollectionService:
    def __init__(self, db: Database, collector: Collector, queue: CollectionQueue):
        self._db = db
        self._collector = collector
        self._queue = queue

    async def enqueue_channel_by_pk(self, pk: int) -> bool:
        channels = await self._db.get_channels()
        channel = next((ch for ch in channels if ch.id == pk), None)
        if not channel:
            return False
        await self._queue.enqueue(channel)
        return True

    async def collect_channel_stats(self, channel: Channel) -> None:
        await self._collector.collect_channel_stats(channel)

    async def collect_all_stats(self) -> None:
        await self._collector.collect_all_stats()

    async def collect_single_channel_full(self, channel: Channel) -> int:
        return await self._collector.collect_single_channel(channel, full=True)
