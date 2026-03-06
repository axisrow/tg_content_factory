from __future__ import annotations

import asyncio
import logging

from src.database import Database
from src.models import Channel
from src.telegram.collector import Collector

logger = logging.getLogger(__name__)


class CollectionQueue:
    def __init__(self, collector: Collector, db: Database):
        self._collector = collector
        self._db = db
        self._queue: asyncio.Queue[tuple[int, Channel, bool]] = asyncio.Queue()
        self._worker: asyncio.Task | None = None
        self._current_task_id: int | None = None

    async def enqueue(self, channel: Channel, force: bool = False) -> int:
        task_id = await self._db.create_collection_task(channel.channel_id, channel.title)
        await self._queue.put((task_id, channel, force))
        self._ensure_worker()
        return task_id

    async def cancel_task(self, task_id: int) -> bool:
        if task_id == self._current_task_id:
            await self._collector.cancel()
            return await self._db.cancel_collection_task(task_id)
        return await self._db.cancel_collection_task(task_id)

    def _ensure_worker(self) -> None:
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(self._run_worker())

    async def _run_worker(self) -> None:
        while True:
            try:
                task_id, channel, force = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                if self._queue.empty():
                    break
                continue
            except asyncio.CancelledError:
                break

            # Check if task was cancelled while waiting in queue
            task = await self._db.get_collection_task(task_id)
            if task and task.status == "cancelled":
                self._queue.task_done()
                continue

            # Channel may become filtered after being queued.
            fresh_channel = None
            if channel.id is not None:
                fresh_channel = await self._db.get_channel_by_pk(channel.id)
            if fresh_channel is not None:
                channel = fresh_channel
            if channel.is_filtered and not force:
                await self._db.cancel_collection_task(task_id)
                logger.info(
                    "Task %d skipped: channel %d is filtered",
                    task_id,
                    channel.channel_id,
                )
                self._queue.task_done()
                continue

            self._current_task_id = task_id
            try:
                await self._db.update_collection_task(task_id, "running")

                async def _progress(count: int) -> None:
                    await self._db.update_collection_task_progress(task_id, count)

                count = await self._collector.collect_single_channel(
                    channel, full=True, progress_callback=_progress, force=force
                )
                if self._collector.is_cancelled:
                    await self._db.cancel_collection_task(task_id)
                    logger.info("Task %d cancelled during collection", task_id)
                else:
                    await self._db.update_collection_task(
                        task_id, "completed", messages_collected=count
                    )
                    logger.info(
                        "Collected %d messages from channel %d", count, channel.channel_id
                    )
            except Exception as exc:
                await self._db.update_collection_task(
                    task_id, "failed", error=str(exc)[:500]
                )
                logger.exception(
                    "Collection failed for channel %d", channel.channel_id
                )
            finally:
                self._current_task_id = None
                self._queue.task_done()

    async def shutdown(self) -> None:
        if self._worker and not self._worker.done():
            self._worker.cancel()
            try:
                await self._worker
            except asyncio.CancelledError:
                pass
