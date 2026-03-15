from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from src.database import Database
from src.database.bundles import ChannelBundle
from src.models import CollectionTaskType, SqStatsTaskPayload

if TYPE_CHECKING:
    from src.services.collection_service import CollectionService

logger = logging.getLogger(__name__)


class TaskEnqueuer:
    """Thin service for creating tasks in DB with deduplication."""

    def __init__(
        self,
        db: Database,
        channels: ChannelBundle,
        collection_service: CollectionService,
    ):
        self._db = db
        self._channels = channels
        self._collection_service = collection_service

    async def enqueue_all_channels(self):
        """Delegate to CollectionService for channel collection tasks."""
        return await self._collection_service.enqueue_all_channels()

    async def enqueue_notification_search(self) -> int | None:
        """Create a NOTIFICATION_SEARCH task if none is already pending/running."""
        has = await self._db.repos.tasks.has_active_task(
            CollectionTaskType.NOTIFICATION_SEARCH
        )
        if has:
            logger.debug("NOTIFICATION_SEARCH task already active, skipping")
            return None
        task_id = await self._db.repos.tasks.create_generic_task(
            CollectionTaskType.NOTIFICATION_SEARCH,
            title="Поиск по запросам",
        )
        logger.info("Enqueued NOTIFICATION_SEARCH task #%d", task_id)
        return task_id

    async def enqueue_sq_stats(self, sq_id: int) -> int | None:
        """Create a SQ_STATS task for a specific search query, with dedup."""
        has = await self._db.repos.tasks.has_active_task(
            CollectionTaskType.SQ_STATS,
            payload_filter_key="sq_id",
            payload_filter_value=sq_id,
        )
        if has:
            logger.debug("SQ_STATS task for sq_id=%d already active, skipping", sq_id)
            return None
        task_id = await self._db.repos.tasks.create_generic_task(
            CollectionTaskType.SQ_STATS,
            title=f"Статистика запроса #{sq_id}",
            payload=SqStatsTaskPayload(sq_id=sq_id),
        )
        logger.info("Enqueued SQ_STATS task #%d for sq_id=%d", task_id, sq_id)
        return task_id

    async def enqueue_photo_due(self) -> int | None:
        """Create a PHOTO_DUE task if none is already pending/running."""
        has = await self._db.repos.tasks.has_active_task(CollectionTaskType.PHOTO_DUE)
        if has:
            return None
        task_id = await self._db.repos.tasks.create_generic_task(
            CollectionTaskType.PHOTO_DUE,
            title="Отправка фото",
        )
        return task_id

    async def enqueue_photo_auto(self) -> int | None:
        """Create a PHOTO_AUTO task if none is already pending/running."""
        has = await self._db.repos.tasks.has_active_task(CollectionTaskType.PHOTO_AUTO)
        if has:
            return None
        task_id = await self._db.repos.tasks.create_generic_task(
            CollectionTaskType.PHOTO_AUTO,
            title="Автозагрузка фото",
        )
        return task_id
