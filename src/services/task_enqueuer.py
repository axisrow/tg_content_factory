from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from src.database import Database
from src.models import (
    CollectionTaskType,
    ContentGenerateTaskPayload,
    ContentPublishTaskPayload,
    PipelineRunTaskPayload,
    SqStatsTaskPayload,
)

if TYPE_CHECKING:
    from src.services.collection_service import CollectionService

logger = logging.getLogger(__name__)


class TaskEnqueuer:
    """Thin service for creating tasks in DB with deduplication."""

    def __init__(
        self,
        db: Database,
        collection_service: CollectionService,
    ):
        self._db = db
        self._collection_service = collection_service

    async def enqueue_all_channels(self):
        """Delegate to CollectionService for channel collection tasks."""
        return await self._collection_service.enqueue_all_channels()

    async def enqueue_sq_stats(self, sq_id: int) -> int | None:
        """Create a SQ_STATS task for a specific search query, with dedup."""
        has = await self._db.repos.tasks.has_active_task(
            CollectionTaskType.SQ_STATS,
            payload_filter_key="sq_id",
            payload_filter_value=sq_id,
        )
        if has:
            logger.info("SQ_STATS task for sq_id=%d already active, skipping", sq_id)
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

    async def enqueue_pipeline_run(self, pipeline_id: int) -> int | None:
        """Create a PIPELINE_RUN task for a specific pipeline with deduplication."""
        has = await self._db.repos.tasks.has_active_task(
            CollectionTaskType.PIPELINE_RUN,
            payload_filter_key="pipeline_id",
            payload_filter_value=pipeline_id,
        )
        if has:
            logger.info("PIPELINE_RUN for pipeline_id=%d already active, skipping", pipeline_id)
            return None
        task_id = await self._db.repos.tasks.create_generic_task(
            CollectionTaskType.PIPELINE_RUN,
            title=f"Pipeline run #{pipeline_id}",
            payload=PipelineRunTaskPayload(pipeline_id=pipeline_id),
        )
        logger.info("Enqueued PIPELINE_RUN task #%d for pipeline_id=%d", task_id, pipeline_id)
        return task_id

    async def enqueue_content_generate(self, pipeline_id: int) -> int | None:
        """Create a CONTENT_GENERATE task for a specific pipeline with deduplication."""
        has = await self._db.repos.tasks.has_active_task(
            CollectionTaskType.CONTENT_GENERATE,
            payload_filter_key="pipeline_id",
            payload_filter_value=pipeline_id,
        )
        if has:
            logger.info("CONTENT_GENERATE for pipeline_id=%d already active, skipping", pipeline_id)
            return None
        task_id = await self._db.repos.tasks.create_generic_task(
            CollectionTaskType.CONTENT_GENERATE,
            title=f"Content generate #{pipeline_id}",
            payload=ContentGenerateTaskPayload(pipeline_id=pipeline_id),
        )
        logger.info("Enqueued CONTENT_GENERATE task #%d for pipeline_id=%d", task_id, pipeline_id)
        return task_id

    async def enqueue_content_publish(self, pipeline_id: int | None = None) -> int | None:
        """Create a CONTENT_PUBLISH task for publishing approved drafts."""
        has = await self._db.repos.tasks.has_active_task(CollectionTaskType.CONTENT_PUBLISH)
        if has:
            logger.info("CONTENT_PUBLISH task already active, skipping")
            return None
        task_id = await self._db.repos.tasks.create_generic_task(
            CollectionTaskType.CONTENT_PUBLISH,
            title="Content publish",
            payload=ContentPublishTaskPayload(pipeline_id=pipeline_id),
        )
        logger.info("Enqueued CONTENT_PUBLISH task #%d", task_id)
        return task_id
