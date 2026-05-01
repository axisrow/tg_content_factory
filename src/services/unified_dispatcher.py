from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Awaitable, Callable, TypeVar

from src.database.bundles import ChannelBundle, PipelineBundle, SearchQueryBundle
from src.database.repositories.collection_tasks import CollectionTasksRepository
from src.models import CollectionTask, CollectionTaskStatus, CollectionTaskType
from src.services.task_handlers import (
    ContentTaskHandler,
    PhotoTaskHandler,
    PipelineTaskHandler,
    StatsTaskHandler,
    TaskHandler,
    TaskHandlerContext,
    TranslationTaskHandler,
)
from src.services.task_handlers.base import build_image_service
from src.telegram.collector import Collector

if TYPE_CHECKING:
    from src.database import Database
    from src.search.engine import SearchEngine
    from src.services.image_generation_service import ImageGenerationService
    from src.services.photo_auto_upload_service import PhotoAutoUploadService
    from src.services.photo_task_service import PhotoTaskService
    from src.telegram.notifier import Notifier

logger = logging.getLogger(__name__)
TTaskHandler = TypeVar("TTaskHandler", bound=TaskHandler)

_HANDLER_CLASSES = (
    StatsTaskHandler,
    PhotoTaskHandler,
    PipelineTaskHandler,
    ContentTaskHandler,
    TranslationTaskHandler,
)

HANDLED_TYPES = [
    task_type.value
    for handler_cls in _HANDLER_CLASSES
    for task_type in handler_cls.task_types
]


class UnifiedDispatcher:
    """Polls DB for non-CHANNEL_COLLECT tasks and dispatches them to task handlers."""

    def __init__(
        self,
        collector: Collector,
        channel_bundle: ChannelBundle,
        tasks_repo: CollectionTasksRepository,
        *,
        sq_bundle: SearchQueryBundle | None = None,
        photo_task_service: PhotoTaskService | None = None,
        photo_auto_upload_service: PhotoAutoUploadService | None = None,
        poll_interval_sec: float = 5.0,
        channel_timeout_sec: float = 120.0,
        search_engine: "SearchEngine" | None = None,
        pipeline_bundle: PipelineBundle | None = None,
        db: "Database" | None = None,
        client_pool: object | None = None,
        notifier: "Notifier | None" = None,
        config: object | None = None,
        llm_provider_service: object | None = None,
    ):
        self._collector = collector
        self._channel_bundle = channel_bundle
        self._tasks = tasks_repo
        self._sq_bundle = sq_bundle
        self._photo_task_service = photo_task_service
        self._photo_auto_upload_service = photo_auto_upload_service
        self._poll_interval_sec = poll_interval_sec
        self._channel_timeout_sec = channel_timeout_sec
        self._search_engine = search_engine
        self._pipeline_bundle = pipeline_bundle
        self._db = db
        self._client_pool = client_pool
        self._notifier = notifier
        self._config = config
        self._llm_provider_service = llm_provider_service
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    def _handler_context(self) -> TaskHandlerContext:
        return TaskHandlerContext(
            collector=self._collector,
            channel_bundle=self._channel_bundle,
            tasks=self._tasks,
            stop_event=self._stop_event,
            sq_bundle=self._sq_bundle,
            photo_task_service=self._photo_task_service,
            photo_auto_upload_service=self._photo_auto_upload_service,
            poll_interval_sec=self._poll_interval_sec,
            channel_timeout_sec=self._channel_timeout_sec,
            search_engine=self._search_engine,
            pipeline_bundle=self._pipeline_bundle,
            db=self._db,
            client_pool=self._client_pool,
            notifier=self._notifier,
            config=self._config,
            llm_provider_service=self._llm_provider_service,
        )

    def _handler(self, handler_cls: type[TTaskHandler]) -> TTaskHandler:
        return handler_cls(self._handler_context())

    async def _build_image_service(self) -> "ImageGenerationService":
        return await build_image_service(self._handler_context())

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        recovered = await self._tasks.requeue_running_generic_tasks_on_startup(
            datetime.now(timezone.utc), HANDLED_TYPES
        )
        if recovered:
            logger.warning("Recovered %d interrupted generic tasks on startup", recovered)
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def _run_loop(self) -> None:
        idle_interval = self._poll_interval_sec
        active_interval = max(1.0, self._poll_interval_sec / 5)
        current_interval = idle_interval

        while not self._stop_event.is_set():
            task: CollectionTask | None = None
            try:
                task = await self._tasks.claim_next_due_generic_task(
                    datetime.now(timezone.utc), HANDLED_TYPES
                )
                if task is None:
                    current_interval = idle_interval
                    await asyncio.sleep(current_interval)
                    continue

                current_interval = active_interval
                await self._dispatch(task)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Unified dispatcher loop failure")
                if task and task.id is not None:
                    try:
                        fresh = await self._tasks.get_collection_task(task.id)
                        if fresh and fresh.status == CollectionTaskStatus.RUNNING:
                            await self._tasks.update_collection_task(
                                task.id,
                                CollectionTaskStatus.FAILED,
                                error="Task failed with unexpected dispatcher error",
                            )
                    except Exception:
                        logger.exception("Failed to mark broken task as failed")
                await asyncio.sleep(current_interval)

    def _handler_map(self) -> dict[CollectionTaskType, Callable[[CollectionTask], Awaitable[None]]]:
        try:
            return self.__handler_map
        except AttributeError:
            self.__handler_map = {
                CollectionTaskType.STATS_ALL: self._handle_stats_all,
                CollectionTaskType.SQ_STATS: self._handle_sq_stats,
                CollectionTaskType.PHOTO_DUE: self._handle_photo_due,
                CollectionTaskType.PHOTO_AUTO: self._handle_photo_auto,
                CollectionTaskType.PIPELINE_RUN: self._handle_pipeline_run,
                CollectionTaskType.CONTENT_GENERATE: self._handle_content_generate,
                CollectionTaskType.CONTENT_PUBLISH: self._handle_content_publish,
                CollectionTaskType.TRANSLATE_BATCH: self._handle_translate_batch,
            }
            return self.__handler_map

    async def _dispatch(self, task: CollectionTask) -> None:
        handler = self._handler_map().get(task.task_type)
        if handler is None:
            await self._tasks.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error=f"Unknown task type: {task.task_type}",
            )
            return
        await handler(task)

    async def _handle_stats_all(self, task: CollectionTask) -> None:
        await self._handler(StatsTaskHandler).handle_stats_all(task)

    async def _handle_sq_stats(self, task: CollectionTask) -> None:
        await self._handler(StatsTaskHandler).handle_sq_stats(task)

    async def _handle_photo_due(self, task: CollectionTask) -> None:
        await self._handler(PhotoTaskHandler).handle_photo_due(task)

    async def _handle_photo_auto(self, task: CollectionTask) -> None:
        await self._handler(PhotoTaskHandler).handle_photo_auto(task)

    async def _handle_pipeline_run(self, task: CollectionTask) -> None:
        await self._handler(PipelineTaskHandler).handle_pipeline_run(task)

    async def _handle_content_generate(self, task: CollectionTask) -> None:
        await self._handler(ContentTaskHandler).handle_content_generate(task)

    async def _handle_content_publish(self, task: CollectionTask) -> None:
        await self._handler(ContentTaskHandler).handle_content_publish(task)

    async def _handle_translate_batch(self, task: CollectionTask) -> None:
        await self._handler(TranslationTaskHandler).handle_translate_batch(task)
