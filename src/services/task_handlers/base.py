from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from src.database.bundles import ChannelBundle, PipelineBundle, SearchQueryBundle
from src.database.repositories.collection_tasks import CollectionTasksRepository
from src.models import CollectionTask, CollectionTaskType
from src.telegram.collector import Collector

if TYPE_CHECKING:
    from src.database import Database
    from src.search.engine import SearchEngine
    from src.services.image_generation_service import ImageGenerationService
    from src.services.photo_auto_upload_service import PhotoAutoUploadService
    from src.services.photo_task_service import PhotoTaskService
    from src.telegram.notifier import Notifier

logger = logging.getLogger(__name__)


class TaskHandler(Protocol):
    task_types: tuple[CollectionTaskType, ...]

    async def handle(self, task: CollectionTask) -> None: ...


@dataclass(slots=True)
class TaskHandlerContext:
    collector: Collector
    channel_bundle: ChannelBundle
    tasks: CollectionTasksRepository
    stop_event: asyncio.Event
    sq_bundle: SearchQueryBundle | None = None
    photo_task_service: PhotoTaskService | None = None
    photo_auto_upload_service: PhotoAutoUploadService | None = None
    poll_interval_sec: float = 5.0
    channel_timeout_sec: float = 120.0
    search_engine: "SearchEngine | None" = None
    pipeline_bundle: PipelineBundle | None = None
    db: "Database | None" = None
    client_pool: object | None = None
    notifier: "Notifier | None" = None
    config: object | None = None
    llm_provider_service: object | None = None


async def build_image_service(context: TaskHandlerContext) -> "ImageGenerationService":
    from src.services.image_generation_service import ImageGenerationService

    adapters = None
    if context.db and context.config:
        try:
            from src.services.image_provider_service import ImageProviderService

            svc = ImageProviderService(context.db, context.config)
            configs = await svc.load_provider_configs()
            built = svc.build_adapters(configs)
            if configs:
                adapters = built
            elif built:
                adapters = built
        except Exception:
            logger.warning("Failed to load image provider configs from DB", exc_info=True)
            adapters = {}
    return ImageGenerationService(adapters=adapters)


async def resolve_llm_provider_service(context: TaskHandlerContext) -> object:
    if context.llm_provider_service is not None:
        return context.llm_provider_service
    from src.services.provider_service import build_provider_service

    return await build_provider_service(context.db, context.config)
