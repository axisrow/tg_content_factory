from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from tg_agent.database.bundles import ChannelBundle, PipelineBundle, SearchQueryBundle
from tg_agent.database.repositories.collection_tasks import CollectionTasksRepository
from tg_agent.models import CollectionTask, CollectionTaskType
from tg_agent.telegram.collector import Collector

if TYPE_CHECKING:
    from tg_agent.config import AppConfig
    from tg_agent.database import Database
    from tg_agent.search.engine import SearchEngine
    from tg_agent.services.content_generation_service import ContentGenerationService
    from tg_agent.services.image_generation_service import ImageGenerationService
    from tg_agent.services.photo_auto_upload_service import PhotoAutoUploadService
    from tg_agent.services.photo_task_service import PhotoTaskService
    from tg_agent.services.ping_monitor_service import PingMonitorService
    from tg_agent.services.seeding_publisher import SeedingPublisherService
    from tg_agent.telegram.client_pool import ClientPool
    from tg_agent.telegram.notifier import Notifier

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
    ping_monitor_service: PingMonitorService | None = None
    # Chat-seeding publisher (#1757): worker-only (live sends through the pool).
    seeding_publisher: SeedingPublisherService | None = None
    poll_interval_sec: float = 5.0
    channel_timeout_sec: float = 120.0
    search_engine: "SearchEngine | None" = None
    pipeline_bundle: PipelineBundle | None = None
    db: "Database | None" = None
    client_pool: "ClientPool | None" = None
    notifier: "Notifier | None" = None
    config: "AppConfig | None" = None
    llm_provider_service: object | None = None


async def build_image_service(context: TaskHandlerContext) -> "ImageGenerationService":
    from tg_agent.services.image_generation_service import ImageGenerationService

    adapters = None
    db = context.db
    config = context.config
    if db is not None and config is not None:
        try:
            from tg_agent.services.image_provider_service import ImageProviderService

            svc = ImageProviderService(db, config)
            configs = await svc.load_provider_configs()
            built = svc.build_adapters(configs)
            if configs:
                adapters = built
            elif built:
                adapters = built
        except Exception:
            logger.warning("Failed to load image provider configs from DB", exc_info=True)
            adapters = {}
    # Opt-in production rate-limit / daily cost cap (#814); None unless the
    # operator enabled production_limits, so default behavior is unchanged.
    limits = None
    if db is not None and config is not None:
        from tg_agent.services.production_limits_service import ProductionLimitsService

        limits = ProductionLimitsService.from_config(db, config)
    return ImageGenerationService(adapters=adapters, limits=limits)


async def resolve_llm_provider_service(context: TaskHandlerContext) -> object:
    if context.llm_provider_service is not None:
        return context.llm_provider_service
    from tg_agent.services.provider_service import build_provider_service

    return await build_provider_service(context.db, context.config)


async def build_content_generation_service(context: TaskHandlerContext) -> "ContentGenerationService":
    """Assemble a ContentGenerationService with its collaborators from the context.

    Shared by the CONTENT_GENERATE and PIPELINE_RUN task handlers so the wiring
    (draft notifications + image service + provider/quality services) lives in
    one place.
    """
    from tg_agent.services.content_generation_service import ContentGenerationService
    from tg_agent.services.draft_notification_service import DraftNotificationService
    from tg_agent.services.quality_scoring_service import QualityScoringService

    db = context.db
    search_engine = context.search_engine
    assert db is not None
    assert search_engine is not None
    notification_service = DraftNotificationService(db, context.notifier)
    image_service = await build_image_service(context)
    provider_service = await resolve_llm_provider_service(context)
    quality_service = QualityScoringService(db, provider_service=provider_service)
    return ContentGenerationService(
        db,
        search_engine,
        config=context.config,
        image_service=image_service,
        notification_service=notification_service,
        quality_service=quality_service,
        client_pool=context.client_pool,
        provider_service=provider_service,
    )
