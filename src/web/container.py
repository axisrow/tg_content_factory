from __future__ import annotations

import asyncio
from dataclasses import dataclass

from fastapi.templating import Jinja2Templates

from src.agent.manager import AgentManager
from src.collection_queue import CollectionQueue
from src.config import AppConfig
from src.database import Database
from src.database.bundles import (
    AccountBundle,
    ChannelBundle,
    CollectionBundle,
    DatabaseRepositories,
    NotificationBundle,
    PhotoLoaderBundle,
    PipelineBundle,
    SchedulerBundle,
    SearchBundle,
    SearchQueryBundle,
)
from src.scheduler.service import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
from src.services.notification_target_service import NotificationTargetService
from src.services.photo_auto_upload_service import PhotoAutoUploadService
from src.services.photo_publish_service import PhotoPublishService
from src.services.photo_task_service import PhotoTaskService
from src.services.provider_service import RuntimeProviderRegistry
from src.services.task_enqueuer import TaskEnqueuer
from src.services.telegram_command_dispatcher import TelegramCommandDispatcher
from src.services.translation_service import TranslationService
from src.services.unified_dispatcher import UnifiedDispatcher
from src.telegram.auth import TelegramAuth
from src.telegram.client_pool import ClientPool
from src.telegram.collector import Collector
from src.telegram.notifier import Notifier
from src.web.log_handler import LogBuffer
from src.web.timing import TimingBuffer


@dataclass(slots=True)
class AppContainer:
    runtime_mode: str
    config: AppConfig
    db: Database
    repos: DatabaseRepositories
    account_bundle: AccountBundle
    channel_bundle: ChannelBundle
    collection_bundle: CollectionBundle
    notification_bundle: NotificationBundle
    pipeline_bundle: PipelineBundle
    photo_loader_bundle: PhotoLoaderBundle
    search_bundle: SearchBundle
    scheduler_bundle: SchedulerBundle
    search_query_bundle: SearchQueryBundle
    auth: TelegramAuth
    pool: ClientPool | object
    notification_target_service: NotificationTargetService
    notifier: Notifier | None
    photo_publish_service: PhotoPublishService
    photo_task_service: PhotoTaskService
    photo_auto_upload_service: PhotoAutoUploadService
    collector: Collector | object
    collection_queue: CollectionQueue | None
    task_enqueuer: TaskEnqueuer | None
    unified_dispatcher: UnifiedDispatcher | None
    telegram_command_dispatcher: TelegramCommandDispatcher | None
    search_engine: SearchEngine
    ai_search: AISearchEngine
    scheduler: SchedulerManager | object
    templates: Jinja2Templates
    log_buffer: LogBuffer | None
    timing_buffer: TimingBuffer | None
    session_secret: str
    bg_tasks: set[asyncio.Task]
    agent_manager: AgentManager | None = None
    translation_service: TranslationService | None = None
    llm_provider_service: RuntimeProviderRegistry | None = None
    shutting_down: bool = False
