"""The worker runtime's services (the only runtime since #1868)."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from tg_agent.agent.manager import AgentManager
from tg_agent.collection_queue import CollectionQueue
from tg_agent.config import AppConfig
from tg_agent.database import Database
from tg_agent.database.bundles import (
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
from tg_agent.live_runtime_pause import LiveRuntimePauseGate
from tg_agent.plugin_api import PluginRuntime
from tg_agent.scheduler.service import SchedulerManager
from tg_agent.search.ai_search import AISearchEngine
from tg_agent.search.engine import SearchEngine
from tg_agent.services.notification_target_service import NotificationTargetService
from tg_agent.services.photo_auto_upload_service import PhotoAutoUploadService
from tg_agent.services.photo_publish_service import PhotoPublishService
from tg_agent.services.photo_task_service import PhotoTaskService
from tg_agent.services.ping_monitor_service import PingMonitorService
from tg_agent.services.provider_service import RuntimeProviderRegistry
from tg_agent.services.seeding_publisher import SeedingPublisherService
from tg_agent.services.task_enqueuer import TaskEnqueuer
from tg_agent.services.telegram_command_dispatcher import TelegramCommandDispatcher
from tg_agent.services.translation_service import TranslationService
from tg_agent.services.unified_dispatcher import UnifiedDispatcher
from tg_agent.telegram.client_pool import ClientPool
from tg_agent.telegram.collector import Collector
from tg_agent.telegram.dm_catchup import DmCatchupService
from tg_agent.telegram.dm_listener import DmListener
from tg_agent.telegram.notifier import Notifier
from tg_shared.auth import TelegramAuth


@dataclass(slots=True)
class RuntimeContainer:
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
    pool: ClientPool
    notification_target_service: NotificationTargetService
    notifier: Notifier | None
    photo_publish_service: PhotoPublishService
    photo_task_service: PhotoTaskService
    photo_auto_upload_service: PhotoAutoUploadService
    collector: Collector
    collection_queue: CollectionQueue | None
    task_enqueuer: TaskEnqueuer | None
    unified_dispatcher: UnifiedDispatcher | None
    telegram_command_dispatcher: TelegramCommandDispatcher | None
    search_engine: SearchEngine
    ai_search: AISearchEngine
    scheduler: SchedulerManager
    bg_tasks: set[asyncio.Task]
    live_runtime_pause_gate: LiveRuntimePauseGate | None = None
    agent_manager: AgentManager | None = None
    translation_service: TranslationService | None = None
    llm_provider_service: RuntimeProviderRegistry | None = None
    # Real-time incoming-DM listener (#1426); a gateway project gets the
    # gateway's DM consumer instead (#1868).
    dm_listener: DmListener | None = None
    dm_catchup: DmCatchupService | None = None
    # Dormant-DM ping monitors (#1415): live history reads and sends.
    ping_monitor_service: PingMonitorService | None = None
    # Chat-seeding publisher (#1757): live sends through the pool.
    seeding_publisher: SeedingPublisherService | None = None
    plugins: PluginRuntime | None = None
    shutting_down: bool = False
