from __future__ import annotations

import asyncio
from dataclasses import dataclass

from fastapi.templating import Jinja2Templates

from src.collection_queue import CollectionQueue
from src.config import AppConfig
from src.database import Database
from src.database.bundles import (
    AccountBundle,
    ChannelBundle,
    CollectionBundle,
    DatabaseRepositories,
    NotificationBundle,
    SchedulerBundle,
    SearchBundle,
    SearchQueryBundle,
)
from src.scheduler.manager import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
from src.services.notification_target_service import NotificationTargetService
from src.services.stats_task_dispatcher import StatsTaskDispatcher
from src.telegram.auth import TelegramAuth
from src.telegram.client_pool import ClientPool
from src.telegram.collector import Collector
from src.telegram.notifier import Notifier
from src.web.log_handler import LogBuffer


@dataclass(slots=True)
class AppContainer:
    config: AppConfig
    db: Database
    repos: DatabaseRepositories
    account_bundle: AccountBundle
    channel_bundle: ChannelBundle
    collection_bundle: CollectionBundle
    notification_bundle: NotificationBundle
    search_bundle: SearchBundle
    scheduler_bundle: SchedulerBundle
    search_query_bundle: SearchQueryBundle
    auth: TelegramAuth
    pool: ClientPool
    notification_target_service: NotificationTargetService
    notifier: Notifier | None
    collector: Collector
    collection_queue: CollectionQueue | None
    stats_dispatcher: StatsTaskDispatcher | None
    search_engine: SearchEngine
    ai_search: AISearchEngine
    scheduler: SchedulerManager
    templates: Jinja2Templates
    log_buffer: LogBuffer | None
    session_secret: str
    bg_tasks: set[asyncio.Task]
    shutting_down: bool = False
