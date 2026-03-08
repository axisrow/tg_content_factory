from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from fastapi import Request
from fastapi.templating import Jinja2Templates

from src.collection_queue import CollectionQueue
from src.database import Database
from src.database.bundles import (
    AccountBundle,
    ChannelBundle,
    CollectionBundle,
    NotificationBundle,
    SchedulerBundle,
    SearchBundle,
)
from src.scheduler.manager import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
from src.services.account_service import AccountService
from src.services.channel_service import ChannelService
from src.services.collection_service import CollectionService
from src.services.keyword_service import KeywordService
from src.services.notification_service import NotificationService
from src.services.notification_target_service import NotificationTargetService
from src.services.scheduler_service import SchedulerService
from src.services.search_service import SearchService
from src.services.stats_task_dispatcher import StatsTaskDispatcher
from src.telegram.auth import TelegramAuth
from src.telegram.client_pool import ClientPool
from src.telegram.collector import Collector
from src.telegram.notifier import Notifier
from src.web.container import AppContainer
from src.web.log_handler import LogBuffer
from src.web.bootstrap import TEMPLATES_DIR

T = TypeVar("T")


def _request_cached(request: Request, key: str, factory: Callable[[], T]) -> T:
    value = getattr(request.state, key, None)
    if value is None:
        value = factory()
        setattr(request.state, key, value)
    return value


def get_container(request: Request) -> AppContainer:
    container = getattr(request.app.state, "container", None)
    if container is not None:
        return container

    db = request.app.state.db
    repos = db.repos
    account_bundle = AccountBundle.from_database(db)
    channel_bundle = ChannelBundle.from_database(db)
    collection_bundle = CollectionBundle.from_database(db)
    notification_bundle = NotificationBundle.from_database(db)
    search_bundle = SearchBundle.from_database(db)
    scheduler_bundle = SchedulerBundle.from_database(db)
    return AppContainer(
        config=request.app.state.config,
        db=db,
        repos=repos,
        account_bundle=account_bundle,
        channel_bundle=channel_bundle,
        collection_bundle=collection_bundle,
        notification_bundle=notification_bundle,
        search_bundle=search_bundle,
        scheduler_bundle=scheduler_bundle,
        auth=request.app.state.auth,
        pool=request.app.state.pool,
        notification_target_service=getattr(
            request.app.state,
            "notification_target_service",
            NotificationTargetService(notification_bundle, request.app.state.pool),
        ),
        notifier=getattr(request.app.state, "notifier", None),
        collector=request.app.state.collector,
        collection_queue=getattr(request.app.state, "collection_queue", None),
        stats_dispatcher=getattr(request.app.state, "stats_dispatcher", None),
        search_engine=request.app.state.search_engine,
        ai_search=request.app.state.ai_search,
        scheduler=request.app.state.scheduler,
        templates=getattr(
            request.app.state,
            "templates",
            Jinja2Templates(directory=str(TEMPLATES_DIR)),
        ),
        log_buffer=getattr(request.app.state, "log_buffer", None),
        session_secret=getattr(request.app.state, "session_secret", ""),
        bg_tasks=getattr(request.app.state, "bg_tasks", set()),
        shutting_down=getattr(request.app.state, "shutting_down", False),
    )


def get_db(request: Request) -> Database:
    return get_container(request).db


def get_account_bundle(request: Request) -> AccountBundle:
    return get_container(request).account_bundle


def get_channel_bundle(request: Request) -> ChannelBundle:
    return get_container(request).channel_bundle


def get_collection_bundle(request: Request) -> CollectionBundle:
    return get_container(request).collection_bundle


def get_notification_bundle(request: Request) -> NotificationBundle:
    return get_container(request).notification_bundle


def get_search_bundle(request: Request) -> SearchBundle:
    return get_container(request).search_bundle


def get_scheduler_bundle(request: Request) -> SchedulerBundle:
    return get_container(request).scheduler_bundle


def get_pool(request: Request) -> ClientPool:
    return get_container(request).pool


def get_collector(request: Request) -> Collector:
    return get_container(request).collector


def get_queue(request: Request) -> CollectionQueue:
    return get_container(request).collection_queue


def get_stats_dispatcher(request: Request) -> StatsTaskDispatcher:
    return get_container(request).stats_dispatcher


def get_scheduler(request: Request) -> SchedulerManager:
    return get_container(request).scheduler


def get_search_engine(request: Request) -> SearchEngine:
    return get_container(request).search_engine


def get_ai_search(request: Request) -> AISearchEngine:
    return get_container(request).ai_search


def get_auth(request: Request) -> TelegramAuth:
    return get_container(request).auth


def get_templates(request: Request) -> Jinja2Templates:
    return get_container(request).templates


def get_notification_target_service(request: Request) -> NotificationTargetService:
    return get_container(request).notification_target_service


def get_notifier(request: Request) -> Notifier:
    return get_container(request).notifier


def get_log_buffer(request: Request) -> LogBuffer:
    return get_container(request).log_buffer


def is_shutting_down(request: Request) -> bool:
    return get_container(request).shutting_down


def channel_service(request: Request) -> ChannelService:
    return _request_cached(
        request,
        "_channel_service",
        lambda: ChannelService(get_channel_bundle(request), get_pool(request), get_queue(request)),
    )


def keyword_service(request: Request) -> KeywordService:
    return _request_cached(
        request,
        "_keyword_service",
        lambda: KeywordService(get_collection_bundle(request)),
    )


def account_service(request: Request) -> AccountService:
    return _request_cached(
        request,
        "_account_service",
        lambda: AccountService(get_account_bundle(request), get_pool(request)),
    )


def collection_service(request: Request) -> CollectionService:
    return _request_cached(
        request,
        "_collection_service",
        lambda: CollectionService(
            get_channel_bundle(request),
            get_collector(request),
            get_queue(request),
        ),
    )


def search_service(request: Request) -> SearchService:
    return _request_cached(
        request,
        "_search_service",
        lambda: SearchService(get_search_engine(request), get_ai_search(request)),
    )


def notification_service(request: Request) -> NotificationService:
    return _request_cached(
        request,
        "_notification_service",
        lambda: NotificationService(
            get_notification_bundle(request),
            get_notification_target_service(request),
            get_container(request).config.notifications.bot_name_prefix,
            get_container(request).config.notifications.bot_username_prefix,
        ),
    )


def scheduler_service(request: Request) -> SchedulerService:
    return _request_cached(
        request, "_scheduler_service", lambda: SchedulerService(get_scheduler(request))
    )
