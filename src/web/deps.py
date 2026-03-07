from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from fastapi import Request
from fastapi.templating import Jinja2Templates

from src.collection_queue import CollectionQueue
from src.database import Database
from src.scheduler.manager import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
from src.services.account_service import AccountService
from src.services.channel_service import ChannelService
from src.services.collection_service import CollectionService
from src.services.keyword_service import KeywordService
from src.services.notification_target_service import NotificationTargetService
from src.services.scheduler_service import SchedulerService
from src.services.search_service import SearchService
from src.telegram.auth import TelegramAuth
from src.telegram.client_pool import ClientPool
from src.telegram.collector import Collector
from src.web.log_handler import LogBuffer

T = TypeVar("T")


def _request_cached(request: Request, key: str, factory: Callable[[], T]) -> T:
    value = getattr(request.state, key, None)
    if value is None:
        value = factory()
        setattr(request.state, key, value)
    return value


def get_db(request: Request) -> Database:
    return request.app.state.db


def get_pool(request: Request) -> ClientPool:
    return request.app.state.pool


def get_collector(request: Request) -> Collector:
    return request.app.state.collector


def get_queue(request: Request) -> CollectionQueue:
    queue = getattr(request.app.state, "collection_queue", None)
    if queue is None:
        queue = CollectionQueue(get_collector(request), get_db(request))
        request.app.state.collection_queue = queue
    return queue


def get_scheduler(request: Request) -> SchedulerManager:
    return request.app.state.scheduler


def get_search_engine(request: Request) -> SearchEngine:
    return request.app.state.search_engine


def get_ai_search(request: Request) -> AISearchEngine:
    return request.app.state.ai_search


def get_auth(request: Request) -> TelegramAuth:
    return request.app.state.auth


def get_templates(request: Request) -> Jinja2Templates:
    return request.app.state.templates


def get_notification_target_service(request: Request) -> NotificationTargetService:
    service = getattr(request.app.state, "notification_target_service", None)
    if service is None:
        service = NotificationTargetService(get_db(request), get_pool(request))
        request.app.state.notification_target_service = service
    return service


def channel_service(request: Request) -> ChannelService:
    return _request_cached(
        request,
        "_channel_service",
        lambda: ChannelService(get_db(request), get_pool(request), get_queue(request)),
    )


def keyword_service(request: Request) -> KeywordService:
    return _request_cached(request, "_keyword_service", lambda: KeywordService(get_db(request)))


def account_service(request: Request) -> AccountService:
    return _request_cached(
        request,
        "_account_service",
        lambda: AccountService(get_db(request), get_pool(request)),
    )


def collection_service(request: Request) -> CollectionService:
    return _request_cached(
        request,
        "_collection_service",
        lambda: CollectionService(get_db(request), get_collector(request), get_queue(request)),
    )


def search_service(request: Request) -> SearchService:
    return _request_cached(
        request,
        "_search_service",
        lambda: SearchService(get_search_engine(request), get_ai_search(request)),
    )


def scheduler_service(request: Request) -> SchedulerService:
    return _request_cached(
        request, "_scheduler_service", lambda: SchedulerService(get_scheduler(request))
    )


def get_log_buffer(request: Request) -> LogBuffer:
    return request.app.state.log_buffer
