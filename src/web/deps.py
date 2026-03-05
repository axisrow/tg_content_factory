from __future__ import annotations

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
from src.services.scheduler_service import SchedulerService
from src.services.search_service import SearchService
from src.telegram.auth import TelegramAuth
from src.telegram.client_pool import ClientPool
from src.telegram.collector import Collector


def get_db(request: Request) -> Database:
    return request.app.state.db


def get_pool(request: Request) -> ClientPool:
    return request.app.state.pool


def get_collector(request: Request) -> Collector:
    return request.app.state.collector


def get_queue(request: Request) -> CollectionQueue:
    return request.app.state.collection_queue


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


def channel_service(request: Request) -> ChannelService:
    return ChannelService(get_db(request), get_pool(request))


def keyword_service(request: Request) -> KeywordService:
    return KeywordService(get_db(request))


def account_service(request: Request) -> AccountService:
    return AccountService(get_db(request), get_pool(request))


def collection_service(request: Request) -> CollectionService:
    return CollectionService(get_db(request), get_collector(request), get_queue(request))


def search_service(request: Request) -> SearchService:
    return SearchService(get_search_engine(request), get_ai_search(request))


def scheduler_service(request: Request) -> SchedulerService:
    return SchedulerService(get_scheduler(request))
