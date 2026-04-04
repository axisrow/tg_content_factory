from __future__ import annotations

import pytest

from src.database.repositories.accounts import AccountsRepository
from src.database.repositories.channel_stats import ChannelStatsRepository
from src.database.repositories.channels import ChannelsRepository
from src.database.repositories.collection_tasks import CollectionTasksRepository
from src.database.repositories.content_pipelines import ContentPipelinesRepository
from src.database.repositories.filters import FilterRepository
from src.database.repositories.messages import MessagesRepository
from src.database.repositories.notification_bots import NotificationBotsRepository
from src.database.repositories.search_log import SearchLogRepository
from src.database.repositories.search_queries import SearchQueriesRepository
from src.database.repositories.settings import SettingsRepository
from src.models import Channel


@pytest.fixture
async def accounts_repo(db):
    return AccountsRepository(db.db)


@pytest.fixture
async def channel_stats_repo(db):
    return ChannelStatsRepository(db.db)


@pytest.fixture
async def channels_repo(db):
    return ChannelsRepository(db.db)


@pytest.fixture
async def collection_tasks_repo(db):
    return CollectionTasksRepository(db.db)


@pytest.fixture
async def content_pipelines_repo(db):
    await db.add_channel(Channel(channel_id=1001, title="Source A"))
    await db.add_channel(Channel(channel_id=1002, title="Source B"))
    return ContentPipelinesRepository(db.db)


@pytest.fixture
async def filters_repo(db):
    return FilterRepository(db.db)


@pytest.fixture
async def messages_repo(db):
    return MessagesRepository(db.db)


@pytest.fixture
async def notification_bots_repo(db):
    return NotificationBotsRepository(db.db)


@pytest.fixture
async def search_log_repo(db):
    return SearchLogRepository(db.db)


@pytest.fixture
async def search_queries_repo(db):
    return SearchQueriesRepository(db.db)


@pytest.fixture
async def settings_repo(db):
    return SettingsRepository(db.db)
