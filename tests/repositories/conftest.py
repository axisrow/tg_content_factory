from __future__ import annotations

from pathlib import Path

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

_REPO_FACTORIES = {
    "test_accounts_repository.py": AccountsRepository,
    "test_channel_stats_repository.py": ChannelStatsRepository,
    "test_channels_repository.py": ChannelsRepository,
    "test_collection_tasks_repository.py": CollectionTasksRepository,
    "test_filters_repository.py": FilterRepository,
    "test_messages_repository.py": MessagesRepository,
    "test_notification_bots_repository.py": NotificationBotsRepository,
    "test_search_log_repository.py": SearchLogRepository,
    "test_search_queries_repository.py": SearchQueriesRepository,
    "test_settings_repository.py": SettingsRepository,
}


@pytest.fixture
async def repo(request, db):
    """Provide the repository under test for repository modules."""
    filename = Path(str(request.fspath)).name

    if filename == "test_content_pipelines_repository.py":
        await db.add_channel(Channel(channel_id=1001, title="Source A"))
        await db.add_channel(Channel(channel_id=1002, title="Source B"))
        return ContentPipelinesRepository(db.db)

    factory = _REPO_FACTORIES.get(filename)
    if factory is None:
        raise LookupError(f"No shared repo fixture registered for {filename}")
    return factory(db.db)
