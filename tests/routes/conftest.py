"""Common fixtures for route tests."""

from __future__ import annotations

import base64
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from src.agent.manager import AgentManager
from src.collection_queue import CollectionQueue
from src.config import AppConfig
from src.database import Database
from src.models import Account, Channel
from src.scheduler.service import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
from src.telegram.auth import TelegramAuth
from src.telegram.collector import Collector
from src.web.app import create_app


@pytest.fixture
async def base_app(tmp_path):
    """Create configured app + db with account and channel."""
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"

    app = create_app(config)
    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db
    app.state.config = config

    pool_mock = MagicMock()
    pool_mock.clients = {"+1234567890": MagicMock()}
    pool_mock.get_dialogs_for_phone = AsyncMock(return_value=[])
    pool_mock.resolve_channel = AsyncMock(
        return_value={
            "channel_id": -1001234567890,
            "title": "Test Channel",
            "username": "testchannel",
            "channel_type": "channel",
        }
    )
    pool_mock.get_forum_topics = AsyncMock(return_value=[])
    pool_mock.remove_client = AsyncMock()
    pool_mock.connect_client = AsyncMock()
    pool_mock.disconnect_client = AsyncMock()
    app.state.pool = pool_mock

    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None

    collector = Collector(pool_mock, db, config.scheduler)
    app.state.collector = collector

    collection_queue = CollectionQueue(collector, db)
    app.state.collection_queue = collection_queue

    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "test_secret_key"
    app.state.shutting_down = False

    await db.add_account(Account(phone="+1234567890", session_string="test_session"))
    await db.add_channel(Channel(channel_id=100, title="Test Channel"))

    yield app, db, pool_mock

    await collection_queue.shutdown()
    await db.close()


@pytest.fixture
async def route_client(base_app):
    """AsyncClient with Basic auth."""
    app, db, pool_mock = base_app
    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={
            "Authorization": f"Basic {auth_header}",
            "Origin": "http://test",
        },
    ) as c:
        c._transport_app = app
        yield c


@pytest.fixture
def pool_mock(base_app):
    """Mock ClientPool with basic methods."""
    _, _, pool = base_app
    return pool


@pytest.fixture
def agent_manager_mock():
    """Mock AgentManager."""
    m = MagicMock(spec=AgentManager)
    m.available = True

    runtime = MagicMock()
    runtime.claude_available = False
    runtime.deepagents_available = False
    runtime.dev_mode_enabled = False
    runtime.backend_override = None
    runtime.selected_backend = "deepagents"
    runtime.fallback_model = None
    runtime.fallback_provider = None
    runtime.using_override = False
    runtime.error = None

    m.get_runtime_status = AsyncMock(return_value=runtime)
    m.cancel_stream = AsyncMock(return_value=False)
    m.estimate_prompt_tokens = AsyncMock(return_value=100)

    async def _fake_stream(*a, **kw):
        yield 'data: {"delta": "hi"}\n\n'
        yield 'data: {"done": true, "full_text": "hi"}\n\n'

    m.chat_stream = _fake_stream

    return m


# === Helper functions ===


async def _add_channel(db: Database, channel_id: int = 100, title: str = "Test") -> int:
    """Add channel, return PK."""
    await db.add_channel(Channel(channel_id=channel_id, title=title))
    channels = await db.get_channels_with_counts()
    return next(c.id for c in channels if c.channel_id == channel_id)


async def _add_filtered_channel(
    db: Database, channel_id: int = 200, title: str = "Filtered"
) -> int:
    """Add filtered channel, return PK."""
    await db.add_channel(Channel(channel_id=channel_id, title=title))
    channels = await db.get_channels_with_counts(active_only=False, include_filtered=True)
    pk = next(c.id for c in channels if c.channel_id == channel_id)
    await db.set_channel_filtered(pk, True)
    return pk


async def _enable_dev_mode(db: Database):
    """Enable developer mode."""
    await db.set_setting("agent_dev_mode_enabled", "1")
