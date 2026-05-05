"""Common fixtures for route tests."""

from __future__ import annotations

import base64
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from src.agent.manager import AgentManager
from src.collection_queue import CollectionQueue
from src.config import AppConfig, DatabaseConfig
from src.database import Database
from src.models import Account, Channel, RuntimeSnapshot
from src.scheduler.service import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
from src.services.provider_service import RuntimeProviderRegistry
from src.telegram.auth import TelegramAuth
from src.telegram.collector import Collector
from src.web.app import create_app
from src.web.bootstrap import build_container_with_templates
from src.web.log_handler import LogBuffer


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
    app.state.llm_provider_service = RuntimeProviderRegistry()
    app.state.session_secret = "test_secret_key"
    app.state.shutting_down = False

    await db.add_account(Account(phone="+1234567890", session_string="test_session"))
    await db.add_channel(Channel(channel_id=100, title="Test Channel"))

    # Route tests reuse the web process model (`runtime_mode="web"` in real
    # setups), but they instantiate a live ClientPool-like mock instead of the
    # Snapshot shim. The `/scheduler/` health path now treats a missing
    # `worker_heartbeat` snapshot as `state=worker_down` (fix for #457), which
    # would regress every pre-existing scheduler test. Stamp a fresh heartbeat
    # so tests keep exercising the same pre-#457 states (healthy / no_clients /
    # all_flooded / degraded). Individual tests that want to exercise the new
    # `worker_down` branch should delete this snapshot.
    await db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="worker_heartbeat",
            payload={"status": "alive"},
            updated_at=datetime.now(timezone.utc),
        )
    )

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


# === Web-mode fixture (#457 round 3) =================================
#
# Why: `base_app` above is convenience — it wires a live `CollectionQueue`
# + real `SchedulerManager`, which *looks* like the worker runtime. That
# masked the #457 regressions (500 on /scheduler/tasks/clear-pending-collect,
# 500 in pipelines /run, silent-no-op /scheduler/start) because route tests
# never saw the production web-mode wiring — where `collection_queue=None`,
# `task_enqueuer=None`, `unified_dispatcher=None`, and `pool`/`collector`/
# `scheduler` are `SnapshotClientPool` / `SnapshotCollector` /
# `SnapshotSchedulerManager` shims.
#
# This fixture goes through the real `build_container_with_templates`
# bootstrap with `runtime_mode="web"` — the exact same wiring that
# `python -m src.main serve` uses in production. Tests that want to exercise
# the web-mode code path should take this fixture instead of `base_app`.

_WEB_MODE_PASSWORD = "testpass"


@pytest.fixture
async def web_mode_app(tmp_path):
    """Production-like web-mode app — the same bootstrap path as `serve`."""
    config = AppConfig(database=DatabaseConfig(path=str(tmp_path / "web_mode.db")))
    config.web.password = _WEB_MODE_PASSWORD
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"

    container = await build_container_with_templates(
        config,
        log_buffer=LogBuffer(),
        templates=None,
        runtime_mode="web",
    )

    # Seed one account + one channel so scheduler/channels pages render cleanly.
    await container.db.add_account(
        Account(phone="+1234567890", session_string="test_session")
    )
    await container.db.add_channel(Channel(channel_id=-1001, title="Web Mode Test Channel"))

    # Stamp a fresh worker_heartbeat so /scheduler/ doesn't auto-flip to the
    # `worker_down` banner unless a test removes it explicitly.
    await container.db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="worker_heartbeat",
            payload={"status": "alive"},
            updated_at=datetime.now(timezone.utc),
        )
    )

    app = create_app(config)
    # Copy every relevant attribute from the real container — this is the exact
    # surface `src/web/app.py:lifespan` sets up when `serve` boots.
    for attr in (
        "db", "config", "pool", "collector", "scheduler", "auth", "notifier",
        "collection_queue", "unified_dispatcher", "telegram_command_dispatcher",
        "task_enqueuer", "agent_manager", "search_engine", "ai_search",
        "llm_provider_service", "runtime_mode",
    ):
        setattr(app.state, attr, getattr(container, attr, None))
    app.state.session_secret = "test_secret_key"
    app.state.shutting_down = False

    try:
        yield app, container
    finally:
        await container.db.close()


@pytest.fixture
async def web_mode_client(web_mode_app):
    """HTTP client against a production-like web-mode app.

    `follow_redirects=False` is intentional — round 3 tests assert on the
    Location header (e.g. filter-preserve redirects), which would be lost
    by the default follow-redirects behaviour.
    """
    app, _ = web_mode_app
    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(f":{_WEB_MODE_PASSWORD}".encode()).decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=False,
        headers={
            "Authorization": f"Basic {auth_header}",
            "Origin": "http://test",
        },
    ) as c:
        yield c


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
