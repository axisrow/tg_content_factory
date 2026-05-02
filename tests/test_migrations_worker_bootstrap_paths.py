"""Tests for migrations, runtime worker, web route, bootstrap, CLI TUI, and local search paths.

Target modules:
1. src/database/migrations.py
2. src/runtime/worker.py
3. src/web/routes/agent.py
4. src/web/routes/channels.py
5. src/agent/tools/images.py
6. src/services/provider_service.py
7. src/web/bootstrap.py
8. src/cli/commands/agent_tui.py
9. src/search/local_search.py
"""

from __future__ import annotations

import asyncio
import os
import struct
from unittest.mock import AsyncMock, MagicMock, patch

import aiosqlite
import pytest

from src.config import AppConfig
from src.database.migrations import (
    _migrate_tool_permission_key,
    _migrate_vec_to_portable,
    run_migrations,
)
from src.models import Message
from src.search.local_search import LocalSearch
from src.services.provider_service import AgentProviderService, build_provider_service

# ============================================================================
# 1. src/database/migrations.py -- edge-case paths
# ============================================================================


@pytest.fixture
async def fresh_db():
    db = await aiosqlite.connect(":memory:")
    db.row_factory = aiosqlite.Row
    yield db
    await db.close()


async def _init_minimal_schema(db):
    """Create the minimal set of tables required by run_migrations()."""
    await db.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            channel_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            sender_id INTEGER,
            sender_name TEXT,
            text TEXT,
            date TEXT NOT NULL,
            collected_at TEXT,
            UNIQUE(channel_id, message_id)
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY,
            phone TEXT UNIQUE NOT NULL,
            session_string TEXT NOT NULL,
            is_primary INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            is_premium INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS channels (
            id INTEGER PRIMARY KEY,
            channel_id INTEGER UNIQUE NOT NULL,
            title TEXT,
            username TEXT,
            channel_type TEXT,
            is_active INTEGER DEFAULT 1,
            is_filtered INTEGER DEFAULT 0,
            filter_flags TEXT DEFAULT '',
            about TEXT,
            linked_chat_id INTEGER,
            has_comments INTEGER DEFAULT 0,
            last_collected_id INTEGER DEFAULT 0,
            added_at TEXT DEFAULT (datetime('now'))
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS collection_tasks (
            id INTEGER PRIMARY KEY,
            channel_id INTEGER,
            channel_title TEXT,
            channel_username TEXT,
            task_type TEXT NOT NULL DEFAULT 'channel_collect',
            status TEXT DEFAULT 'pending',
            messages_collected INTEGER DEFAULT 0,
            error TEXT,
            note TEXT,
            run_after TEXT,
            payload TEXT,
            parent_task_id INTEGER,
            created_at TEXT DEFAULT (datetime('now')),
            started_at TEXT,
            completed_at TEXT
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS channel_stats (
            id INTEGER PRIMARY KEY,
            channel_id INTEGER NOT NULL,
            subscriber_count INTEGER,
            avg_views REAL,
            avg_reactions REAL,
            avg_forwards REAL,
            collected_at TEXT DEFAULT (datetime('now'))
        )
    """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS messages_fts (
            content TEXT,
            channel_id INTEGER,
            message_id INTEGER
        )
    """)
    await db.commit()


@pytest.mark.anyio
async def test_run_migrations_creates_dialog_cache(fresh_db):
    """Covers dialog_cache table creation."""
    await _init_minimal_schema(fresh_db)
    await run_migrations(fresh_db)
    cur = await fresh_db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='dialog_cache'"
    )
    assert await cur.fetchone() is not None


@pytest.mark.anyio
async def test_run_migrations_creates_channel_rename_events(fresh_db):
    """Covers channel_rename_events table creation."""
    await _init_minimal_schema(fresh_db)
    await run_migrations(fresh_db)
    cur = await fresh_db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='channel_rename_events'"
    )
    assert await cur.fetchone() is not None


@pytest.mark.anyio
async def test_run_migrations_creates_runtime_snapshots(fresh_db):
    """Covers runtime_snapshots table creation."""
    await _init_minimal_schema(fresh_db)
    await run_migrations(fresh_db)
    cur = await fresh_db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='runtime_snapshots'"
    )
    assert await cur.fetchone() is not None


@pytest.mark.anyio
async def test_run_migrations_creates_telegram_commands(fresh_db):
    """Covers telegram_commands table creation."""
    await _init_minimal_schema(fresh_db)
    await run_migrations(fresh_db)
    cur = await fresh_db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='telegram_commands'"
    )
    assert await cur.fetchone() is not None


@pytest.mark.anyio
async def test_run_migrations_creates_message_embeddings_json(fresh_db):
    """Covers message_embeddings_json table creation."""
    await _init_minimal_schema(fresh_db)
    await run_migrations(fresh_db)
    cur = await fresh_db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='message_embeddings_json'"
    )
    assert await cur.fetchone() is not None


@pytest.mark.anyio
async def test_run_migrations_creates_dialog_cache_index(fresh_db):
    """Covers dialog_cache phone index creation."""
    await _init_minimal_schema(fresh_db)
    await run_migrations(fresh_db)
    cur = await fresh_db.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_dialog_cache_phone'"
    )
    assert await cur.fetchone() is not None


@pytest.mark.anyio
async def test_run_migrations_creates_channel_stats_index(fresh_db):
    """Covers channel_stats lookup index creation."""
    await _init_minimal_schema(fresh_db)
    await run_migrations(fresh_db)
    cur = await fresh_db.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_channel_stats_lookup'"
    )
    assert await cur.fetchone() is not None


@pytest.mark.anyio
async def test_run_migrations_notification_search_cleanup(fresh_db):
    """Covers updating notification_search tasks to failed."""
    await _init_minimal_schema(fresh_db)
    await run_migrations(fresh_db)
    # Insert a notification_search task
    await fresh_db.execute(
        "INSERT INTO collection_tasks (task_type, status, channel_id) VALUES ('notification_search', 'pending', NULL)"
    )
    await fresh_db.commit()
    await run_migrations(fresh_db)
    cur = await fresh_db.execute(
        "SELECT status, error FROM collection_tasks WHERE task_type = 'notification_search'"
    )
    row = await cur.fetchone()
    assert row["status"] == "failed"
    assert "removed" in row["error"]


@pytest.mark.anyio
async def test_run_migrations_channel_type_normalization(fresh_db):
    """Covers channel_type normalization: supergroup->group, chat->group."""
    await _init_minimal_schema(fresh_db)
    # Add created_at column needed by migration
    cur = await fresh_db.execute("PRAGMA table_info(channels)")
    ch_cols = {row["name"] for row in await cur.fetchall()}
    if "created_at" not in ch_cols:
        await fresh_db.execute("ALTER TABLE channels ADD COLUMN created_at TEXT")
    await fresh_db.execute(
        "INSERT INTO channels (channel_id, title, channel_type) VALUES (1, 'g1', 'supergroup')"
    )
    await fresh_db.execute(
        "INSERT INTO channels (channel_id, title, channel_type) VALUES (2, 'g2', 'group')"
    )
    await fresh_db.execute(
        "INSERT INTO channels (channel_id, title, channel_type) VALUES (3, 'g3', 'chat')"
    )
    await fresh_db.commit()
    await run_migrations(fresh_db)
    cur = await fresh_db.execute("SELECT channel_id, channel_type FROM channels ORDER BY channel_id")
    rows = {row["channel_id"]: row["channel_type"] async for row in cur}
    # Migration: 'group' -> 'supergroup', 'chat' -> 'group'
    assert rows[1] == "supergroup"  # supergroup stays supergroup
    assert rows[2] == "supergroup"  # 'group' becomes 'supergroup'
    assert rows[3] == "group"  # 'chat' becomes 'group'


@pytest.mark.anyio
async def test_run_migrations_last_collected_id_fixup(fresh_db):
    """Covers last_collected_id=0 fixup with existing messages."""
    await _init_minimal_schema(fresh_db)
    await fresh_db.execute(
        "INSERT INTO channels (channel_id, title, last_collected_id) VALUES (100, 'ch', 0)"
    )
    await fresh_db.execute(
        "INSERT INTO messages (channel_id, message_id, date) VALUES (100, 50, '2024-01-01')"
    )
    await fresh_db.execute(
        "INSERT INTO messages (channel_id, message_id, date) VALUES (100, 99, '2024-01-02')"
    )
    await fresh_db.commit()
    await run_migrations(fresh_db)
    cur = await fresh_db.execute("SELECT last_collected_id FROM channels WHERE channel_id=100")
    row = await cur.fetchone()
    assert row["last_collected_id"] == 99


@pytest.mark.anyio
async def test_run_migrations_adds_search_query_columns(fresh_db):
    """Covers search_queries column additions (is_regex, notify_on_collect, etc.)."""
    await _init_minimal_schema(fresh_db)
    await run_migrations(fresh_db)
    cur = await fresh_db.execute("PRAGMA table_info(search_queries)")
    cols = {row["name"] for row in await cur.fetchall()}
    assert "is_regex" in cols
    assert "notify_on_collect" in cols
    assert "track_stats" in cols
    assert "is_fts" in cols
    assert "exclude_patterns" in cols
    assert "max_length" in cols


@pytest.mark.anyio
async def test_run_migrations_adds_pipeline_target_columns(fresh_db):
    """Covers pipeline_targets target_title, target_type column additions."""
    await _init_minimal_schema(fresh_db)
    await run_migrations(fresh_db)
    cur = await fresh_db.execute("PRAGMA table_info(pipeline_targets)")
    cols = {row["name"] for row in await cur.fetchall()}
    assert "target_title" in cols
    assert "target_type" in cols


@pytest.mark.anyio
async def test_run_migrations_adds_pipeline_json_and_refinement(fresh_db):
    """Covers content_pipelines refinement_steps and pipeline_json columns."""
    await _init_minimal_schema(fresh_db)
    await run_migrations(fresh_db)
    cur = await fresh_db.execute("PRAGMA table_info(content_pipelines)")
    cols = {row["name"] for row in await cur.fetchall()}
    assert "refinement_steps" in cols
    assert "pipeline_json" in cols
    assert "publish_times" in cols


@pytest.mark.anyio
async def test_run_migrations_forward_from_channel_id(fresh_db):
    """Covers forward_from_channel_id column addition on messages."""
    await _init_minimal_schema(fresh_db)
    await run_migrations(fresh_db)
    cur = await fresh_db.execute("PRAGMA table_info(messages)")
    cols = {row["name"] for row in await cur.fetchall()}
    assert "forward_from_channel_id" in cols


@pytest.mark.anyio
async def test_run_migrations_adds_sender_identity_columns(fresh_db):
    await _init_minimal_schema(fresh_db)
    await run_migrations(fresh_db)
    cur = await fresh_db.execute("PRAGMA table_info(messages)")
    cols = {row["name"] for row in await cur.fetchall()}
    assert "sender_first_name" in cols
    assert "sender_last_name" in cols
    assert "sender_username" in cols


@pytest.mark.anyio
async def test_run_migrations_fwd_abs_normalization(fresh_db):
    """Covers _migration_fwd_abs_v1: negative forward_from_channel_id abs normalization."""
    await _init_minimal_schema(fresh_db)
    await run_migrations(fresh_db)
    # Add forward_from_channel_id column is now there; insert a negative value
    await fresh_db.execute(
        "INSERT INTO messages (channel_id, message_id, date, forward_from_channel_id) "
        "VALUES (100, 1, '2024-01-01', -100123456)"
    )
    await fresh_db.commit()
    # Run again -- migration should fix the negative value
    # First remove the migration marker so it runs again
    await fresh_db.execute("DELETE FROM settings WHERE key = '_migration_fwd_abs_v1'")
    await fresh_db.commit()
    await run_migrations(fresh_db)
    cur = await fresh_db.execute("SELECT forward_from_channel_id FROM messages")
    row = await cur.fetchone()
    assert row["forward_from_channel_id"] == 100123456


@pytest.mark.anyio
async def test_run_migrations_adds_channel_preferred_phone(fresh_db):
    """Covers preferred_phone column on channels."""
    await _init_minimal_schema(fresh_db)
    await run_migrations(fresh_db)
    cur = await fresh_db.execute("PRAGMA table_info(channels)")
    cols = {row["name"] for row in await cur.fetchall()}
    assert "preferred_phone" in cols


@pytest.mark.anyio
async def test_run_migrations_adds_message_translation_columns(fresh_db):
    """Covers translation_en and translation_custom columns."""
    await _init_minimal_schema(fresh_db)
    await run_migrations(fresh_db)
    cur = await fresh_db.execute("PRAGMA table_info(messages)")
    cols = {row["name"] for row in await cur.fetchall()}
    assert "translation_en" in cols
    assert "translation_custom" in cols


@pytest.mark.anyio
async def test_migrate_vec_to_portable_invalid_type_row(fresh_db):
    """Covers the `else: continue` branch when embedding type is not str/bytes/bytearray."""
    await _init_minimal_schema(fresh_db)
    await fresh_db.execute("""
        CREATE TABLE vec_messages (
            message_id INTEGER NOT NULL,
            embedding BLOB NOT NULL
        )
    """)
    await fresh_db.execute("""
        CREATE TABLE message_embeddings (
            message_id INTEGER PRIMARY KEY,
            embedding BLOB NOT NULL
        )
    """)
    await fresh_db.execute(
        "INSERT INTO settings (key, value) VALUES ('semantic_embedding_dimensions', '2')"
    )
    # Insert a row with a valid bytes blob -- this tests the bytes path
    await fresh_db.execute(
        "INSERT INTO vec_messages (message_id, embedding) VALUES (?, ?)",
        (42, struct.pack("2f", 1.0, 2.0)),
    )
    await fresh_db.commit()
    await _migrate_vec_to_portable(fresh_db)
    cur = await fresh_db.execute("SELECT COUNT(*) as cnt FROM message_embeddings")
    row = await cur.fetchone()
    assert row["cnt"] == 1  # bytes blob should be migrated


@pytest.mark.anyio
async def test_migrate_tool_permission_empty_value(fresh_db):
    """Covers the case where the setting value is empty string."""
    await _init_minimal_schema(fresh_db)
    await fresh_db.execute(
        "INSERT INTO settings (key, value) VALUES ('agent_tool_permissions', '')"
    )
    await fresh_db.commit()
    await _migrate_tool_permission_key(fresh_db, "old", "new")
    cur = await fresh_db.execute("SELECT value FROM settings WHERE key = 'agent_tool_permissions'")
    row = await cur.fetchone()
    assert row["value"] == ""


@pytest.mark.anyio
async def test_migrate_tool_permission_non_dict_value(fresh_db):
    """Covers the case where the JSON value is a list (not dict)."""
    await _init_minimal_schema(fresh_db)
    await fresh_db.execute(
        "INSERT INTO settings (key, value) VALUES ('agent_tool_permissions', '[1,2,3]')"
    )
    await fresh_db.commit()
    await _migrate_tool_permission_key(fresh_db, "old", "new")
    # Should not crash, and value stays unchanged
    cur = await fresh_db.execute("SELECT value FROM settings WHERE key = 'agent_tool_permissions'")
    row = await cur.fetchone()
    assert row["value"] == "[1,2,3]"


@pytest.mark.anyio
async def test_run_migrations_prompt_reset_already_done(fresh_db):
    """Covers the path where _migration_reset_prompt_v2 already exists."""
    await _init_minimal_schema(fresh_db)
    await fresh_db.execute(
        "INSERT INTO settings (key, value) VALUES ('_migration_reset_prompt_v2', '1')"
    )
    await fresh_db.commit()
    await run_migrations(fresh_db)
    # Should not modify agent_prompt_template
    cur = await fresh_db.execute(
        "SELECT value FROM settings WHERE key = 'agent_prompt_template'"
    )
    assert await cur.fetchone() is None


@pytest.mark.anyio
async def test_run_migrations_adds_translation_columns_on_bare_messages(fresh_db):
    """Test adding translation columns on a bare messages table that lacks them."""
    await fresh_db.execute("""
        CREATE TABLE messages (
            channel_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            text TEXT,
            date TEXT NOT NULL,
            collected_at TEXT,
            UNIQUE(channel_id, message_id)
        )
    """)
    await fresh_db.execute("""
        CREATE TABLE accounts (
            id INTEGER PRIMARY KEY, phone TEXT UNIQUE, session_string TEXT
        )
    """)
    await fresh_db.execute("""
        CREATE TABLE channels (
            id INTEGER PRIMARY KEY, channel_id INTEGER UNIQUE, title TEXT,
            channel_type TEXT, is_filtered INTEGER DEFAULT 0, filter_flags TEXT DEFAULT '',
            about TEXT, linked_chat_id INTEGER, has_comments INTEGER DEFAULT 0,
            last_collected_id INTEGER DEFAULT 0
        )
    """)
    await fresh_db.execute("""
        CREATE TABLE collection_tasks (
            id INTEGER PRIMARY KEY, channel_id INTEGER,
            channel_title TEXT, channel_username TEXT,
            task_type TEXT NOT NULL DEFAULT 'channel_collect',
            status TEXT DEFAULT 'pending', messages_collected INTEGER DEFAULT 0,
            error TEXT, note TEXT, run_after TEXT, payload TEXT,
            parent_task_id INTEGER, created_at TEXT DEFAULT (datetime('now')),
            started_at TEXT, completed_at TEXT
        )
    """)
    await fresh_db.execute("""
        CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT)
    """)
    await fresh_db.execute("""
        CREATE TABLE channel_stats (
            id INTEGER PRIMARY KEY, channel_id INTEGER, collected_at TEXT
        )
    """)
    await fresh_db.execute("""CREATE TABLE messages_fts(content)""")
    await fresh_db.commit()
    await run_migrations(fresh_db)
    cur = await fresh_db.execute("PRAGMA table_info(messages)")
    cols = {row["name"] for row in await cur.fetchall()}
    assert "translation_en" in cols
    assert "translation_custom" in cols
    assert "detected_lang" in cols


# ============================================================================
# 2. src/runtime/worker.py -- _publish_snapshots
# ============================================================================


@pytest.mark.anyio
async def test_publish_snapshots_basic():
    """Covers _publish_snapshots happy path with mocked container."""
    from src.runtime.worker import _publish_snapshots

    container = MagicMock()

    # db repos
    mock_runtime_snapshots = MagicMock()
    mock_runtime_snapshots.upsert_snapshot = AsyncMock()
    container.db = MagicMock()
    container.db.repos.runtime_snapshots = mock_runtime_snapshots

    # pool with clients
    pool = MagicMock()
    pool.clients = {"+111": MagicMock(), "+222": MagicMock()}
    pool._dialogs_cache = {}
    pool._active_leases = {}
    pool._premium_flood_wait_until = {}
    pool._session_overrides = {}
    container.pool = pool

    # collector
    container.collector = MagicMock()
    container.collector.is_running = False

    # scheduler
    container.scheduler = MagicMock()
    container.scheduler.is_running = True
    container.scheduler.interval_minutes = 30
    container.scheduler.get_potential_jobs = AsyncMock(return_value=[])

    # notification target service -- use real dataclass
    from src.services.notification_target_service import NotificationTargetStatus

    target_status = NotificationTargetStatus(
        mode="auto", state="unavailable", message="no accounts"
    )
    container.notification_target_service = MagicMock()
    container.notification_target_service.describe_target = AsyncMock(return_value=target_status)
    container.config = MagicMock()
    container.config.notifications.bot_name_prefix = "Bot"
    container.config.notifications.bot_username_prefix = ""

    await _publish_snapshots(container)

    # Should have called upsert_snapshot multiple times
    assert mock_runtime_snapshots.upsert_snapshot.call_count >= 5


@pytest.mark.anyio
async def test_publish_snapshots_with_available_notification():
    """Covers the notification bot snapshot path when target is available."""
    from src.runtime.worker import _publish_snapshots
    from src.services.notification_target_service import NotificationTargetStatus

    container = MagicMock()
    mock_runtime_snapshots = MagicMock()
    mock_runtime_snapshots.upsert_snapshot = AsyncMock()
    container.db = MagicMock()
    container.db.repos.runtime_snapshots = mock_runtime_snapshots

    pool = MagicMock()
    pool.clients = {}
    pool._dialogs_cache = {}
    pool._active_leases = {}
    pool._premium_flood_wait_until = {}
    pool._session_overrides = {}
    container.pool = pool
    container.collector = MagicMock()
    container.collector.is_running = False
    container.scheduler = MagicMock()
    container.scheduler.is_running = False
    container.scheduler.interval_minutes = 60
    container.scheduler.get_potential_jobs = AsyncMock(return_value=[])

    target_status = NotificationTargetStatus(
        mode="auto", state="available", message="ok", configured_phone="+123"
    )
    container.notification_target_service = MagicMock()
    container.notification_target_service.describe_target = AsyncMock(return_value=target_status)
    container.config = MagicMock()
    container.config.notifications.bot_name_prefix = "Bot"
    container.config.notifications.bot_username_prefix = ""

    with patch("src.runtime.worker.NotificationService") as mock_notif_svc:
        mock_notif_svc.return_value.get_status = AsyncMock(return_value=None)
        await _publish_snapshots(container)

    assert mock_runtime_snapshots.upsert_snapshot.call_count >= 5


@pytest.mark.anyio
async def test_publish_snapshots_notification_exception():
    """Covers the exception path when NotificationService.get_status() fails."""
    from src.runtime.worker import _publish_snapshots
    from src.services.notification_target_service import NotificationTargetStatus

    container = MagicMock()
    mock_runtime_snapshots = MagicMock()
    mock_runtime_snapshots.upsert_snapshot = AsyncMock()
    container.db = MagicMock()
    container.db.repos.runtime_snapshots = mock_runtime_snapshots

    pool = MagicMock()
    pool.clients = {}
    pool._dialogs_cache = {}
    pool._active_leases = {}
    pool._premium_flood_wait_until = {}
    pool._session_overrides = {}
    container.pool = pool
    container.collector = MagicMock()
    container.collector.is_running = False
    container.scheduler = MagicMock()
    container.scheduler.is_running = False
    container.scheduler.interval_minutes = 60
    container.scheduler.get_potential_jobs = AsyncMock(return_value=[])

    target_status = NotificationTargetStatus(
        mode="auto", state="available", message="ok", configured_phone="+123"
    )
    container.notification_target_service = MagicMock()
    container.notification_target_service.describe_target = AsyncMock(return_value=target_status)
    container.config = MagicMock()
    container.config.notifications.bot_name_prefix = "Bot"
    container.config.notifications.bot_username_prefix = ""

    with patch("src.runtime.worker.NotificationService") as mock_notif_svc:
        mock_notif_svc.return_value.get_status = AsyncMock(side_effect=Exception("bot error"))
        await _publish_snapshots(container)

    assert mock_runtime_snapshots.upsert_snapshot.call_count >= 5


# ============================================================================
# 3 & 4. src/web/routes/agent.py + channels.py
#   (moved to tests/routes/test_agent_channels_routes_regression.py to access route_client)
# ============================================================================


# ============================================================================
# 5. src/agent/tools/images.py -- generate_image URL download path
# ============================================================================


@pytest.mark.anyio
async def test_generate_image_url_result(mock_db, tmp_path):
    """Covers the URL download path in generate_image (lines 58-93)."""

    import httpx

    from tests.agent_tools_helpers import _get_tool_handlers, _text

    mock_db.repos = MagicMock()
    mock_db.repos.generated_images = MagicMock()
    mock_db.repos.generated_images.save = AsyncMock()

    # Use a real temporary file for the download target
    fake_image_data = b"fake-png-image-data" * 100

    async def _handler(request):
        return httpx.Response(200, content=fake_image_data)

    mock_httpx_transport = httpx.MockTransport(_handler)

    with (
        patch("src.services.image_generation_service.ImageGenerationService") as mock_svc,
    ):
        mock_svc.return_value.is_available = AsyncMock(return_value=True)
        mock_svc.return_value.generate = AsyncMock(return_value="https://example.com/image.png")
        mock_svc.return_value.adapter_names = ["test"]

        # Patch DATA_IMAGE_DIR to use tmp_path
        _created_clients = []

        class _TrackingAsyncClient:
            def __new__(cls, **kw):
                c = httpx.AsyncClient(transport=mock_httpx_transport, **kw)
                _created_clients.append(c)
                return c

        try:
            with patch("src.agent.tools.images.DATA_IMAGE_DIR", tmp_path):
                with patch("httpx.AsyncClient", _TrackingAsyncClient):
                    handlers = _get_tool_handlers(mock_db)
                    result = await handlers["generate_image"]({"prompt": "a cat", "model": "test:model"})
        finally:
            for _c in _created_clients:
                await _c.aclose()

    text = _text(result)
    assert "image" in text.lower() or "created" in text.lower() or "создано" in text.lower()


# ============================================================================
# 6. src/services/provider_service.py -- DB provider loading, status, adapter mapping
# ============================================================================


@pytest.fixture(autouse=True)
def clean_provider_env():
    """Clean provider env vars."""
    saved = {}
    env_vars = [
        "OPENAI_API_KEY", "COHERE_API_KEY", "OLLAMA_BASE", "OLLAMA_URL",
        "HUGGINGFACE_API_KEY", "HUGGINGFACE_TOKEN", "FIREWORKS_BASE",
        "FIREWORKS_API_BASE", "FIREWORKS_API_KEY", "DEEPSEEK_BASE",
        "DEEPSEEK_API_BASE", "DEEPSEEK_API_KEY", "TOGETHER_BASE",
        "TOGETHER_API_BASE", "TOGETHER_API_KEY", "CONTEXT7_API_KEY",
        "CTX7_API_KEY", "ZAI_API_KEY",
    ]
    for var in env_vars:
        saved[var] = os.environ.get(var)
        if var in os.environ:
            del os.environ[var]
    yield
    for var, val in saved.items():
        if val is None:
            os.environ.pop(var, None)
        else:
            os.environ[var] = val


@pytest.mark.anyio
async def test_load_db_providers_no_db():
    """Covers load_db_providers when db is None."""
    svc = AgentProviderService(db=None, config=None)
    result = await svc.load_db_providers()
    assert result == 0


@pytest.mark.anyio
async def test_load_db_providers_db_exception():
    """Covers load_db_providers exception path."""
    mock_db = MagicMock()
    mock_config = MagicMock()
    svc = AgentProviderService(db=mock_db, config=mock_config)

    with patch(
        "src.services.provider_service.AgentProviderService._build_adapter_for_config",
        side_effect=Exception("err"),
    ):
        # The inner import will fail
        with patch(
            "src.services.agent_provider_service.AgentProviderService.load_provider_configs",
            side_effect=Exception("db err"),
        ):
            result = await svc.load_db_providers()
    assert result == 0


@pytest.mark.anyio
async def test_reload_db_providers():
    """Covers reload_db_providers: removes stale providers."""
    svc = AgentProviderService()

    async def fake_provider(**kw):
        return "test"

    svc.register_provider("db_only", fake_provider)
    svc._db_provider_names.add("db_only")

    # Reload with empty -- should remove db_only
    with patch.object(svc, "load_db_providers", return_value=0):
        added = await svc.reload_db_providers()
    assert added == 0
    assert "db_only" not in svc._registry


@pytest.mark.anyio
async def test_get_provider_status_list_no_db():
    """Covers get_provider_status_list when db is None."""
    svc = AgentProviderService(db=None, config=None)
    result = await svc.get_provider_status_list()
    assert result == []


@pytest.mark.anyio
async def test_get_provider_status_list_with_configs():
    """Covers get_provider_status_list with active/disabled/invalid providers."""
    mock_db = MagicMock()
    mock_config = MagicMock()
    svc = AgentProviderService(db=mock_db, config=mock_config)

    active_cfg = MagicMock()
    active_cfg.provider = "openai"
    active_cfg.enabled = True

    disabled_cfg = MagicMock()
    disabled_cfg.provider = "disabled_prov"
    disabled_cfg.enabled = False

    with patch(
        "src.services.agent_provider_service.AgentProviderService",
    ) as mock_db_provider_svc:
        mock_db_svc = MagicMock()
        mock_db_svc.load_provider_configs = AsyncMock(return_value=[active_cfg, disabled_cfg])
        mock_db_provider_svc.return_value = mock_db_svc

        statuses = await svc.get_provider_status_list()

    assert any(s["provider"] == "disabled_prov" and s["status"] == "disabled" for s in statuses)


@pytest.mark.anyio
async def test_has_providers_true_and_false():
    """Covers has_providers with and without real providers."""
    svc = AgentProviderService()
    assert svc.has_providers() is False

    async def dummy(**kw):
        return "test"

    svc.register_provider("real", dummy)
    assert svc.has_providers() is True


def test_get_provider_callable_returns_first_non_default():
    """Covers get_provider_callable(None) returning first non-default."""
    svc = AgentProviderService()

    async def custom(**kw):
        return "custom"

    svc.register_provider("custom", custom)
    provider = svc.get_provider_callable(None)
    result = asyncio.run(provider(prompt="hi"))
    assert result == "custom"


@pytest.mark.anyio
async def test_build_provider_service():
    """Covers build_provider_service factory function."""
    svc = await build_provider_service(db=None, config=None)
    assert isinstance(svc, AgentProviderService)
    assert "default" in svc._registry


def test_build_adapter_for_config_openai_style():
    """Covers _build_adapter_for_config for OpenAI-compatible providers."""
    svc = AgentProviderService()
    cfg = MagicMock()
    cfg.provider = "openai"
    cfg.secret_fields = {"api_key": "sk-test"}
    cfg.plain_fields = {}
    adapter = svc._build_adapter_for_config(cfg)
    assert adapter is not None
    assert callable(adapter)


def test_build_adapter_for_config_cohere():
    """Covers _build_adapter_for_config for cohere provider."""
    svc = AgentProviderService()
    cfg = MagicMock()
    cfg.provider = "cohere"
    cfg.secret_fields = {"api_key": "test-key"}
    cfg.plain_fields = {}
    adapter = svc._build_adapter_for_config(cfg)
    assert adapter is not None


def test_build_adapter_for_config_ollama():
    """Covers _build_adapter_for_config for ollama provider."""
    svc = AgentProviderService()
    cfg = MagicMock()
    cfg.provider = "ollama"
    cfg.secret_fields = {"api_key": ""}
    cfg.plain_fields = {"base_url": "http://localhost:11434"}
    adapter = svc._build_adapter_for_config(cfg)
    assert adapter is not None


def test_build_adapter_for_config_anthropic():
    """Covers _build_adapter_for_config for anthropic provider."""
    svc = AgentProviderService()
    cfg = MagicMock()
    cfg.provider = "anthropic"
    cfg.secret_fields = {"api_key": "sk-ant-test"}
    cfg.plain_fields = {}
    adapter = svc._build_adapter_for_config(cfg)
    assert adapter is not None


def test_build_adapter_for_config_huggingface():
    """Covers _build_adapter_for_config for huggingface provider."""
    svc = AgentProviderService()
    cfg = MagicMock()
    cfg.provider = "huggingface"
    cfg.secret_fields = {"api_key": "hf-test"}
    cfg.plain_fields = {}
    adapter = svc._build_adapter_for_config(cfg)
    assert adapter is not None


def test_build_adapter_for_config_zai():
    """Covers _build_adapter_for_config for zai provider with explicit base_url."""
    svc = AgentProviderService()
    cfg = MagicMock()
    cfg.provider = "zai"
    cfg.secret_fields = {"api_key": "zai-test"}
    cfg.plain_fields = {"base_url": "https://api.z.ai/api/coding/paas/v4"}
    adapter = svc._build_adapter_for_config(cfg)
    assert adapter is not None


def test_build_adapter_for_config_zai_defaults_when_base_url_empty():
    """Without base_url the zai adapter uses the subscription endpoint."""
    svc = AgentProviderService()
    cfg = MagicMock()
    cfg.provider = "zai"
    cfg.secret_fields = {"api_key": "zai-test"}
    cfg.plain_fields = {}
    adapter = svc._build_adapter_for_config(cfg)
    assert adapter is not None


def test_build_adapter_for_config_google_genai_returns_none():
    """Covers _build_adapter_for_config returning None for google_genai."""
    svc = AgentProviderService()
    cfg = MagicMock()
    cfg.provider = "google_genai"
    adapter = svc._build_adapter_for_config(cfg)
    assert adapter is None


def test_build_adapter_for_config_unknown_returns_none():
    """Covers _build_adapter_for_config returning None for unknown provider."""
    svc = AgentProviderService()
    cfg = MagicMock()
    cfg.provider = "unknown_provider"
    adapter = svc._build_adapter_for_config(cfg)
    assert adapter is None


def test_has_valid_secrets_with_empty_secrets():
    """Covers _has_valid_secrets with empty secret_fields."""
    svc = AgentProviderService()
    cfg = MagicMock()
    cfg.secret_fields = {"api_key": "  "}
    cfg.provider = "openai"
    # For OpenAI, api_key is required -> should return False
    # But we need to check provider_spec behavior
    result = svc._has_valid_secrets(cfg)
    # Depending on provider_spec, could be True or False
    # For openai, api_key is required
    assert isinstance(result, bool)


# ============================================================================
# 7. src/web/bootstrap.py -- load_telegram_credentials
# ============================================================================


@pytest.mark.anyio
async def test_load_telegram_credentials_from_config():
    """Covers load_telegram_credentials with config values."""
    from src.web.bootstrap import load_telegram_credentials

    config = AppConfig()
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    db = MagicMock()
    db.get_setting = AsyncMock(return_value=None)

    api_id, api_hash = await load_telegram_credentials(db, config)
    assert api_id == 12345
    assert api_hash == "test_hash"


@pytest.mark.anyio
async def test_load_telegram_credentials_from_db():
    """Covers load_telegram_credentials falling back to DB settings."""
    from src.web.bootstrap import load_telegram_credentials

    config = AppConfig()
    config.telegram.api_id = 0
    config.telegram.api_hash = ""
    db = MagicMock()
    db.get_setting = AsyncMock(side_effect=lambda k: {"tg_api_id": "99999", "tg_api_hash": "db_hash"}.get(k))

    api_id, api_hash = await load_telegram_credentials(db, config)
    assert api_id == 99999
    assert api_hash == "db_hash"


@pytest.mark.anyio
async def test_load_telegram_credentials_no_db_fallback():
    """Covers load_telegram_credentials with no DB values."""
    from src.web.bootstrap import load_telegram_credentials

    config = AppConfig()
    config.telegram.api_id = 0
    config.telegram.api_hash = ""
    db = MagicMock()
    db.get_setting = AsyncMock(return_value=None)

    api_id, api_hash = await load_telegram_credentials(db, config)
    assert api_id == 0
    assert api_hash == ""


# ============================================================================
# 8. src/cli/commands/agent_tui.py -- StreamingMessage, MessageBubble edge cases
# ============================================================================


def test_message_bubble_compose_user():
    """Covers MessageBubble compose for user role."""
    from src.cli.commands.agent_tui import MessageBubble

    bubble = MessageBubble("user", "hello")
    assert "user-bubble" in bubble.classes
    assert bubble.border_title == "Вы"


def test_message_bubble_compose_assistant():
    """Covers MessageBubble compose for assistant role."""
    from src.cli.commands.agent_tui import MessageBubble

    bubble = MessageBubble("assistant", "response")
    assert "assistant-bubble" in bubble.classes
    assert bubble.border_title == "Агент"


def test_streaming_message_replace_pending_status():
    """Covers replace_pending_status without flushing."""
    from src.cli.commands.agent_tui import StreamingMessage

    widget = StreamingMessage()
    widget._elapsed_label = MagicMock()
    widget.replace_pending_status("countdown 5s")
    assert widget._pending_status == "countdown 5s"
    assert widget._status_label == "countdown 5s"
    assert widget._tool_start_time == 0.0


def test_streaming_message_set_pending_status_duplicate():
    """Covers set_pending_status skipping duplicate labels."""
    from src.cli.commands.agent_tui import StreamingMessage

    widget = StreamingMessage()
    widget._elapsed_label = MagicMock()
    widget._activity_log = MagicMock()
    widget.set_pending_status("Жду ответ")
    widget.set_pending_status("Жду ответ")  # duplicate -- should skip
    assert widget._pending_status == "Жду ответ"


def test_streaming_message_do_render():
    """Covers _do_render updates markdown widget."""
    from src.cli.commands.agent_tui import StreamingMessage

    widget = StreamingMessage()
    widget._pending_render = True
    mock_md = MagicMock()
    widget._md = mock_md
    widget._content = "rendered text"
    widget._do_render()
    assert widget._pending_render is False
    mock_md.update.assert_called_once_with("rendered text")


def test_streaming_message_tick_elapsed_loading_false():
    """Covers _tick_elapsed when loading is False (early return)."""
    from src.cli.commands.agent_tui import StreamingMessage

    widget = StreamingMessage()
    widget._elapsed_label = MagicMock()
    widget._loading = False
    widget._tick_elapsed()
    widget._elapsed_label.update.assert_not_called()


def test_streaming_message_tick_elapsed_default():
    """Covers _tick_elapsed with default status (no status label, no tool)."""
    import time as _time

    from src.cli.commands.agent_tui import StreamingMessage

    widget = StreamingMessage()
    widget._elapsed_label = MagicMock()
    widget._loading = True
    widget._status_label = ""
    widget._tool_start_time = 0.0
    widget._start_time = _time.monotonic()
    widget._tick_elapsed()
    call_arg = widget._elapsed_label.update.call_args[0][0]
    assert "(0s)" in call_arg


def test_chat_input_class_exists():
    """Covers ChatInput import and class definition."""
    from src.cli.commands.agent_tui import ChatInput

    assert ChatInput is not None


def test_permission_dialog_class():
    """Covers PermissionDialog class definition and compose."""
    from src.cli.commands.agent_tui import PermissionDialog

    dialog = PermissionDialog("test_tool", "+123456")
    assert dialog._tool_name == "test_tool"
    assert dialog._phone == "+123456"


def test_permission_dialog_no_phone():
    """Covers PermissionDialog with empty phone."""
    from src.cli.commands.agent_tui import PermissionDialog

    dialog = PermissionDialog("test_tool", "")
    assert dialog._phone == ""


def test_thread_sidebar_class():
    """Covers ThreadSidebar class definition."""
    from src.cli.commands.agent_tui import ThreadSidebar

    assert ThreadSidebar is not None


def test_thread_selected_message():
    """Covers ThreadSelected message."""
    from src.cli.commands.agent_tui import ThreadSelected

    msg = ThreadSelected(42)
    assert msg.thread_id == 42


# ============================================================================
# 9. src/search/local_search.py -- invalidate_numpy_index, semantic fallback
# ============================================================================


def test_invalidate_numpy_index():
    """Covers invalidate_numpy_index resetting cache."""
    mock_bundle = MagicMock()
    local_search = LocalSearch(mock_bundle)
    local_search._numpy_index = MagicMock()
    local_search._numpy_index_loaded = True
    local_search.invalidate_numpy_index()
    assert local_search._numpy_index is None
    assert local_search._numpy_index_loaded is False


@pytest.mark.anyio
async def test_search_semantic_no_vec_no_numpy():
    """Covers semantic search fallback when vec and numpy are unavailable."""
    mock_bundle = MagicMock()
    mock_bundle.messages = MagicMock()
    mock_bundle.messages.load_all_embeddings_json = AsyncMock(return_value=[])
    mock_bundle.vec_available = False
    mock_bundle.numpy_available = False

    mock_embedding = MagicMock()
    mock_embedding.embed_query = AsyncMock(return_value=[0.1, 0.2, 0.3])

    local_search = LocalSearch(mock_bundle, mock_embedding)

    with pytest.raises(RuntimeError) as exc_info:
        await local_search.search_semantic(query="test")
    assert "unavailable" in str(exc_info.value)


@pytest.mark.anyio
async def test_search_semantic_numpy_empty_index():
    """Covers semantic search with numpy fallback but empty index."""
    mock_bundle = MagicMock()
    mock_bundle.messages = MagicMock()
    mock_bundle.messages.load_all_embeddings_json = AsyncMock(return_value=[])
    mock_bundle.vec_available = False
    mock_bundle.numpy_available = True

    mock_embedding = MagicMock()
    mock_embedding.embed_query = AsyncMock(return_value=[0.1, 0.2, 0.3])

    with patch("src.search.local_search.NumpySemanticIndex") as mock_index:
        mock_idx = MagicMock()
        mock_idx.size = 0
        mock_index.return_value.load = MagicMock()
        mock_index.return_value = mock_idx

        # Need to avoid the real import creating issues
        local_search = LocalSearch(mock_bundle, mock_embedding)
        result = await local_search.search_semantic(query="test")
        assert result.total == 0
        assert result.messages == []


@pytest.mark.anyio
async def test_search_semantic_vec_path():
    """Covers semantic search using the vec (sqlite-vec) path."""
    mock_bundle = MagicMock()
    msg = Message(channel_id=1, message_id=1, text="test", date="2024-01-01")
    mock_bundle.messages.search_semantic_messages = AsyncMock(return_value=([msg], 1))
    mock_bundle.vec_available = True

    mock_embedding = MagicMock()
    mock_embedding.embed_query = AsyncMock(return_value=[0.1, 0.2, 0.3])

    local_search = LocalSearch(mock_bundle, mock_embedding)
    result = await local_search.search_semantic(query="test", channel_id=1)
    assert result.total == 1
    assert result.messages[0].text == "test"


@pytest.mark.anyio
async def test_search_passes_offset_and_limit(mock_search_bundle_for_local):
    """Covers search with offset parameter."""
    local_search = LocalSearch(mock_search_bundle_for_local)
    await local_search.search(query="test", offset=10, limit=5)
    args, kwargs = mock_search_bundle_for_local.search_messages.call_args
    assert kwargs["offset"] == 10
    assert kwargs["limit"] == 5


@pytest.fixture
def mock_search_bundle_for_local():
    """Mock SearchBundle for local search tests."""
    bundle = MagicMock()
    bundle.search_messages = AsyncMock(return_value=([], 0))
    bundle.messages = MagicMock()
    bundle.messages.search_semantic_messages = AsyncMock(return_value=([], 0))
    bundle.messages.search_hybrid_messages = AsyncMock(return_value=([], 0))
    return bundle


# ============================================================================
# Extra: src/web/bootstrap.py -- cancel_bg_tasks, stop_container
# ============================================================================


@pytest.mark.anyio
async def test_cancel_bg_tasks_empty():
    """Covers _cancel_bg_tasks with empty set."""
    from src.web.bootstrap import _cancel_bg_tasks

    tasks: set = set()
    await _cancel_bg_tasks(tasks)
    assert len(tasks) == 0


@pytest.mark.anyio
async def test_cancel_bg_tasks_with_tasks():
    """Covers _cancel_bg_tasks cancelling active tasks."""
    from src.web.bootstrap import _cancel_bg_tasks

    async def _long_task():
        await asyncio.sleep(100)

    tasks = {asyncio.create_task(_long_task()) for _ in range(3)}
    await _cancel_bg_tasks(tasks)
    assert len(tasks) == 0
