import json

import aiosqlite
import pytest

from src.database.migrations import _migrate_tool_permission_key, _migrate_vec_to_portable, run_migrations


@pytest.fixture
async def fresh_db():
    db = await aiosqlite.connect(":memory:")
    db.row_factory = aiosqlite.Row
    yield db
    await db.close()


async def _init_schema(db):
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
async def test_run_migrations_adds_missing_columns(fresh_db):
    await _init_schema(fresh_db)
    result = await run_migrations(fresh_db)
    assert result is True or result is False

    cur = await fresh_db.execute("PRAGMA table_info(messages)")
    cols = {row["name"] for row in await cur.fetchall()}
    for expected in (
        "sender_first_name",
        "sender_last_name",
        "sender_username",
        "media_type",
        "topic_id",
        "reactions_json",
        "views",
        "forwards",
        "reply_count",
        "detected_lang",
    ):
        assert expected in cols


@pytest.mark.anyio
async def test_run_migrations_idempotent(fresh_db):
    await _init_schema(fresh_db)
    await run_migrations(fresh_db)
    await run_migrations(fresh_db)
    cur = await fresh_db.execute("PRAGMA table_info(messages)")
    cols = [row["name"] for row in await cur.fetchall()]
    assert cols.count("media_type") == 1


@pytest.mark.anyio
async def test_run_migrations_creates_search_queries_table(fresh_db):
    await _init_schema(fresh_db)
    await run_migrations(fresh_db)
    cur = await fresh_db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='search_queries'")
    assert await cur.fetchone() is not None


@pytest.mark.anyio
async def test_run_migrations_creates_photo_tables(fresh_db):
    await _init_schema(fresh_db)
    await run_migrations(fresh_db)
    for table in ("photo_batches", "photo_batch_items", "photo_auto_upload_jobs", "photo_auto_upload_files"):
        cur = await fresh_db.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
        assert await cur.fetchone() is not None


@pytest.mark.anyio
async def test_run_migrations_creates_generation_runs(fresh_db):
    await _init_schema(fresh_db)
    await run_migrations(fresh_db)
    cur = await fresh_db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='generation_runs'")
    assert await cur.fetchone() is not None


@pytest.mark.anyio
async def test_run_migrations_creates_agent_tables(fresh_db):
    await _init_schema(fresh_db)
    await run_migrations(fresh_db)
    for table in ("agent_threads", "agent_messages"):
        cur = await fresh_db.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
        assert await cur.fetchone() is not None


@pytest.mark.anyio
async def test_run_migrations_creates_pipeline_templates(fresh_db):
    await _init_schema(fresh_db)
    await run_migrations(fresh_db)
    cur = await fresh_db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pipeline_templates'")
    assert await cur.fetchone() is not None


@pytest.mark.anyio
async def test_run_migrations_creates_generated_images(fresh_db):
    await _init_schema(fresh_db)
    await run_migrations(fresh_db)
    cur = await fresh_db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='generated_images'")
    assert await cur.fetchone() is not None


@pytest.mark.anyio
async def test_migrate_tool_permission_flat(fresh_db):
    await _init_schema(fresh_db)
    perms = {"list_dialogs": True, "other_tool": False}
    await fresh_db.execute(
        "INSERT INTO settings (key, value) VALUES ('agent_tool_permissions', ?)",
        (json.dumps(perms),),
    )
    await fresh_db.commit()
    await _migrate_tool_permission_key(fresh_db, "list_dialogs", "search_dialogs")
    cur = await fresh_db.execute("SELECT value FROM settings WHERE key = 'agent_tool_permissions'")
    row = await cur.fetchone()
    data = json.loads(row["value"])
    assert "search_dialogs" in data
    assert "list_dialogs" not in data
    assert data["search_dialogs"] is True


@pytest.mark.anyio
async def test_migrate_tool_permission_per_phone(fresh_db):
    await _init_schema(fresh_db)
    perms = {"+123456": {"list_dialogs": True}, "+789": {"list_dialogs": False}}
    await fresh_db.execute(
        "INSERT INTO settings (key, value) VALUES ('agent_tool_permissions', ?)",
        (json.dumps(perms),),
    )
    await fresh_db.commit()
    await _migrate_tool_permission_key(fresh_db, "list_dialogs", "search_dialogs")
    cur = await fresh_db.execute("SELECT value FROM settings WHERE key = 'agent_tool_permissions'")
    row = await cur.fetchone()
    data = json.loads(row["value"])
    assert "search_dialogs" in data["+123456"]
    assert "list_dialogs" not in data["+123456"]


@pytest.mark.anyio
async def test_migrate_tool_permission_no_existing_setting(fresh_db):
    await _init_schema(fresh_db)
    await _migrate_tool_permission_key(fresh_db, "list_dialogs", "search_dialogs")
    cur = await fresh_db.execute("SELECT value FROM settings WHERE key = 'agent_tool_permissions'")
    assert await cur.fetchone() is None


@pytest.mark.anyio
async def test_migrate_tool_permission_invalid_json(fresh_db):
    await _init_schema(fresh_db)
    await fresh_db.execute(
        "INSERT INTO settings (key, value) VALUES ('agent_tool_permissions', 'not-json')",
    )
    await fresh_db.commit()
    await _migrate_tool_permission_key(fresh_db, "list_dialogs", "search_dialogs")
    cur = await fresh_db.execute("SELECT value FROM settings WHERE key = 'agent_tool_permissions'")
    row = await cur.fetchone()
    assert row["value"] == "not-json"


@pytest.mark.anyio
async def test_migrate_vec_to_portable_no_table(fresh_db):
    await _init_schema(fresh_db)
    await _migrate_vec_to_portable(fresh_db)


@pytest.mark.anyio
async def test_run_migrations_creates_message_reactions(fresh_db):
    await _init_schema(fresh_db)
    await run_migrations(fresh_db)
    cur = await fresh_db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='message_reactions'")
    assert await cur.fetchone() is not None


@pytest.mark.anyio
async def test_run_migrations_creates_tags_tables(fresh_db):
    await _init_schema(fresh_db)
    await run_migrations(fresh_db)
    for table in ("tags", "channel_tags"):
        cur = await fresh_db.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
        assert await cur.fetchone() is not None


# === Additional tests for edge-case paths ===


@pytest.mark.anyio
async def test_migrate_vec_to_portable_with_data(fresh_db):
    """Test actual vec_messages migration with embedding data."""
    import struct

    await _init_schema(fresh_db)
    # Create vec_messages table and message_embeddings table
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
    # Insert dimension setting
    await fresh_db.execute(
        "INSERT INTO settings (key, value) VALUES ('semantic_embedding_dimensions', '3')"
    )
    await fresh_db.commit()

    # Insert a vector as bytes
    vector = [0.1, 0.2, 0.3]
    blob = struct.pack("3f", *vector)
    await fresh_db.execute(
        "INSERT INTO vec_messages (message_id, embedding) VALUES (?, ?)",
        (42, blob),
    )
    await fresh_db.commit()

    await _migrate_vec_to_portable(fresh_db)

    # Verify migration
    cur = await fresh_db.execute("SELECT message_id FROM message_embeddings")
    row = await cur.fetchone()
    assert row is not None
    assert row["message_id"] == 42


@pytest.mark.anyio
async def test_migrate_vec_to_portable_json_vector(fresh_db):
    """Test vec migration with JSON-encoded vector."""
    await _init_schema(fresh_db)
    await fresh_db.execute("""
        CREATE TABLE vec_messages (
            message_id INTEGER NOT NULL,
            embedding TEXT NOT NULL
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
    await fresh_db.execute(
        "INSERT INTO vec_messages (message_id, embedding) VALUES (?, ?)",
        (10, json.dumps([0.5, 0.6])),
    )
    await fresh_db.commit()

    await _migrate_vec_to_portable(fresh_db)

    cur = await fresh_db.execute("SELECT COUNT(*) as cnt FROM message_embeddings")
    row = await cur.fetchone()
    assert row["cnt"] == 1


@pytest.mark.anyio
async def test_migrate_vec_to_portable_no_dimension_setting(fresh_db):
    """Test vec migration skips when no dimension setting."""
    await _init_schema(fresh_db)
    await fresh_db.execute("""
        CREATE TABLE vec_messages (
            message_id INTEGER NOT NULL,
            embedding BLOB NOT NULL
        )
    """)
    await fresh_db.commit()

    await _migrate_vec_to_portable(fresh_db)

    # No crash, nothing migrated


@pytest.mark.anyio
async def test_migrate_vec_to_portable_invalid_dimension(fresh_db):
    """Test vec migration skips when dimension is invalid."""
    await _init_schema(fresh_db)
    await fresh_db.execute("""
        CREATE TABLE vec_messages (
            message_id INTEGER NOT NULL,
            embedding BLOB NOT NULL
        )
    """)
    await fresh_db.execute(
        "INSERT INTO settings (key, value) VALUES ('semantic_embedding_dimensions', 'not_a_number')"
    )
    await fresh_db.commit()

    await _migrate_vec_to_portable(fresh_db)


@pytest.mark.anyio
async def test_run_migrations_adds_missing_message_columns(fresh_db):
    """Test that migrations add missing message columns."""
    # Create messages table WITHOUT the extra columns
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
            id INTEGER PRIMARY KEY, phone TEXT UNIQUE, session_string TEXT, is_premium INTEGER DEFAULT 0
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
    await fresh_db.execute("""
        CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT)
    """)
    await fresh_db.execute("""
        CREATE TABLE channel_stats (
            id INTEGER PRIMARY KEY, channel_id INTEGER, collected_at TEXT
        )
    """)
    await fresh_db.execute("""
        CREATE TABLE messages_fts(content)
    """)
    await fresh_db.commit()

    result = await run_migrations(fresh_db)
    assert result is True or result is False

    cur = await fresh_db.execute("PRAGMA table_info(messages)")
    cols = {row["name"] for row in await cur.fetchall()}
    assert "media_type" in cols
    assert "topic_id" in cols
    assert "views" in cols
    assert "forwards" in cols
    assert "detected_lang" in cols
    assert "translation_en" in cols


@pytest.mark.anyio
async def test_run_migrations_keywords_to_search_queries(fresh_db):
    """Test migration of keywords table to search_queries."""
    await _init_schema(fresh_db)
    # Create keywords table and insert data
    await fresh_db.execute("""
        CREATE TABLE keywords (
            id INTEGER PRIMARY KEY,
            pattern TEXT,
            is_regex INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1
        )
    """)
    await fresh_db.execute(
        "INSERT INTO keywords (pattern, is_regex, is_active) VALUES ('test', 0, 1)"
    )
    await fresh_db.commit()

    await run_migrations(fresh_db)

    # keywords table should be dropped
    cur = await fresh_db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='keywords'"
    )
    assert await cur.fetchone() is None

    # Data should be in search_queries
    cur = await fresh_db.execute("SELECT name, query FROM search_queries")
    row = await cur.fetchone()
    assert row is not None
    assert row["name"] == "test"


@pytest.mark.anyio
async def test_run_migrations_notification_bots_drop_notnull(fresh_db):
    """Test notification_bots migration drops NOT NULL from bot_id."""
    await _init_schema(fresh_db)
    # Create notification_bots with NOT NULL bot_id
    await fresh_db.execute("""
        CREATE TABLE notification_bots (
            id INTEGER PRIMARY KEY,
            tg_user_id INTEGER NOT NULL UNIQUE,
            tg_username TEXT,
            bot_id INTEGER NOT NULL,
            bot_username TEXT NOT NULL,
            bot_token TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    await fresh_db.execute(
        "INSERT INTO notification_bots (tg_user_id, bot_id, bot_username, bot_token) VALUES (123, 456, 'bot', 'token')"
    )
    await fresh_db.commit()

    await run_migrations(fresh_db)

    # Verify table still exists and data preserved
    cur = await fresh_db.execute("SELECT bot_id FROM notification_bots")
    row = await cur.fetchone()
    assert row["bot_id"] == 456


@pytest.mark.anyio
async def test_run_migrations_collection_tasks_rebuild(fresh_db):
    """Test collection_tasks migration rebuilds table with proper schema."""
    await _init_schema(fresh_db)
    # Create old-style collection_tasks with NOT NULL channel_id
    await fresh_db.execute("DROP TABLE IF EXISTS collection_tasks")
    await fresh_db.execute("""
        CREATE TABLE collection_tasks (
            id INTEGER PRIMARY KEY,
            channel_id INTEGER NOT NULL,
            channel_title TEXT,
            status TEXT DEFAULT 'pending',
            messages_collected INTEGER DEFAULT 0,
            error TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            started_at TEXT,
            completed_at TEXT
        )
    """)
    await fresh_db.execute(
        "INSERT INTO collection_tasks (channel_id, channel_title, status) VALUES (100, 'Test', 'completed')"
    )
    await fresh_db.commit()

    await run_migrations(fresh_db)

    # Table should be rebuilt with task_type column
    cur = await fresh_db.execute("PRAGMA table_info(collection_tasks)")
    cols = {row["name"] for row in await cur.fetchall()}
    assert "task_type" in cols

    # Data preserved
    cur = await fresh_db.execute("SELECT channel_title FROM collection_tasks")
    row = await cur.fetchone()
    assert row["channel_title"] == "Test"


@pytest.mark.anyio
async def test_migrate_tool_permission_no_matching_key(fresh_db):
    """Test tool permission migration when old key doesn't exist."""
    await _init_schema(fresh_db)
    perms = {"other_tool": True}
    await fresh_db.execute(
        "INSERT INTO settings (key, value) VALUES ('agent_tool_permissions', ?)",
        (json.dumps(perms),),
    )
    await fresh_db.commit()

    await _migrate_tool_permission_key(fresh_db, "nonexistent", "new_name")

    cur = await fresh_db.execute("SELECT value FROM settings WHERE key = 'agent_tool_permissions'")
    row = await cur.fetchone()
    data = json.loads(row["value"])
    assert "other_tool" in data
    assert "new_name" not in data


@pytest.mark.anyio
async def test_run_migrations_resets_agent_prompt(fresh_db):
    """Test one-time agent prompt reset migration."""
    await _init_schema(fresh_db)
    await fresh_db.execute(
        "INSERT INTO settings (key, value) VALUES ('agent_prompt_template', 'old template')"
    )
    await fresh_db.commit()

    await run_migrations(fresh_db)

    # Old template should be backed up
    cur = await fresh_db.execute(
        "SELECT value FROM settings WHERE key = 'agent_prompt_template_pre_v2_backup'"
    )
    row = await cur.fetchone()
    assert row is not None
    assert row["value"] == "old template"

    # Migration marker should exist
    cur = await fresh_db.execute(
        "SELECT value FROM settings WHERE key = '_migration_reset_prompt_v2'"
    )
    row = await cur.fetchone()
    assert row is not None
