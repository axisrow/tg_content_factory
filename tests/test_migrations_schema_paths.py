from __future__ import annotations

import aiosqlite
import pytest

from src.database.migrations import ensure_columns, ensure_indexes, run_migrations, table_columns


@pytest.fixture
async def fresh_db():
    db = await aiosqlite.connect(":memory:")
    db.row_factory = aiosqlite.Row
    yield db
    await db.close()


async def _columns(db: aiosqlite.Connection, table: str) -> set[str]:
    cur = await db.execute(f"PRAGMA table_info({table})")
    return {row["name"] for row in await cur.fetchall()}


@pytest.mark.anyio
async def test_table_columns_returns_empty_for_missing_table(fresh_db):
    assert await table_columns(fresh_db, "missing_table") == set()


@pytest.mark.anyio
async def test_ensure_columns_adds_only_missing_columns(fresh_db):
    await fresh_db.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY, existing TEXT)")
    await ensure_columns(
        fresh_db,
        "sample",
        {
            "existing": "existing TEXT",
            "added": "added INTEGER DEFAULT 0",
        },
    )
    await ensure_columns(fresh_db, "sample", {"added": "added INTEGER DEFAULT 0"})

    cols = await _columns(fresh_db, "sample")
    assert cols == {"id", "existing", "added"}


@pytest.mark.anyio
async def test_ensure_indexes_is_idempotent(fresh_db):
    await fresh_db.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY, value TEXT)")
    await ensure_indexes(
        fresh_db,
        ("CREATE INDEX IF NOT EXISTS idx_sample_value ON sample(value)",),
    )
    await ensure_indexes(
        fresh_db,
        ("CREATE INDEX IF NOT EXISTS idx_sample_value ON sample(value)",),
    )
    cur = await fresh_db.execute("SELECT name FROM sqlite_master WHERE type='index' AND name='idx_sample_value'")
    assert await cur.fetchone() is not None


@pytest.mark.anyio
async def test_run_migrations_repairs_minimal_legacy_schema(fresh_db):
    await fresh_db.executescript("""
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            channel_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            text TEXT,
            date TEXT NOT NULL,
            UNIQUE(channel_id, message_id)
        );
        CREATE TABLE accounts (
            id INTEGER PRIMARY KEY,
            phone TEXT UNIQUE NOT NULL,
            session_string TEXT NOT NULL
        );
        CREATE TABLE channels (
            id INTEGER PRIMARY KEY,
            channel_id INTEGER UNIQUE NOT NULL,
            title TEXT
        );
        CREATE TABLE collection_tasks (
            id INTEGER PRIMARY KEY,
            channel_id INTEGER,
            status TEXT DEFAULT 'pending'
        );
        CREATE TABLE settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
    """)
    await run_migrations(fresh_db)

    assert {
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
        "translation_en",
        "translation_custom",
        "forward_from_channel_id",
    } <= await _columns(fresh_db, "messages")
    assert {"is_premium", "flood_wait_until"} <= await _columns(fresh_db, "accounts")
    assert {"channel_type", "preferred_phone"} <= await _columns(fresh_db, "channels")
    assert {"task_type", "run_after", "payload", "parent_task_id"} <= await _columns(
        fresh_db, "collection_tasks"
    )


@pytest.mark.anyio
async def test_run_migrations_creates_schema_owned_tables(fresh_db):
    await run_migrations(fresh_db)
    cur = await fresh_db.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row["name"] for row in await cur.fetchall()}
    assert {
        "search_queries",
        "photo_batches",
        "photo_auto_upload_jobs",
        "generation_runs",
        "notification_bots",
        "agent_threads",
        "agent_messages",
        "message_reactions",
        "message_embeddings_json",
        "generated_images",
        "pipeline_templates",
        "tags",
        "channel_tags",
        "channel_rename_events",
    } <= tables


@pytest.mark.anyio
async def test_run_migrations_preserves_legacy_data_without_rewrites(fresh_db):
    await fresh_db.executescript("""
        CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY,
            channel_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            text TEXT,
            reactions_json TEXT,
            forward_from_channel_id INTEGER,
            date TEXT NOT NULL,
            UNIQUE(channel_id, message_id)
        );
    """)
    await fresh_db.execute(
        "INSERT INTO settings (key, value) VALUES ('agent_prompt_template', 'old template')"
    )
    await fresh_db.execute(
        """
        INSERT INTO messages (
            channel_id, message_id, text, reactions_json, forward_from_channel_id, date
        ) VALUES (-100, 1, 'hello', '[{"emoji":"like","count":2}]', -100123456, '2025-01-01')
        """
    )
    await fresh_db.commit()

    await run_migrations(fresh_db)

    cur = await fresh_db.execute("SELECT value FROM settings WHERE key = 'agent_prompt_template'")
    assert (await cur.fetchone())["value"] == "old template"
    cur = await fresh_db.execute("SELECT forward_from_channel_id FROM messages")
    assert (await cur.fetchone())["forward_from_channel_id"] == -100123456
    cur = await fresh_db.execute("SELECT COUNT(*) AS cnt FROM message_reactions")
    assert (await cur.fetchone())["cnt"] == 0
