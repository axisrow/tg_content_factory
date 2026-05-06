from __future__ import annotations

import json

import aiosqlite
import pytest

from src.database.migrations import (
    _migrate_tool_permission_key,
    _migrate_vec_to_portable,
    _migrate_zai_empty_base_url_to_coding,
    _migrate_zai_legacy_base_url,
    run_migrations,
)


async def _connect(path: str) -> aiosqlite.Connection:
    conn = await aiosqlite.connect(path)
    conn.row_factory = aiosqlite.Row
    return conn


async def _table_names(conn: aiosqlite.Connection) -> set[str]:
    cur = await conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return {row["name"] for row in await cur.fetchall()}


async def _columns(conn: aiosqlite.Connection, table: str) -> set[str]:
    cur = await conn.execute(f"PRAGMA table_info({table})")
    return {row["name"] for row in await cur.fetchall()}


@pytest.mark.anyio
@pytest.mark.aiosqlite_serial
async def test_run_migrations_creates_missing_schema_tables(tmp_path):
    conn = await _connect(str(tmp_path / "fresh.db"))
    try:
        result = await run_migrations(conn)
        assert isinstance(result, bool)

        tables = await _table_names(conn)
        for table in (
            "generation_runs",
            "notification_bots",
            "agent_threads",
            "agent_messages",
            "message_embeddings_json",
            "generated_images",
            "pipeline_templates",
        ):
            assert table in tables
    finally:
        await conn.close()


@pytest.mark.anyio
@pytest.mark.aiosqlite_serial
async def test_run_migrations_adds_missing_columns_and_indexes(tmp_path):
    conn = await _connect(str(tmp_path / "legacy.db"))
    try:
        await conn.executescript("""
            CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
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
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                text TEXT,
                date TEXT NOT NULL,
                UNIQUE(channel_id, message_id)
            );
            CREATE TABLE collection_tasks (
                id INTEGER PRIMARY KEY,
                channel_id INTEGER,
                status TEXT DEFAULT 'pending'
            );
            CREATE TABLE search_queries (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                query TEXT NOT NULL
            );
            CREATE TABLE content_pipelines (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                prompt_template TEXT NOT NULL
            );
            CREATE TABLE pipeline_targets (
                id INTEGER PRIMARY KEY,
                pipeline_id INTEGER NOT NULL,
                phone TEXT NOT NULL,
                target_dialog_id INTEGER NOT NULL
            );
            CREATE TABLE generation_runs (
                id INTEGER PRIMARY KEY,
                pipeline_id INTEGER,
                status TEXT NOT NULL DEFAULT 'pending'
            );
        """)
        await conn.execute(
            "INSERT INTO messages (channel_id, message_id, date) VALUES (-1001, 1, '2025-01-01')"
        )
        await conn.execute(
            "INSERT INTO settings (key, value) VALUES ('agent_prompt_template', 'legacy')"
        )
        await conn.commit()

        await run_migrations(conn)
        await run_migrations(conn)

        assert {"is_premium", "flood_wait_until"} <= await _columns(conn, "accounts")
        assert {"preferred_phone", "channel_type", "is_filtered"} <= await _columns(conn, "channels")
        assert {
            "forward_from_channel_id",
            "detected_lang",
            "translation_en",
            "translation_custom",
        } <= await _columns(conn, "messages")
        assert {"task_type", "run_after", "payload", "parent_task_id"} <= await _columns(
            conn, "collection_tasks"
        )
        assert {"publish_times", "refinement_steps", "pipeline_json", "account_phone"} <= await _columns(
            conn, "content_pipelines"
        )
        assert {"image_url", "moderation_status", "quality_score", "variants"} <= await _columns(
            conn, "generation_runs"
        )

        cur = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name = ?",
            ("idx_collection_tasks_type_status_run_after",),
        )
        assert await cur.fetchone() is not None

        cur = await conn.execute("SELECT value FROM settings WHERE key = 'agent_prompt_template'")
        row = await cur.fetchone()
        assert row["value"] == "legacy"
    finally:
        await conn.close()


@pytest.mark.anyio
@pytest.mark.aiosqlite_serial
async def test_run_migrations_rebuilds_legacy_collection_tasks_channel_id_notnull(tmp_path):
    conn = await _connect(str(tmp_path / "legacy_collection_tasks.db"))
    try:
        await conn.executescript("""
            CREATE TABLE collection_tasks (
                id INTEGER PRIMARY KEY,
                channel_id INTEGER NOT NULL,
                channel_title TEXT,
                status TEXT DEFAULT 'pending',
                messages_collected INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now'))
            );
        """)
        await conn.execute(
            "INSERT INTO collection_tasks (id, channel_id, channel_title) VALUES (1, 0, 'All')"
        )
        await conn.commit()

        await run_migrations(conn)

        cur = await conn.execute("PRAGMA table_info(collection_tasks)")
        columns = {row["name"]: row for row in await cur.fetchall()}
        assert bool(columns["channel_id"]["notnull"]) is False
        assert "task_type" in columns

        cur = await conn.execute("SELECT channel_id, task_type FROM collection_tasks WHERE id = 1")
        row = await cur.fetchone()
        assert row["channel_id"] is None
        assert row["task_type"] == "stats_all"

        await conn.execute(
            "INSERT INTO collection_tasks (channel_id, channel_title, task_type) "
            "VALUES (NULL, 'Stats', 'stats_all')"
        )
        await conn.commit()
    finally:
        await conn.close()


@pytest.mark.anyio
@pytest.mark.aiosqlite_serial
async def test_legacy_data_migration_helpers_preserve_upgrade_contracts(tmp_path):
    conn = await _connect(str(tmp_path / "legacy_helpers.db"))
    try:
        provider_payload = json.dumps(
            [
                {"provider": "zai", "plain_fields": {"base_url": "https://api.z.ai/api/anthropic"}},
                {"provider": "zai", "plain_fields": {"base_url": ""}},
            ]
        )
        permissions_payload = json.dumps({"list_dialogs": False})
        await conn.executescript("""
            CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE message_embeddings (message_id INTEGER PRIMARY KEY, embedding BLOB NOT NULL);
            CREATE TABLE vec_messages (message_id INTEGER PRIMARY KEY, embedding TEXT);
        """)
        await conn.execute(
            "INSERT INTO settings (key, value) VALUES ('agent_deepagents_providers_v1', ?)",
            (provider_payload,),
        )
        await conn.execute(
            "INSERT INTO settings (key, value) VALUES ('agent_tool_permissions', ?)",
            (permissions_payload,),
        )
        await conn.execute(
            "INSERT INTO vec_messages (message_id, embedding) VALUES (1, '[0.1, 0.2]')"
        )
        await conn.commit()

        await _migrate_vec_to_portable(conn)
        await _migrate_zai_legacy_base_url(conn)
        await _migrate_zai_empty_base_url_to_coding(conn)
        await _migrate_tool_permission_key(conn, "list_dialogs", "search_dialogs")

        cur = await conn.execute("SELECT COUNT(*) AS cnt FROM message_embeddings")
        assert (await cur.fetchone())["cnt"] == 0
        cur = await conn.execute("SELECT value FROM settings WHERE key = 'agent_deepagents_providers_v1'")
        provider_rows = json.loads((await cur.fetchone())["value"])
        assert provider_rows[0]["plain_fields"]["base_url"] == "https://api.z.ai/api/paas/v4"
        assert provider_rows[1]["plain_fields"]["base_url"] == "https://api.z.ai/api/coding/paas/v4"
        cur = await conn.execute("SELECT value FROM settings WHERE key = 'agent_tool_permissions'")
        permissions = json.loads((await cur.fetchone())["value"])
        assert permissions == {"search_dialogs": False}
    finally:
        await conn.close()
