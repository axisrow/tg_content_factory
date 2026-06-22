from __future__ import annotations

import json

import aiosqlite
import pytest

from src.database.migrations import (
    SCHEMA_REPAIR_INDEXES,
    _backfill_messages_fts_if_empty,
    _ensure_initial_analyze,
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

        assert {"is_primary", "is_premium", "flood_wait_until"} <= await _columns(conn, "accounts")
        # #733: single-primary partial unique index is created on legacy DBs.
        cur = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name = ?",
            ("idx_accounts_single_primary",),
        )
        assert await cur.fetchone() is not None
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
async def test_run_migrations_dedupes_multiple_primary_accounts(tmp_path):
    """#733: a legacy DB corrupted with >1 primary account is collapsed to a
    single primary (lowest id wins) before the single-primary unique index is
    created — otherwise the index creation would fail."""
    conn = await _connect(str(tmp_path / "double_primary.db"))
    try:
        await conn.executescript("""
            CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                phone TEXT UNIQUE NOT NULL,
                session_string TEXT NOT NULL,
                is_primary INTEGER DEFAULT 0
            );
        """)
        await conn.execute(
            "INSERT INTO accounts (id, phone, session_string, is_primary) VALUES (1, '+71', 's1', 1)"
        )
        await conn.execute(
            "INSERT INTO accounts (id, phone, session_string, is_primary) VALUES (2, '+72', 's2', 1)"
        )
        await conn.commit()

        await run_migrations(conn)

        cur = await conn.execute("SELECT id FROM accounts WHERE is_primary = 1")
        primaries = [row["id"] for row in await cur.fetchall()]
        assert primaries == [1], "lowest-id primary kept, others demoted"

        cur = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name = ?",
            ("idx_accounts_single_primary",),
        )
        assert await cur.fetchone() is not None
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
        await conn.execute(
            "INSERT INTO collection_tasks (id, channel_id, channel_title) VALUES (2, -1001, 'Chan')"
        )
        await conn.execute(
            "INSERT INTO collection_tasks (id, channel_id, channel_title) VALUES (3, 777, 'Other')"
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

        # Regression (#868 review): the rebuild copies the snapshot, drops, and renames
        # under ONE BEGIN IMMEDIATE — every original row must survive the swap, not just
        # the first. (The snapshot-then-swap window was previously outside the lock, so a
        # concurrent writer could be lost; here we at least lock down full data retention.)
        cur = await conn.execute("SELECT id, channel_id FROM collection_tasks ORDER BY id")
        rows = {r["id"]: r["channel_id"] for r in await cur.fetchall()}
        assert rows == {1: None, 2: -1001, 3: 777}

        await conn.execute(
            "INSERT INTO collection_tasks (channel_id, channel_title, task_type) "
            "VALUES (NULL, 'Stats', 'stats_all')"
        )
        await conn.commit()

        # Idempotent: re-running migrations on the already-rebuilt table is a no-op
        # (channel_id is now nullable, so the rebuild branch is skipped) and must not
        # drop the row just inserted or leave a dangling transaction.
        await run_migrations(conn)
        cur = await conn.execute("SELECT COUNT(*) AS c FROM collection_tasks")
        assert (await cur.fetchone())["c"] == 4
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


async def _fts_match_ids(conn: aiosqlite.Connection, query: str) -> list[int]:
    cur = await conn.execute(
        "SELECT rowid FROM messages_fts WHERE messages_fts MATCH ? ORDER BY rowid",
        (query,),
    )
    return [row["rowid"] for row in await cur.fetchall()]


async def _index_names(conn: aiosqlite.Connection, table: str) -> set[str]:
    cur = await conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name = ?",
        (table,),
    )
    return {row["name"] for row in await cur.fetchall()}


@pytest.mark.anyio
@pytest.mark.aiosqlite_serial
async def test_backfill_rebuilds_empty_fts_for_existing_messages(tmp_path):
    """An empty FTS index over a non-empty messages table is repopulated (#760)."""
    conn = await _connect(str(tmp_path / "stale_fts.db"))
    try:
        await run_migrations(conn)

        # Insert messages, then wipe the FTS index to simulate a database whose
        # rows predate messages_fts (or a dump/restore that never indexed them).
        await conn.execute(
            "INSERT INTO messages (channel_id, message_id, date, text) VALUES (?, ?, ?, ?)",
            (111, 1, "2026-01-01T00:00:00", "привет мир"),
        )
        await conn.execute(
            "INSERT INTO messages (channel_id, message_id, date, text) VALUES (?, ?, ?, ?)",
            (111, 2, "2026-01-02T00:00:00", "погода сегодня хорошая"),
        )
        await conn.execute("INSERT INTO messages_fts(messages_fts) VALUES('delete-all')")
        await conn.commit()
        assert await _fts_match_ids(conn, "привет") == []  # index is now empty

        await _backfill_messages_fts_if_empty(conn)
        await conn.commit()

        assert await _fts_match_ids(conn, "привет") == [1]
        assert await _fts_match_ids(conn, "погода") == [2]
    finally:
        await conn.close()


@pytest.mark.anyio
@pytest.mark.aiosqlite_serial
async def test_run_migrations_drops_obsolete_idx_messages_text(tmp_path):
    """idx_messages_text is a ~21 GB dead weight (#760): dropped on existing DBs,
    never recreated, and the other messages indexes are left intact."""
    conn = await _connect(str(tmp_path / "with_text_index.db"))
    try:
        # Stand up a legacy DB that still carries the obsolete index.
        await conn.executescript("""
            CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                text TEXT,
                date TEXT NOT NULL,
                UNIQUE(channel_id, message_id)
            );
            CREATE INDEX idx_messages_text ON messages(text);
        """)
        await conn.commit()
        assert "idx_messages_text" in await _index_names(conn, "messages")

        await run_migrations(conn)

        indexes = await _index_names(conn, "messages")
        # The obsolete index is gone and the schema does not recreate it.
        assert "idx_messages_text" not in indexes
        # The still-useful indexes the schema does declare are untouched.
        assert {"idx_messages_channel_date", "idx_messages_date"} <= indexes
    finally:
        await conn.close()


@pytest.mark.anyio
@pytest.mark.aiosqlite_serial
async def test_backfill_is_idempotent_and_skips_populated_index(tmp_path):
    """Re-running the backfill must not duplicate rows or error on a full index."""
    conn = await _connect(str(tmp_path / "populated_fts.db"))
    try:
        await run_migrations(conn)
        await conn.execute(
            "INSERT INTO messages (channel_id, message_id, date, text) VALUES (?, ?, ?, ?)",
            (222, 1, "2026-01-01T00:00:00", "уникальное слово"),
        )
        await conn.commit()

        # Trigger already indexed the row; backfill should detect a populated
        # index and skip the rebuild entirely.
        await _backfill_messages_fts_if_empty(conn)
        await _backfill_messages_fts_if_empty(conn)
        await conn.commit()

        assert await _fts_match_ids(conn, "уникальное") == [1]
    finally:
        await conn.close()


@pytest.mark.anyio
@pytest.mark.aiosqlite_serial
async def test_backfill_noop_on_empty_messages(tmp_path):
    """No messages → nothing to index, and no error."""
    conn = await _connect(str(tmp_path / "empty.db"))
    try:
        await run_migrations(conn)
        await _backfill_messages_fts_if_empty(conn)
        assert await _fts_match_ids(conn, "что угодно") == []
    finally:
        await conn.close()


@pytest.mark.anyio
@pytest.mark.aiosqlite_serial
async def test_run_migrations_idx_messages_text_drop_is_idempotent(tmp_path):
    """A fresh DB never has the index; running migrations twice must not error."""
    conn = await _connect(str(tmp_path / "fresh_no_index.db"))
    try:
        await run_migrations(conn)
        await run_migrations(conn)
        assert "idx_messages_text" not in await _index_names(conn, "messages")
    finally:
        await conn.close()


@pytest.mark.anyio
@pytest.mark.aiosqlite_serial
async def test_run_migrations_backfills_message_reactions_date(tmp_path):
    """Legacy reaction rows get their date filled from the parent message (#760),
    and the date-emoji index is created."""
    conn = await _connect(str(tmp_path / "legacy_reactions.db"))
    try:
        # Legacy schema: message_reactions WITHOUT a date column.
        await conn.executescript("""
            CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE messages (
                id INTEGER PRIMARY KEY,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                text TEXT,
                date TEXT NOT NULL,
                UNIQUE(channel_id, message_id)
            );
            CREATE TABLE message_reactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                emoji TEXT NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                UNIQUE(channel_id, message_id, emoji)
            );
        """)
        await conn.execute(
            "INSERT INTO messages (channel_id, message_id, date, text) VALUES (?, ?, ?, ?)",
            (100, 1, "2026-06-01T12:00:00", "hi"),
        )
        await conn.execute(
            "INSERT INTO message_reactions (channel_id, message_id, emoji, count) VALUES (?, ?, ?, ?)",
            (100, 1, "👍", 5),
        )
        await conn.commit()

        await run_migrations(conn)

        # Column added, index created, legacy row backfilled from the message.
        assert "date" in await _columns(conn, "message_reactions")
        cur = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name = ?",
            ("idx_message_reactions_date_emoji",),
        )
        assert await cur.fetchone() is not None
        cur = await conn.execute(
            "SELECT date FROM message_reactions WHERE channel_id = 100 AND message_id = 1"
        )
        assert (await cur.fetchone())["date"] == "2026-06-01T12:00:00"

        # Backfill is gated: a second run is a no-op (flag set) and does not error.
        await run_migrations(conn)
        cur = await conn.execute(
            "SELECT value FROM settings WHERE key = '_migration_reactions_date_backfill_v1'"
        )
        assert (await cur.fetchone())["value"] == "1"

        # Regression (PR #945 review): the date-emoji index must NOT be built up
        # front with the other repair indexes — that would force the 6.8M-row
        # backfill UPDATE to maintain it per row. It is created by the backfill
        # function itself, after the UPDATE.
        assert not any("idx_message_reactions_date_emoji" in s for s in SCHEMA_REPAIR_INDEXES)
    finally:
        await conn.close()


@pytest.mark.anyio
@pytest.mark.aiosqlite_serial
async def test_initial_analyze_seeds_planner_statistics(tmp_path):
    """The one-off ANALYZE populates sqlite_stat1 on a DB with data so the planner
    is no longer blind (#760), and the settings gate makes it idempotent."""
    conn = await _connect(str(tmp_path / "no_stats.db"))
    try:
        await run_migrations(conn)
        # Give the indexed table enough rows that ANALYZE records statistics.
        await conn.executemany(
            "INSERT INTO messages (channel_id, message_id, date, text) VALUES (?, ?, ?, ?)",
            [(777, i, "2026-01-01T00:00:00", f"row {i}") for i in range(50)],
        )
        await conn.commit()

        # run_migrations already set the gate (and ran ANALYZE on the then-empty
        # table). Clear the gate and re-run so ANALYZE records real statistics for
        # the now-populated table.
        await conn.execute("DELETE FROM settings WHERE key = '_migration_analyze_v1'")
        await conn.commit()

        await _ensure_initial_analyze(conn)
        await conn.commit()

        # sqlite_stat1 now holds actual row-count statistics for messages.
        cur = await conn.execute(
            "SELECT count(*) AS n FROM sqlite_stat1 WHERE tbl = 'messages'"
        )
        assert (await cur.fetchone())["n"] > 0
        cur = await conn.execute("SELECT value FROM settings WHERE key = '_migration_analyze_v1'")
        assert (await cur.fetchone())["value"] == "1"  # gate set

        # Second call short-circuits on the gate and must not error.
        await _ensure_initial_analyze(conn)
    finally:
        await conn.close()
