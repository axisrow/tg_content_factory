from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence

import aiosqlite

from src.database.schema import SCHEMA_SQL

logger = logging.getLogger(__name__)


ColumnSpec = Mapping[str, str]


SCHEMA_REPAIR_COLUMNS: Mapping[str, ColumnSpec] = {
    "accounts": {
        "flood_wait_until": "flood_wait_until TEXT",
        "is_premium": "is_premium INTEGER DEFAULT 0",
    },
    "channels": {
        "channel_type": "channel_type TEXT",
        "is_filtered": "is_filtered INTEGER DEFAULT 0",
        "filter_flags": "filter_flags TEXT DEFAULT ''",
        "about": "about TEXT",
        "linked_chat_id": "linked_chat_id INTEGER",
        "has_comments": "has_comments INTEGER DEFAULT 0",
        "created_at": "created_at TEXT",
        "preferred_phone": "preferred_phone TEXT",
    },
    "messages": {
        "sender_first_name": "sender_first_name TEXT",
        "sender_last_name": "sender_last_name TEXT",
        "sender_username": "sender_username TEXT",
        "message_kind": "message_kind TEXT",
        "media_type": "media_type TEXT",
        "service_action_raw": "service_action_raw TEXT",
        "service_action_semantic": "service_action_semantic TEXT",
        "service_action_payload_json": "service_action_payload_json TEXT",
        "sender_kind": "sender_kind TEXT",
        "topic_id": "topic_id INTEGER",
        "reactions_json": "reactions_json TEXT",
        "views": "views INTEGER",
        "forwards": "forwards INTEGER",
        "reply_count": "reply_count INTEGER",
        "collected_at": "collected_at TEXT",
        "forward_from_channel_id": "forward_from_channel_id INTEGER",
        "detected_lang": "detected_lang TEXT",
        "translation_en": "translation_en TEXT",
        "translation_custom": "translation_custom TEXT",
    },
    "collection_tasks": {
        "channel_username": "channel_username TEXT",
        "task_type": "task_type TEXT NOT NULL DEFAULT 'channel_collect'",
        "note": "note TEXT",
        "run_after": "run_after TEXT",
        "payload": "payload TEXT",
        "parent_task_id": "parent_task_id INTEGER",
    },
    "search_queries": {
        "is_regex": "is_regex INTEGER DEFAULT 0",
        "is_fts": "is_fts INTEGER DEFAULT 0",
        "notify_on_collect": "notify_on_collect INTEGER DEFAULT 0",
        "track_stats": "track_stats INTEGER DEFAULT 1",
        "exclude_patterns": "exclude_patterns TEXT DEFAULT ''",
        "max_length": "max_length INTEGER DEFAULT NULL",
    },
    "notification_bots": {
        "tg_username": "tg_username TEXT",
        "bot_id": "bot_id INTEGER",
        "bot_username": "bot_username TEXT",
        "bot_token": "bot_token TEXT",
        "created_at": "created_at TEXT",
    },
    "content_pipelines": {
        "llm_model": "llm_model TEXT",
        "image_model": "image_model TEXT",
        "publish_mode": "publish_mode TEXT NOT NULL DEFAULT 'moderated'",
        "generation_backend": "generation_backend TEXT NOT NULL DEFAULT 'chain'",
        "is_active": "is_active INTEGER NOT NULL DEFAULT 1",
        "last_generated_id": "last_generated_id INTEGER NOT NULL DEFAULT 0",
        "generate_interval_minutes": "generate_interval_minutes INTEGER NOT NULL DEFAULT 60",
        "publish_times": "publish_times TEXT",
        "refinement_steps": "refinement_steps TEXT",
        "pipeline_json": "pipeline_json TEXT",
        "account_phone": "account_phone TEXT",
    },
    "pipeline_targets": {
        "target_title": "target_title TEXT",
        "target_type": "target_type TEXT",
    },
    "generation_runs": {
        "pipeline_id": "pipeline_id INTEGER",
        "status": "status TEXT NOT NULL DEFAULT 'pending'",
        "prompt": "prompt TEXT",
        "generated_text": "generated_text TEXT",
        "metadata": "metadata TEXT",
        "created_at": "created_at TEXT",
        "updated_at": "updated_at TEXT",
        "image_url": "image_url TEXT",
        "moderation_status": "moderation_status TEXT DEFAULT 'pending'",
        "published_at": "published_at TEXT",
        "quality_score": "quality_score REAL",
        "quality_issues": "quality_issues TEXT",
        "variants": "variants TEXT",
        "selected_variant": "selected_variant INTEGER",
    },
    "message_embeddings_json": {
        "embedding": "embedding TEXT",
        "dims": "dims INTEGER NOT NULL DEFAULT 0",
    },
    "generated_images": {
        "model": "model TEXT",
        "image_url": "image_url TEXT",
        "local_path": "local_path TEXT",
        "created_at": "created_at TEXT",
    },
    "pipeline_templates": {
        "description": "description TEXT",
        "category": "category TEXT",
        "template_json": "template_json TEXT",
        "is_builtin": "is_builtin INTEGER DEFAULT 0",
        "created_at": "created_at TEXT",
    },
    "agent_threads": {
        "title": "title TEXT NOT NULL DEFAULT 'Новый тред'",
        "created_at": "created_at TEXT",
    },
    "agent_messages": {
        "thread_id": "thread_id INTEGER",
        "role": "role TEXT",
        "content": "content TEXT",
        "created_at": "created_at TEXT",
    },
}


SCHEMA_REPAIR_INDEXES: Sequence[str] = (
    "CREATE INDEX IF NOT EXISTS idx_messages_detected_lang ON messages(detected_lang)",
    """
    CREATE INDEX IF NOT EXISTS idx_messages_fwd_from_channel
    ON messages(forward_from_channel_id) WHERE forward_from_channel_id IS NOT NULL
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_collection_tasks_type_status_run_after
    ON collection_tasks(task_type, status, run_after)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_generation_runs_pipeline_status
    ON generation_runs(pipeline_id, status)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_generation_runs_moderation
    ON generation_runs(moderation_status, pipeline_id)
    """,
)


async def table_exists(db: aiosqlite.Connection, table: str) -> bool:
    cur = await db.execute(
        "SELECT 1 FROM sqlite_master WHERE type IN ('table', 'view') AND name = ? LIMIT 1",
        (table,),
    )
    return await cur.fetchone() is not None


async def table_columns(db: aiosqlite.Connection, table: str) -> set[str]:
    if not await table_exists(db, table):
        return set()
    cur = await db.execute(f"PRAGMA table_info({table})")
    return {row["name"] if hasattr(row, "keys") else row[1] for row in await cur.fetchall()}


async def ensure_columns(db: aiosqlite.Connection, table: str, columns: ColumnSpec) -> None:
    existing = await table_columns(db, table)
    if not existing:
        return
    for column_name, column_sql in columns.items():
        if column_name not in existing:
            await db.execute(f"ALTER TABLE {table} ADD COLUMN {column_sql}")


async def ensure_indexes(db: aiosqlite.Connection, index_statements: Sequence[str]) -> None:
    for statement in index_statements:
        await db.execute(statement)


async def _rebuild_collection_tasks_if_channel_id_notnull(db: aiosqlite.Connection) -> None:
    if not await table_exists(db, "collection_tasks"):
        return

    cur = await db.execute("PRAGMA table_info(collection_tasks)")
    rows = await cur.fetchall()
    columns = {row["name"]: row for row in rows}
    channel_id_row = columns.get("channel_id")
    if channel_id_row is None or not bool(channel_id_row["notnull"]):
        return

    def expr(column: str, fallback: str = "NULL") -> str:
        return column if column in columns else fallback

    channel_id_expr = "CASE WHEN channel_id = 0 THEN NULL ELSE channel_id END"
    task_type_expr = (
        "task_type"
        if "task_type" in columns
        else "CASE WHEN channel_id = 0 THEN 'stats_all' ELSE 'channel_collect' END"
    )

    await db.execute("DROP TABLE IF EXISTS collection_tasks_tmp")
    await db.execute("""
        CREATE TABLE collection_tasks_tmp (
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
    await db.execute(
        f"""
        INSERT INTO collection_tasks_tmp (
            id,
            channel_id,
            channel_title,
            channel_username,
            task_type,
            status,
            messages_collected,
            error,
            note,
            run_after,
            payload,
            parent_task_id,
            created_at,
            started_at,
            completed_at
        )
        SELECT
            {expr("id")},
            {channel_id_expr},
            {expr("channel_title")},
            {expr("channel_username")},
            {task_type_expr},
            {expr("status", "'pending'")},
            {expr("messages_collected", "0")},
            {expr("error")},
            {expr("note")},
            {expr("run_after")},
            {expr("payload")},
            {expr("parent_task_id")},
            {expr("created_at", "datetime('now')")},
            {expr("started_at")},
            {expr("completed_at")}
        FROM collection_tasks
        """
    )
    await db.execute("DROP TABLE collection_tasks")
    await db.execute("ALTER TABLE collection_tasks_tmp RENAME TO collection_tasks")
    logger.info("Migrated collection_tasks: removed NOT NULL from channel_id")


async def _ensure_fts5_available(db: aiosqlite.Connection) -> bool:
    try:
        await db.execute("CREATE VIRTUAL TABLE IF NOT EXISTS temp._fts5_probe USING fts5(content)")
        await db.execute("DROP TABLE IF EXISTS temp._fts5_probe")
    except Exception as exc:
        logger.warning("FTS5 unavailable; full-text search will use fallback queries: %s", exc)
        return False
    return True


async def run_migrations(db: aiosqlite.Connection) -> bool:
    """Repair the SQLite schema without rewriting existing user data.

    SQLite's ``CREATE TABLE IF NOT EXISTS`` does not add columns to existing
    tables. This function keeps the app bootable on older local databases by
    creating missing tables from the canonical schema and adding missing columns
    that SQLite can add in place. A small set of legacy upgrade migrations is
    retained where additive repair cannot preserve existing runtime contracts.
    """
    await _rebuild_collection_tasks_if_channel_id_notnull(db)

    for table, columns in SCHEMA_REPAIR_COLUMNS.items():
        await ensure_columns(db, table, columns)

    await db.executescript(SCHEMA_SQL)

    for table, columns in SCHEMA_REPAIR_COLUMNS.items():
        await ensure_columns(db, table, columns)

    await ensure_indexes(db, SCHEMA_REPAIR_INDEXES)
    fts_available = await _ensure_fts5_available(db)

    legacy_dialog_search_key = "_".join(("search", "my", "telegram"))
    await _migrate_tool_permission_key(db, "list_dialogs", legacy_dialog_search_key)
    await _migrate_tool_permission_key(db, legacy_dialog_search_key, "search_dialogs")

    cur = await db.execute(
        "SELECT value FROM settings WHERE key = '_migration_zai_base_url_v1' LIMIT 1"
    )
    if not await cur.fetchone():
        await _migrate_zai_legacy_base_url(db)
        await db.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES ('_migration_zai_base_url_v1', '1')"
        )

    cur = await db.execute(
        "SELECT value FROM settings WHERE key = '_migration_zai_base_url_v2' LIMIT 1"
    )
    if not await cur.fetchone():
        await _migrate_zai_empty_base_url_to_coding(db)
        await db.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES ('_migration_zai_base_url_v2', '1')"
        )

    await db.commit()
    return fts_available


async def _migrate_vec_to_portable(db: aiosqlite.Connection) -> None:
    """Deprecated compatibility shim; data migrations are no longer run."""
    _ = db


async def _migrate_zai_legacy_base_url(db: aiosqlite.Connection) -> None:
    """Rewrite legacy Z.AI Anthropic-compatible base_url to the OpenAI-compatible default."""
    import json

    from src.agent.provider_registry import (
        ZAI_GENERAL_BASE_URL,
        is_zai_legacy_anthropic_base_url,
    )

    cur = await db.execute(
        "SELECT value FROM settings WHERE key = 'agent_deepagents_providers_v1' LIMIT 1"
    )
    row = await cur.fetchone()
    if not row or not row["value"]:
        return
    try:
        data = json.loads(row["value"])
    except (json.JSONDecodeError, TypeError):
        return
    if not isinstance(data, list):
        return

    changed = False
    for item in data:
        if not isinstance(item, dict) or item.get("provider") != "zai":
            continue
        plain = item.get("plain_fields")
        if not isinstance(plain, dict):
            continue
        current = str(plain.get("base_url", "") or "")
        if is_zai_legacy_anthropic_base_url(current):
            plain["base_url"] = ZAI_GENERAL_BASE_URL
            item["last_validation_error"] = ""
            changed = True

    if not changed:
        return

    await db.execute(
        "UPDATE settings SET value = ? WHERE key = 'agent_deepagents_providers_v1'",
        (json.dumps(data, ensure_ascii=False),),
    )
    logger.info("Migrated legacy Z.AI Anthropic-compatible base_url to %s", ZAI_GENERAL_BASE_URL)


async def _migrate_zai_empty_base_url_to_coding(db: aiosqlite.Connection) -> None:
    """Backfill empty Z.AI base_url values to the Coding Plan endpoint."""
    import json

    from src.agent.provider_registry import ZAI_CODING_BASE_URL

    cur = await db.execute(
        "SELECT value FROM settings WHERE key = 'agent_deepagents_providers_v1' LIMIT 1"
    )
    row = await cur.fetchone()
    if not row or not row["value"]:
        return
    try:
        data = json.loads(row["value"])
    except (json.JSONDecodeError, TypeError):
        return
    if not isinstance(data, list):
        return

    changed = False
    for item in data:
        if not isinstance(item, dict) or item.get("provider") != "zai":
            continue
        plain = item.get("plain_fields")
        if not isinstance(plain, dict):
            continue
        current = (str(plain.get("base_url", "") or "")).strip().rstrip("/")
        if current == "":
            plain["base_url"] = ZAI_CODING_BASE_URL
            item["last_validation_error"] = ""
            changed = True

    if not changed:
        return

    await db.execute(
        "UPDATE settings SET value = ? WHERE key = 'agent_deepagents_providers_v1'",
        (json.dumps(data, ensure_ascii=False),),
    )
    logger.info("Migrated empty Z.AI base_url to %s", ZAI_CODING_BASE_URL)


async def _migrate_tool_permission_key(
    db: aiosqlite.Connection,
    old_key: str,
    new_key: str,
) -> None:
    """Rename a tool key inside the agent_tool_permissions JSON setting."""
    import json

    cur = await db.execute("SELECT value FROM settings WHERE key = 'agent_tool_permissions' LIMIT 1")
    row = await cur.fetchone()
    if not row or not row["value"]:
        return
    try:
        data = json.loads(row["value"])
    except (json.JSONDecodeError, TypeError):
        return
    if not isinstance(data, dict):
        return

    changed = False
    first_val = next(iter(data.values()), None) if data else None
    if isinstance(first_val, dict):
        for perms in data.values():
            if isinstance(perms, dict) and old_key in perms:
                perms[new_key] = perms.pop(old_key)
                changed = True
    elif old_key in data:
        data[new_key] = data.pop(old_key)
        changed = True

    if not changed:
        return

    await db.execute(
        "UPDATE settings SET value = ? WHERE key = 'agent_tool_permissions'",
        (json.dumps(data, ensure_ascii=False),),
    )
    logger.info("Migrated tool permission key %r -> %r in agent_tool_permissions", old_key, new_key)
