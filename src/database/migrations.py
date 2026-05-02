from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def _migrate_vec_to_portable(db: aiosqlite.Connection) -> None:
    """Migrate legacy vec_messages (sqlite-vec) data to portable message_embeddings table."""
    import json
    import struct

    cur = await db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='vec_messages'"
    )
    if not await cur.fetchone():
        return

    cur = await db.execute(
        "SELECT value FROM settings WHERE key = 'semantic_embedding_dimensions' LIMIT 1"
    )
    dim_row = await cur.fetchone()
    if not dim_row or not dim_row["value"]:
        return
    try:
        int(dim_row["value"])
    except (TypeError, ValueError):
        logger.warning("Invalid semantic_embedding_dimensions setting %r; skipping migration", dim_row["value"])
        return

    cur = await db.execute("SELECT message_id, embedding FROM vec_messages")
    rows = await cur.fetchall()
    if not rows:
        return

    migrated = 0
    for row in rows:
        msg_id = row["message_id"]
        raw = row["embedding"]
        if isinstance(raw, str):
            vector = json.loads(raw)
            blob = struct.pack(f"{len(vector)}f", *vector)
        elif isinstance(raw, (bytes, bytearray)):
            blob = bytes(raw)
        else:
            continue
        await db.execute(
            "INSERT OR IGNORE INTO message_embeddings (message_id, embedding) VALUES (?, ?)",
            (msg_id, blob),
        )
        migrated += 1

    await db.commit()
    logger.info("Migrated %d embeddings from vec_messages to message_embeddings", migrated)


async def run_migrations(db: aiosqlite.Connection) -> bool:
    """Run schema migrations. Returns True if FTS5 is available, False otherwise."""
    cur = await db.execute("PRAGMA table_info(messages)")
    msg_columns = {row["name"] for row in await cur.fetchall()}
    if "sender_first_name" not in msg_columns:
        await db.execute("ALTER TABLE messages ADD COLUMN sender_first_name TEXT")
        await db.commit()
    if "sender_last_name" not in msg_columns:
        await db.execute("ALTER TABLE messages ADD COLUMN sender_last_name TEXT")
        await db.commit()
    if "sender_username" not in msg_columns:
        await db.execute("ALTER TABLE messages ADD COLUMN sender_username TEXT")
        await db.commit()
    if "media_type" not in msg_columns:
        await db.execute("ALTER TABLE messages ADD COLUMN media_type TEXT")
        await db.commit()
    if "message_kind" not in msg_columns:
        await db.execute("ALTER TABLE messages ADD COLUMN message_kind TEXT")
        await db.commit()
    if "service_action_raw" not in msg_columns:
        await db.execute("ALTER TABLE messages ADD COLUMN service_action_raw TEXT")
        await db.commit()
    if "service_action_semantic" not in msg_columns:
        await db.execute("ALTER TABLE messages ADD COLUMN service_action_semantic TEXT")
        await db.commit()
    if "service_action_payload_json" not in msg_columns:
        await db.execute("ALTER TABLE messages ADD COLUMN service_action_payload_json TEXT")
        await db.commit()
    if "sender_kind" not in msg_columns:
        await db.execute("ALTER TABLE messages ADD COLUMN sender_kind TEXT")
        await db.commit()
    if "topic_id" not in msg_columns:
        await db.execute("ALTER TABLE messages ADD COLUMN topic_id INTEGER")
        await db.commit()
    if "reactions_json" not in msg_columns:
        await db.execute("ALTER TABLE messages ADD COLUMN reactions_json TEXT")
        await db.commit()
    if "views" not in msg_columns:
        await db.execute("ALTER TABLE messages ADD COLUMN views INTEGER")
        await db.commit()
    if "forwards" not in msg_columns:
        await db.execute("ALTER TABLE messages ADD COLUMN forwards INTEGER")
        await db.commit()
    if "reply_count" not in msg_columns:
        await db.execute("ALTER TABLE messages ADD COLUMN reply_count INTEGER")
        await db.commit()
    if "detected_lang" not in msg_columns:
        await db.execute("ALTER TABLE messages ADD COLUMN detected_lang TEXT")
        await db.commit()
    if "translation_en" not in msg_columns:
        await db.execute("ALTER TABLE messages ADD COLUMN translation_en TEXT")
        await db.commit()
    if "translation_custom" not in msg_columns:
        await db.execute("ALTER TABLE messages ADD COLUMN translation_custom TEXT")
        await db.commit()

    await db.execute("CREATE INDEX IF NOT EXISTS idx_messages_detected_lang ON messages(detected_lang)")
    await db.commit()

    cur = await db.execute("PRAGMA table_info(accounts)")
    acc_columns = {row["name"] for row in await cur.fetchall()}
    if "is_premium" not in acc_columns:
        await db.execute("ALTER TABLE accounts ADD COLUMN is_premium INTEGER DEFAULT 0")
        await db.commit()

    cur = await db.execute("PRAGMA table_info(channels)")
    ch_columns = {row["name"] for row in await cur.fetchall()}
    if "channel_type" not in ch_columns:
        await db.execute("ALTER TABLE channels ADD COLUMN channel_type TEXT")
        await db.commit()
    if "is_filtered" not in ch_columns:
        await db.execute("ALTER TABLE channels ADD COLUMN is_filtered INTEGER DEFAULT 0")
        await db.commit()
    if "filter_flags" not in ch_columns:
        await db.execute("ALTER TABLE channels ADD COLUMN filter_flags TEXT DEFAULT ''")
        await db.commit()
    if "about" not in ch_columns:
        await db.execute("ALTER TABLE channels ADD COLUMN about TEXT")
        await db.commit()
    if "linked_chat_id" not in ch_columns:
        await db.execute("ALTER TABLE channels ADD COLUMN linked_chat_id INTEGER")
        await db.commit()
    if "has_comments" not in ch_columns:
        await db.execute("ALTER TABLE channels ADD COLUMN has_comments INTEGER DEFAULT 0")
        await db.commit()
    if "created_at" not in ch_columns:
        await db.execute("ALTER TABLE channels ADD COLUMN created_at TEXT")
        await db.commit()
    if "preferred_phone" not in ch_columns:
        await db.execute("ALTER TABLE channels ADD COLUMN preferred_phone TEXT")
        await db.commit()

    cur = await db.execute("PRAGMA table_info(collection_tasks)")
    task_rows = await cur.fetchall()
    task_columns = {row["name"] for row in task_rows}
    task_column_meta = {row["name"]: row for row in task_rows}
    if "run_after" not in task_columns:
        await db.execute("ALTER TABLE collection_tasks ADD COLUMN run_after TEXT")
        await db.commit()
    if "payload" not in task_columns:
        await db.execute("ALTER TABLE collection_tasks ADD COLUMN payload TEXT")
        await db.commit()
    if "parent_task_id" not in task_columns:
        await db.execute("ALTER TABLE collection_tasks ADD COLUMN parent_task_id INTEGER")
        await db.commit()
    if "channel_username" not in task_columns:
        await db.execute("ALTER TABLE collection_tasks ADD COLUMN channel_username TEXT")
        await db.commit()
    if "note" not in task_columns:
        await db.execute("ALTER TABLE collection_tasks ADD COLUMN note TEXT")
        await db.commit()
    channel_id_row = task_column_meta.get("channel_id")
    channel_id_notnull = bool(channel_id_row["notnull"]) if channel_id_row is not None else False
    if "task_type" not in task_columns or channel_id_notnull:
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
        await db.execute("""
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
                id,
                CASE WHEN channel_id = 0 THEN NULL ELSE channel_id END,
                channel_title,
                channel_username,
                CASE WHEN channel_id = 0 THEN 'stats_all' ELSE 'channel_collect' END,
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
            FROM collection_tasks
            """)
        await db.execute("DROP TABLE collection_tasks")
        await db.execute("ALTER TABLE collection_tasks_tmp RENAME TO collection_tasks")
        await db.commit()
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_collection_tasks_type_status_run_after
        ON collection_tasks(task_type, status, run_after)
        """)
    await db.commit()

    await db.execute("""
        CREATE TABLE IF NOT EXISTS telegram_commands (
            id INTEGER PRIMARY KEY,
            command_type TEXT NOT NULL,
            payload TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            requested_by TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            started_at TEXT,
            finished_at TEXT,
            error TEXT,
            result_payload TEXT
        )
        """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_telegram_commands_status_id
        ON telegram_commands(status, id)
        """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS runtime_snapshots (
            snapshot_type TEXT NOT NULL,
            scope TEXT NOT NULL DEFAULT 'global',
            payload TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (snapshot_type, scope)
        )
        """)
    await db.commit()

    # Remove legacy notification_search tasks (path removed in favour of notify_on_collect)
    await db.execute("""
        UPDATE collection_tasks
        SET status = 'failed', error = 'removed: NOTIFICATION_SEARCH path deleted'
        WHERE task_type = 'notification_search' AND status IN ('pending', 'running')
        """)
    await db.commit()

    # Remap legacy channel_type values — only run if 'chat' values still exist
    cur = await db.execute("SELECT COUNT(*) FROM channels WHERE channel_type = 'chat'")
    row = await cur.fetchone()
    if row[0] > 0:
        await db.execute("""
            UPDATE channels SET channel_type = CASE
                WHEN channel_type = 'chat' THEN 'group'
                WHEN channel_type = 'group' THEN 'supergroup'
                ELSE channel_type
            END
            WHERE channel_type IN ('chat', 'group')
        """)
        await db.commit()

    await db.execute("""
        UPDATE channels SET last_collected_id = (
            SELECT COALESCE(MAX(message_id), 0)
            FROM messages WHERE messages.channel_id = channels.channel_id
        ) WHERE last_collected_id = 0 AND EXISTS (
            SELECT 1 FROM messages WHERE messages.channel_id = channels.channel_id
        )
        """)
    await db.commit()

    await db.execute("""
        CREATE TABLE IF NOT EXISTS notification_bots (
            id INTEGER PRIMARY KEY,
            tg_user_id INTEGER NOT NULL UNIQUE,
            tg_username TEXT,
            bot_id INTEGER,
            bot_username TEXT NOT NULL,
            bot_token TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        )
        """)
    await db.commit()

    # Migrate existing notification_bots table: drop NOT NULL on bot_id
    cur = await db.execute("PRAGMA table_info(notification_bots)")
    nb_columns = {row["name"]: row for row in await cur.fetchall()}
    if "bot_id" in nb_columns and nb_columns["bot_id"]["notnull"]:
        await db.execute("""
            CREATE TABLE notification_bots_tmp (
                id INTEGER PRIMARY KEY,
                tg_user_id INTEGER NOT NULL UNIQUE,
                tg_username TEXT,
                bot_id INTEGER,
                bot_username TEXT NOT NULL,
                bot_token TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            )
            """)
        await db.execute("""
            INSERT INTO notification_bots_tmp
                (id, tg_user_id, tg_username, bot_id, bot_username, bot_token, created_at)
            SELECT id, tg_user_id, tg_username, bot_id, bot_username, bot_token, created_at
            FROM notification_bots
            """)
        await db.execute("DROP TABLE notification_bots")
        await db.execute("ALTER TABLE notification_bots_tmp RENAME TO notification_bots")
        await db.commit()
        logger.info("Migrated notification_bots: removed NOT NULL from bot_id")

    await db.execute("""
        CREATE TABLE IF NOT EXISTS search_queries (
            id               INTEGER PRIMARY KEY,
            name             TEXT NOT NULL,
            query            TEXT NOT NULL,
            is_active        INTEGER DEFAULT 1,
            interval_minutes INTEGER NOT NULL DEFAULT 60,
            created_at       TEXT DEFAULT (datetime('now'))
        )
        """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS search_query_stats (
            id          INTEGER PRIMARY KEY,
            query_id    INTEGER NOT NULL,
            match_count INTEGER NOT NULL DEFAULT 0,
            recorded_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (query_id) REFERENCES search_queries(id)
        )
        """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_sqs_query_date
            ON search_query_stats(query_id, recorded_at)
        """)
    await db.commit()

    # Migrate search_queries: add new columns
    cur = await db.execute("PRAGMA table_info(search_queries)")
    sq_columns = {row["name"] for row in await cur.fetchall()}
    if "is_regex" not in sq_columns:
        await db.execute("ALTER TABLE search_queries ADD COLUMN is_regex INTEGER DEFAULT 0")
        await db.commit()
    if "notify_on_collect" not in sq_columns:
        await db.execute(
            "ALTER TABLE search_queries ADD COLUMN notify_on_collect INTEGER DEFAULT 0"
        )
        await db.commit()
    if "track_stats" not in sq_columns:
        await db.execute("ALTER TABLE search_queries ADD COLUMN track_stats INTEGER DEFAULT 1")
        await db.commit()
    if "is_fts" not in sq_columns:
        await db.execute("ALTER TABLE search_queries ADD COLUMN is_fts INTEGER DEFAULT 0")
        await db.commit()
    await db.execute("""
        CREATE TABLE IF NOT EXISTS content_pipelines (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            prompt_template TEXT NOT NULL,
            llm_model TEXT,
            image_model TEXT,
            publish_mode TEXT NOT NULL DEFAULT 'moderated',
            generation_backend TEXT NOT NULL DEFAULT 'chain',
            is_active INTEGER NOT NULL DEFAULT 1,
            last_generated_id INTEGER NOT NULL DEFAULT 0,
            generate_interval_minutes INTEGER NOT NULL DEFAULT 60,
            created_at TEXT DEFAULT (datetime('now'))
        )
        """)
    # Generation runs table for RAG drafts
    await db.execute("""
        CREATE TABLE IF NOT EXISTS generation_runs (
            id INTEGER PRIMARY KEY,
            pipeline_id INTEGER,
            status TEXT NOT NULL DEFAULT 'pending',
            prompt TEXT,
            generated_text TEXT,
            metadata TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT
        )
        """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_generation_runs_pipeline_status
        ON generation_runs(pipeline_id, status)
        """)
    # Ensure new columns for moderation and publishing exist (PR #117)
    cur = await db.execute("PRAGMA table_info(generation_runs)")
    gr_columns = {row["name"] for row in await cur.fetchall()}
    if "image_url" not in gr_columns:
        await db.execute("ALTER TABLE generation_runs ADD COLUMN image_url TEXT")
        await db.commit()
    if "moderation_status" not in gr_columns:
        await db.execute(
            "ALTER TABLE generation_runs ADD COLUMN moderation_status TEXT DEFAULT 'pending'"
        )
        await db.commit()
    if "published_at" not in gr_columns:
        await db.execute("ALTER TABLE generation_runs ADD COLUMN published_at TEXT")
        await db.commit()
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_generation_runs_moderation ON generation_runs(moderation_status, pipeline_id)"
    )
    # Ensure quality scoring columns exist (PR #128)
    if "quality_score" not in gr_columns:
        await db.execute("ALTER TABLE generation_runs ADD COLUMN quality_score REAL")
        await db.commit()
    if "quality_issues" not in gr_columns:
        await db.execute("ALTER TABLE generation_runs ADD COLUMN quality_issues TEXT")
        await db.commit()
    # Ensure A/B testing columns exist (PR #129)
    if "variants" not in gr_columns:
        await db.execute("ALTER TABLE generation_runs ADD COLUMN variants TEXT")
        await db.commit()
    if "selected_variant" not in gr_columns:
        await db.execute("ALTER TABLE generation_runs ADD COLUMN selected_variant INTEGER")
        await db.commit()
    # Ensure publish_times column exists in content_pipelines (PR #125)
    cur = await db.execute("PRAGMA table_info(content_pipelines)")
    cp_columns = {row["name"] for row in await cur.fetchall()}
    if "publish_times" not in cp_columns:
        await db.execute("ALTER TABLE content_pipelines ADD COLUMN publish_times TEXT")
        await db.commit()
    await db.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_sources (
            id INTEGER PRIMARY KEY,
            pipeline_id INTEGER NOT NULL,
            channel_id INTEGER NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(pipeline_id, channel_id),
            FOREIGN KEY (pipeline_id) REFERENCES content_pipelines(id) ON DELETE CASCADE,
            FOREIGN KEY (channel_id) REFERENCES channels(channel_id) ON DELETE RESTRICT
        )
        """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_targets (
            id INTEGER PRIMARY KEY,
            pipeline_id INTEGER NOT NULL,
            phone TEXT NOT NULL,
            target_dialog_id INTEGER NOT NULL,
            target_title TEXT,
            target_type TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(pipeline_id, phone, target_dialog_id),
            FOREIGN KEY (pipeline_id) REFERENCES content_pipelines(id) ON DELETE CASCADE
        )
        """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_content_pipelines_active
        ON content_pipelines(is_active, id)
        """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_pipeline_sources_pipeline
        ON pipeline_sources(pipeline_id)
        """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_pipeline_targets_pipeline
        ON pipeline_targets(pipeline_id)
        """)
    await db.commit()

    cur = await db.execute("PRAGMA table_info(pipeline_targets)")
    pipeline_target_columns = {row["name"] for row in await cur.fetchall()}
    if "target_title" not in pipeline_target_columns:
        await db.execute("ALTER TABLE pipeline_targets ADD COLUMN target_title TEXT")
        await db.commit()
    if "target_type" not in pipeline_target_columns:
        await db.execute("ALTER TABLE pipeline_targets ADD COLUMN target_type TEXT")
        await db.commit()
    if "exclude_patterns" not in sq_columns:
        await db.execute("ALTER TABLE search_queries ADD COLUMN exclude_patterns TEXT DEFAULT ''")
        await db.commit()
    if "max_length" not in sq_columns:
        await db.execute("ALTER TABLE search_queries ADD COLUMN max_length INTEGER DEFAULT NULL")
        await db.commit()

    # Migrate keywords → search_queries and drop keywords table
    cur = await db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='keywords'")
    if await cur.fetchone():
        await db.execute("""
            INSERT INTO search_queries
                (name, query, is_regex, is_active, notify_on_collect, track_stats)
            SELECT pattern, pattern, is_regex, is_active, 1, 0 FROM keywords
            """)
        await db.execute("DROP TABLE keywords")
        await db.commit()
        logger.info("Migrated keywords → search_queries and dropped keywords table")

    await db.execute("""
        CREATE TABLE IF NOT EXISTS photo_batches (
            id INTEGER PRIMARY KEY,
            phone TEXT NOT NULL,
            target_dialog_id INTEGER NOT NULL,
            target_title TEXT,
            target_type TEXT,
            send_mode TEXT NOT NULL DEFAULT 'album',
            caption TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            error TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            last_run_at TEXT
        )
        """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS photo_batch_items (
            id INTEGER PRIMARY KEY,
            batch_id INTEGER,
            phone TEXT NOT NULL,
            target_dialog_id INTEGER NOT NULL,
            target_title TEXT,
            target_type TEXT,
            file_paths TEXT NOT NULL,
            send_mode TEXT NOT NULL DEFAULT 'album',
            caption TEXT,
            schedule_at TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            error TEXT,
            telegram_message_ids TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            started_at TEXT,
            completed_at TEXT,
            FOREIGN KEY (batch_id) REFERENCES photo_batches(id)
        )
        """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_photo_batch_items_status_schedule
        ON photo_batch_items(status, schedule_at)
        """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_photo_batch_items_batch_id
        ON photo_batch_items(batch_id)
        """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS photo_auto_upload_jobs (
            id INTEGER PRIMARY KEY,
            phone TEXT NOT NULL,
            target_dialog_id INTEGER NOT NULL,
            target_title TEXT,
            target_type TEXT,
            folder_path TEXT NOT NULL,
            send_mode TEXT NOT NULL DEFAULT 'album',
            caption TEXT,
            interval_minutes INTEGER NOT NULL DEFAULT 60,
            is_active INTEGER NOT NULL DEFAULT 1,
            error TEXT,
            last_run_at TEXT,
            last_seen_marker TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
        """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS photo_auto_upload_files (
            id INTEGER PRIMARY KEY,
            job_id INTEGER NOT NULL,
            file_path TEXT NOT NULL,
            sent_at TEXT DEFAULT (datetime('now')),
            UNIQUE(job_id, file_path),
            FOREIGN KEY (job_id) REFERENCES photo_auto_upload_jobs(id)
        )
        """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS dialog_cache (
            id INTEGER PRIMARY KEY,
            phone TEXT NOT NULL,
            dialog_id INTEGER NOT NULL,
            title TEXT,
            username TEXT,
            channel_type TEXT NOT NULL,
            deactivate INTEGER NOT NULL DEFAULT 0,
            is_own INTEGER NOT NULL DEFAULT 0,
            cached_at TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(phone, dialog_id)
        )
        """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_dialog_cache_phone
        ON dialog_cache(phone)
        """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_channel_stats_lookup
        ON channel_stats(channel_id, collected_at DESC, id DESC)
        """)
    await db.commit()

    fts_available = True
    cur = await db.execute("SELECT value FROM settings WHERE key = 'fts5_initialized'")
    if not await cur.fetchone():
        try:
            await db.execute("INSERT INTO messages_fts(messages_fts) VALUES('rebuild')")
            await db.execute(
                "INSERT OR IGNORE INTO settings (key, value) VALUES ('fts5_initialized', '1')"
            )
            await db.commit()
            logger.info("FTS5 index built for existing messages")
        except Exception as exc:
            fts_available = False
            logger.error("FTS5 index build failed — full-text search unavailable: %s", exc)

    # Agent chat tables
    await db.execute("""
        CREATE TABLE IF NOT EXISTS agent_threads (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            title      TEXT NOT NULL DEFAULT 'Новый тред',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS agent_messages (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            thread_id  INTEGER NOT NULL REFERENCES agent_threads(id) ON DELETE CASCADE,
            role       TEXT NOT NULL CHECK(role IN ('user', 'assistant')),
            content    TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """)
    await db.commit()

    # Normalized reactions table
    await db.execute("""
        CREATE TABLE IF NOT EXISTS message_reactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id INTEGER NOT NULL,
            message_id INTEGER NOT NULL,
            emoji TEXT NOT NULL,
            count INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (channel_id, message_id)
                REFERENCES messages(channel_id, message_id) ON DELETE CASCADE,
            UNIQUE(channel_id, message_id, emoji)
        )
        """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_message_reactions_channel_msg
        ON message_reactions(channel_id, message_id)
        """)
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_message_reactions_emoji
        ON message_reactions(emoji)
        """)
    await db.commit()

    # Backfill message_reactions from existing reactions_json data
    try:
        await db.execute("""
            INSERT OR IGNORE INTO message_reactions (channel_id, message_id, emoji, count)
            SELECT m.channel_id, m.message_id,
                   json_extract(j.value, '$.emoji'),
                   json_extract(j.value, '$.count')
            FROM messages m, json_each(m.reactions_json) j
            WHERE m.reactions_json IS NOT NULL
              AND json_valid(m.reactions_json) = 1
            """)
        await db.commit()
        logger.info("Backfilled message_reactions from reactions_json")
    except Exception as exc:
        logger.warning("Failed to backfill message_reactions from reactions_json: %s", exc)

    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_collected_at ON messages(collected_at)"
    )
    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_date ON messages(date)"
    )
    await db.commit()

    await _migrate_vec_to_portable(db)

    # Portable semantic backend: JSON-serialised embeddings table (issue #173)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS message_embeddings_json (
            message_id INTEGER PRIMARY KEY,
            embedding  TEXT NOT NULL,
            dims       INTEGER NOT NULL
        )
        """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS generated_images (
            id INTEGER PRIMARY KEY,
            prompt TEXT NOT NULL,
            model TEXT,
            image_url TEXT,
            local_path TEXT,
            created_at TEXT DEFAULT (datetime('now'))
        )
        """)
    await db.commit()

    # Multi-step pipeline refinement (issue #240)
    cur = await db.execute("PRAGMA table_info(content_pipelines)")
    cp2_columns = {row["name"] for row in await cur.fetchall()}
    if "refinement_steps" not in cp2_columns:
        await db.execute("ALTER TABLE content_pipelines ADD COLUMN refinement_steps TEXT")
        await db.commit()
    # Node-based pipeline graph JSON (issue #343)
    if "pipeline_json" not in cp2_columns:
        await db.execute("ALTER TABLE content_pipelines ADD COLUMN pipeline_json TEXT")
        await db.commit()
    if "account_phone" not in cp2_columns:
        await db.execute("ALTER TABLE content_pipelines ADD COLUMN account_phone TEXT")
        await db.commit()

    # Pipeline templates table (issue #343)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            description TEXT,
            category TEXT,
            template_json TEXT NOT NULL,
            is_builtin INTEGER DEFAULT 0,
            created_at TEXT DEFAULT (datetime('now'))
        )
        """)

    # Channel tags (issue #230)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS tags (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL
        )
        """)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS channel_tags (
            channel_pk INTEGER NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
            tag_id     INTEGER NOT NULL REFERENCES tags(id) ON DELETE CASCADE,
            PRIMARY KEY (channel_pk, tag_id)
        )
        """)
    await db.commit()

    # Channel rename events (pending user decisions)
    await db.execute("""
        CREATE TABLE IF NOT EXISTS channel_rename_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            channel_id  INTEGER NOT NULL,
            old_title   TEXT,
            new_title   TEXT,
            old_username TEXT,
            new_username TEXT,
            created_at  TEXT DEFAULT (datetime('now')),
            decided_at  TEXT,
            decision    TEXT
        )
        """)
    await db.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_rename_events_pending
        ON channel_rename_events(channel_id) WHERE decision IS NULL
        """)
    await db.commit()

    legacy_dialog_search_key = "_".join(("search", "my", "telegram"))

    # Preserve historical rename path for older DBs, then migrate to the canonical tool id.
    await _migrate_tool_permission_key(db, "list_dialogs", legacy_dialog_search_key)
    await _migrate_tool_permission_key(db, legacy_dialog_search_key, "search_dialogs")

    # forward_from_channel_id on messages for cross-channel citation tracking (issue #330)
    cur = await db.execute("PRAGMA table_info(messages)")
    msg2_columns = {row["name"] for row in await cur.fetchall()}
    if "forward_from_channel_id" not in msg2_columns:
        await db.execute(
            "ALTER TABLE messages ADD COLUMN forward_from_channel_id INTEGER"
        )
        await db.commit()
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_messages_fwd_from_channel
        ON messages(forward_from_channel_id) WHERE forward_from_channel_id IS NOT NULL
    """)
    await db.commit()

    # Reset agent prompt template to new default (AI Telegram client) — one-time migration
    cur = await db.execute("SELECT value FROM settings WHERE key = '_migration_reset_prompt_v2'")
    if not await cur.fetchone():
        cur = await db.execute("SELECT value FROM settings WHERE key = 'agent_prompt_template'")
        old_prompt_row = await cur.fetchone()
        if old_prompt_row and old_prompt_row["value"]:
            logger.warning(
                "Resetting agent_prompt_template to new default; old value backed up to "
                "'agent_prompt_template_pre_v2_backup'. Restore it manually if needed."
            )
            await db.execute(
                "INSERT OR IGNORE INTO settings (key, value) VALUES ('agent_prompt_template_pre_v2_backup', ?)",
                (old_prompt_row["value"],),
            )
        await db.execute("DELETE FROM settings WHERE key = 'agent_prompt_template'")
        await db.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES ('_migration_reset_prompt_v2', '1')"
        )
        await db.commit()

    # Fix forward_from_channel_id normalization (issue #381)
    # Old code stored -PeerChannel.channel_id; correct value is positive channel_id.
    cur = await db.execute("SELECT value FROM settings WHERE key = '_migration_fwd_abs_v1' LIMIT 1")
    if not await cur.fetchone():
        await db.execute(
            "UPDATE messages SET forward_from_channel_id = ABS(forward_from_channel_id) "
            "WHERE forward_from_channel_id < 0"
        )
        await db.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES ('_migration_fwd_abs_v1', '1')"
        )
        await db.commit()

    # Rewrite legacy Anthropic-compatible Z.AI base_url to the OpenAI-compatible
    # default. Older versions stored https://api.z.ai/api/anthropic[/v1] which
    # the deepagents runtime now rejects (see #518/#519/#526).
    cur = await db.execute(
        "SELECT value FROM settings WHERE key = '_migration_zai_base_url_v1' LIMIT 1"
    )
    if not await cur.fetchone():
        await _migrate_zai_legacy_base_url(db)
        await db.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES ('_migration_zai_base_url_v1', '1')"
        )
        await db.commit()

    # Backfill empty Z.AI base_url values to the Coding Plan endpoint. Explicit
    # general PaaS URLs are left intact because pay-per-token PaaS and Coding
    # Plan endpoints are not interchangeable.
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


async def _migrate_zai_legacy_base_url(db: aiosqlite.Connection) -> None:
    """Rewrite legacy Z.AI Anthropic-compatible base_url to the OpenAI-compatible default.

    The deepagents runtime calls Z.AI through ``init_chat_model("openai", base_url=...)``
    which only works against the ``/api/paas/v4`` family. Existing installs may still
    have ``https://api.z.ai/api/anthropic`` saved from earlier versions; this rewrites
    them in-place and clears the stale ``last_validation_error`` so the UI stops
    showing the old message.
    """
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
    await db.commit()
    logger.info("Migrated legacy Z.AI Anthropic-compatible base_url to %s", ZAI_GENERAL_BASE_URL)


async def _migrate_zai_empty_base_url_to_coding(db: aiosqlite.Connection) -> None:
    """Backfill empty Z.AI base_url values to the Coding Plan endpoint.

    Older versions could save a Z.AI provider with an empty URL because runtime
    loading supplied a default. Empty URLs are now invalid, so this preserves
    upgrade behavior for those installs while leaving explicit pay-per-token
    PaaS URLs untouched.
    """
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
    await db.commit()
    logger.info("Migrated empty Z.AI base_url to %s", ZAI_CODING_BASE_URL)


async def _migrate_tool_permission_key(db: aiosqlite.Connection, old_key: str, new_key: str) -> None:
    """Rename a tool name key inside the agent_tool_permissions JSON setting (both flat and per-phone formats)."""
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
        # Per-phone format: {"phone": {"tool_name": bool, ...}, ...}
        for phone, perms in data.items():
            if isinstance(perms, dict) and old_key in perms:
                perms[new_key] = perms.pop(old_key)
                changed = True
    else:
        # Flat format: {"tool_name": bool, ...}
        if old_key in data:
            data[new_key] = data.pop(old_key)
            changed = True

    if changed:
        await db.execute(
            "UPDATE settings SET value = ? WHERE key = 'agent_tool_permissions'",
            (json.dumps(data, ensure_ascii=False),),
        )
        await db.commit()
        logger.info("Migrated tool permission key %r → %r in agent_tool_permissions", old_key, new_key)
