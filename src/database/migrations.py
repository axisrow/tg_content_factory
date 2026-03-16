from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def run_migrations(db: aiosqlite.Connection) -> bool:
    """Run schema migrations. Returns True if FTS5 is available, False otherwise."""
    cur = await db.execute("PRAGMA table_info(messages)")
    msg_columns = {row["name"] for row in await cur.fetchall()}
    if "media_type" not in msg_columns:
        await db.execute("ALTER TABLE messages ADD COLUMN media_type TEXT")
        await db.commit()
    if "topic_id" not in msg_columns:
        await db.execute("ALTER TABLE messages ADD COLUMN topic_id INTEGER")
        await db.commit()
    if "reactions_json" not in msg_columns:
        await db.execute("ALTER TABLE messages ADD COLUMN reactions_json TEXT")
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
        await db.execute(
            """
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
            """
        )
        await db.execute(
            """
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
            """
        )
        await db.execute("DROP TABLE collection_tasks")
        await db.execute("ALTER TABLE collection_tasks_tmp RENAME TO collection_tasks")
        await db.commit()
    await db.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_collection_tasks_type_status_run_after
        ON collection_tasks(task_type, status, run_after)
        """
    )
    await db.commit()

    # Remove legacy notification_search tasks (path removed in favour of notify_on_collect)
    await db.execute(
        """
        UPDATE collection_tasks
        SET status = 'failed', error = 'removed: NOTIFICATION_SEARCH path deleted'
        WHERE task_type = 'notification_search' AND status IN ('pending', 'running')
        """
    )
    await db.commit()

    await db.execute("UPDATE channels SET channel_type='supergroup' WHERE channel_type='group'")
    await db.execute("UPDATE channels SET channel_type='group' WHERE channel_type='chat'")
    await db.commit()

    await db.execute(
        """
        UPDATE channels SET last_collected_id = (
            SELECT COALESCE(MAX(message_id), 0)
            FROM messages WHERE messages.channel_id = channels.channel_id
        ) WHERE last_collected_id = 0 AND EXISTS (
            SELECT 1 FROM messages WHERE messages.channel_id = channels.channel_id
        )
        """
    )
    await db.commit()

    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS notification_bots (
            id INTEGER PRIMARY KEY,
            tg_user_id INTEGER NOT NULL UNIQUE,
            tg_username TEXT,
            bot_id INTEGER,
            bot_username TEXT NOT NULL,
            bot_token TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now'))
        )
        """
    )
    await db.commit()

    # Migrate existing notification_bots table: drop NOT NULL on bot_id
    cur = await db.execute("PRAGMA table_info(notification_bots)")
    nb_columns = {row["name"]: row for row in await cur.fetchall()}
    if "bot_id" in nb_columns and nb_columns["bot_id"]["notnull"]:
        await db.execute(
            """
            CREATE TABLE notification_bots_tmp (
                id INTEGER PRIMARY KEY,
                tg_user_id INTEGER NOT NULL UNIQUE,
                tg_username TEXT,
                bot_id INTEGER,
                bot_username TEXT NOT NULL,
                bot_token TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        await db.execute(
            """
            INSERT INTO notification_bots_tmp
                (id, tg_user_id, tg_username, bot_id, bot_username, bot_token, created_at)
            SELECT id, tg_user_id, tg_username, bot_id, bot_username, bot_token, created_at
            FROM notification_bots
            """
        )
        await db.execute("DROP TABLE notification_bots")
        await db.execute("ALTER TABLE notification_bots_tmp RENAME TO notification_bots")
        await db.commit()
        logger.info("Migrated notification_bots: removed NOT NULL from bot_id")

    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS search_queries (
            id               INTEGER PRIMARY KEY,
            name             TEXT NOT NULL,
            query            TEXT NOT NULL,
            is_active        INTEGER DEFAULT 1,
            interval_minutes INTEGER NOT NULL DEFAULT 60,
            created_at       TEXT DEFAULT (datetime('now'))
        )
        """
    )
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS search_query_stats (
            id          INTEGER PRIMARY KEY,
            query_id    INTEGER NOT NULL,
            match_count INTEGER NOT NULL DEFAULT 0,
            recorded_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (query_id) REFERENCES search_queries(id)
        )
        """
    )
    await db.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sqs_query_date
            ON search_query_stats(query_id, recorded_at)
        """
    )
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
    if "exclude_patterns" not in sq_columns:
        await db.execute(
            "ALTER TABLE search_queries ADD COLUMN exclude_patterns TEXT DEFAULT ''"
        )
        await db.commit()
    if "max_length" not in sq_columns:
        await db.execute(
            "ALTER TABLE search_queries ADD COLUMN max_length INTEGER DEFAULT NULL"
        )
        await db.commit()

    # Migrate keywords → search_queries and drop keywords table
    cur = await db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='keywords'"
    )
    if await cur.fetchone():
        await db.execute(
            """
            INSERT INTO search_queries
                (name, query, is_regex, is_active, notify_on_collect, track_stats)
            SELECT pattern, pattern, is_regex, is_active, 1, 0 FROM keywords
            """
        )
        await db.execute("DROP TABLE keywords")
        await db.commit()
        logger.info("Migrated keywords → search_queries and dropped keywords table")

    await db.execute(
        """
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
        """
    )
    await db.execute(
        """
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
        """
    )
    await db.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_photo_batch_items_status_schedule
        ON photo_batch_items(status, schedule_at)
        """
    )
    await db.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_photo_batch_items_batch_id
        ON photo_batch_items(batch_id)
        """
    )
    await db.execute(
        """
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
        """
    )
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS photo_auto_upload_files (
            id INTEGER PRIMARY KEY,
            job_id INTEGER NOT NULL,
            file_path TEXT NOT NULL,
            sent_at TEXT DEFAULT (datetime('now')),
            UNIQUE(job_id, file_path),
            FOREIGN KEY (job_id) REFERENCES photo_auto_upload_jobs(id)
        )
        """
    )
    await db.execute(
        """
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
        """
    )
    await db.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dialog_cache_phone
        ON dialog_cache(phone)
        """
    )
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
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_threads (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            title      TEXT NOT NULL DEFAULT 'Новый тред',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    await db.execute(
        """
        CREATE TABLE IF NOT EXISTS agent_messages (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            thread_id  INTEGER NOT NULL REFERENCES agent_threads(id) ON DELETE CASCADE,
            role       TEXT NOT NULL CHECK(role IN ('user', 'assistant')),
            content    TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
        """
    )
    await db.commit()

    # Normalized reactions table
    await db.execute(
        """
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
        """
    )
    await db.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_message_reactions_channel_msg
        ON message_reactions(channel_id, message_id)
        """
    )
    await db.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_message_reactions_emoji
        ON message_reactions(emoji)
        """
    )
    await db.commit()

    # Backfill message_reactions from existing reactions_json data
    try:
        await db.execute(
            """
            INSERT OR IGNORE INTO message_reactions (channel_id, message_id, emoji, count)
            SELECT m.channel_id, m.message_id,
                   json_extract(j.value, '$.emoji'),
                   json_extract(j.value, '$.count')
            FROM messages m, json_each(m.reactions_json) j
            WHERE m.reactions_json IS NOT NULL
              AND json_valid(m.reactions_json) = 1
            """
        )
        await db.commit()
        logger.info("Backfilled message_reactions from reactions_json")
    except Exception as exc:
        logger.warning("Failed to backfill message_reactions from reactions_json: %s", exc)

    await db.execute(
        "CREATE INDEX IF NOT EXISTS idx_messages_collected_at ON messages(collected_at)"
    )
    await db.commit()

    return fts_available
