from __future__ import annotations

import logging

import aiosqlite

logger = logging.getLogger(__name__)


async def run_migrations(db: aiosqlite.Connection) -> None:
    cur = await db.execute("PRAGMA table_info(messages)")
    columns = {row["name"] for row in await cur.fetchall()}
    if "media_type" not in columns:
        await db.execute("ALTER TABLE messages ADD COLUMN media_type TEXT")
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
            logger.warning("FTS5 index build failed (FTS5 may be unavailable): %s", exc)
