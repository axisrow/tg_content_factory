from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

import aiosqlite

from src.models import Account, Channel, ChannelStats, CollectionTask, Keyword, Message

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY,
    phone TEXT UNIQUE NOT NULL,
    session_string TEXT NOT NULL,
    is_primary INTEGER DEFAULT 0,
    is_active INTEGER DEFAULT 1,
    flood_wait_until TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS channels (
    id INTEGER PRIMARY KEY,
    channel_id INTEGER UNIQUE NOT NULL,
    title TEXT,
    username TEXT,
    is_active INTEGER DEFAULT 1,
    last_collected_id INTEGER DEFAULT 0,
    added_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY,
    channel_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL,
    sender_id INTEGER,
    sender_name TEXT,
    text TEXT,
    media_type TEXT,
    date TEXT NOT NULL,
    collected_at TEXT DEFAULT (datetime('now')),
    UNIQUE(channel_id, message_id)
);

CREATE TABLE IF NOT EXISTS keywords (
    id INTEGER PRIMARY KEY,
    pattern TEXT NOT NULL,
    is_regex INTEGER DEFAULT 0,
    is_active INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS collection_tasks (
    id INTEGER PRIMARY KEY,
    channel_id INTEGER NOT NULL,
    channel_title TEXT,
    status TEXT DEFAULT 'pending',
    messages_collected INTEGER DEFAULT 0,
    error TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    started_at TEXT,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS search_log (
    id INTEGER PRIMARY KEY,
    phone TEXT NOT NULL,
    query TEXT NOT NULL,
    results_count INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_search_log_phone_date
    ON search_log(phone, created_at);
CREATE TABLE IF NOT EXISTS channel_stats (
    id INTEGER PRIMARY KEY,
    channel_id INTEGER NOT NULL,
    subscriber_count INTEGER,
    avg_views REAL,
    avg_reactions REAL,
    avg_forwards REAL,
    collected_at TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (channel_id) REFERENCES channels(channel_id)
);

CREATE INDEX IF NOT EXISTS idx_channel_stats_channel_date
    ON channel_stats(channel_id, collected_at);
CREATE INDEX IF NOT EXISTS idx_messages_text ON messages(text);
CREATE INDEX IF NOT EXISTS idx_messages_channel_date ON messages(channel_id, date);
"""


class Database:
    def __init__(self, db_path: str = "data/tg_search.db"):
        self._db_path = db_path
        self._db: aiosqlite.Connection | None = None

    async def initialize(self) -> None:
        Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.executescript(_SCHEMA)
        await self._db.commit()
        await self._migrate()

    async def _migrate(self) -> None:
        assert self._db is not None
        cur = await self._db.execute("PRAGMA table_info(messages)")
        columns = {row["name"] for row in await cur.fetchall()}
        if "media_type" not in columns:
            await self._db.execute("ALTER TABLE messages ADD COLUMN media_type TEXT")
            await self._db.commit()

        cur = await self._db.execute("PRAGMA table_info(accounts)")
        acc_columns = {row["name"] for row in await cur.fetchall()}
        if "is_premium" not in acc_columns:
            await self._db.execute(
                "ALTER TABLE accounts ADD COLUMN is_premium INTEGER DEFAULT 0"
            )
            await self._db.commit()

        cur = await self._db.execute("PRAGMA table_info(channels)")
        ch_columns = {row["name"] for row in await cur.fetchall()}
        if "channel_type" not in ch_columns:
            await self._db.execute(
                "ALTER TABLE channels ADD COLUMN channel_type TEXT"
            )
            await self._db.commit()

        # Fix channels where messages were collected but last_collected_id was not updated
        await self._db.execute("""
            UPDATE channels SET last_collected_id = (
                SELECT COALESCE(MAX(message_id), 0)
                FROM messages WHERE messages.channel_id = channels.channel_id
            ) WHERE last_collected_id = 0 AND EXISTS (
                SELECT 1 FROM messages WHERE messages.channel_id = channels.channel_id
            )
        """)
        await self._db.commit()

    async def close(self) -> None:
        if self._db:
            await self._db.close()

    async def execute(self, sql: str, params: tuple = ()) -> aiosqlite.Cursor:
        assert self._db is not None
        return await self._db.execute(sql, params)

    # --- Accounts ---

    async def add_account(self, account: Account) -> int:
        assert self._db is not None
        cur = await self._db.execute(
            """INSERT INTO accounts (phone, session_string, is_primary, is_active, is_premium)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(phone) DO UPDATE SET
                   session_string=excluded.session_string,
                   is_premium=excluded.is_premium""",
            (
                account.phone,
                account.session_string,
                int(account.is_primary),
                int(account.is_active),
                int(account.is_premium),
            ),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    async def get_accounts(self, active_only: bool = False) -> list[Account]:
        assert self._db is not None
        sql = "SELECT * FROM accounts"
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY is_primary DESC, id ASC"
        cur = await self._db.execute(sql)
        rows = await cur.fetchall()
        return [
            Account(
                id=r["id"],
                phone=r["phone"],
                session_string=r["session_string"],
                is_primary=bool(r["is_primary"]),
                is_active=bool(r["is_active"]),
                is_premium=bool(r["is_premium"]) if r["is_premium"] is not None else False,
                flood_wait_until=(
                    datetime.fromisoformat(r["flood_wait_until"]) if r["flood_wait_until"] else None
                ),
                created_at=datetime.fromisoformat(r["created_at"]) if r["created_at"] else None,
            )
            for r in rows
        ]

    async def update_account_flood(self, phone: str, until: datetime | None) -> None:
        assert self._db is not None
        await self._db.execute(
            "UPDATE accounts SET flood_wait_until = ? WHERE phone = ?",
            (until.isoformat() if until else None, phone),
        )
        await self._db.commit()

    async def update_account_premium(self, phone: str, is_premium: bool) -> None:
        assert self._db is not None
        await self._db.execute(
            "UPDATE accounts SET is_premium = ? WHERE phone = ?",
            (int(is_premium), phone),
        )
        await self._db.commit()

    async def set_account_active(self, account_id: int, active: bool) -> None:
        assert self._db is not None
        await self._db.execute(
            "UPDATE accounts SET is_active = ? WHERE id = ?", (int(active), account_id)
        )
        await self._db.commit()

    async def delete_account(self, account_id: int) -> None:
        assert self._db is not None
        await self._db.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
        await self._db.commit()

    # --- Channels ---

    async def add_channel(self, channel: Channel) -> int:
        assert self._db is not None
        cur = await self._db.execute(
            """INSERT INTO channels (channel_id, title, username, channel_type, is_active)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(channel_id) DO UPDATE
               SET title=excluded.title, username=excluded.username,
                   channel_type=excluded.channel_type""",
            (
                channel.channel_id, channel.title, channel.username,
                channel.channel_type, int(channel.is_active),
            ),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    async def get_channels(self, active_only: bool = False) -> list[Channel]:
        assert self._db is not None
        sql = "SELECT * FROM channels"
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY id ASC"
        cur = await self._db.execute(sql)
        rows = await cur.fetchall()
        return [
            Channel(
                id=r["id"],
                channel_id=r["channel_id"],
                title=r["title"],
                username=r["username"],
                channel_type=r["channel_type"] if "channel_type" in r.keys() else None,
                is_active=bool(r["is_active"]),
                last_collected_id=r["last_collected_id"],
                added_at=datetime.fromisoformat(r["added_at"]) if r["added_at"] else None,
            )
            for r in rows
        ]

    async def get_channels_with_counts(self, active_only: bool = False) -> list[Channel]:
        assert self._db is not None
        sql = """
            SELECT c.*, COALESCE(cnt.total, 0) AS message_count
            FROM channels c
            LEFT JOIN (
                SELECT channel_id, COUNT(*) AS total FROM messages GROUP BY channel_id
            ) cnt ON c.channel_id = cnt.channel_id
        """
        if active_only:
            sql += " WHERE c.is_active = 1"
        sql += " ORDER BY c.id ASC"
        cur = await self._db.execute(sql)
        rows = await cur.fetchall()
        return [
            Channel(
                id=r["id"],
                channel_id=r["channel_id"],
                title=r["title"],
                username=r["username"],
                channel_type=r["channel_type"] if "channel_type" in r.keys() else None,
                is_active=bool(r["is_active"]),
                last_collected_id=r["last_collected_id"],
                added_at=datetime.fromisoformat(r["added_at"]) if r["added_at"] else None,
                message_count=r["message_count"],
            )
            for r in rows
        ]

    async def update_channel_last_id(self, channel_id: int, last_id: int) -> None:
        assert self._db is not None
        await self._db.execute(
            "UPDATE channels SET last_collected_id = ? WHERE channel_id = ?",
            (last_id, channel_id),
        )
        await self._db.commit()

    async def set_channel_active(self, pk: int, active: bool) -> None:
        assert self._db is not None
        await self._db.execute(
            "UPDATE channels SET is_active = ? WHERE id = ?", (int(active), pk)
        )
        await self._db.commit()

    async def delete_channel(self, pk: int) -> None:
        assert self._db is not None
        row = await self._db.execute_fetchall(
            "SELECT channel_id FROM channels WHERE id = ?", (pk,)
        )
        if row:
            channel_id = row[0][0]
            await self._db.execute(
                "DELETE FROM messages WHERE channel_id = ?", (channel_id,)
            )
            await self._db.execute(
                "DELETE FROM channel_stats WHERE channel_id = ?", (channel_id,)
            )
        await self._db.execute("DELETE FROM channels WHERE id = ?", (pk,))
        await self._db.commit()

    # --- Messages ---

    async def insert_message(self, msg: Message) -> bool:
        """Insert message with deduplication. Returns True if inserted."""
        assert self._db is not None
        try:
            await self._db.execute(
                """INSERT OR IGNORE INTO messages
                   (channel_id, message_id, sender_id, sender_name, text, media_type, date)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    msg.channel_id,
                    msg.message_id,
                    msg.sender_id,
                    msg.sender_name,
                    msg.text,
                    msg.media_type,
                    msg.date.isoformat(),
                ),
            )
            await self._db.commit()
            return self._db.total_changes > 0
        except Exception:
            return False

    async def insert_messages_batch(self, messages: list[Message]) -> int:
        """Batch insert with deduplication. Returns count of new messages."""
        assert self._db is not None
        if not messages:
            return 0
        data = [
            (
                m.channel_id, m.message_id, m.sender_id, m.sender_name,
                m.text, m.media_type, m.date.isoformat(),
            )
            for m in messages
        ]
        try:
            cur = await self._db.executemany(
                """INSERT OR IGNORE INTO messages
                   (channel_id, message_id, sender_id, sender_name, text, media_type, date)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                data,
            )
            await self._db.commit()
            return cur.rowcount if cur.rowcount >= 0 else len(messages)
        except Exception as e:
            logger.error("Failed to insert batch of %d messages: %s", len(messages), e)
            return 0

    async def search_messages(
        self,
        query: str = "",
        channel_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[Message], int]:
        """Search messages by text and filters. Returns (messages, total_count)."""
        assert self._db is not None
        conditions = []
        params: list = []

        if query:
            conditions.append("m.text LIKE ?")
            params.append(f"%{query}%")
        if channel_id:
            conditions.append("m.channel_id = ?")
            params.append(channel_id)
        if date_from:
            conditions.append("m.date >= ?")
            params.append(date_from)
        if date_to:
            conditions.append("m.date <= ?")
            params.append(date_to)

        where = " WHERE " + " AND ".join(conditions) if conditions else ""

        count_cur = await self._db.execute(
            f"SELECT COUNT(*) as cnt FROM messages m{where}", tuple(params)
        )
        row = await count_cur.fetchone()
        total = row["cnt"] if row else 0

        cur = await self._db.execute(
            f"""SELECT m.*, c.title as channel_title, c.username as channel_username
                FROM messages m
                LEFT JOIN channels c ON m.channel_id = c.channel_id
                {where}
                ORDER BY m.date DESC
                LIMIT ? OFFSET ?""",
            (*params, limit, offset),
        )
        rows = await cur.fetchall()
        messages = [
            Message(
                id=r["id"],
                channel_id=r["channel_id"],
                message_id=r["message_id"],
                sender_id=r["sender_id"],
                sender_name=r["sender_name"],
                text=r["text"],
                media_type=r["media_type"],
                date=datetime.fromisoformat(r["date"]),
                collected_at=(
                    datetime.fromisoformat(r["collected_at"]) if r["collected_at"] else None
                ),
                channel_title=r["channel_title"],
                channel_username=r["channel_username"],
            )
            for r in rows
        ]
        return messages, total

    async def get_stats(self) -> dict:
        assert self._db is not None
        stats = {}
        for table in ("accounts", "channels", "messages", "keywords"):
            cur = await self._db.execute(f"SELECT COUNT(*) as cnt FROM {table}")  # noqa: S608
            row = await cur.fetchone()
            stats[table] = row["cnt"] if row else 0
        return stats

    # --- Keywords ---

    async def add_keyword(self, keyword: Keyword) -> int:
        assert self._db is not None
        cur = await self._db.execute(
            "INSERT INTO keywords (pattern, is_regex, is_active) VALUES (?, ?, ?)",
            (keyword.pattern, int(keyword.is_regex), int(keyword.is_active)),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    async def get_keywords(self, active_only: bool = False) -> list[Keyword]:
        assert self._db is not None
        sql = "SELECT * FROM keywords"
        if active_only:
            sql += " WHERE is_active = 1"
        cur = await self._db.execute(sql)
        rows = await cur.fetchall()
        return [
            Keyword(
                id=r["id"],
                pattern=r["pattern"],
                is_regex=bool(r["is_regex"]),
                is_active=bool(r["is_active"]),
            )
            for r in rows
        ]

    async def set_keyword_active(self, keyword_id: int, active: bool) -> None:
        assert self._db is not None
        await self._db.execute(
            "UPDATE keywords SET is_active = ? WHERE id = ?", (int(active), keyword_id)
        )
        await self._db.commit()

    async def delete_keyword(self, keyword_id: int) -> None:
        assert self._db is not None
        await self._db.execute("DELETE FROM keywords WHERE id = ?", (keyword_id,))
        await self._db.commit()

    # --- Collection Tasks ---

    async def create_collection_task(self, channel_id: int, channel_title: str | None) -> int:
        assert self._db is not None
        cur = await self._db.execute(
            "INSERT INTO collection_tasks (channel_id, channel_title) VALUES (?, ?)",
            (channel_id, channel_title),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    async def update_collection_task_progress(
        self, task_id: int, messages_collected: int
    ) -> None:
        """Lightweight update: only bump messages_collected without changing status."""
        assert self._db is not None
        await self._db.execute(
            "UPDATE collection_tasks SET messages_collected = ? WHERE id = ?",
            (messages_collected, task_id),
        )
        await self._db.commit()

    async def update_collection_task(
        self,
        task_id: int,
        status: str,
        messages_collected: int | None = None,
        error: str | None = None,
    ) -> None:
        assert self._db is not None
        now = datetime.utcnow().isoformat()
        sets = ["status = ?"]
        params: list = [status]
        if status == "running":
            sets.append("started_at = ?")
            params.append(now)
        if status in ("completed", "failed"):
            sets.append("completed_at = ?")
            params.append(now)
        if messages_collected is not None:
            sets.append("messages_collected = ?")
            params.append(messages_collected)
        if error is not None:
            sets.append("error = ?")
            params.append(error)
        params.append(task_id)
        await self._db.execute(
            f"UPDATE collection_tasks SET {', '.join(sets)} WHERE id = ?",
            tuple(params),
        )
        await self._db.commit()

    async def get_collection_tasks(self, limit: int = 20) -> list[CollectionTask]:
        assert self._db is not None
        cur = await self._db.execute(
            "SELECT * FROM collection_tasks ORDER BY id DESC LIMIT ?", (limit,)
        )
        rows = await cur.fetchall()
        return [
            CollectionTask(
                id=r["id"],
                channel_id=r["channel_id"],
                channel_title=r["channel_title"],
                status=r["status"],
                messages_collected=r["messages_collected"],
                error=r["error"],
                created_at=(
                    datetime.fromisoformat(r["created_at"]) if r["created_at"] else None
                ),
                started_at=(
                    datetime.fromisoformat(r["started_at"]) if r["started_at"] else None
                ),
                completed_at=(
                    datetime.fromisoformat(r["completed_at"]) if r["completed_at"] else None
                ),
            )
            for r in rows
        ]

    async def cancel_collection_task(self, task_id: int) -> bool:
        assert self._db is not None
        now = datetime.utcnow().isoformat()
        cur = await self._db.execute(
            "UPDATE collection_tasks SET status = 'cancelled', completed_at = ? "
            "WHERE id = ? AND status IN ('pending', 'running')",
            (now, task_id),
        )
        await self._db.commit()
        return cur.rowcount > 0

    # --- Search Log ---

    async def log_search(self, phone: str, query: str, results_count: int) -> None:
        """Record a completed search."""
        assert self._db is not None
        await self._db.execute(
            "INSERT INTO search_log (phone, query, results_count) VALUES (?, ?, ?)",
            (phone, query, results_count),
        )
        await self._db.commit()

    async def get_recent_searches(self, limit: int = 20) -> list[dict]:
        """Return recent searches from search_log for display."""
        assert self._db is not None
        cur = await self._db.execute(
            "SELECT * FROM search_log ORDER BY id DESC LIMIT ?", (limit,)
        )
        rows = await cur.fetchall()
        return [
            {
                "id": r["id"],
                "phone": r["phone"],
                "query": r["query"],
                "results_count": r["results_count"],
                "created_at": r["created_at"],
            }
            for r in rows
        ]

    # --- Channel Stats ---

    async def save_channel_stats(self, stats: ChannelStats) -> int:
        assert self._db is not None
        cur = await self._db.execute(
            """INSERT INTO channel_stats
               (channel_id, subscriber_count, avg_views, avg_reactions, avg_forwards)
               VALUES (?, ?, ?, ?, ?)""",
            (
                stats.channel_id,
                stats.subscriber_count,
                stats.avg_views,
                stats.avg_reactions,
                stats.avg_forwards,
            ),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    async def get_channel_stats(
        self, channel_id: int, limit: int = 1
    ) -> list[ChannelStats]:
        assert self._db is not None
        cur = await self._db.execute(
            "SELECT * FROM channel_stats WHERE channel_id = ? "
            "ORDER BY collected_at DESC LIMIT ?",
            (channel_id, limit),
        )
        rows = await cur.fetchall()
        return [
            ChannelStats(
                id=r["id"],
                channel_id=r["channel_id"],
                subscriber_count=r["subscriber_count"],
                avg_views=r["avg_views"],
                avg_reactions=r["avg_reactions"],
                avg_forwards=r["avg_forwards"],
                collected_at=(
                    datetime.fromisoformat(r["collected_at"])
                    if r["collected_at"]
                    else None
                ),
            )
            for r in rows
        ]

    async def get_latest_stats_for_all(self) -> dict[int, ChannelStats]:
        assert self._db is not None
        cur = await self._db.execute(
            """SELECT cs.* FROM channel_stats cs
               INNER JOIN (
                   SELECT channel_id, MAX(collected_at) AS max_date
                   FROM channel_stats GROUP BY channel_id
               ) latest ON cs.channel_id = latest.channel_id
                        AND cs.collected_at = latest.max_date"""
        )
        rows = await cur.fetchall()
        return {
            r["channel_id"]: ChannelStats(
                id=r["id"],
                channel_id=r["channel_id"],
                subscriber_count=r["subscriber_count"],
                avg_views=r["avg_views"],
                avg_reactions=r["avg_reactions"],
                avg_forwards=r["avg_forwards"],
                collected_at=(
                    datetime.fromisoformat(r["collected_at"])
                    if r["collected_at"]
                    else None
                ),
            )
            for r in rows
        }

    # --- Settings ---

    async def get_setting(self, key: str) -> str | None:
        assert self._db is not None
        cur = await self._db.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = await cur.fetchone()
        return row["value"] if row else None

    async def set_setting(self, key: str, value: str) -> None:
        assert self._db is not None
        await self._db.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        await self._db.commit()
