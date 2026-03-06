from __future__ import annotations

import logging
from datetime import datetime

import aiosqlite

from src.models import Message

logger = logging.getLogger(__name__)


class MessagesRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    async def insert_message(self, msg: Message) -> bool:
        try:
            cur = await self._db.execute(
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
            return cur.rowcount > 0
        except Exception:
            return False

    async def insert_messages_batch(self, messages: list[Message]) -> int:
        if not messages:
            return 0
        data = [
            (
                m.channel_id,
                m.message_id,
                m.sender_id,
                m.sender_name,
                m.text,
                m.media_type,
                m.date.isoformat(),
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
        except Exception as exc:
            logger.error("Failed to insert batch of %d messages: %s", len(messages), exc)
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
        # Exclude messages from filtered channels; allow messages whose channel
        # is not yet in the channels table (NULL join) for backward-compat.
        conditions: list[str] = ["(c.is_filtered IS NULL OR c.is_filtered = 0)"]
        params: list = []

        if channel_id:
            conditions.append("m.channel_id = ?")
            params.append(channel_id)
        if date_from:
            conditions.append("m.date >= ?")
            params.append(date_from)
        if date_to:
            conditions.append("m.date <= ?")
            params.append(date_to)

        channel_join = " LEFT JOIN channels c ON m.channel_id = c.channel_id"
        where = " WHERE " + " AND ".join(conditions)

        if query:
            fts_query = '"' + query.replace('"', '""') + '"'
            fts_join = (
                " INNER JOIN (SELECT rowid FROM messages_fts"
                " WHERE messages_fts MATCH ?) AS fts ON m.id = fts.rowid"
            )
            count_cur = await self._db.execute(
                f"SELECT COUNT(*) as cnt FROM messages m{fts_join}{channel_join}{where}",
                (fts_query, *params),
            )
            row = await count_cur.fetchone()
            total = row["cnt"] if row else 0

            cur = await self._db.execute(
                f"""SELECT m.*, c.title as channel_title, c.username as channel_username
                    FROM messages m{fts_join}{channel_join}
                    {where}
                    ORDER BY m.date DESC
                    LIMIT ? OFFSET ?""",
                (fts_query, *params, limit, offset),
            )
        else:
            count_cur = await self._db.execute(
                f"SELECT COUNT(*) as cnt FROM messages m{channel_join}{where}", tuple(params)
            )
            row = await count_cur.fetchone()
            total = row["cnt"] if row else 0

            cur = await self._db.execute(
                f"""SELECT m.*, c.title as channel_title, c.username as channel_username
                    FROM messages m{channel_join}
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

    async def delete_messages_for_channel(self, channel_id: int) -> int:
        cur = await self._db.execute(
            "DELETE FROM messages WHERE channel_id = ?", (channel_id,)
        )
        await self._db.commit()
        return cur.rowcount or 0

    async def get_stats(self) -> dict:
        stats: dict[str, int] = {}
        queries = {
            "accounts": "SELECT COUNT(*) as cnt FROM accounts",
            "channels": "SELECT COUNT(*) as cnt FROM channels",
            "messages": "SELECT COUNT(*) as cnt FROM messages",
            "keywords": "SELECT COUNT(*) as cnt FROM keywords",
        }
        for table, sql in queries.items():
            cur = await self._db.execute(sql)
            row = await cur.fetchone()
            stats[table] = row["cnt"] if row else 0
        return stats
