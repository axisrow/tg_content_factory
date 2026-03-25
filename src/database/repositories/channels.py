from __future__ import annotations

from datetime import datetime

import aiosqlite

from src.models import Channel


class ChannelsRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    async def add_channel(self, channel: Channel) -> int:
        cur = await self._db.execute(
            """INSERT INTO channels (channel_id, title, username, channel_type, is_active,
                                     about, linked_chat_id, has_comments)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(channel_id) DO UPDATE
               SET title=excluded.title, username=excluded.username,
                   channel_type=excluded.channel_type,
                   is_active=excluded.is_active,
                   about=excluded.about, linked_chat_id=excluded.linked_chat_id,
                   has_comments=excluded.has_comments""",
            (
                channel.channel_id,
                channel.title,
                channel.username,
                channel.channel_type,
                int(channel.is_active),
                channel.about,
                channel.linked_chat_id,
                int(channel.has_comments),
            ),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    @staticmethod
    def _map_channel(row: aiosqlite.Row) -> Channel:
        keys = row.keys()
        return Channel(
            id=row["id"],
            channel_id=row["channel_id"],
            title=row["title"],
            username=row["username"],
            channel_type=row["channel_type"],
            is_active=bool(row["is_active"]),
            is_filtered=bool(row["is_filtered"]) if "is_filtered" in keys else False,
            filter_flags=(
                row["filter_flags"] if "filter_flags" in keys and row["filter_flags"] else ""
            ),
            about=row["about"] if "about" in keys else None,
            linked_chat_id=row["linked_chat_id"] if "linked_chat_id" in keys else None,
            has_comments=bool(row["has_comments"]) if "has_comments" in keys and row["has_comments"] else False,
            last_collected_id=row["last_collected_id"],
            added_at=datetime.fromisoformat(row["added_at"]) if row["added_at"] else None,
            message_count=(
                row["message_count"]
                if "message_count" in keys and row["message_count"] is not None
                else 0
            ),
        )

    async def get_channels(
        self, active_only: bool = False, include_filtered: bool = True
    ) -> list[Channel]:
        conditions = []
        if active_only:
            conditions.append("is_active = 1")
        if not include_filtered:
            conditions.append("is_filtered = 0")
        sql = "SELECT * FROM channels"
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY id ASC"
        cur = await self._db.execute(sql)
        rows = await cur.fetchall()
        return [self._map_channel(r) for r in rows]

    async def get_channel_by_pk(self, pk: int) -> Channel | None:
        cur = await self._db.execute("SELECT * FROM channels WHERE id = ?", (pk,))
        row = await cur.fetchone()
        if not row:
            return None
        return self._map_channel(row)

    async def get_channel_by_channel_id(self, channel_id: int) -> Channel | None:
        cur = await self._db.execute(
            "SELECT * FROM channels WHERE channel_id = ?",
            (channel_id,),
        )
        row = await cur.fetchone()
        if not row:
            return None
        return self._map_channel(row)

    async def get_channels_with_counts(
        self, active_only: bool = False, include_filtered: bool = True
    ) -> list[Channel]:
        sql = """
            SELECT c.*, COALESCE(cnt.total, 0) AS message_count
            FROM channels c
            LEFT JOIN (
                SELECT channel_id, COUNT(*) AS total FROM messages GROUP BY channel_id
            ) cnt ON c.channel_id = cnt.channel_id
        """
        conditions = []
        if active_only:
            conditions.append("c.is_active = 1")
        if not include_filtered:
            conditions.append("c.is_filtered = 0")
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)
        sql += " ORDER BY c.id ASC"
        cur = await self._db.execute(sql)
        rows = await cur.fetchall()
        return [self._map_channel(r) for r in rows]

    async def update_channel_last_id(self, channel_id: int, last_id: int) -> None:
        await self._db.execute(
            "UPDATE channels SET last_collected_id = ? WHERE channel_id = ?",
            (last_id, channel_id),
        )
        await self._db.commit()

    async def set_channel_active(self, pk: int, active: bool) -> None:
        await self._db.execute("UPDATE channels SET is_active = ? WHERE id = ?", (int(active), pk))
        await self._db.commit()

    async def set_channel_filtered(self, pk: int, filtered: bool) -> None:
        if filtered:
            await self._db.execute(
                "UPDATE channels SET is_filtered = 1, filter_flags = 'manual' WHERE id = ?",
                (pk,),
            )
        else:
            await self._db.execute(
                "UPDATE channels SET is_filtered = 0, filter_flags = '' WHERE id = ?",
                (pk,),
            )
        await self._db.commit()

    async def set_filtered_bulk(
        self, updates: list[tuple[int, str]], *, commit: bool = True
    ) -> int:
        if not updates:
            return 0
        updated_rows = 0
        for channel_id, flags_csv in updates:
            cur = await self._db.execute(
                "UPDATE channels SET is_filtered = 1, filter_flags = ? WHERE channel_id = ?",
                (flags_csv, channel_id),
            )
            rowcount = cur.rowcount if cur.rowcount is not None else 0
            if rowcount > 0:
                updated_rows += rowcount
        if commit:
            await self._db.commit()
        return updated_rows

    async def reset_all_filters(self, *, commit: bool = True) -> int:
        cur = await self._db.execute("UPDATE channels SET is_filtered = 0, filter_flags = ''")
        if commit:
            await self._db.commit()
        rowcount = cur.rowcount if cur.rowcount is not None else 0
        return rowcount if rowcount > 0 else 0

    async def set_channel_type(self, channel_id: int, channel_type: str) -> None:
        await self._db.execute(
            "UPDATE channels SET channel_type=? WHERE channel_id=?",
            (channel_type, channel_id),
        )
        await self._db.commit()

    async def update_channel_meta(
        self, channel_id: int, *, username: str | None, title: str | None
    ) -> None:
        await self._db.execute(
            "UPDATE channels SET username = ?, title = ? WHERE channel_id = ?",
            (username, title, channel_id),
        )
        await self._db.commit()

    async def update_channel_full_meta(
        self, channel_id: int, *, about: str | None, linked_chat_id: int | None, has_comments: bool
    ) -> None:
        await self._db.execute(
            "UPDATE channels SET about = ?, linked_chat_id = ?, has_comments = ? WHERE channel_id = ?",
            (about, linked_chat_id, int(has_comments), channel_id),
        )
        await self._db.commit()

    async def get_forum_topics(self, channel_id: int) -> list[dict]:
        cur = await self._db.execute(
            "SELECT topic_id, title FROM forum_topics WHERE channel_id = ? ORDER BY topic_id",
            (channel_id,),
        )
        rows = await cur.fetchall()
        return [{"id": row["topic_id"], "title": row["title"]} for row in rows]

    async def upsert_forum_topics(self, channel_id: int, topics: list[dict]) -> None:
        await self._db.execute("DELETE FROM forum_topics WHERE channel_id = ?", (channel_id,))
        if topics:
            await self._db.executemany(
                "INSERT INTO forum_topics (channel_id, topic_id, title, updated_at)"
                " VALUES (?, ?, ?, datetime('now'))",
                [(channel_id, t["id"], t["title"]) for t in topics],
            )
        await self._db.commit()

    async def delete_channel(self, pk: int) -> None:
        cur = await self._db.execute("SELECT channel_id FROM channels WHERE id = ?", (pk,))
        row = await cur.fetchone()
        if row:
            channel_id = row["channel_id"]
            await self._db.execute("DELETE FROM messages WHERE channel_id = ?", (channel_id,))
            await self._db.execute("DELETE FROM channel_stats WHERE channel_id = ?", (channel_id,))
            await self._db.execute("DELETE FROM forum_topics WHERE channel_id = ?", (channel_id,))
        await self._db.execute("DELETE FROM channels WHERE id = ?", (pk,))
        await self._db.commit()
