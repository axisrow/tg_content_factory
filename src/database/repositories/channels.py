from __future__ import annotations

from datetime import datetime

import aiosqlite

from src.models import Channel


class ChannelsRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    async def add_channel(self, channel: Channel) -> int:
        cur = await self._db.execute(
            """INSERT INTO channels (channel_id, title, username, channel_type, is_active)
               VALUES (?, ?, ?, ?, ?)
               ON CONFLICT(channel_id) DO UPDATE
               SET title=excluded.title, username=excluded.username,
                   channel_type=excluded.channel_type""",
            (
                channel.channel_id,
                channel.title,
                channel.username,
                channel.channel_type,
                int(channel.is_active),
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
            last_collected_id=row["last_collected_id"],
            added_at=datetime.fromisoformat(row["added_at"]) if row["added_at"] else None,
            message_count=(
                row["message_count"]
                if "message_count" in keys and row["message_count"] is not None
                else 0
            ),
        )

    async def get_channels(self, active_only: bool = False) -> list[Channel]:
        sql = "SELECT * FROM channels"
        if active_only:
            sql += " WHERE is_active = 1"
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

    async def get_channels_with_counts(self, active_only: bool = False) -> list[Channel]:
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
        return [self._map_channel(r) for r in rows]

    async def update_channel_last_id(self, channel_id: int, last_id: int) -> None:
        await self._db.execute(
            "UPDATE channels SET last_collected_id = ? WHERE channel_id = ?",
            (last_id, channel_id),
        )
        await self._db.commit()

    async def set_channel_active(self, pk: int, active: bool) -> None:
        await self._db.execute(
            "UPDATE channels SET is_active = ? WHERE id = ?", (int(active), pk)
        )
        await self._db.commit()

    async def delete_channel(self, pk: int) -> None:
        cur = await self._db.execute("SELECT channel_id FROM channels WHERE id = ?", (pk,))
        row = await cur.fetchone()
        if row:
            channel_id = row["channel_id"]
            await self._db.execute("DELETE FROM messages WHERE channel_id = ?", (channel_id,))
            await self._db.execute("DELETE FROM channel_stats WHERE channel_id = ?", (channel_id,))
        await self._db.execute("DELETE FROM channels WHERE id = ?", (pk,))
        await self._db.commit()
