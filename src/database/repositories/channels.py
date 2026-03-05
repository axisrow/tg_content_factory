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

    async def get_channels(self, active_only: bool = False) -> list[Channel]:
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
        row = await self._db.execute_fetchall(
            "SELECT channel_id FROM channels WHERE id = ?", (pk,)
        )
        if row:
            channel_id = row[0][0]
            await self._db.execute("DELETE FROM messages WHERE channel_id = ?", (channel_id,))
            await self._db.execute("DELETE FROM channel_stats WHERE channel_id = ?", (channel_id,))
        await self._db.execute("DELETE FROM channels WHERE id = ?", (pk,))
        await self._db.commit()
