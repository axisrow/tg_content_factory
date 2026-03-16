from __future__ import annotations

from datetime import datetime

import aiosqlite

from src.models import ChannelStats


class ChannelStatsRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    async def save_channel_stats(self, stats: ChannelStats) -> int:
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

    async def get_channel_stats(self, channel_id: int, limit: int = 1) -> list[ChannelStats]:
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
                    if r["collected_at"] else None
                ),
            )
            for r in rows
        ]

    async def get_latest_stats_for_all(self) -> dict[int, ChannelStats]:
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
                    if r["collected_at"] else None
                ),
            )
            for r in rows
        }

    async def get_previous_subscriber_counts(self) -> dict[int, int | None]:
        """Return the second-most-recent subscriber count per channel.

        Channels with only one recorded stats entry are not included in the result.
        """
        cur = await self._db.execute(
            """WITH ranked AS (
                   SELECT channel_id, subscriber_count,
                          ROW_NUMBER() OVER (
                              PARTITION BY channel_id ORDER BY collected_at DESC, id DESC
                          ) AS rn
                   FROM channel_stats
               )
               SELECT channel_id, subscriber_count FROM ranked WHERE rn = 2"""
        )
        rows = await cur.fetchall()
        return {r["channel_id"]: r["subscriber_count"] for r in rows}
