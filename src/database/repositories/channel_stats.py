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
                    datetime.fromisoformat(r["collected_at"]) if r["collected_at"] else None
                ),
            )
            for r in rows
        ]

    async def get_latest_stats_for_all(self) -> dict[int, ChannelStats]:
        cur = await self._db.execute("""WITH ranked AS (
                   SELECT *, ROW_NUMBER() OVER (
                       PARTITION BY channel_id ORDER BY collected_at DESC, id DESC
                   ) AS rn FROM channel_stats
               )
               SELECT * FROM ranked WHERE rn = 1""")
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
                    datetime.fromisoformat(r["collected_at"]) if r["collected_at"] else None
                ),
            )
            for r in rows
        }

    async def get_previous_subscriber_counts(self) -> dict[int, int | None]:
        """Return the second-most-recent subscriber count per channel.

        Channels with only one recorded stats entry are not included in the result.
        """
        cur = await self._db.execute("""WITH ranked AS (
                   SELECT channel_id, subscriber_count,
                          ROW_NUMBER() OVER (
                              PARTITION BY channel_id ORDER BY collected_at DESC, id DESC
                          ) AS rn
                   FROM channel_stats
               )
               SELECT channel_id, subscriber_count FROM ranked WHERE rn = 2""")
        rows = await cur.fetchall()
        return {r["channel_id"]: r["subscriber_count"] for r in rows}

    async def get_latest_and_previous_stats(
        self,
    ) -> tuple[dict[int, ChannelStats], dict[int, int | None]]:
        """Fetch latest stats and previous subscriber counts in a single query.

        Returns (latest_stats, prev_subscriber_counts) — same as calling
        get_latest_stats_for_all() + get_previous_subscriber_counts() but with
        one DB round-trip instead of two.
        """
        cur = await self._db.execute("""WITH ranked AS (
                   SELECT *, ROW_NUMBER() OVER (
                       PARTITION BY channel_id ORDER BY collected_at DESC, id DESC
                   ) AS rn FROM channel_stats
               )
               SELECT * FROM ranked WHERE rn <= 2""")
        rows = await cur.fetchall()
        latest: dict[int, ChannelStats] = {}
        previous: dict[int, int | None] = {}
        for r in rows:
            rn = r["rn"]
            if rn == 1:
                latest[r["channel_id"]] = ChannelStats(
                    id=r["id"],
                    channel_id=r["channel_id"],
                    subscriber_count=r["subscriber_count"],
                    avg_views=r["avg_views"],
                    avg_reactions=r["avg_reactions"],
                    avg_forwards=r["avg_forwards"],
                    collected_at=(
                        datetime.fromisoformat(r["collected_at"]) if r["collected_at"] else None
                    ),
                )
            elif rn == 2:
                previous[r["channel_id"]] = r["subscriber_count"]
        return latest, previous

    async def get_subscriber_history(
        self, channel_id: int, days: int = 30
    ) -> list[dict]:
        """Return subscriber_count time series for a channel over the last N days."""
        cur = await self._db.execute(
            "SELECT collected_at, subscriber_count "
            "FROM channel_stats "
            "WHERE channel_id = ? AND collected_at >= datetime('now', ?) "
            "ORDER BY collected_at ASC",
            (channel_id, f"-{days} days"),
        )
        return [dict(r) for r in await cur.fetchall()]

    async def get_subscriber_count_at(self, channel_id: int, days_ago: int) -> int | None:
        """Get the subscriber count closest to N days ago for a channel."""
        cur = await self._db.execute(
            "SELECT subscriber_count FROM channel_stats "
            "WHERE channel_id = ? AND collected_at <= datetime('now', ?) "
            "ORDER BY collected_at DESC LIMIT 1",
            (channel_id, f"-{days_ago} days"),
        )
        row = await cur.fetchone()
        return row["subscriber_count"] if row else None
