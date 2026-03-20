from __future__ import annotations

import logging
from dataclasses import dataclass

from src.database import Database

logger = logging.getLogger(__name__)


@dataclass
class TrendingTopic:
    keyword: str
    count: int


@dataclass
class TrendingChannel:
    channel_id: int
    title: str | None
    username: str | None
    avg_views: float
    message_count: int


@dataclass
class TrendingEmoji:
    emoji: str
    count: int


@dataclass
class MessageVelocity:
    date: str
    count: int


@dataclass
class PeakHour:
    hour: int
    count: int


class TrendService:
    """Trend analysis over collected messages."""

    def __init__(self, db: Database) -> None:
        self._db = db

    async def get_trending_topics(self, days: int = 7, limit: int = 20) -> list[TrendingTopic]:
        """Return top keywords by frequency in messages from the last N days.

        Uses a simple word-frequency approach on the messages table so it works
        even without FTS5.  Short words (<4 chars) and stop-words are skipped.
        """
        rows = await self._db.execute_fetchall(
            """
            SELECT text FROM messages
            WHERE date >= date('now', ?)
              AND COALESCE(TRIM(text), '') <> ''
            """,
            (f"-{days} days",),
        )
        word_counts: dict[str, int] = {}
        stop_words = {
            "и", "в", "на", "с", "по", "не", "это", "то", "что",
            "как", "из", "за", "от", "для", "или", "но", "а",
            "the", "and", "is", "in", "to", "of", "a", "for",
        }
        for row in rows:
            text = row["text"] or ""
            for word in text.split():
                w = word.lower().strip(".,!?:;\"'()[]{}–—")
                if len(w) >= 4 and w not in stop_words and w.isalpha():
                    word_counts[w] = word_counts.get(w, 0) + 1
        sorted_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)
        return [TrendingTopic(keyword=w, count=c) for w, c in sorted_words[:limit]]

    async def get_trending_channels(self, days: int = 7, limit: int = 10) -> list[TrendingChannel]:
        """Return channels with the highest average views in the last N days."""
        rows = await self._db.execute_fetchall(
            """
            SELECT c.channel_id, c.title, c.username,
                   COALESCE(AVG(m.views), 0) AS avg_views,
                   COUNT(m.id) AS message_count
            FROM messages m
            JOIN channels c ON m.channel_id = c.channel_id
            WHERE m.date >= date('now', ?)
              AND m.views IS NOT NULL
              AND (c.is_filtered IS NULL OR c.is_filtered = 0)
            GROUP BY c.channel_id, c.title, c.username
            HAVING COUNT(m.id) >= 3
            ORDER BY avg_views DESC
            LIMIT ?
            """,
            (f"-{days} days", limit),
        )
        return [
            TrendingChannel(
                channel_id=int(r["channel_id"]),
                title=r["title"],
                username=r["username"],
                avg_views=float(r["avg_views"]),
                message_count=int(r["message_count"]),
            )
            for r in rows
        ]

    async def get_trending_emojis(self, days: int = 7, limit: int = 15) -> list[TrendingEmoji]:
        """Return most-used reaction emojis from the last N days."""
        rows = await self._db.execute_fetchall(
            """
            SELECT mr.emoji, SUM(mr.count) AS total
            FROM message_reactions mr
            JOIN messages m ON mr.channel_id = m.channel_id AND mr.message_id = m.message_id
            WHERE m.date >= date('now', ?)
            GROUP BY mr.emoji
            ORDER BY total DESC
            LIMIT ?
            """,
            (f"-{days} days", limit),
        )
        return [TrendingEmoji(emoji=r["emoji"], count=int(r["total"])) for r in rows]

    async def get_message_velocity(self, channel_id: int, days: int = 30) -> list[MessageVelocity]:
        """Return daily message count for a specific channel."""
        rows = await self._db.execute_fetchall(
            """
            SELECT date(m.date) AS day, COUNT(*) AS cnt
            FROM messages m
            WHERE m.channel_id = ?
              AND m.date >= date('now', ?)
            GROUP BY day
            ORDER BY day ASC
            """,
            (channel_id, f"-{days} days"),
        )
        return [MessageVelocity(date=r["day"], count=int(r["cnt"])) for r in rows]

    async def get_peak_hours(self, channel_id: int, days: int = 30) -> list[PeakHour]:
        """Return message count distribution by hour of day for a channel."""
        rows = await self._db.execute_fetchall(
            """
            SELECT CAST(strftime('%H', m.date) AS INTEGER) AS hour, COUNT(*) AS cnt
            FROM messages m
            WHERE m.channel_id = ?
              AND m.date >= date('now', ?)
            GROUP BY hour
            ORDER BY hour ASC
            """,
            (channel_id, f"-{days} days"),
        )
        return [PeakHour(hour=int(r["hour"]), count=int(r["cnt"])) for r in rows]
