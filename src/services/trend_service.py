from __future__ import annotations

import logging
from dataclasses import dataclass

from sklearn.feature_extraction.text import TfidfVectorizer

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
        """Return top keywords by TF-IDF score from messages in the last N days.

        Uses TfidfVectorizer to automatically suppress ubiquitous low-signal words
        (greetings, filler, stop-words) without a hardcoded list.
        """
        batch_size = 5000
        offset = 0
        texts: list[str] = []

        while True:
            rows = await self._db.execute_fetchall(
                """
                SELECT text FROM messages
                WHERE date >= date('now', ?)
                  AND COALESCE(TRIM(text), '') <> ''
                LIMIT ? OFFSET ?
                """,
                (f"-{days} days", batch_size, offset),
            )
            if not rows:
                break
            texts.extend(row["text"] or "" for row in rows)
            if len(rows) < batch_size:
                break
            offset += batch_size

        if not texts:
            return []

        vectorizer = TfidfVectorizer(
            token_pattern=r"(?u)\b[а-яёa-z]{4,}\b",  # 4+ символов, RU + EN
            max_df=0.85,  # слова в >85% сообщений — шум, убираются автоматически
            min_df=2,  # слова менее чем в 2 сообщениях — слишком редкие
        )
        try:
            tfidf_matrix = vectorizer.fit_transform(texts)
        except ValueError:
            # Нет слов после фильтрации (например, корпус слишком мал)
            return []

        feature_names = vectorizer.get_feature_names_out()
        scores = tfidf_matrix.sum(axis=0).A1  # суммарный TF-IDF score по всем документам

        top_indices = scores.argsort()[::-1][:limit]
        return [
            TrendingTopic(keyword=feature_names[i], count=int(tfidf_matrix.getcol(i).nnz))
            for i in top_indices
        ]

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
