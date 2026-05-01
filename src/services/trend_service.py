from __future__ import annotations

import asyncio
import html
import importlib
import logging
import re
import warnings
from collections import Counter
from dataclasses import dataclass

from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS, TfidfVectorizer

from src.database import Database

logger = logging.getLogger(__name__)
# jieba imports pkg_resources; pytest treats that deprecation as an error.
with warnings.catch_warnings():
    warnings.filterwarnings(
        "ignore",
        message="pkg_resources is deprecated as an API.*",
        category=Warning,
    )
    jieba = importlib.import_module("jieba")
jieba.setLogLevel(logging.WARNING)


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

    _TOPIC_BATCH_SIZE = 5000
    _MAX_TOPIC_DOCUMENTS = 10000
    _URL_RE = re.compile(r"https?://\S+|www\.\S+|t\.me/\S+", re.IGNORECASE)
    _HTML_TAG_RE = re.compile(r"<[^>]+>")
    _TECH_TOKEN_RE = re.compile(
        r"\b(?:https?|www|html?|amp|nbsp|quot|lt|gt|href|target|blank|utm_[a-z]+)\b",
        re.IGNORECASE,
    )
    _LATIN_CYRILLIC_TOKEN_RE = re.compile(r"(?u)\b[а-яёa-z]{4,}\b")
    _HAN_CHAR_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")
    _HAN_TOKEN_RE = re.compile(r"^[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]{2,}$")
    _TOPIC_NOISE_WORDS = frozenset({
        "content",
        "data",
        "need",
        "needed",
        "needs",
        "page",
        "pages",
        "require",
        "required",
        "requires",
        "site",
        "sites",
    })
    _STOP_WORDS = ENGLISH_STOP_WORDS | _TOPIC_NOISE_WORDS | frozenset({
        "about",
        "after",
        "again",
        "also",
        "because",
        "been",
        "before",
        "being",
        "between",
        "could",
        "from",
        "have",
        "into",
        "more",
        "most",
        "only",
        "other",
        "over",
        "some",
        "than",
        "that",
        "their",
        "then",
        "there",
        "these",
        "they",
        "this",
        "those",
        "through",
        "under",
        "very",
        "were",
        "what",
        "when",
        "where",
        "which",
        "while",
        "with",
        "would",
        "если",
        "или",
        "как",
        "для",
        "при",
        "про",
        "что",
        "это",
        "этот",
        "эти",
        "они",
        "она",
        "оно",
        "уже",
        "еще",
        "ещё",
        "были",
        "было",
        "будет",
        "после",
        "перед",
        "только",
        "очень",
        "можно",
        "когда",
        "где",
        "или",
        "также",
        "которые",
        "который",
        "которая",
    })
    _CHINESE_STOP_WORDS = frozenset({
        "一个",
        "一些",
        "以及",
        "他们",
        "但是",
        "你们",
        "因为",
        "为了",
        "什么",
        "今天",
        "只是",
        "可以",
        "可能",
        "已经",
        "并且",
        "我们",
        "所以",
        "时候",
        "明天",
        "然后",
        "现在",
        "由于",
        "自己",
        "这个",
        "这些",
        "这样",
        "这种",
        "这里",
        "进行",
        "还是",
        "那些",
        "那么",
        "那里",
        "需要",
    })

    def __init__(self, db: Database) -> None:
        self._db = db

    async def get_trending_topics(self, days: int = 7, limit: int = 20) -> list[TrendingTopic]:
        """Return top keywords ranked by TF-IDF from recent messages.

        Uses a bounded corpus of recent messages to avoid unbounded memory usage
        and keeps raw mention counts so downstream callers can continue to render
        the results as "mentions".
        """
        if limit <= 0:
            return []

        offset = 0
        texts: list[str] = []

        while len(texts) < self._MAX_TOPIC_DOCUMENTS:
            batch_size = min(self._TOPIC_BATCH_SIZE, self._MAX_TOPIC_DOCUMENTS - len(texts))
            rows = await self._db.execute_fetchall(
                """
                SELECT text FROM messages
                WHERE date >= date('now', ?)
                  AND COALESCE(TRIM(text), '') <> ''
                ORDER BY date DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (f"-{days} days", batch_size, offset),
            )
            if not rows:
                break
            texts.extend(row["text"] or "" for row in rows)
            if len(rows) < batch_size:
                break
            offset += len(rows)

        if not texts:
            return []

        if len(texts) == self._MAX_TOPIC_DOCUMENTS:
            logger.info(
                "Capped trending-topic corpus at %d most recent messages",
                self._MAX_TOPIC_DOCUMENTS,
            )

        cleaned_texts = [cleaned for text in texts if (cleaned := self._cleanup_topic_text(text))]
        if not cleaned_texts:
            return []

        return await asyncio.to_thread(self._rank_trending_topics, cleaned_texts, limit)

    @classmethod
    def _cleanup_topic_text(cls, text: str) -> str:
        text = html.unescape(text)
        text = cls._URL_RE.sub(" ", text)
        text = cls._HTML_TAG_RE.sub(" ", text)
        text = cls._TECH_TOKEN_RE.sub(" ", text)
        return text

    def _rank_trending_topics(self, texts: list[str], limit: int) -> list[TrendingTopic]:
        tokenized_texts = [self._analyze_topic_text(text) for text in texts]
        if not any(tokenized_texts):
            return []

        vectorizer = TfidfVectorizer(
            analyzer=self._identity_analyzer,
            token_pattern=None,
            max_df=0.85,
            min_df=2,
        )
        try:
            tfidf_matrix = vectorizer.fit_transform(tokenized_texts)
        except ValueError:
            return []

        feature_names = vectorizer.get_feature_names_out()
        scores = tfidf_matrix.sum(axis=0).A1
        mention_counts: Counter[str] = Counter()

        for tokens in tokenized_texts:
            mention_counts.update(tokens)

        top_indices = scores.argsort()[::-1]
        topics: list[TrendingTopic] = []
        for index in top_indices:
            keyword = feature_names[index]
            topics.append(TrendingTopic(keyword=keyword, count=mention_counts[keyword]))
            if len(topics) >= limit:
                break
        return topics

    @staticmethod
    def _identity_analyzer(tokens: list[str]) -> list[str]:
        return tokens

    @classmethod
    def _analyze_topic_text(cls, text: str) -> list[str]:
        text = text.lower()
        tokens = [
            token
            for token in cls._LATIN_CYRILLIC_TOKEN_RE.findall(text)
            if token not in cls._STOP_WORDS
        ]

        if not cls._HAN_CHAR_RE.search(text):
            return tokens

        for token in jieba.cut(text):
            token = token.strip()
            if token in cls._CHINESE_STOP_WORDS:
                continue
            if cls._HAN_TOKEN_RE.fullmatch(token):
                tokens.append(token)

        return tokens

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

    async def get_message_velocity(self, channel_id: int | None = None, days: int = 30) -> list[MessageVelocity]:
        """Return daily message count for one channel or all channels."""
        channel_filter = "AND m.channel_id = ?" if channel_id is not None else ""
        params: tuple[object, ...]
        if channel_id is not None:
            params = (f"-{days} days", channel_id)
        else:
            params = (f"-{days} days",)
        rows = await self._db.execute_fetchall(
            f"""
            SELECT date(m.date) AS day, COUNT(*) AS cnt
            FROM messages m
            WHERE m.date >= date('now', ?)
              {channel_filter}
            GROUP BY day
            ORDER BY day ASC
            """,
            params,
        )
        return [MessageVelocity(date=r["day"], count=int(r["cnt"])) for r in rows]

    async def get_peak_hours(self, channel_id: int | None = None, days: int = 30) -> list[PeakHour]:
        """Return message count distribution by hour for one channel or all channels."""
        channel_filter = "AND m.channel_id = ?" if channel_id is not None else ""
        params: tuple[object, ...]
        if channel_id is not None:
            params = (f"-{days} days", channel_id)
        else:
            params = (f"-{days} days",)
        rows = await self._db.execute_fetchall(
            f"""
            SELECT CAST(strftime('%H', m.date) AS INTEGER) AS hour, COUNT(*) AS cnt
            FROM messages m
            WHERE m.date >= date('now', ?)
              {channel_filter}
            GROUP BY hour
            ORDER BY hour ASC
            """,
            params,
        )
        return [PeakHour(hour=int(r["hour"]), count=int(r["cnt"])) for r in rows]
