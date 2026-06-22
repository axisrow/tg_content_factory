"""Repository for channel rating verdicts (#966)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

import aiosqlite

from src.models import ChannelRating

if TYPE_CHECKING:
    from src.database.facade import Database


class ChannelRatingsRepository:
    def __init__(self, db: aiosqlite.Connection, *, database: "Database | None" = None):
        self._db = db
        self._database = database

    @staticmethod
    def _to_rating(row: aiosqlite.Row) -> ChannelRating:
        return ChannelRating(
            channel_id=row["channel_id"],
            title=row["title"],
            username=row["username"],
            useful=row["useful"],
            genre=row["genre"],
            confidence=row["confidence"] or 0.0,
            reason=row["reason"],
            emoji_trash_score=row["emoji_trash_score"],
            flag_count=row["flag_count"] or 0,
            n_total=row["n_total"] or 0,
            updated_at=(
                datetime.fromisoformat(row["updated_at"])
                if row["updated_at"]
                else None
            ),
        )

    async def upsert(self, rating: ChannelRating) -> None:
        now = (rating.updated_at or datetime.now(tz=timezone.utc)).isoformat()
        await self._database.execute_write(
            "INSERT INTO channel_ratings "
            "(channel_id, title, username, useful, genre, confidence, reason, "
            " emoji_trash_score, flag_count, n_total, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(channel_id) DO UPDATE SET "
            "  title=excluded.title, username=excluded.username, useful=excluded.useful, "
            "  genre=excluded.genre, confidence=excluded.confidence, reason=excluded.reason, "
            "  emoji_trash_score=excluded.emoji_trash_score, "
            # flag_count is a human-audit signal — never let a machine
            # re-classification (which builds the row with flag_count=0) reset it
            # (review on #966). A future manual-flag op uses a dedicated method.
            "  flag_count=channel_ratings.flag_count, "
            "  n_total=excluded.n_total, updated_at=excluded.updated_at",
            (
                rating.channel_id,
                rating.title,
                rating.username,
                rating.useful,
                rating.genre,
                rating.confidence,
                rating.reason,
                rating.emoji_trash_score,
                rating.flag_count,
                rating.n_total,
                now,
            ),
        )

    async def get(self, channel_id: int) -> ChannelRating | None:
        cur = await self._db.execute(
            "SELECT * FROM channel_ratings WHERE channel_id = ?", (channel_id,)
        )
        row = await cur.fetchone()
        return self._to_rating(row) if row else None

    async def list_ratings(
        self,
        *,
        useful: str | None = None,
        genre: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[ChannelRating]:
        where: list[str] = []
        params: list[object] = []
        if useful is not None:
            where.append("useful = ?")
            params.append(useful)
        if genre is not None:
            where.append("genre = ?")
            params.append(genre)
        clause = f"WHERE {' AND '.join(where)} " if where else ""
        params.extend([limit, offset])
        cur = await self._db.execute(
            f"SELECT * FROM channel_ratings {clause}"
            "ORDER BY confidence DESC, channel_id ASC LIMIT ? OFFSET ?",
            tuple(params),
        )
        return [self._to_rating(r) for r in await cur.fetchall()]

    async def count(self) -> int:
        cur = await self._db.execute("SELECT COUNT(*) AS c FROM channel_ratings")
        row = await cur.fetchone()
        return row["c"] if row else 0
