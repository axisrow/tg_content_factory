from __future__ import annotations

from datetime import datetime

import aiosqlite

from src.models import SearchQuery, SearchQueryDailyStat


class SearchQueriesRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    async def add(self, sq: SearchQuery) -> int:
        cur = await self._db.execute(
            "INSERT INTO search_queries (name, query, is_active, interval_minutes) "
            "VALUES (?, ?, ?, ?)",
            (sq.name, sq.query, int(sq.is_active), sq.interval_minutes),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    async def get_all(self, active_only: bool = False) -> list[SearchQuery]:
        sql = "SELECT * FROM search_queries"
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY id"
        cur = await self._db.execute(sql)
        rows = await cur.fetchall()
        return [self._row_to_model(r) for r in rows]

    async def get_by_id(self, sq_id: int) -> SearchQuery | None:
        cur = await self._db.execute("SELECT * FROM search_queries WHERE id = ?", (sq_id,))
        row = await cur.fetchone()
        return self._row_to_model(row) if row else None

    async def set_active(self, sq_id: int, active: bool) -> None:
        await self._db.execute(
            "UPDATE search_queries SET is_active = ? WHERE id = ?", (int(active), sq_id)
        )
        await self._db.commit()

    async def delete(self, sq_id: int) -> None:
        await self._db.execute("DELETE FROM search_query_stats WHERE query_id = ?", (sq_id,))
        await self._db.execute("DELETE FROM search_queries WHERE id = ?", (sq_id,))
        await self._db.commit()

    async def record_stat(self, query_id: int, match_count: int) -> None:
        # One stat per query per day: delete existing entry for today, then insert
        await self._db.execute(
            "DELETE FROM search_query_stats "
            "WHERE query_id = ? AND date(recorded_at) = date('now')",
            (query_id,),
        )
        await self._db.execute(
            "INSERT INTO search_query_stats (query_id, match_count) VALUES (?, ?)",
            (query_id, match_count),
        )
        await self._db.commit()

    async def get_daily_stats(
        self, query_id: int, days: int = 30
    ) -> list[SearchQueryDailyStat]:
        cur = await self._db.execute(
            """
            SELECT date(recorded_at) AS day, SUM(match_count) AS count
            FROM search_query_stats
            WHERE query_id = ?
              AND recorded_at >= datetime('now', ?)
            GROUP BY day
            ORDER BY day
            """,
            (query_id, f"-{days} days"),
        )
        rows = await cur.fetchall()
        return [SearchQueryDailyStat(day=r["day"], count=r["count"]) for r in rows]

    async def get_stats_for_all(self, days: int = 30) -> dict[int, list[SearchQueryDailyStat]]:
        cur = await self._db.execute(
            """
            SELECT query_id, date(recorded_at) AS day, SUM(match_count) AS count
            FROM search_query_stats
            WHERE recorded_at >= datetime('now', ?)
            GROUP BY query_id, day
            ORDER BY query_id, day
            """,
            (f"-{days} days",),
        )
        rows = await cur.fetchall()
        result: dict[int, list[SearchQueryDailyStat]] = {}
        for r in rows:
            result.setdefault(r["query_id"], []).append(
                SearchQueryDailyStat(day=r["day"], count=r["count"])
            )
        return result

    async def get_last_recorded_at(self, query_id: int) -> str | None:
        cur = await self._db.execute(
            "SELECT MAX(recorded_at) AS last FROM search_query_stats WHERE query_id = ?",
            (query_id,),
        )
        row = await cur.fetchone()
        return row["last"] if row else None

    async def get_last_recorded_at_all(self) -> dict[int, str]:
        cur = await self._db.execute(
            "SELECT query_id, MAX(recorded_at) AS last "
            "FROM search_query_stats GROUP BY query_id"
        )
        rows = await cur.fetchall()
        return {r["query_id"]: r["last"] for r in rows if r["last"]}

    @staticmethod
    def _row_to_model(row) -> SearchQuery:
        return SearchQuery(
            id=row["id"],
            name=row["name"],
            query=row["query"],
            is_active=bool(row["is_active"]),
            interval_minutes=row["interval_minutes"],
            created_at=datetime.fromisoformat(row["created_at"]) if row["created_at"] else None,
        )
