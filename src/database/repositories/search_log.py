from __future__ import annotations

import aiosqlite


class SearchLogRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    async def log_search(self, phone: str, query: str, results_count: int) -> None:
        await self._db.execute(
            "INSERT INTO search_log (phone, query, results_count) VALUES (?, ?, ?)",
            (phone, query, results_count),
        )
        await self._db.commit()

    async def get_recent_searches(self, limit: int = 20) -> list[dict]:
        cur = await self._db.execute("SELECT * FROM search_log ORDER BY id DESC LIMIT ?", (limit,))
        rows = await cur.fetchall()
        return [
            {
                "id": r["id"],
                "phone": r["phone"],
                "query": r["query"],
                "results_count": r["results_count"],
                "created_at": r["created_at"],
            }
            for r in rows
        ]
