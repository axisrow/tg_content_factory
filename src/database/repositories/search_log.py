"""Журнал поисковых запросов (история поиска по аккаунтам)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import aiosqlite

if TYPE_CHECKING:
    from src.database.facade import Database


class SearchLogRepository:
    """Append-only журнал поисков: кто (`phone`), что (`query`), сколько нашёл.

    Историческая лента для аналитики/UI «недавние запросы»; только запись и
    чтение последних записей, без апдейтов.
    """

    def __init__(
        self,
        db: aiosqlite.Connection,
        *,
        database: "Database | None" = None,
    ):
        self._db = db
        self._database = database

    async def log_search(self, phone: str, query: str, results_count: int) -> None:
        """Записать факт поиска: телефон, запрос и число найденных результатов."""
        await self._database.execute_write(
            "INSERT INTO search_log (phone, query, results_count) VALUES (?, ?, ?)",
            (phone, query, results_count),
        )

    async def get_recent_searches(self, limit: int = 20) -> list[dict]:
        """Последние `limit` записей журнала поиска (новые сверху)."""
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
