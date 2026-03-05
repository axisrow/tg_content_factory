from __future__ import annotations

import aiosqlite

from src.models import Keyword


class KeywordsRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    async def add_keyword(self, keyword: Keyword) -> int:
        cur = await self._db.execute(
            "INSERT INTO keywords (pattern, is_regex, is_active) VALUES (?, ?, ?)",
            (keyword.pattern, int(keyword.is_regex), int(keyword.is_active)),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    async def get_keywords(self, active_only: bool = False) -> list[Keyword]:
        sql = "SELECT * FROM keywords"
        if active_only:
            sql += " WHERE is_active = 1"
        cur = await self._db.execute(sql)
        rows = await cur.fetchall()
        return [
            Keyword(
                id=r["id"],
                pattern=r["pattern"],
                is_regex=bool(r["is_regex"]),
                is_active=bool(r["is_active"]),
            )
            for r in rows
        ]

    async def set_keyword_active(self, keyword_id: int, active: bool) -> None:
        await self._db.execute(
            "UPDATE keywords SET is_active = ? WHERE id = ?", (int(active), keyword_id)
        )
        await self._db.commit()

    async def delete_keyword(self, keyword_id: int) -> None:
        await self._db.execute("DELETE FROM keywords WHERE id = ?", (keyword_id,))
        await self._db.commit()
