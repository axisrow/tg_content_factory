from __future__ import annotations

from src.database import Database
from src.models import Keyword


class KeywordService:
    def __init__(self, db: Database):
        self._db = db

    async def add(self, pattern: str, is_regex: bool) -> int:
        return await self._db.add_keyword(Keyword(pattern=pattern, is_regex=is_regex))

    async def list(self):
        return await self._db.get_keywords()

    async def toggle(self, keyword_id: int) -> None:
        keywords = await self._db.get_keywords()
        for kw in keywords:
            if kw.id == keyword_id:
                await self._db.set_keyword_active(keyword_id, not kw.is_active)
                return

    async def delete(self, keyword_id: int) -> None:
        await self._db.delete_keyword(keyword_id)
