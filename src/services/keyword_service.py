from __future__ import annotations

from src.database import Database
from src.database.bundles import CollectionBundle
from src.models import Keyword


class KeywordService:
    def __init__(self, collection: CollectionBundle | Database):
        if isinstance(collection, Database):
            collection = CollectionBundle.from_database(collection)
        self._collection = collection

    async def add(self, pattern: str, is_regex: bool) -> int:
        return await self._collection.add_keyword(Keyword(pattern=pattern, is_regex=is_regex))

    async def list(self):
        return await self._collection.list_keywords()

    async def toggle(self, keyword_id: int) -> None:
        keywords = await self._collection.list_keywords()
        for kw in keywords:
            if kw.id == keyword_id:
                await self._collection.set_keyword_active(keyword_id, not kw.is_active)
                return

    async def delete(self, keyword_id: int) -> None:
        await self._collection.delete_keyword(keyword_id)
