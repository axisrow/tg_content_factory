from __future__ import annotations

import aiosqlite

from src.models import GeneratedImage


class GeneratedImagesRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    @staticmethod
    def _to_model(row: aiosqlite.Row) -> GeneratedImage:
        return GeneratedImage(
            id=row["id"],
            prompt=row["prompt"],
            model=row["model"] if "model" in row.keys() else None,
            image_url=row["image_url"] if "image_url" in row.keys() else None,
            local_path=row["local_path"] if "local_path" in row.keys() else None,
            created_at=row["created_at"] if "created_at" in row.keys() else None,
        )

    async def save(self, prompt: str, model: str | None, image_url: str | None, local_path: str | None) -> int:
        cur = await self._db.execute(
            "INSERT INTO generated_images (prompt, model, image_url, local_path) VALUES (?, ?, ?, ?)",
            (prompt, model, image_url, local_path),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    async def list_recent(self, limit: int = 50) -> list[GeneratedImage]:
        cur = await self._db.execute(
            "SELECT * FROM generated_images ORDER BY id DESC LIMIT ?", (limit,)
        )
        return [self._to_model(row) for row in await cur.fetchall()]

    async def get_by_id(self, image_id: int) -> GeneratedImage | None:
        cur = await self._db.execute(
            "SELECT * FROM generated_images WHERE id = ?", (image_id,)
        )
        row = await cur.fetchone()
        return self._to_model(row) if row else None
