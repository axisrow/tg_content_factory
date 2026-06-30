"""Репозиторий каталога сгенерированных изображений (промпт/модель/путь)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import aiosqlite

from src.models import GeneratedImage

if TYPE_CHECKING:
    from src.database.facade import Database


class GeneratedImagesRepository:
    """Каталог сгенерированных изображений: промпт, модель, URL/локальный путь.

    Append-only история результатов image-генерации для галереи и переиспользования.
    """

    def __init__(
        self,
        db: aiosqlite.Connection,
        *,
        database: "Database | None" = None,
    ):
        self._db = db
        self._database = database

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
        """Сохранить запись о сгенерированном изображении. Возвращает id новой строки."""
        assert self._database is not None, (
            "GeneratedImagesRepository.save requires a Database reference"
        )
        cur = await self._database.execute_write(
            "INSERT INTO generated_images (prompt, model, image_url, local_path) VALUES (?, ?, ?, ?)",
            (prompt, model, image_url, local_path),
        )
        return cur.lastrowid or 0

    async def list_recent(self, limit: int = 50) -> list[GeneratedImage]:
        """Последние `limit` сгенерированных изображений (новые сверху)."""
        cur = await self._db.execute(
            "SELECT * FROM generated_images ORDER BY id DESC LIMIT ?", (limit,)
        )
        return [self._to_model(row) for row in await cur.fetchall()]

    async def get_by_id(self, image_id: int) -> GeneratedImage | None:
        """Изображение по id, либо None."""
        cur = await self._db.execute(
            "SELECT * FROM generated_images WHERE id = ?", (image_id,)
        )
        row = await cur.fetchone()
        return self._to_model(row) if row else None
