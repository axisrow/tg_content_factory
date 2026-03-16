from __future__ import annotations

from datetime import datetime

import aiosqlite

from src.models import Pipeline


class PipelinesRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    async def add(self, pipeline: Pipeline) -> int:
        cur = await self._db.execute(
            "INSERT INTO pipelines (name, is_active) VALUES (?, ?)",
            (pipeline.name, int(pipeline.is_active)),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    async def get_all(self, active_only: bool = False) -> list[Pipeline]:
        sql = "SELECT * FROM pipelines"
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY id"
        cur = await self._db.execute(sql)
        rows = await cur.fetchall()
        return [self._row_to_model(row) for row in rows]

    async def get_by_id(self, pipeline_id: int) -> Pipeline | None:
        cur = await self._db.execute("SELECT * FROM pipelines WHERE id = ?", (pipeline_id,))
        row = await cur.fetchone()
        return self._row_to_model(row) if row else None

    async def set_active(self, pipeline_id: int, active: bool) -> None:
        await self._db.execute(
            "UPDATE pipelines SET is_active = ? WHERE id = ?",
            (int(active), pipeline_id),
        )
        await self._db.commit()

    async def update(self, pipeline_id: int, pipeline: Pipeline) -> None:
        await self._db.execute(
            "UPDATE pipelines SET name = ? WHERE id = ?",
            (pipeline.name, pipeline_id),
        )
        await self._db.commit()

    async def delete(self, pipeline_id: int) -> None:
        await self._db.execute("DELETE FROM pipelines WHERE id = ?", (pipeline_id,))
        await self._db.commit()

    @staticmethod
    def _row_to_model(row) -> Pipeline:
        return Pipeline(
            id=row["id"],
            name=row["name"],
            is_active=bool(row["is_active"]),
            created_at=datetime.fromisoformat(row["created_at"]) if row["created_at"] else None,
        )
