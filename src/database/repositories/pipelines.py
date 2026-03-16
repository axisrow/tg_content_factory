from __future__ import annotations

import json
from datetime import datetime

import aiosqlite

from src.models import Pipeline, PipelineTarget


class PipelinesRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    async def add(self, pipeline: Pipeline) -> int:
        cur = await self._db.execute(
            """
            INSERT INTO pipelines (
                name,
                phone,
                source_channel_ids_json,
                targets_json,
                prompt_template,
                llm_model
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                pipeline.name,
                pipeline.phone,
                json.dumps(pipeline.source_channel_ids),
                json.dumps([target.model_dump() for target in pipeline.targets]),
                pipeline.prompt_template,
                pipeline.llm_model,
            ),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    async def list(self, phone: str | None = None) -> list[Pipeline]:
        sql = "SELECT * FROM pipelines"
        params: tuple[object, ...] = ()
        if phone:
            sql += " WHERE phone = ?"
            params = (phone,)
        sql += " ORDER BY id ASC"
        cur = await self._db.execute(sql, params)
        rows = await cur.fetchall()
        return [self._row_to_model(row) for row in rows]

    async def get_by_id(self, pipeline_id: int) -> Pipeline | None:
        cur = await self._db.execute("SELECT * FROM pipelines WHERE id = ?", (pipeline_id,))
        row = await cur.fetchone()
        return self._row_to_model(row) if row else None

    async def update(self, pipeline_id: int, pipeline: Pipeline) -> None:
        await self._db.execute(
            """
            UPDATE pipelines
            SET name = ?,
                phone = ?,
                source_channel_ids_json = ?,
                targets_json = ?,
                prompt_template = ?,
                llm_model = ?,
                updated_at = datetime('now')
            WHERE id = ?
            """,
            (
                pipeline.name,
                pipeline.phone,
                json.dumps(pipeline.source_channel_ids),
                json.dumps([target.model_dump() for target in pipeline.targets]),
                pipeline.prompt_template,
                pipeline.llm_model,
                pipeline_id,
            ),
        )
        await self._db.commit()

    async def delete(self, pipeline_id: int) -> None:
        await self._db.execute("DELETE FROM pipelines WHERE id = ?", (pipeline_id,))
        await self._db.commit()

    @staticmethod
    def _row_to_model(row: aiosqlite.Row) -> Pipeline:
        source_ids = json.loads(row["source_channel_ids_json"] or "[]")
        targets_raw = json.loads(row["targets_json"] or "[]")
        return Pipeline(
            id=row["id"],
            name=row["name"],
            phone=row["phone"],
            source_channel_ids=[int(item) for item in source_ids],
            targets=[PipelineTarget.model_validate(item) for item in targets_raw],
            prompt_template=row["prompt_template"],
            llm_model=row["llm_model"],
            created_at=datetime.fromisoformat(row["created_at"]) if row["created_at"] else None,
            updated_at=datetime.fromisoformat(row["updated_at"]) if row["updated_at"] else None,
        )
