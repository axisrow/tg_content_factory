from __future__ import annotations

import json
from datetime import datetime

import aiosqlite

from src.models import Pipeline, PipelinePublishMode, PipelineTarget


class PipelinesRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    async def add(self, pipeline: Pipeline) -> int:
        cur = await self._db.execute(
            "INSERT INTO pipelines "
            "(name, phone, source_channel_ids, targets, prompt_template, "
            "llm_model, publish_mode, is_active) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                pipeline.name,
                pipeline.phone,
                json.dumps(pipeline.source_channel_ids),
                json.dumps([t.model_dump() for t in pipeline.targets]),
                pipeline.prompt_template,
                pipeline.llm_model,
                pipeline.publish_mode.value,
                int(pipeline.is_active),
            ),
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
        return [self._row_to_model(r) for r in rows]

    async def get_by_id(self, pipeline_id: int) -> Pipeline | None:
        cur = await self._db.execute(
            "SELECT * FROM pipelines WHERE id = ?", (pipeline_id,)
        )
        row = await cur.fetchone()
        return self._row_to_model(row) if row else None

    async def update(self, pipeline_id: int, pipeline: Pipeline) -> None:
        await self._db.execute(
            "UPDATE pipelines SET name = ?, phone = ?, source_channel_ids = ?, "
            "targets = ?, prompt_template = ?, llm_model = ?, publish_mode = ? "
            "WHERE id = ?",
            (
                pipeline.name,
                pipeline.phone,
                json.dumps(pipeline.source_channel_ids),
                json.dumps([t.model_dump() for t in pipeline.targets]),
                pipeline.prompt_template,
                pipeline.llm_model,
                pipeline.publish_mode.value,
                pipeline_id,
            ),
        )
        await self._db.commit()

    async def set_active(self, pipeline_id: int, active: bool) -> None:
        await self._db.execute(
            "UPDATE pipelines SET is_active = ? WHERE id = ?",
            (int(active), pipeline_id),
        )
        await self._db.commit()

    async def delete(self, pipeline_id: int) -> None:
        await self._db.execute("DELETE FROM pipelines WHERE id = ?", (pipeline_id,))
        await self._db.commit()

    @staticmethod
    def _row_to_model(row) -> Pipeline:
        try:
            source_ids = json.loads(row["source_channel_ids"]) if row["source_channel_ids"] else []
        except (json.JSONDecodeError, TypeError):
            source_ids = []
        try:
            raw_targets = json.loads(row["targets"]) if row["targets"] else []
        except (json.JSONDecodeError, TypeError):
            raw_targets = []
        targets = []
        for t in raw_targets:
            try:
                targets.append(PipelineTarget.model_validate(t))
            except Exception:
                pass
        try:
            publish_mode = PipelinePublishMode(row["publish_mode"])
        except (ValueError, KeyError):
            publish_mode = PipelinePublishMode.DRAFT
        return Pipeline(
            id=row["id"],
            name=row["name"],
            phone=row["phone"],
            source_channel_ids=source_ids,
            targets=targets,
            prompt_template=row["prompt_template"],
            llm_model=row["llm_model"],
            publish_mode=publish_mode,
            is_active=bool(row["is_active"]),
            created_at=(
                datetime.fromisoformat(row["created_at"]) if row["created_at"] else None
            ),
        )
