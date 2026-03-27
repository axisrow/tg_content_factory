from __future__ import annotations

import json
from datetime import datetime

import aiosqlite

from src.models import (
    ContentPipeline,
    PipelineGenerationBackend,
    PipelinePublishMode,
    PipelineSource,
    PipelineTarget,
)


def _dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


class ContentPipelinesRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    @staticmethod
    def _to_pipeline(row: aiosqlite.Row) -> ContentPipeline:
        return ContentPipeline(
            id=row["id"],
            name=row["name"],
            prompt_template=row["prompt_template"],
            llm_model=row["llm_model"],
            image_model=row["image_model"],
            publish_mode=PipelinePublishMode(row["publish_mode"]),
            generation_backend=PipelineGenerationBackend(row["generation_backend"]),
            is_active=bool(row["is_active"]),
            last_generated_id=row["last_generated_id"],
            generate_interval_minutes=row["generate_interval_minutes"],
            publish_times=row["publish_times"] if "publish_times" in row.keys() else None,
            refinement_steps=(
                json.loads(row["refinement_steps"])
                if "refinement_steps" in row.keys() and row["refinement_steps"]
                else []
            ),
            created_at=_dt(row["created_at"]),
        )

    @staticmethod
    def _to_source(row: aiosqlite.Row) -> PipelineSource:
        return PipelineSource(
            id=row["id"],
            pipeline_id=row["pipeline_id"],
            channel_id=row["channel_id"],
            created_at=_dt(row["created_at"]),
        )

    @staticmethod
    def _to_target(row: aiosqlite.Row) -> PipelineTarget:
        return PipelineTarget(
            id=row["id"],
            pipeline_id=row["pipeline_id"],
            phone=row["phone"],
            dialog_id=row["target_dialog_id"],
            title=row["target_title"],
            dialog_type=row["target_type"],
            created_at=_dt(row["created_at"]),
        )

    async def add(
        self,
        pipeline: ContentPipeline,
        source_channel_ids: list[int],
        targets: list[PipelineTarget],
    ) -> int:
        await self._db.execute("BEGIN IMMEDIATE")
        try:
            cur = await self._db.execute(
                """
                INSERT INTO content_pipelines (
                    name, prompt_template, llm_model, image_model, publish_mode,
                    generation_backend, is_active, last_generated_id, generate_interval_minutes,
                    publish_times
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    pipeline.name,
                    pipeline.prompt_template,
                    pipeline.llm_model,
                    pipeline.image_model,
                    pipeline.publish_mode.value,
                    pipeline.generation_backend.value,
                    int(pipeline.is_active),
                    pipeline.last_generated_id,
                    pipeline.generate_interval_minutes,
                    pipeline.publish_times,
                ),
            )
            pipeline_id = cur.lastrowid or 0
            await self._replace_sources_no_commit(pipeline_id, source_channel_ids)
            await self._replace_targets_no_commit(pipeline_id, targets)
            await self._db.commit()
            return pipeline_id
        except Exception:
            await self._db.rollback()
            raise

    async def get_all(self, active_only: bool = False) -> list[ContentPipeline]:
        sql = "SELECT * FROM content_pipelines"
        params: tuple[object, ...] = ()
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY id"
        cur = await self._db.execute(sql, params)
        return [self._to_pipeline(row) for row in await cur.fetchall()]

    async def get_by_id(self, pipeline_id: int) -> ContentPipeline | None:
        cur = await self._db.execute(
            "SELECT * FROM content_pipelines WHERE id = ?",
            (pipeline_id,),
        )
        row = await cur.fetchone()
        return self._to_pipeline(row) if row else None

    async def update_generate_interval(self, pipeline_id: int, minutes: int) -> None:
        await self._db.execute(
            "UPDATE content_pipelines SET generate_interval_minutes = ? WHERE id = ?",
            (minutes, pipeline_id),
        )
        await self._db.commit()

    async def update(
        self,
        pipeline_id: int,
        pipeline: ContentPipeline,
        source_channel_ids: list[int],
        targets: list[PipelineTarget],
    ) -> bool:
        await self._db.execute("BEGIN IMMEDIATE")
        try:
            cur = await self._db.execute(
                """
                UPDATE content_pipelines
                SET name = ?, prompt_template = ?, llm_model = ?, image_model = ?,
                    publish_mode = ?, generation_backend = ?, is_active = ?,
                    generate_interval_minutes = ?, publish_times = ?
                WHERE id = ?
                """,
                (
                    pipeline.name,
                    pipeline.prompt_template,
                    pipeline.llm_model,
                    pipeline.image_model,
                    pipeline.publish_mode.value,
                    pipeline.generation_backend.value,
                    int(pipeline.is_active),
                    pipeline.generate_interval_minutes,
                    pipeline.publish_times,
                    pipeline_id,
                ),
            )
            if not cur.rowcount:
                await self._db.rollback()
                return False
            await self._replace_sources_no_commit(pipeline_id, source_channel_ids)
            await self._replace_targets_no_commit(pipeline_id, targets)
            await self._db.commit()
            return True
        except Exception:
            await self._db.rollback()
            raise

    async def set_refinement_steps(self, pipeline_id: int, steps: list[dict]) -> None:
        await self._db.execute(
            "UPDATE content_pipelines SET refinement_steps = ? WHERE id = ?",
            (json.dumps(steps, ensure_ascii=False), pipeline_id),
        )
        await self._db.commit()

    async def set_active(self, pipeline_id: int, active: bool) -> None:
        await self._db.execute(
            "UPDATE content_pipelines SET is_active = ? WHERE id = ?",
            (int(active), pipeline_id),
        )
        await self._db.commit()

    async def set_last_generated_id(self, pipeline_id: int, value: int) -> None:
        await self._db.execute(
            "UPDATE content_pipelines SET last_generated_id = ? WHERE id = ?",
            (value, pipeline_id),
        )
        await self._db.commit()

    async def delete(self, pipeline_id: int) -> None:
        await self._db.execute("DELETE FROM content_pipelines WHERE id = ?", (pipeline_id,))
        await self._db.commit()

    async def list_sources(self, pipeline_id: int) -> list[PipelineSource]:
        cur = await self._db.execute(
            "SELECT * FROM pipeline_sources WHERE pipeline_id = ? ORDER BY id",
            (pipeline_id,),
        )
        return [self._to_source(row) for row in await cur.fetchall()]

    async def list_targets(self, pipeline_id: int) -> list[PipelineTarget]:
        cur = await self._db.execute(
            "SELECT * FROM pipeline_targets WHERE pipeline_id = ? ORDER BY id",
            (pipeline_id,),
        )
        return [self._to_target(row) for row in await cur.fetchall()]

    async def _replace_sources_no_commit(
        self,
        pipeline_id: int,
        source_channel_ids: list[int],
    ) -> None:
        await self._db.execute("DELETE FROM pipeline_sources WHERE pipeline_id = ?", (pipeline_id,))
        if source_channel_ids:
            await self._db.executemany(
                "INSERT INTO pipeline_sources (pipeline_id, channel_id) VALUES (?, ?)",
                [(pipeline_id, channel_id) for channel_id in source_channel_ids],
            )

    async def _replace_targets_no_commit(
        self,
        pipeline_id: int,
        targets: list[PipelineTarget],
    ) -> None:
        await self._db.execute("DELETE FROM pipeline_targets WHERE pipeline_id = ?", (pipeline_id,))
        if targets:
            await self._db.executemany(
                """
                INSERT INTO pipeline_targets (
                    pipeline_id, phone, target_dialog_id, target_title, target_type
                ) VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (pipeline_id, target.phone, target.dialog_id, target.title, target.dialog_type)
                    for target in targets
                ],
            )
