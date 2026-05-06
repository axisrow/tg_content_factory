from __future__ import annotations

import json

import aiosqlite

from src.database.repositories._transactions import begin_immediate
from src.models import (
    ContentPipeline,
    PipelineGenerationBackend,
    PipelineGraph,
    PipelinePublishMode,
    PipelineSource,
    PipelineTarget,
)
from src.utils.datetime import parse_datetime


class ContentPipelinesRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    @staticmethod
    def _to_pipeline(row: aiosqlite.Row) -> ContentPipeline:
        keys = row.keys()
        pipeline_json_raw = row["pipeline_json"] if "pipeline_json" in keys else None
        pipeline_graph: PipelineGraph | None = None
        if pipeline_json_raw:
            try:
                pipeline_graph = PipelineGraph.from_json(pipeline_json_raw)
            except Exception:
                pipeline_graph = None
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
            publish_times=row["publish_times"] if "publish_times" in keys else None,
            refinement_steps=(
                json.loads(row["refinement_steps"])
                if "refinement_steps" in keys and row["refinement_steps"]
                else []
            ),
            pipeline_json=pipeline_graph,
            account_phone=row["account_phone"] if "account_phone" in keys else None,
            created_at=parse_datetime(row["created_at"]),
        )

    @staticmethod
    def _to_source(row: aiosqlite.Row) -> PipelineSource:
        return PipelineSource(
            id=row["id"],
            pipeline_id=row["pipeline_id"],
            channel_id=row["channel_id"],
            created_at=parse_datetime(row["created_at"]),
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
            created_at=parse_datetime(row["created_at"]),
        )

    async def add(
        self,
        pipeline: ContentPipeline,
        source_channel_ids: list[int],
        targets: list[PipelineTarget],
    ) -> int:
        await begin_immediate(self._db)
        try:
            pipeline_json_str = pipeline.pipeline_json.to_json() if pipeline.pipeline_json else None
            cur = await self._db.execute(
                """
                INSERT INTO content_pipelines (
                    name, prompt_template, llm_model, image_model, publish_mode,
                    generation_backend, is_active, last_generated_id, generate_interval_minutes,
                    publish_times, pipeline_json, account_phone
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    pipeline_json_str,
                    pipeline.account_phone,
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
        await begin_immediate(self._db)
        try:
            pipeline_json_str = pipeline.pipeline_json.to_json() if pipeline.pipeline_json else None
            cur = await self._db.execute(
                """
                UPDATE content_pipelines
                SET name = ?, prompt_template = ?, llm_model = ?, image_model = ?,
                    publish_mode = ?, generation_backend = ?, is_active = ?,
                    generate_interval_minutes = ?, publish_times = ?, pipeline_json = ?,
                    account_phone = ?
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
                    pipeline_json_str,
                    pipeline.account_phone,
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

    async def set_pipeline_json(self, pipeline_id: int, graph: PipelineGraph | None) -> None:
        value = graph.to_json() if graph else None
        await self._db.execute(
            "UPDATE content_pipelines SET pipeline_json = ? WHERE id = ?",
            (value, pipeline_id),
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

    async def batch_sources(self, pipeline_ids: list[int]) -> list[PipelineSource]:
        """Load sources for multiple pipelines in one query."""
        if not pipeline_ids:
            return []
        placeholders = ",".join("?" * len(pipeline_ids))
        cur = await self._db.execute(
            f"SELECT * FROM pipeline_sources WHERE pipeline_id IN ({placeholders}) ORDER BY id",
            pipeline_ids,
        )
        return [self._to_source(row) for row in await cur.fetchall()]

    async def batch_targets(self, pipeline_ids: list[int]) -> list[PipelineTarget]:
        """Load targets for multiple pipelines in one query."""
        if not pipeline_ids:
            return []
        placeholders = ",".join("?" * len(pipeline_ids))
        cur = await self._db.execute(
            f"SELECT * FROM pipeline_targets WHERE pipeline_id IN ({placeholders}) ORDER BY id",
            pipeline_ids,
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
