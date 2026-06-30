"""Репозиторий контент-пайплайнов и их связей источник/цель.

Доступ через `db.repos.content_pipelines`. Хранит сам пайплайн
(`content_pipelines` — промпт, модели, режим публикации, DAG `pipeline_json`,
A/B-настройки) и две дочерние таблицы: `pipeline_sources` (каналы-источники,
по channel_id) и `pipeline_targets` (диалоги-получатели публикации). Источники и
цели всегда заменяются целиком в одной транзакции с самим пайплайном.
"""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any

import aiosqlite

from src.database.pool import ReadConnection
from src.models import (
    ContentPipeline,
    PipelineGenerationBackend,
    PipelineGraph,
    PipelinePublishMode,
    PipelineSource,
    PipelineTarget,
)
from src.utils.datetime import parse_datetime

if TYPE_CHECKING:
    from src.database.facade import Database

logger = logging.getLogger(__name__)


class ContentPipelinesRepository:
    """CRUD контент-пайплайнов вместе с их источниками и целями публикации."""

    def __init__(
        self,
        db: ReadConnection,
        *,
        database: "Database | None" = None,
    ):
        self._db = db
        self._database = database

    @staticmethod
    def _to_pipeline(row: aiosqlite.Row) -> ContentPipeline:
        keys = row.keys()
        pipeline_json_raw = row["pipeline_json"] if "pipeline_json" in keys else None
        pipeline_graph: PipelineGraph | None = None
        if pipeline_json_raw:
            try:
                pipeline_graph = PipelineGraph.from_json(pipeline_json_raw)
            except Exception:
                # A malformed graph silently falls back to legacy RAG, ignoring every
                # configured DAG node. Log it so the misconfiguration is diagnosable (#676).
                logger.warning(
                    "Pipeline %s: failed to deserialize pipeline_json; falling back to legacy RAG",
                    row["id"] if "id" in keys else "?",
                    exc_info=True,
                )
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
            ab_num_variants=(
                row["ab_num_variants"]
                if "ab_num_variants" in keys and row["ab_num_variants"] is not None
                else 1
            ),
            ab_auto_select=(
                bool(row["ab_auto_select"])
                if "ab_auto_select" in keys and row["ab_auto_select"] is not None
                else False
            ),
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
        """Создать пайплайн вместе с его источниками и целями (одной транзакцией); вернуть новый id."""
        assert self._database is not None, (
            "ContentPipelinesRepository.add requires a Database reference"
        )
        async with self._database.transaction() as conn:
            pipeline_json_str = pipeline.pipeline_json.to_json() if pipeline.pipeline_json else None
            cur = await conn.execute(
                """
                INSERT INTO content_pipelines (
                    name, prompt_template, llm_model, image_model, publish_mode,
                    generation_backend, is_active, last_generated_id, generate_interval_minutes,
                    publish_times, pipeline_json, account_phone, ab_num_variants, ab_auto_select
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    pipeline.ab_num_variants,
                    int(pipeline.ab_auto_select),
                ),
            )
            pipeline_id = cur.lastrowid or 0
            await self._replace_sources_no_commit(pipeline_id, source_channel_ids, conn=conn)
            await self._replace_targets_no_commit(pipeline_id, targets, conn=conn)
            return pipeline_id

    async def get_all(self, active_only: bool = False) -> list[ContentPipeline]:
        """Все пайплайны по возрастанию id; с ``active_only`` — только активные."""
        sql = "SELECT * FROM content_pipelines"
        params: tuple[object, ...] = ()
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY id"
        cur = await self._db.execute(sql, params)
        return [self._to_pipeline(row) for row in await cur.fetchall()]

    async def get_by_id(self, pipeline_id: int) -> ContentPipeline | None:
        """Один пайплайн по id, либо ``None`` если такого нет."""
        cur = await self._db.execute(
            "SELECT * FROM content_pipelines WHERE id = ?",
            (pipeline_id,),
        )
        row = await cur.fetchone()
        return self._to_pipeline(row) if row else None

    async def update_generate_interval(self, pipeline_id: int, minutes: int) -> None:
        """Изменить интервал автогенерации (в минутах) для пайплайна."""
        assert self._database is not None, (
            "ContentPipelinesRepository.update_generate_interval requires a Database reference"
        )
        await self._database.execute_write(
            "UPDATE content_pipelines SET generate_interval_minutes = ? WHERE id = ?",
            (minutes, pipeline_id),
        )

    async def update(
        self,
        pipeline_id: int,
        pipeline: ContentPipeline,
        source_channel_ids: list[int],
        targets: list[PipelineTarget],
    ) -> bool:
        """Полностью обновить пайплайн и заменить его источники/цели (одной транзакцией).

        Возвращает ``False``, если пайплайна с таким id нет (поля ``last_generated_id``
        и ``refinement_steps`` тут не трогаются — для них есть отдельные сеттеры).
        """
        assert self._database is not None, (
            "ContentPipelinesRepository.update requires a Database reference"
        )

        class _NotFoundError(Exception):
            pass

        try:
            async with self._database.transaction() as conn:
                pipeline_json_str = pipeline.pipeline_json.to_json() if pipeline.pipeline_json else None
                cur = await conn.execute(
                    """
                    UPDATE content_pipelines
                    SET name = ?, prompt_template = ?, llm_model = ?, image_model = ?,
                        publish_mode = ?, generation_backend = ?, is_active = ?,
                        generate_interval_minutes = ?, publish_times = ?, pipeline_json = ?,
                        account_phone = ?, ab_num_variants = ?, ab_auto_select = ?
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
                        pipeline.ab_num_variants,
                        int(pipeline.ab_auto_select),
                        pipeline_id,
                    ),
                )
                if not cur.rowcount:
                    raise _NotFoundError
                await self._replace_sources_no_commit(pipeline_id, source_channel_ids, conn=conn)
                await self._replace_targets_no_commit(pipeline_id, targets, conn=conn)
        except _NotFoundError:
            return False
        return True

    async def set_refinement_steps(self, pipeline_id: int, steps: list[dict]) -> None:
        """Сохранить шаги доработки текста (список ``{name, prompt}``) для пайплайна как JSON."""
        assert self._database is not None, (
            "ContentPipelinesRepository.set_refinement_steps requires a Database reference"
        )
        await self._database.execute_write(
            "UPDATE content_pipelines SET refinement_steps = ? WHERE id = ?",
            (json.dumps(steps, ensure_ascii=False), pipeline_id),
        )

    async def set_pipeline_json(self, pipeline_id: int, graph: PipelineGraph | None) -> None:
        """Сохранить (или очистить через ``None``) DAG-конфигурацию пайплайна (issue #343)."""
        assert self._database is not None, (
            "ContentPipelinesRepository.set_pipeline_json requires a Database reference"
        )
        value = graph.to_json() if graph else None
        await self._database.execute_write(
            "UPDATE content_pipelines SET pipeline_json = ? WHERE id = ?",
            (value, pipeline_id),
        )

    async def set_active(self, pipeline_id: int, active: bool) -> None:
        """Включить/выключить пайплайн (неактивный не запускается планировщиком)."""
        assert self._database is not None, (
            "ContentPipelinesRepository.set_active requires a Database reference"
        )
        await self._database.execute_write(
            "UPDATE content_pipelines SET is_active = ? WHERE id = ?",
            (int(active), pipeline_id),
        )

    async def set_last_generated_id(self, pipeline_id: int, value: int) -> None:
        """Сдвинуть курсор последнего обработанного сообщения-источника (инкрементальная генерация)."""
        assert self._database is not None, (
            "ContentPipelinesRepository.set_last_generated_id requires a Database reference"
        )
        await self._database.execute_write(
            "UPDATE content_pipelines SET last_generated_id = ? WHERE id = ?",
            (value, pipeline_id),
        )

    async def delete(self, pipeline_id: int) -> None:
        """Удалить пайплайн по id (источники/цели уходят каскадом по FK)."""
        assert self._database is not None, (
            "ContentPipelinesRepository.delete requires a Database reference"
        )
        await self._database.execute_write("DELETE FROM content_pipelines WHERE id = ?", (pipeline_id,))

    async def list_sources(self, pipeline_id: int) -> list[PipelineSource]:
        """Каналы-источники пайплайна (`pipeline_sources`), по возрастанию id."""
        cur = await self._db.execute(
            "SELECT * FROM pipeline_sources WHERE pipeline_id = ? ORDER BY id",
            (pipeline_id,),
        )
        return [self._to_source(row) for row in await cur.fetchall()]

    async def list_targets(self, pipeline_id: int) -> list[PipelineTarget]:
        """Цели публикации пайплайна (`pipeline_targets`), по возрастанию id."""
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
        conn=None,
    ) -> None:
        # Must run on the caller's transaction connection so the DELETE+INSERT
        # stay inside the write-lock; falling back to self._db would commit them
        # autonomously and let another coroutine interleave (#633).
        executor: Any = conn or self._db
        await executor.execute("DELETE FROM pipeline_sources WHERE pipeline_id = ?", (pipeline_id,))
        if source_channel_ids:
            await executor.executemany(
                "INSERT INTO pipeline_sources (pipeline_id, channel_id) VALUES (?, ?)",
                [(pipeline_id, channel_id) for channel_id in source_channel_ids],
            )

    async def _replace_targets_no_commit(
        self,
        pipeline_id: int,
        targets: list[PipelineTarget],
        conn=None,
    ) -> None:
        executor: Any = conn or self._db
        await executor.execute("DELETE FROM pipeline_targets WHERE pipeline_id = ?", (pipeline_id,))
        if targets:
            await executor.executemany(
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
