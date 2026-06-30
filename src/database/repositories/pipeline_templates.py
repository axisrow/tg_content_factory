"""Репозиторий шаблонов контент-пайплайнов (встроенных и пользовательских)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import aiosqlite

from src.database.pool import ReadConnection
from src.models import PipelineGraph, PipelineTemplate
from src.utils.datetime import parse_datetime

if TYPE_CHECKING:
    from src.database.facade import Database


class PipelineTemplatesRepository:
    """CRUD над шаблонами контент-пайплайнов (граф нод сохранён как JSON).

    Шаблоны бывают встроенные (`is_builtin`, поставляются с приложением и
    досеиваются через `ensure_builtins`) и пользовательские; из шаблона
    создаётся готовый [`ContentPipeline`][src.models.ContentPipeline].
    """

    def __init__(
        self,
        db: ReadConnection,
        *,
        database: "Database | None" = None,
    ):
        self._db = db
        self._database = database

    @staticmethod
    def _to_template(row: aiosqlite.Row) -> PipelineTemplate:
        return PipelineTemplate(
            id=row["id"],
            name=row["name"],
            description=row["description"] or "",
            category=row["category"] or "",
            template_json=PipelineGraph.from_json(row["template_json"]),
            is_builtin=bool(row["is_builtin"]),
            created_at=parse_datetime(row["created_at"]),
        )

    async def list_all(self, category: str | None = None) -> list[PipelineTemplate]:
        """Все шаблоны (опционально одной категории); встроенные идут первыми."""
        if category:
            cur = await self._db.execute(
                "SELECT * FROM pipeline_templates WHERE category = ? ORDER BY is_builtin DESC, id",
                (category,),
            )
        else:
            cur = await self._db.execute(
                "SELECT * FROM pipeline_templates ORDER BY is_builtin DESC, id"
            )
        return [self._to_template(row) for row in await cur.fetchall()]

    async def get_by_id(self, template_id: int) -> PipelineTemplate | None:
        """Шаблон по id, либо None."""
        cur = await self._db.execute(
            "SELECT * FROM pipeline_templates WHERE id = ?", (template_id,)
        )
        row = await cur.fetchone()
        return self._to_template(row) if row else None

    async def get_by_name(self, name: str) -> PipelineTemplate | None:
        """Шаблон по уникальному имени, либо None."""
        cur = await self._db.execute(
            "SELECT * FROM pipeline_templates WHERE name = ?", (name,)
        )
        row = await cur.fetchone()
        return self._to_template(row) if row else None

    async def add(self, template: PipelineTemplate) -> int:
        """Сохранить шаблон (граф сериализуется в JSON). Возвращает id новой строки."""
        assert self._database is not None, (
            "PipelineTemplatesRepository.add requires a Database reference"
        )
        cur = await self._database.execute_write(
            """
            INSERT INTO pipeline_templates (name, description, category, template_json, is_builtin)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                template.name,
                template.description,
                template.category,
                template.template_json.to_json(),
                int(template.is_builtin),
            ),
        )
        return cur.lastrowid or 0

    async def delete(self, template_id: int) -> None:
        """Удалить шаблон по id."""
        assert self._database is not None, (
            "PipelineTemplatesRepository.delete requires a Database reference"
        )
        await self._database.execute_write("DELETE FROM pipeline_templates WHERE id = ?", (template_id,))

    async def ensure_builtins(self, builtins: list[PipelineTemplate]) -> None:
        """Insert builtin templates that don't exist yet (by name)."""
        assert self._database is not None, (
            "PipelineTemplatesRepository.ensure_builtins requires a Database reference"
        )
        async with self._database.transaction() as conn:
            for tpl in builtins:
                cur = await conn.execute(
                    "SELECT id FROM pipeline_templates WHERE name = ? AND is_builtin = 1", (tpl.name,)
                )
                if not await cur.fetchone():
                    await conn.execute(
                        """
                        INSERT INTO pipeline_templates (name, description, category, template_json, is_builtin)
                        VALUES (?, ?, ?, ?, 1)
                        """,
                        (tpl.name, tpl.description, tpl.category, tpl.template_json.to_json()),
                    )
