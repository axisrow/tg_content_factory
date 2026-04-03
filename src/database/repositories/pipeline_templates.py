from __future__ import annotations

from datetime import datetime

import aiosqlite

from src.models import PipelineGraph, PipelineTemplate


def _dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


class PipelineTemplatesRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    @staticmethod
    def _to_template(row: aiosqlite.Row) -> PipelineTemplate:
        return PipelineTemplate(
            id=row["id"],
            name=row["name"],
            description=row["description"] or "",
            category=row["category"] or "",
            template_json=PipelineGraph.from_json(row["template_json"]),
            is_builtin=bool(row["is_builtin"]),
            created_at=_dt(row["created_at"]),
        )

    async def list_all(self, category: str | None = None) -> list[PipelineTemplate]:
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
        cur = await self._db.execute(
            "SELECT * FROM pipeline_templates WHERE id = ?", (template_id,)
        )
        row = await cur.fetchone()
        return self._to_template(row) if row else None

    async def get_by_name(self, name: str) -> PipelineTemplate | None:
        cur = await self._db.execute(
            "SELECT * FROM pipeline_templates WHERE name = ?", (name,)
        )
        row = await cur.fetchone()
        return self._to_template(row) if row else None

    async def add(self, template: PipelineTemplate) -> int:
        cur = await self._db.execute(
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
        await self._db.commit()
        return cur.lastrowid or 0

    async def delete(self, template_id: int) -> None:
        await self._db.execute("DELETE FROM pipeline_templates WHERE id = ?", (template_id,))
        await self._db.commit()

    async def ensure_builtins(self, builtins: list[PipelineTemplate]) -> None:
        """Insert builtin templates that don't exist yet (by name)."""
        for tpl in builtins:
            cur = await self._db.execute(
                "SELECT id FROM pipeline_templates WHERE name = ? AND is_builtin = 1", (tpl.name,)
            )
            if not await cur.fetchone():
                await self._db.execute(
                    """
                    INSERT INTO pipeline_templates (name, description, category, template_json, is_builtin)
                    VALUES (?, ?, ?, ?, 1)
                    """,
                    (tpl.name, tpl.description, tpl.category, tpl.template_json.to_json()),
                )
        await self._db.commit()
