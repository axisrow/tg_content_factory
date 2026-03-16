from __future__ import annotations

import json
from datetime import datetime

import aiosqlite

from src.models import GenerationRun


def _dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


class GenerationRunsRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    async def create_run(self, pipeline_id: int | None, prompt: str) -> int:
        cur = await self._db.execute(
            ("INSERT INTO generation_runs (pipeline_id, status, prompt, created_at) "
             "VALUES (?, 'pending', ?, datetime('now'))"),
            (pipeline_id, prompt),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    async def set_status(self, run_id: int, status: str) -> None:
        await self._db.execute(
            "UPDATE generation_runs SET status = ?, updated_at = datetime('now') WHERE id = ?",
            (status, run_id),
        )
        await self._db.commit()

    async def save_result(
        self, run_id: int, generated_text: str, metadata: dict | None = None
    ) -> None:
        await self._db.execute(
            ("UPDATE generation_runs SET generated_text = ?, metadata = ?, status = 'completed', "
             "updated_at = datetime('now') WHERE id = ?"),
            (generated_text, json.dumps(metadata or {}, ensure_ascii=False), run_id),
        )
        await self._db.commit()

    async def get(self, run_id: int) -> GenerationRun | None:
        cur = await self._db.execute("SELECT * FROM generation_runs WHERE id = ?", (run_id,))
        row = await cur.fetchone()
        if not row:
            return None
        metadata = None
        if row["metadata"]:
            try:
                metadata = json.loads(row["metadata"])
            except Exception:
                metadata = None
        return GenerationRun(
            id=row["id"],
            pipeline_id=row["pipeline_id"],
            status=row["status"],
            prompt=row["prompt"],
            generated_text=row["generated_text"],
            metadata=metadata,
            created_at=_dt(row["created_at"]),
            updated_at=_dt(row["updated_at"]),
        )

    async def list_by_pipeline(
        self, pipeline_id: int, limit: int = 20, offset: int = 0
    ) -> list[GenerationRun]:
        cur = await self._db.execute(
            "SELECT * FROM generation_runs WHERE pipeline_id = ? ORDER BY id DESC LIMIT ? OFFSET ?",
            (pipeline_id, limit, offset),
        )
        rows = await cur.fetchall()
        results: list[GenerationRun] = []
        for row in rows:
            metadata = None
            if row["metadata"]:
                try:
                    metadata = json.loads(row["metadata"])
                except Exception:
                    metadata = None
            results.append(
                GenerationRun(
                    id=row["id"],
                    pipeline_id=row["pipeline_id"],
                    status=row["status"],
                    prompt=row["prompt"],
                    generated_text=row["generated_text"],
                    metadata=metadata,
                    created_at=_dt(row["created_at"]),
                    updated_at=_dt(row["updated_at"]),
                )
            )
        return results
