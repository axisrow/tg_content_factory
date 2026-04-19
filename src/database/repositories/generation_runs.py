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

    @staticmethod
    def _to_generation_run(row: aiosqlite.Row) -> GenerationRun:
        metadata = None
        if row["metadata"]:
            try:
                metadata = json.loads(row["metadata"])
            except Exception:
                metadata = None

        quality_issues = None
        if "quality_issues" in row.keys() and row["quality_issues"]:
            try:
                quality_issues = json.loads(row["quality_issues"])
            except Exception:
                quality_issues = None

        variants = None
        if "variants" in row.keys() and row["variants"]:
            try:
                variants = json.loads(row["variants"])
            except Exception:
                variants = None

        return GenerationRun(
            id=row["id"],
            pipeline_id=row["pipeline_id"],
            status=row["status"],
            prompt=row["prompt"],
            generated_text=row["generated_text"],
            metadata=metadata,
            image_url=row["image_url"] if "image_url" in row.keys() else None,
            moderation_status=(
                row["moderation_status"] if "moderation_status" in row.keys() else "pending"
            )
            or "pending",
            quality_score=row["quality_score"] if "quality_score" in row.keys() else None,
            quality_issues=quality_issues,
            variants=variants,
            selected_variant=row["selected_variant"] if "selected_variant" in row.keys() else None,
            published_at=_dt(row["published_at"] if "published_at" in row.keys() else None),
            created_at=_dt(row["created_at"]),
            updated_at=_dt(row["updated_at"]),
        )

    async def create_run(self, pipeline_id: int | None, prompt: str) -> int:
        cur = await self._db.execute(
            ("INSERT INTO generation_runs (pipeline_id, status, prompt, created_at) "
             "VALUES (?, 'pending', ?, datetime('now'))"),
            (pipeline_id, prompt),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    async def set_status(self, run_id: int, status: str, metadata: dict | None = None) -> None:
        if metadata is not None:
            await self._db.execute(
                ("UPDATE generation_runs SET status = ?, metadata = ?, "
                 "updated_at = datetime('now') WHERE id = ?"),
                (status, json.dumps(metadata, ensure_ascii=False), run_id),
            )
        else:
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

    async def set_moderation_status(self, run_id: int, status: str) -> None:
        await self._db.execute(
            "UPDATE generation_runs SET moderation_status = ?, updated_at = datetime('now') WHERE id = ?",
            (status, run_id),
        )
        await self._db.commit()

    async def set_published_at(self, run_id: int) -> None:
        await self._db.execute(
            ("UPDATE generation_runs SET published_at = datetime('now'), "
             "moderation_status = 'published', updated_at = datetime('now') WHERE id = ?"),
            (run_id,),
        )
        await self._db.commit()

    async def set_quality_score(
        self, run_id: int, score: float, issues: list[str] | None = None
    ) -> None:
        issues_json = json.dumps(issues, ensure_ascii=False) if issues else None
        await self._db.execute(
            ("UPDATE generation_runs SET quality_score = ?, quality_issues = ?, "
             "updated_at = datetime('now') WHERE id = ?"),
            (score, issues_json, run_id),
        )
        await self._db.commit()

    async def set_variants(self, run_id: int, variants: list[str]) -> None:
        await self._db.execute(
            "UPDATE generation_runs SET variants = ?, updated_at = datetime('now') WHERE id = ?",
            (json.dumps(variants, ensure_ascii=False), run_id),
        )
        await self._db.commit()

    async def select_variant(self, run_id: int, variant_index: int, generated_text: str) -> None:
        await self._db.execute(
            ("UPDATE generation_runs SET generated_text = ?, selected_variant = ?, "
             "updated_at = datetime('now') WHERE id = ?"),
            (generated_text, variant_index, run_id),
        )
        await self._db.commit()

    async def list_pending_moderation(
        self,
        pipeline_id: int | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[GenerationRun]:
        if pipeline_id is None:
            cur = await self._db.execute(
                "SELECT * FROM generation_runs WHERE moderation_status IN ('pending', 'approved')"
                " ORDER BY id DESC LIMIT ? OFFSET ?",
                (limit, offset),
            )
        else:
            cur = await self._db.execute(
                "SELECT * FROM generation_runs WHERE moderation_status IN ('pending', 'approved')"
                " AND pipeline_id = ? ORDER BY id DESC LIMIT ? OFFSET ?",
                (pipeline_id, limit, offset),
            )
        rows = await cur.fetchall()
        return [self._to_generation_run(row) for row in rows]

    async def reset_running_on_startup(self) -> int:
        """Reset generation_runs stuck in 'running' state to 'failed' on server startup."""
        cur = await self._db.execute(
            "UPDATE generation_runs SET status = 'failed', updated_at = datetime('now') WHERE status = 'running'",
        )
        await self._db.commit()
        return cur.rowcount or 0

    async def get(self, run_id: int) -> GenerationRun | None:
        cur = await self._db.execute("SELECT * FROM generation_runs WHERE id = ?", (run_id,))
        row = await cur.fetchone()
        if not row:
            return None
        return self._to_generation_run(row)

    async def list_runs_for_calendar(self, days: int = 30) -> list[GenerationRun]:
        cur = await self._db.execute(
            "SELECT * FROM generation_runs WHERE created_at >= date('now', ?) ORDER BY created_at DESC",
            (f"-{days} days",),
        )
        rows = await cur.fetchall()
        return [self._to_generation_run(row) for row in rows]

    async def list_by_pipeline(
        self, pipeline_id: int, limit: int = 20, offset: int = 0
    ) -> list[GenerationRun]:
        cur = await self._db.execute(
            "SELECT * FROM generation_runs WHERE pipeline_id = ? ORDER BY id DESC LIMIT ? OFFSET ?",
            (pipeline_id, limit, offset),
        )
        rows = await cur.fetchall()
        return [self._to_generation_run(row) for row in rows]

    async def list_by_status(self, statuses: list[str], limit: int = 20) -> list[GenerationRun]:
        """List runs filtered by execution status (pending/running/completed/failed)."""
        if not statuses:
            return []
        placeholders = ",".join("?" * len(statuses))
        cur = await self._db.execute(
            f"SELECT * FROM generation_runs WHERE status IN ({placeholders}) ORDER BY id DESC LIMIT ?",
            (*statuses, limit),
        )
        rows = await cur.fetchall()
        return [self._to_generation_run(row) for row in rows]

    async def get_calendar_stats(self) -> dict:
        """Return counts grouped by moderation_status, plus published count."""
        cur = await self._db.execute(
            "SELECT moderation_status, COUNT(*) as cnt FROM generation_runs "
            "WHERE moderation_status IN ('pending', 'approved') "
            "GROUP BY moderation_status"
        )
        rows = await cur.fetchall()
        counts = {row["moderation_status"]: int(row["cnt"]) for row in rows}
        cur2 = await self._db.execute(
            "SELECT COUNT(*) as cnt FROM generation_runs "
            "WHERE moderation_status = 'published'"
        )
        row2 = await cur2.fetchone()
        return {
            "pending": counts.get("pending", 0),
            "approved": counts.get("approved", 0),
            "published": int(row2["cnt"]) if row2 else 0,
        }
