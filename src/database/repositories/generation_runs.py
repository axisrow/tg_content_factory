from __future__ import annotations

from typing import TYPE_CHECKING

import aiosqlite

from src.models import GenerationRun
from src.utils.datetime import parse_datetime
from src.utils.json import safe_json_dumps, safe_json_loads

if TYPE_CHECKING:
    from src.database.facade import Database


class GenerationRunsRepository:
    def __init__(
        self,
        db: aiosqlite.Connection,
        *,
        database: "Database | None" = None,
    ):
        self._db = db
        self._database = database

    @staticmethod
    def _to_generation_run(row: aiosqlite.Row) -> GenerationRun:
        metadata = safe_json_loads(row["metadata"])
        quality_issues = safe_json_loads(row["quality_issues"]) if "quality_issues" in row.keys() else None
        variants = safe_json_loads(row["variants"]) if "variants" in row.keys() else None

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
            published_at=parse_datetime(row["published_at"] if "published_at" in row.keys() else None),
            created_at=parse_datetime(row["created_at"]),
            updated_at=parse_datetime(row["updated_at"]),
        )

    async def create_run(self, pipeline_id: int | None, prompt: str) -> int:
        cur = await self._database.execute_write(
            ("INSERT INTO generation_runs (pipeline_id, status, prompt, created_at) "
             "VALUES (?, 'pending', ?, datetime('now'))"),
            (pipeline_id, prompt),
        )
        return cur.lastrowid or 0

    async def set_status(self, run_id: int, status: str, metadata: dict | None = None) -> None:
        if metadata is not None:
            await self._database.execute_write(
                ("UPDATE generation_runs SET status = ?, metadata = ?, "
                 "updated_at = datetime('now') WHERE id = ?"),
                (status, safe_json_dumps(metadata, ensure_ascii=False), run_id),
            )
        else:
            await self._database.execute_write(
                "UPDATE generation_runs SET status = ?, updated_at = datetime('now') WHERE id = ?",
                (status, run_id),
            )

    async def save_result(
        self, run_id: int, generated_text: str, metadata: dict | None = None
    ) -> None:
        await self._database.execute_write(
            ("UPDATE generation_runs SET generated_text = ?, metadata = ?, status = 'completed', "
             "updated_at = datetime('now') WHERE id = ?"),
            (generated_text, safe_json_dumps(metadata or {}, ensure_ascii=False), run_id),
        )

    async def set_moderation_status(self, run_id: int, status: str) -> None:
        """Set the run's moderation lifecycle state (issue #1036).

        Valid values and what they mean for the content cycle:

        - ``pending``   — awaiting a human moderation decision. Only reachable
          for MODERATED pipelines; it is the create_run / DB default. AUTO
          content never rests here (it has no human review).
        - ``approved``  — cleared for publishing. Set by a human approve action
          (MODERATED) or automatically the moment an AUTO run finishes
          generating (so AUTO skips ``pending`` entirely).
        - ``rejected``  — a human declined the draft; it will not be published.
        - ``published`` — delivered to every target. Set atomically together with
          ``published_at`` by :meth:`set_published_at`; never set here directly.

        Invariant: a run is never simultaneously ``pending`` and carrying a
        ``published_at``. ``list_pending_moderation`` surfaces ``pending`` +
        ``approved`` (drafts and approved-but-not-yet-delivered runs).
        """
        await self._database.execute_write(
            "UPDATE generation_runs SET moderation_status = ?, updated_at = datetime('now') WHERE id = ?",
            (status, run_id),
        )

    async def set_moderation_status_bulk(self, run_ids: list[int], status: str) -> None:
        """Atomically set ``moderation_status`` for many runs (issue #1041).

        Bulk approve/reject previously looped one autocommit
        :meth:`set_moderation_status` per id, so a failure on id N left ids
        1..N-1 committed with no rollback — a half-applied batch that the tool
        still reported as a full success. Wrapping every id in a single
        ``BEGIN IMMEDIATE`` transaction makes the batch all-or-nothing: a
        mid-batch error rolls the whole update back and propagates so the
        caller can surface the failure instead of a partial-success message.
        """
        if not run_ids:
            return
        async with self._database.transaction() as conn:
            await conn.executemany(
                "UPDATE generation_runs SET moderation_status = ?, updated_at = datetime('now') WHERE id = ?",
                [(status, run_id) for run_id in run_ids],
            )

    async def set_image_url(self, run_id: int, image_url: str) -> None:
        await self._database.execute_write(
            "UPDATE generation_runs SET image_url = ?, updated_at = datetime('now') WHERE id = ?",
            (image_url, run_id),
        )

    async def find_orphan_image_url(
        self, pipeline_id: int, exclude_run_id: int | None = None
    ) -> str | None:
        """Return an already-paid-for image URL that a retry can safely reuse (#1117).

        Image generation is billed per request (#958). The paid POST happens
        before the fallible post-image steps (quality scoring, moderation-status
        alignment) in :meth:`ContentGenerationService.generate`. If one of those
        raises, the run is marked ``failed`` and the periodic ``content_generate``
        scheduler job creates a *brand-new* run on its next tick — a different
        ``run_id``, so an in-run ``image_url`` check cannot dedupe it. Without a
        cross-run guard the retry would generate (and pay for) the image again.

        This finds the most recent run of the same pipeline that already persisted
        an ``image_url`` but was never published (``moderation_status !=
        'published'``), so its image is paid for yet unused. The caller reuses that
        URL instead of issuing a second billed POST. Published runs are excluded:
        their image was already delivered, so a fresh run is a fresh post and must
        render its own image. ``exclude_run_id`` skips the in-flight run itself.
        """
        cur = await self._db.execute(
            (
                "SELECT image_url FROM generation_runs "
                "WHERE pipeline_id = ? AND image_url IS NOT NULL AND image_url != '' "
                "AND COALESCE(moderation_status, '') != 'published' "
                "AND id != ? "
                "ORDER BY id DESC LIMIT 1"
            ),
            (pipeline_id, exclude_run_id if exclude_run_id is not None else -1),
        )
        row = await cur.fetchone()
        if not row:
            return None
        return row["image_url"]

    async def set_published_at(self, run_id: int) -> None:
        await self._database.execute_write(
            ("UPDATE generation_runs SET published_at = datetime('now'), "
             "moderation_status = 'published', updated_at = datetime('now') WHERE id = ?"),
            (run_id,),
        )

    async def set_metadata(self, run_id: int, metadata: dict) -> None:
        """Persist the run metadata JSON without touching status or published_at.

        Used to record incremental publish progress (per-target delivery) so a
        retry does not re-send to targets already published.
        """
        await self._database.execute_write(
            "UPDATE generation_runs SET metadata = ?, updated_at = datetime('now') WHERE id = ?",
            (safe_json_dumps(metadata, ensure_ascii=False), run_id),
        )

    async def set_quality_score(
        self, run_id: int, score: float, issues: list[str] | None = None
    ) -> None:
        issues_json = safe_json_dumps(issues, ensure_ascii=False) if issues else None
        await self._database.execute_write(
            ("UPDATE generation_runs SET quality_score = ?, quality_issues = ?, "
             "updated_at = datetime('now') WHERE id = ?"),
            (score, issues_json, run_id),
        )

    async def set_variants(self, run_id: int, variants: list[str]) -> None:
        await self._database.execute_write(
            "UPDATE generation_runs SET variants = ?, updated_at = datetime('now') WHERE id = ?",
            (safe_json_dumps(variants, ensure_ascii=False), run_id),
        )

    async def select_variant(self, run_id: int, variant_index: int, generated_text: str) -> None:
        # Selecting a variant changes generated_text, so any existing
        # quality_score/quality_issues belong to the PREVIOUS text and would be
        # stale — a low-quality selected variant could otherwise hide behind a
        # passing score from the base text (review: Codex, #1068). Clear them on
        # every selection; ContentGenerationService re-scores the selected text
        # in its auto-select path, and a manual selection leaves the run
        # honestly unscored until re-evaluated.
        await self._database.execute_write(
            ("UPDATE generation_runs SET generated_text = ?, selected_variant = ?, "
             "quality_score = NULL, quality_issues = NULL, "
             "updated_at = datetime('now') WHERE id = ?"),
            (generated_text, variant_index, run_id),
        )

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
        cur = await self._database.execute_write(
            "UPDATE generation_runs SET status = 'failed', updated_at = datetime('now') WHERE status = 'running'",
        )
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
        self,
        pipeline_id: int,
        limit: int = 20,
        offset: int = 0,
        status: str | None = None,
        moderation_status: str | None = None,
    ) -> list[GenerationRun]:
        where = "pipeline_id = ?"
        params: list[object] = [pipeline_id]
        if status:
            where += " AND status = ?"
            params.append(status)
        if moderation_status:
            where += " AND moderation_status = ?"
            params.append(moderation_status)
        cur = await self._db.execute(
            f"SELECT * FROM generation_runs WHERE {where} ORDER BY id DESC LIMIT ? OFFSET ?",
            (*params, limit, offset),
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
