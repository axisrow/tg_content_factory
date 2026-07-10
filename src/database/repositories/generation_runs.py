"""Репозиторий запусков генерации контента (таблица ``generation_runs``).

Доступ через `db.repos.generation_runs`. Хранит жизненный цикл одного запуска
пайплайна: сгенерированный текст/картинку, статус выполнения (status) и
отдельный статус модерации (moderation_status: pending → approved/rejected →
published, #1036), A/B-варианты и оценку качества. Запись идёт через
locked-хелперы Database (execute_write/transaction).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import aiosqlite

from src.database.pool import ReadConnection
from src.models import GenerationRun
from src.utils.datetime import parse_datetime
from src.utils.json import safe_json_dumps, safe_json_loads

if TYPE_CHECKING:
    from src.database.facade import Database


class GenerationRunsRepository:
    """CRUD и переходы статусов запусков генерации (`generation_runs`)."""

    def __init__(
        self,
        db: ReadConnection,
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
        """Создать запуск в статусе ``pending`` для пайплайна и промпта; вернуть его id."""
        assert self._database is not None, (
            "GenerationRunsRepository.create_run requires a Database reference"
        )
        cur = await self._database.execute_write(
            ("INSERT INTO generation_runs (pipeline_id, status, prompt, created_at) "
             "VALUES (?, 'pending', ?, datetime('now'))"),
            (pipeline_id, prompt),
        )
        return cur.lastrowid or 0

    async def set_status(self, run_id: int, status: str, metadata: dict | None = None) -> None:
        """Обновить статус выполнения запуска; при переданном ``metadata`` — заодно перезаписать JSON-метаданные."""
        assert self._database is not None, (
            "GenerationRunsRepository.set_status requires a Database reference"
        )
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
        """Сохранить сгенерированный текст и метаданные, переводя запуск в статус ``completed``."""
        assert self._database is not None, (
            "GenerationRunsRepository.save_result requires a Database reference"
        )
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
        assert self._database is not None, (
            "GenerationRunsRepository.set_moderation_status requires a Database reference"
        )
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
        assert self._database is not None, (
            "GenerationRunsRepository.set_moderation_status_bulk requires a Database reference"
        )
        if not run_ids:
            return
        async with self._database.transaction() as conn:
            await conn.executemany(
                "UPDATE generation_runs SET moderation_status = ?, updated_at = datetime('now') WHERE id = ?",
                [(status, run_id) for run_id in run_ids],
            )

    async def set_image_url(self, run_id: int, image_url: str) -> None:
        """Привязать к запуску URL/путь сгенерированной картинки."""
        assert self._database is not None, (
            "GenerationRunsRepository.set_image_url requires a Database reference"
        )
        await self._database.execute_write(
            "UPDATE generation_runs SET image_url = ?, updated_at = datetime('now') WHERE id = ?",
            (image_url, run_id),
        )

    async def find_orphan_image(
        self, pipeline_id: int, exclude_run_id: int | None = None
    ) -> tuple[int, str] | None:
        """Find a paid-for image a retry can reuse, as ``(source_run_id, url)`` (#1117).

        Image generation is billed per request (#958). The paid POST happens
        before the fallible post-image steps (quality scoring, moderation-status
        alignment) in :meth:`ContentGenerationService.generate`. If one of those
        raises, the run is marked ``failed`` and the periodic ``content_generate``
        scheduler job creates a *brand-new* run on its next tick — a different
        ``run_id``, so an in-run ``image_url`` check cannot dedupe it. Without a
        cross-run guard the retry would generate (and pay for) the image again.

        This returns the most recent **failed** run of the same pipeline that still
        carries an ``image_url`` (paid yet stranded), together with its run id so the
        caller can :meth:`claim_orphan_image` — moving the URL onto the retry run AND
        clearing it on the source in one transaction. Returning the id is what lets
        the orphan be *consumed*: a plain reuse-by-value would leave the failed row
        matchable forever, so every later scheduled post would inherit the same stale
        image (review: Codex, Claude). Consuming it bounds reuse to exactly once.

        The ``status = 'failed'`` filter is deliberate and load-bearing. A run only
        leaves a paid-but-orphaned image behind when it failed *after* the image POST
        (the bug window #1117 closes). A run that is ``completed`` — even one still
        awaiting moderation/publish (``moderation_status`` of ``pending``/``approved``,
        not yet ``published``) — is a legitimate post in flight; its image belongs to
        *that* post. Reusing it for the next scheduled run would make every fresh post
        silently inherit the previous post's picture. Filtering on ``failed`` (not
        merely "not published") scopes reuse to exactly the stranded-after-billing
        case. ``exclude_run_id`` skips the in-flight run itself.
        """
        cur = await self._db.execute(
            (
                "SELECT id, image_url FROM generation_runs "
                "WHERE pipeline_id = ? AND status = 'failed' "
                "AND image_url IS NOT NULL AND image_url != '' "
                "AND id != ? "
                "ORDER BY id DESC LIMIT 1"
            ),
            (pipeline_id, exclude_run_id if exclude_run_id is not None else -1),
        )
        row = await cur.fetchone()
        if not row:
            return None
        return (row["id"], row["image_url"])

    async def claim_orphan_image(
        self, source_run_id: int, target_run_id: int, image_url: str
    ) -> None:
        """Move a stranded paid image from a failed run onto the retry run (#1117).

        Atomically (one transaction) sets ``image_url`` on ``target_run_id`` and
        clears it on ``source_run_id``. Clearing the source is what makes reuse a
        one-shot **transfer**, not an unbounded copy: once consumed, the failed run
        no longer matches :meth:`find_orphan_image`, so a later scheduled post will
        not re-inherit the same image (review: Codex, Claude — the stale-image
        defect). Clearing the source is safe: a ``failed`` run is never published
        (the publish path gates on ``status='completed'``), so its ``image_url`` is
        dead bookkeeping once the live retry owns the picture.
        """
        assert self._database is not None, (
            "GenerationRunsRepository.claim_orphan_image requires a Database reference"
        )
        async with self._database.transaction() as conn:
            await conn.execute(
                "UPDATE generation_runs SET image_url = ?, updated_at = datetime('now') WHERE id = ?",
                (image_url, target_run_id),
            )
            await conn.execute(
                "UPDATE generation_runs SET image_url = NULL, updated_at = datetime('now') WHERE id = ?",
                (source_run_id,),
            )

    async def set_published_at(self, run_id: int) -> None:
        """Отметить запуск опубликованным: ставит ``published_at`` и ``moderation_status='published'`` атомарно.

        Единственный путь, проставляющий ``published_at`` синхронно со статусом
        ``published`` — используйте его, а не ручной
        ``set_moderation_status('published')`` (тот пишет произвольную строку
        статуса и ``published_at`` не трогает), иначе получите published-запуск
        без отметки времени.
        """
        assert self._database is not None, (
            "GenerationRunsRepository.set_published_at requires a Database reference"
        )
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
        assert self._database is not None, (
            "GenerationRunsRepository.set_metadata requires a Database reference"
        )
        await self._database.execute_write(
            "UPDATE generation_runs SET metadata = ?, updated_at = datetime('now') WHERE id = ?",
            (safe_json_dumps(metadata, ensure_ascii=False), run_id),
        )

    async def set_quality_score(
        self, run_id: int, score: float, issues: list[str] | None = None
    ) -> None:
        """Сохранить оценку качества запуска и (опционально) список выявленных проблем."""
        assert self._database is not None, (
            "GenerationRunsRepository.set_quality_score requires a Database reference"
        )
        issues_json = safe_json_dumps(issues, ensure_ascii=False) if issues else None
        await self._database.execute_write(
            ("UPDATE generation_runs SET quality_score = ?, quality_issues = ?, "
             "updated_at = datetime('now') WHERE id = ?"),
            (score, issues_json, run_id),
        )

    async def set_variants(self, run_id: int, variants: list[str]) -> None:
        """Сохранить список A/B-вариантов текста запуска (issue #1068)."""
        assert self._database is not None, (
            "GenerationRunsRepository.set_variants requires a Database reference"
        )
        await self._database.execute_write(
            "UPDATE generation_runs SET variants = ?, updated_at = datetime('now') WHERE id = ?",
            (safe_json_dumps(variants, ensure_ascii=False), run_id),
        )

    async def select_variant(self, run_id: int, variant_index: int, generated_text: str) -> None:
        """Сделать выбранный A/B-вариант финальным текстом запуска (issue #1068).

        Заодно обнуляет ``quality_score``/``quality_issues`` — они относились к
        прежнему тексту и были бы устаревшими (см. комментарий ниже).
        """
        assert self._database is not None, (
            "GenerationRunsRepository.select_variant requires a Database reference"
        )
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
        """Очередь модерации: запуски со статусом ``pending`` или ``approved``.

        Это черновики и одобренные, но ещё не доставленные. Без ``pipeline_id`` —
        по всем пайплайнам; страница задаётся ``limit``/``offset``, новые первыми.
        """
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
        assert self._database is not None, (
            "GenerationRunsRepository.reset_running_on_startup requires a Database reference"
        )
        cur = await self._database.execute_write(
            "UPDATE generation_runs SET status = 'failed', updated_at = datetime('now') WHERE status = 'running'",
        )
        return cur.rowcount or 0

    async def get(self, run_id: int) -> GenerationRun | None:
        """Один запуск по id, либо ``None`` если такого нет."""
        cur = await self._db.execute("SELECT * FROM generation_runs WHERE id = ?", (run_id,))
        row = await cur.fetchone()
        if not row:
            return None
        return self._to_generation_run(row)

    async def list_runs_for_calendar(self, days: int = 30) -> list[GenerationRun]:
        """Запуски за последние ``days`` дней (для календаря контента), новые первыми."""
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
        """Запуски одного пайплайна, новые первыми; фильтры по ``status`` и
        ``moderation_status`` опциональны, страница — ``limit``/``offset``."""
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
