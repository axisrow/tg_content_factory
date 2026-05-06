from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from src.database import Database
from src.utils.datetime import parse_required_utc_datetime

logger = logging.getLogger(__name__)


def _parse_db_datetime(value: str) -> datetime:
    return parse_required_utc_datetime(value)


@dataclass
class PipelineStats:
    pipeline_id: int
    pipeline_name: str
    total_generations: int
    total_published: int
    total_rejected: int
    pending_moderation: int
    success_rate: float


@dataclass
class DailyStats:
    date: str
    generations: int
    publications: int
    rejections: int


class ContentAnalyticsService:
    """Service for content generation and publication analytics.

    Provides:
    - Per-pipeline statistics (generations, publications, rejections)
    - Daily stats over time period
    - Success rate calculations
    """

    def __init__(self, db: Database):
        self._db = db

    async def _load_pipeline_rows(self, pipeline_id: int | None = None) -> list[dict]:
        sql = """
            SELECT
                p.id AS pipeline_id,
                p.name AS pipeline_name,
                COUNT(gr.id) AS total_generations,
                COALESCE(SUM(
                    CASE
                        WHEN gr.published_at IS NOT NULL OR gr.moderation_status = 'published'
                        THEN 1 ELSE 0
                    END
                ), 0) AS total_published,
                COALESCE(SUM(
                    CASE WHEN gr.moderation_status = 'rejected' THEN 1 ELSE 0 END
                ), 0) AS total_rejected,
                COALESCE(SUM(
                    CASE WHEN gr.moderation_status = 'pending' THEN 1 ELSE 0 END
                ), 0) AS pending_moderation
            FROM content_pipelines p
            LEFT JOIN generation_runs gr ON gr.pipeline_id = p.id
        """
        params: tuple[object, ...] = ()
        if pipeline_id is not None:
            sql += " WHERE p.id = ?"
            params = (pipeline_id,)
        sql += " GROUP BY p.id, p.name ORDER BY p.id"
        return await self._db.execute_fetchall(sql, params)

    async def _load_runs_in_window(
        self,
        start: datetime,
        end: datetime,
        pipeline_id: int | None = None,
    ) -> list:
        sql = """
            SELECT id, pipeline_id, created_at, published_at, updated_at, moderation_status
            FROM generation_runs
            WHERE (
                (created_at IS NOT NULL AND created_at >= ? AND created_at < ?)
                OR (published_at IS NOT NULL AND published_at >= ? AND published_at < ?)
                OR (updated_at IS NOT NULL AND updated_at >= ? AND updated_at < ?)
            )
        """
        params: tuple[object, ...] = (
            start.isoformat(),
            end.isoformat(),
            start.isoformat(),
            end.isoformat(),
            start.isoformat(),
            end.isoformat(),
        )
        if pipeline_id is not None:
            sql += " AND pipeline_id = ?"
            params += (pipeline_id,)
        return await self._db.execute_fetchall(sql, params)

    async def get_pipeline_stats(self, pipeline_id: int | None = None) -> list[PipelineStats]:
        """Get statistics for all pipelines or a specific one.

        Args:
            pipeline_id: Optional specific pipeline ID, or None for all

        Returns:
            List of PipelineStats for each pipeline
        """
        results: list[PipelineStats] = []
        for row in await self._load_pipeline_rows(pipeline_id):
            total_generations = int(row["total_generations"] or 0)
            total_published = int(row["total_published"] or 0)
            total_rejected = int(row["total_rejected"] or 0)
            pending_moderation = int(row["pending_moderation"] or 0)
            success_rate = (
                total_published / total_generations * 100 if total_generations > 0 else 0.0
            )

            results.append(PipelineStats(
                pipeline_id=row["pipeline_id"],
                pipeline_name=row["pipeline_name"],
                total_generations=total_generations,
                total_published=total_published,
                total_rejected=total_rejected,
                pending_moderation=pending_moderation,
                success_rate=success_rate,
            ))

        return results

    async def get_daily_stats(
        self,
        days: int = 30,
        pipeline_id: int | None = None,
    ) -> list[DailyStats]:
        """Get daily statistics for the past N days.

        Args:
            days: Number of days to look back
            pipeline_id: Optional filter by pipeline

        Returns:
            List of DailyStats, one per day
        """
        now = datetime.now(timezone.utc)
        window_start = (now - timedelta(days=days - 1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        window_end = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        runs = await self._load_runs_in_window(window_start, window_end, pipeline_id)

        results: list[DailyStats] = []
        for i in range(days):
            day_start = (now - timedelta(days=days - 1 - i)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            day_end = day_start + timedelta(days=1)
            day_str = day_start.strftime("%Y-%m-%d")

            generations = sum(
                1 for r in runs
                if r["created_at"]
                and day_start <= _parse_db_datetime(r["created_at"]) < day_end
            )
            publications = sum(
                1 for r in runs
                if r["published_at"]
                and day_start <= _parse_db_datetime(r["published_at"]) < day_end
            )
            rejections = sum(
                1 for r in runs
                if r["moderation_status"] == "rejected"
                and r["updated_at"]
                and day_start <= _parse_db_datetime(r["updated_at"]) < day_end
            )

            results.append(DailyStats(
                date=day_str,
                generations=generations,
                publications=publications,
                rejections=rejections,
            ))

        return results

    async def get_summary(self) -> dict:
        """Get overall summary statistics.

        Returns:
            Dict with total_generations, total_published, total_pending, total_rejected
        """
        rows = await self._load_pipeline_rows()

        return {
            "total_generations": sum(int(row["total_generations"] or 0) for row in rows),
            "total_published": sum(int(row["total_published"] or 0) for row in rows),
            "total_pending": sum(int(row["pending_moderation"] or 0) for row in rows),
            "total_rejected": sum(int(row["total_rejected"] or 0) for row in rows),
            "pipelines_count": len(rows),
        }
