from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from src.database import Database

logger = logging.getLogger(__name__)


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

    async def get_pipeline_stats(self, pipeline_id: int | None = None) -> list[PipelineStats]:
        """Get statistics for all pipelines or a specific one.
        
        Args:
            pipeline_id: Optional specific pipeline ID, or None for all
            
        Returns:
            List of PipelineStats for each pipeline
        """
        if pipeline_id is not None:
            pipelines = await self._db.repos.content_pipelines.get_by_id(pipeline_id)
            pipelines = [pipelines] if pipelines else []
        else:
            pipelines = await self._db.repos.content_pipelines.get_all()

        results: list[PipelineStats] = []
        for pipeline in pipelines:
            if pipeline.id is None:
                continue
            
            runs = await self._db.repos.generation_runs.list_by_pipeline(pipeline.id, limit=1000)
            
            total_generations = len(runs)
            total_published = sum(1 for r in runs if r.moderation_status == "published" or r.published_at is not None)
            total_rejected = sum(1 for r in runs if r.moderation_status == "rejected")
            pending_moderation = sum(1 for r in runs if r.moderation_status == "pending")
            
            completed = total_generations - sum(1 for r in runs if r.status == "pending" or r.status == "running")
            success_rate = (completed / total_generations * 100) if total_generations > 0 else 0.0
            
            results.append(PipelineStats(
                pipeline_id=pipeline.id,
                pipeline_name=pipeline.name,
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
        results: list[DailyStats] = []
        now = datetime.now(timezone.utc)
        
        for i in range(days):
            day_start = (now - timedelta(days=days - 1 - i)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            day_end = day_start + timedelta(days=1)
            day_str = day_start.strftime("%Y-%m-%d")
            
            if pipeline_id is not None:
                runs = await self._db.repos.generation_runs.list_by_pipeline(
                    pipeline_id, limit=1000
                )
            else:
                runs = await self._db.repos.generation_runs.list_pending_moderation(
                    limit=10000
                )
                all_runs = await self._db.repos.generation_runs.list_by_pipeline(
                    (await self._db.repos.content_pipelines.get_all())[0].id if await self._db.repos.content_pipelines.get_all() else 0,
                    limit=10000
                ) if await self._db.repos.content_pipelines.get_all() else []
                runs = all_runs
            
            generations = sum(
                1 for r in runs
                if r.created_at and day_start <= r.created_at < day_end
            )
            publications = sum(
                1 for r in runs
                if r.published_at and day_start <= r.published_at < day_end
            )
            rejections = sum(
                1 for r in runs
                if r.moderation_status == "rejected"
                and r.updated_at and day_start <= r.updated_at < day_end
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
        pipelines = await self._db.repos.content_pipelines.get_all()
        
        total_generations = 0
        total_published = 0
        total_pending = 0
        total_rejected = 0
        
        for pipeline in pipelines:
            if pipeline.id is None:
                continue
            stats = await self.get_pipeline_stats(pipeline.id)
            if stats:
                s = stats[0]
                total_generations += s.total_generations
                total_published += s.total_published
                total_pending += s.pending_moderation
                total_rejected += s.total_rejected
        
        return {
            "total_generations": total_generations,
            "total_published": total_published,
            "total_pending": total_pending,
            "total_rejected": total_rejected,
            "pipelines_count": len(pipelines),
        }
