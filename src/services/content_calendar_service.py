from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.database import Database

logger = logging.getLogger(__name__)


@dataclass
class CalendarEvent:
    run_id: int
    pipeline_id: int
    pipeline_name: str
    status: str
    moderation_status: str
    scheduled_time: datetime | None
    created_at: datetime
    preview: str


@dataclass
class CalendarDay:
    date: str
    events: list[CalendarEvent]


class ContentCalendarService:
    """Service for content calendar functionality.

    Provides:
    - Scheduled publications by day/week
    - Visual timeline of upcoming posts
    - Integration with publish_times config
    """

    def __init__(self, db: Database):
        self._db = db

    async def get_calendar(
        self,
        days: int = 7,
        pipeline_id: int | None = None,
    ) -> list[CalendarDay]:
        """Get calendar events for the next N days."""
        results: list[CalendarDay] = []
        now = datetime.utcnow()

        pipelines = await self._db.repos.content_pipelines.get_all()
        pipelines_by_id = {p.id: p for p in pipelines if p.id is not None}

        # Fetch runs once before the loop
        if pipeline_id is not None:
            runs = await self._db.repos.generation_runs.list_by_pipeline(
                pipeline_id, limit=1000
            )
        else:
            runs = await self._db.repos.generation_runs.list_runs_for_calendar(
                days=days
            )

        for i in range(days):
            day_start = (now + timedelta(days=i)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            day_end = day_start + timedelta(days=1)
            day_str = day_start.strftime("%Y-%m-%d")

            events: list[CalendarEvent] = []

            for run in runs:
                if run.pipeline_id is None:
                    continue

                pipeline = pipelines_by_id.get(run.pipeline_id)
                if pipeline is None:
                    continue

                scheduled = run.published_at or run.created_at
                if scheduled and day_start <= scheduled < day_end:
                    preview = (run.generated_text or "")[:100]
                    if len(preview) == 100:
                        preview += "..."

                    events.append(CalendarEvent(
                        run_id=run.id,
                        pipeline_id=run.pipeline_id,
                        pipeline_name=pipeline.name,
                        status=run.status,
                        moderation_status=run.moderation_status,
                        scheduled_time=scheduled,
                        created_at=run.created_at or now,
                        preview=preview,
                    ))

            results.append(CalendarDay(date=day_str, events=events))

        return results

    async def get_upcoming(
        self,
        limit: int = 20,
        pipeline_id: int | None = None,
    ) -> list[CalendarEvent]:
        """Get upcoming scheduled publications."""
        pipelines = await self._db.repos.content_pipelines.get_all()
        pipelines_by_id = {p.id: p for p in pipelines if p.id is not None}

        runs = await self._db.repos.generation_runs.list_pending_moderation(
            pipeline_id=pipeline_id,
            limit=limit * 2,
        )

        events: list[CalendarEvent] = []
        for run in runs:
            if run.pipeline_id is None:
                continue

            pipeline = pipelines_by_id.get(run.pipeline_id)
            if pipeline is None:
                continue

            if run.moderation_status in ("pending", "approved"):
                preview = (run.generated_text or "")[:100]
                if len(preview) == 100:
                    preview += "..."

                events.append(CalendarEvent(
                    run_id=run.id,
                    pipeline_id=run.pipeline_id,
                    pipeline_name=pipeline.name,
                    status=run.status,
                    moderation_status=run.moderation_status,
                    scheduled_time=run.published_at,
                    created_at=run.created_at or datetime.utcnow(),
                    preview=preview,
                ))

        events.sort(key=lambda e: e.created_at)
        return events[:limit]

    async def get_stats(self) -> dict:
        """Get calendar statistics."""
        return await self._db.repos.generation_runs.get_calendar_stats()
