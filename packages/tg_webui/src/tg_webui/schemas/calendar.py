"""REST response schemas for calendar endpoints (#1070)."""

from __future__ import annotations

from pydantic import BaseModel, Field


class CalendarEventItem(BaseModel):
    """A scheduled/produced publication shown on the calendar."""

    run_id: int
    pipeline_id: int
    pipeline_name: str
    status: str
    moderation_status: str
    scheduled_time: str | None = Field(None, description="ISO 8601 timestamp, or null if unscheduled.")
    preview: str


class CalendarDayItem(BaseModel):
    """All events for a single calendar day."""

    date: str = Field(..., description="Day in YYYY-MM-DD form.")
    events: list[CalendarEventItem]


class CalendarStats(BaseModel):
    """Moderation-state counters for the calendar header."""

    pending: int
    approved: int
    published: int
