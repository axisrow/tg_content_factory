"""Tests for ContentCalendarService."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.services.content_calendar_service import (
    CalendarDay,
    CalendarEvent,
    ContentCalendarService,
)


@pytest.fixture
def mock_db():
    """Mock Database."""
    db = MagicMock()

    # Mock repositories
    db.repos.content_pipelines.get_all = AsyncMock(return_value=[])
    db.repos.generation_runs.list_by_pipeline = AsyncMock(return_value=[])
    db.repos.generation_runs.list_runs_for_calendar = AsyncMock(return_value=[])
    db.repos.generation_runs.list_pending_moderation = AsyncMock(return_value=[])
    db.repos.generation_runs.get_calendar_stats = AsyncMock(return_value={})

    return db


@pytest.fixture
def service(mock_db):
    """ContentCalendarService instance."""
    return ContentCalendarService(mock_db)


# === get_calendar tests ===


@pytest.mark.asyncio
async def test_get_calendar_empty_db(service, mock_db):
    """get_calendar returns empty days when no runs exist."""
    result = await service.get_calendar(days=3)

    assert len(result) == 3
    for day in result:
        assert isinstance(day, CalendarDay)
        assert day.events == []


@pytest.mark.asyncio
async def test_get_calendar_with_events_filters_by_date(service, mock_db):
    """get_calendar filters events by date range."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    # Create mock pipeline
    mock_pipeline = MagicMock()
    mock_pipeline.id = 1
    mock_pipeline.name = "Test Pipeline"
    mock_db.repos.content_pipelines.get_all = AsyncMock(return_value=[mock_pipeline])

    # Create mock run within the date range
    mock_run = MagicMock()
    mock_run.id = 1
    mock_run.pipeline_id = 1
    mock_run.status = "completed"
    mock_run.moderation_status = "approved"
    mock_run.generated_text = "Test content"
    mock_run.created_at = now
    mock_run.published_at = now

    mock_db.repos.generation_runs.list_runs_for_calendar = AsyncMock(
        return_value=[mock_run]
    )

    result = await service.get_calendar(days=1)

    assert len(result) == 1
    assert len(result[0].events) == 1
    assert result[0].events[0].pipeline_name == "Test Pipeline"


@pytest.mark.asyncio
async def test_get_calendar_filters_by_pipeline(service, mock_db):
    """get_calendar filters by pipeline_id when provided."""
    mock_pipeline = MagicMock()
    mock_pipeline.id = 1
    mock_pipeline.name = "Test Pipeline"
    mock_db.repos.content_pipelines.get_all = AsyncMock(return_value=[mock_pipeline])

    await service.get_calendar(days=7, pipeline_id=1)

    mock_db.repos.generation_runs.list_by_pipeline.assert_called_once_with(
        1, limit=1000
    )
    mock_db.repos.generation_runs.list_runs_for_calendar.assert_not_called()


@pytest.mark.asyncio
async def test_get_calendar_skips_runs_without_pipeline(service, mock_db):
    """get_calendar skips runs without pipeline_id."""
    mock_run = MagicMock()
    mock_run.pipeline_id = None

    mock_db.repos.generation_runs.list_runs_for_calendar = AsyncMock(
        return_value=[mock_run]
    )

    result = await service.get_calendar(days=1)

    assert len(result[0].events) == 0


# === get_upcoming tests ===


@pytest.mark.asyncio
async def test_get_upcoming_empty_db(service, mock_db):
    """get_upcoming returns empty list when no pending runs."""
    result = await service.get_upcoming(limit=10)

    assert result == []


@pytest.mark.asyncio
async def test_get_upcoming_filters_by_status(service, mock_db):
    """get_upcoming only includes pending or approved runs."""
    mock_pipeline = MagicMock()
    mock_pipeline.id = 1
    mock_pipeline.name = "Test Pipeline"
    mock_db.repos.content_pipelines.get_all = AsyncMock(return_value=[mock_pipeline])

    # Create run with pending moderation
    mock_run = MagicMock()
    mock_run.id = 1
    mock_run.pipeline_id = 1
    mock_run.status = "completed"
    mock_run.moderation_status = "pending"
    mock_run.generated_text = "Test content"
    mock_run.created_at = datetime.now(timezone.utc).replace(tzinfo=None)
    mock_run.published_at = None

    mock_db.repos.generation_runs.list_pending_moderation = AsyncMock(
        return_value=[mock_run]
    )

    result = await service.get_upcoming(limit=10)

    assert len(result) == 1
    assert result[0].moderation_status == "pending"


@pytest.mark.asyncio
async def test_get_upcoming_respects_limit(service, mock_db):
    """get_upcoming respects the limit parameter."""
    mock_pipeline = MagicMock()
    mock_pipeline.id = 1
    mock_pipeline.name = "Test Pipeline"
    mock_db.repos.content_pipelines.get_all = AsyncMock(return_value=[mock_pipeline])

    # Create multiple runs
    runs = []
    for i in range(10):
        run = MagicMock()
        run.id = i
        run.pipeline_id = 1
        run.status = "completed"
        run.moderation_status = "pending"
        run.generated_text = f"Content {i}"
        run.created_at = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(minutes=i)
        run.published_at = None
        runs.append(run)

    mock_db.repos.generation_runs.list_pending_moderation = AsyncMock(
        return_value=runs
    )

    result = await service.get_upcoming(limit=5)

    assert len(result) == 5


@pytest.mark.asyncio
async def test_get_upcoming_filters_by_pipeline(service, mock_db):
    """get_upcoming filters by pipeline_id when provided."""
    await service.get_upcoming(limit=10, pipeline_id=1)

    mock_db.repos.generation_runs.list_pending_moderation.assert_called_once()


# === get_stats tests ===


@pytest.mark.asyncio
async def test_get_stats_delegates_to_repo(service, mock_db):
    """get_stats delegates to generation_runs repository."""
    mock_db.repos.generation_runs.get_calendar_stats = AsyncMock(
        return_value={"total": 10, "approved": 5}
    )

    result = await service.get_stats()

    mock_db.repos.generation_runs.get_calendar_stats.assert_called_once()
    assert result == {"total": 10, "approved": 5}


@pytest.mark.asyncio
async def test_get_stats_empty(service, mock_db):
    """get_stats returns empty dict when no data."""
    mock_db.repos.generation_runs.get_calendar_stats = AsyncMock(return_value={})

    result = await service.get_stats()

    assert result == {}


# === CalendarEvent dataclass tests ===


def test_calendar_event_dataclass():
    """CalendarEvent dataclass stores all fields correctly."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    event = CalendarEvent(
        run_id=1,
        pipeline_id=1,
        pipeline_name="Test",
        status="completed",
        moderation_status="approved",
        scheduled_time=now,
        created_at=now,
        preview="Test content...",
    )

    assert event.run_id == 1
    assert event.pipeline_name == "Test"
    assert event.preview == "Test content..."


# === CalendarDay dataclass tests ===


def test_calendar_day_dataclass():
    """CalendarDay dataclass stores all fields correctly."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    event = CalendarEvent(
        run_id=1,
        pipeline_id=1,
        pipeline_name="Test",
        status="completed",
        moderation_status="approved",
        scheduled_time=now,
        created_at=now,
        preview="Test",
    )

    day = CalendarDay(date="2024-01-15", events=[event])

    assert day.date == "2024-01-15"
    assert len(day.events) == 1
