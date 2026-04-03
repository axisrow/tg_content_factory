"""Tests for TaskEnqueuer service."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import (
    CollectionTaskType,
    ContentGenerateTaskPayload,
    ContentPublishTaskPayload,
    PipelineRunTaskPayload,
    SqStatsTaskPayload,
)
from src.services.task_enqueuer import TaskEnqueuer

# === enqueue_content_generate tests ===


@pytest.mark.asyncio
async def test_enqueue_content_generate_creates_task(task_enqueuer, mock_db):
    """Creates CONTENT_GENERATE task when no active task exists."""
    mock_db.repos.tasks.has_active_task = AsyncMock(return_value=False)
    mock_db.repos.tasks.create_generic_task = AsyncMock(return_value=101)

    result = await task_enqueuer.enqueue_content_generate(pipeline_id=5)

    mock_db.repos.tasks.has_active_task.assert_called_once_with(
        CollectionTaskType.CONTENT_GENERATE,
        payload_filter_key="pipeline_id",
        payload_filter_value=5,
    )
    mock_db.repos.tasks.create_generic_task.assert_called_once()
    args, kwargs = mock_db.repos.tasks.create_generic_task.call_args
    assert args[0] == CollectionTaskType.CONTENT_GENERATE
    assert "Content generate #5" in kwargs.get("title", "")
    payload = kwargs.get("payload")
    assert isinstance(payload, ContentGenerateTaskPayload)
    assert payload.pipeline_id == 5
    assert result == 101


@pytest.mark.asyncio
async def test_enqueue_content_generate_skips_if_active(task_enqueuer, mock_db):
    """Skips creation if active task for same pipeline exists."""
    mock_db.repos.tasks.has_active_task = AsyncMock(return_value=True)

    result = await task_enqueuer.enqueue_content_generate(pipeline_id=5)

    mock_db.repos.tasks.create_generic_task.assert_not_called()
    assert result is None


# === enqueue_content_publish tests ===


@pytest.mark.asyncio
async def test_enqueue_content_publish_creates_task(task_enqueuer, mock_db):
    """Creates CONTENT_PUBLISH task when no active task exists."""
    mock_db.repos.tasks.has_active_task = AsyncMock(return_value=False)
    mock_db.repos.tasks.create_generic_task = AsyncMock(return_value=202)

    result = await task_enqueuer.enqueue_content_publish()

    mock_db.repos.tasks.has_active_task.assert_called_once_with(
        CollectionTaskType.CONTENT_PUBLISH
    )
    mock_db.repos.tasks.create_generic_task.assert_called_once()
    args, kwargs = mock_db.repos.tasks.create_generic_task.call_args
    assert args[0] == CollectionTaskType.CONTENT_PUBLISH
    assert "Content publish" in kwargs.get("title", "")
    payload = kwargs.get("payload")
    assert isinstance(payload, ContentPublishTaskPayload)
    assert result == 202


@pytest.mark.asyncio
async def test_enqueue_content_publish_with_pipeline_id(task_enqueuer, mock_db):
    """Creates CONTENT_PUBLISH task with pipeline_id in payload."""
    mock_db.repos.tasks.has_active_task = AsyncMock(return_value=False)
    mock_db.repos.tasks.create_generic_task = AsyncMock(return_value=203)

    result = await task_enqueuer.enqueue_content_publish(pipeline_id=7)

    args, kwargs = mock_db.repos.tasks.create_generic_task.call_args
    payload = kwargs.get("payload")
    assert isinstance(payload, ContentPublishTaskPayload)
    assert payload.pipeline_id == 7
    assert result == 203


@pytest.fixture
def mock_db():
    """Mock database."""
    db = MagicMock()
    db.repos = MagicMock()
    db.repos.tasks = MagicMock()
    return db


@pytest.fixture
def mock_collection_service():
    """Mock collection service."""
    service = MagicMock()
    service.enqueue_all_channels = AsyncMock(return_value=MagicMock(queued_count=5))
    return service


@pytest.fixture
def task_enqueuer(mock_db, mock_collection_service):
    """TaskEnqueuer instance."""
    return TaskEnqueuer(mock_db, mock_collection_service)


# === enqueue_all_channels tests ===


@pytest.mark.asyncio
async def test_enqueue_all_channels_delegates(task_enqueuer, mock_collection_service):
    """Delegates to collection service."""
    mock_result = MagicMock(queued_count=3)
    mock_collection_service.enqueue_all_channels.return_value = mock_result

    result = await task_enqueuer.enqueue_all_channels()

    mock_collection_service.enqueue_all_channels.assert_called_once()
    assert result == mock_result


# === enqueue_sq_stats tests ===


@pytest.mark.asyncio
async def test_enqueue_sq_stats_creates_task(task_enqueuer, mock_db):
    """Creates SQ_STATS task when no active task exists."""
    mock_db.repos.tasks.has_active_task = AsyncMock(return_value=False)
    mock_db.repos.tasks.create_generic_task = AsyncMock(return_value=42)

    result = await task_enqueuer.enqueue_sq_stats(sq_id=7)

    mock_db.repos.tasks.has_active_task.assert_called_once_with(
        CollectionTaskType.SQ_STATS,
        payload_filter_key="sq_id",
        payload_filter_value=7,
    )
    mock_db.repos.tasks.create_generic_task.assert_called_once()
    args, kwargs = mock_db.repos.tasks.create_generic_task.call_args
    assert args[0] == CollectionTaskType.SQ_STATS  # task_type as positional
    assert "Статистика запроса #7" in kwargs.get("title", "")
    payload = kwargs.get("payload")
    assert isinstance(payload, SqStatsTaskPayload)
    assert payload.sq_id == 7
    assert result == 42


@pytest.mark.asyncio
async def test_enqueue_sq_stats_skips_if_active(task_enqueuer, mock_db):
    """Skips creation if active task already exists."""
    mock_db.repos.tasks.has_active_task = AsyncMock(return_value=True)

    result = await task_enqueuer.enqueue_sq_stats(sq_id=7)

    mock_db.repos.tasks.create_generic_task.assert_not_called()
    assert result is None


# === enqueue_photo_due tests ===


@pytest.mark.asyncio
async def test_enqueue_photo_due_creates_task(task_enqueuer, mock_db):
    """Creates PHOTO_DUE task when no active task exists."""
    mock_db.repos.tasks.has_active_task = AsyncMock(return_value=False)
    mock_db.repos.tasks.create_generic_task = AsyncMock(return_value=15)

    result = await task_enqueuer.enqueue_photo_due()

    mock_db.repos.tasks.has_active_task.assert_called_once_with(CollectionTaskType.PHOTO_DUE)
    mock_db.repos.tasks.create_generic_task.assert_called_once()
    args, kwargs = mock_db.repos.tasks.create_generic_task.call_args
    assert args[0] == CollectionTaskType.PHOTO_DUE
    assert "Отправка фото" in kwargs.get("title", "")
    assert result == 15


@pytest.mark.asyncio
async def test_enqueue_photo_due_skips_if_active(task_enqueuer, mock_db):
    """Skips creation if active task exists."""
    mock_db.repos.tasks.has_active_task = AsyncMock(return_value=True)

    result = await task_enqueuer.enqueue_photo_due()

    mock_db.repos.tasks.create_generic_task.assert_not_called()
    assert result is None


# === enqueue_photo_auto tests ===


@pytest.mark.asyncio
async def test_enqueue_photo_auto_creates_task(task_enqueuer, mock_db):
    """Creates PHOTO_AUTO task when no active task exists."""
    mock_db.repos.tasks.has_active_task = AsyncMock(return_value=False)
    mock_db.repos.tasks.create_generic_task = AsyncMock(return_value=16)

    result = await task_enqueuer.enqueue_photo_auto()

    mock_db.repos.tasks.has_active_task.assert_called_once_with(CollectionTaskType.PHOTO_AUTO)
    mock_db.repos.tasks.create_generic_task.assert_called_once()
    args, kwargs = mock_db.repos.tasks.create_generic_task.call_args
    assert args[0] == CollectionTaskType.PHOTO_AUTO
    assert "Автозагрузка фото" in kwargs.get("title", "")
    assert result == 16


@pytest.mark.asyncio
async def test_enqueue_photo_auto_skips_if_active(task_enqueuer, mock_db):
    """Skips creation if active task exists."""
    mock_db.repos.tasks.has_active_task = AsyncMock(return_value=True)

    result = await task_enqueuer.enqueue_photo_auto()

    mock_db.repos.tasks.create_generic_task.assert_not_called()
    assert result is None


# === enqueue_pipeline_run tests ===


@pytest.mark.asyncio
async def test_enqueue_pipeline_run_creates_task(task_enqueuer, mock_db):
    """Creates PIPELINE_RUN task when no active task exists."""
    mock_db.repos.tasks.has_active_task = AsyncMock(return_value=False)
    mock_db.repos.tasks.create_generic_task = AsyncMock(return_value=99)

    result = await task_enqueuer.enqueue_pipeline_run(pipeline_id=3)

    mock_db.repos.tasks.has_active_task.assert_called_once_with(
        CollectionTaskType.PIPELINE_RUN,
        payload_filter_key="pipeline_id",
        payload_filter_value=3,
    )
    mock_db.repos.tasks.create_generic_task.assert_called_once()
    args, kwargs = mock_db.repos.tasks.create_generic_task.call_args
    assert args[0] == CollectionTaskType.PIPELINE_RUN
    assert "Pipeline run #3" in kwargs.get("title", "")
    payload = kwargs.get("payload")
    assert isinstance(payload, PipelineRunTaskPayload)
    assert payload.pipeline_id == 3
    assert result == 99


@pytest.mark.asyncio
async def test_enqueue_pipeline_run_skips_if_active(task_enqueuer, mock_db):
    """Skips creation if active task for same pipeline exists."""
    mock_db.repos.tasks.has_active_task = AsyncMock(return_value=True)

    result = await task_enqueuer.enqueue_pipeline_run(pipeline_id=3)

    mock_db.repos.tasks.create_generic_task.assert_not_called()
    assert result is None
