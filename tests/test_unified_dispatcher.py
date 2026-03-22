"""Tests for UnifiedDispatcher."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import (
    Channel,
    CollectionTask,
    CollectionTaskStatus,
    CollectionTaskType,
    PipelineRunTaskPayload,
    SqStatsTaskPayload,
    StatsAllTaskPayload,
)
from src.services.unified_dispatcher import HANDLED_TYPES, UnifiedDispatcher


@pytest.fixture
def mock_collector():
    """Mock Collector."""
    collector = MagicMock()
    collector.is_running = False
    collector.delay_between_channels_sec = 0.1
    collector.collect_channel_stats = AsyncMock(return_value=MagicMock(subscriber_count=100))
    collector.get_stats_availability = AsyncMock(
        return_value=MagicMock(state="available", next_available_at_utc=None)
    )
    return collector


@pytest.fixture
def mock_channel_bundle():
    """Mock ChannelBundle."""
    bundle = MagicMock()
    bundle.get_by_channel_id = AsyncMock(
        return_value=Channel(channel_id=123, title="Test Channel")
    )
    return bundle


@pytest.fixture
def mock_tasks_repo():
    """Mock CollectionTasksRepository."""
    repo = MagicMock()
    repo.claim_next_due_generic_task = AsyncMock(return_value=None)
    repo.requeue_running_generic_tasks_on_startup = AsyncMock(return_value=0)
    repo.update_collection_task = AsyncMock()
    repo.update_collection_task_progress = AsyncMock()
    repo.get_collection_task = AsyncMock()
    repo.create_stats_continuation_task = AsyncMock(return_value=999)
    return repo


@pytest.fixture
def mock_sq_bundle():
    """Mock SearchQueryBundle."""
    from datetime import date

    today = date.today().isoformat()
    bundle = MagicMock()
    bundle.get_by_id = AsyncMock(return_value=MagicMock(query="test query"))
    bundle.get_fts_daily_stats_for_query = AsyncMock(
        return_value=[MagicMock(day=today, count=42)]
    )
    bundle.record_stat = AsyncMock()
    return bundle


@pytest.fixture
def mock_photo_task_service():
    """Mock PhotoTaskService."""
    service = MagicMock()
    service.run_due = AsyncMock(return_value=5)
    return service


@pytest.fixture
def mock_photo_auto_upload_service():
    """Mock PhotoAutoUploadService."""
    service = MagicMock()
    service.run_due = AsyncMock(return_value=3)
    return service


@pytest.fixture
def dispatcher(
    mock_collector,
    mock_channel_bundle,
    mock_tasks_repo,
):
    """Basic UnifiedDispatcher instance."""
    return UnifiedDispatcher(
        mock_collector,
        mock_channel_bundle,
        mock_tasks_repo,
        poll_interval_sec=0.01,
        channel_timeout_sec=1.0,
    )


# === HANDLED_TYPES constant ===


def test_handled_types_contains_expected():
    """HANDLED_TYPES contains expected task types."""
    assert "stats_all" in HANDLED_TYPES
    assert "sq_stats" in HANDLED_TYPES
    assert "photo_due" in HANDLED_TYPES
    assert "photo_auto" in HANDLED_TYPES
    assert "pipeline_run" in HANDLED_TYPES
    assert "content_generate" in HANDLED_TYPES
    assert "content_publish" in HANDLED_TYPES


# === start/stop tests ===


@pytest.mark.asyncio
async def test_start_creates_task(dispatcher):
    """start() creates background task."""
    await dispatcher.start()
    assert dispatcher._task is not None
    assert not dispatcher._task.done()
    await dispatcher.stop()


@pytest.mark.asyncio
async def test_start_idempotent(dispatcher):
    """start() is idempotent - second call does nothing."""
    await dispatcher.start()
    task1 = dispatcher._task
    await dispatcher.start()
    assert dispatcher._task is task1
    await dispatcher.stop()


@pytest.mark.asyncio
async def test_stop_cancels_task(dispatcher):
    """stop() cancels background task."""
    await dispatcher.start()
    await dispatcher.stop()
    assert dispatcher._task is None


@pytest.mark.asyncio
async def test_stop_without_start(dispatcher):
    """stop() without start() does nothing."""
    await dispatcher.stop()  # Should not raise


@pytest.mark.asyncio
async def test_start_requeues_interrupted_tasks(mock_collector, mock_channel_bundle, mock_tasks_repo):
    """start() requeues interrupted tasks on startup."""
    mock_tasks_repo.requeue_running_generic_tasks_on_startup.return_value = 3

    dispatcher = UnifiedDispatcher(
        mock_collector,
        mock_channel_bundle,
        mock_tasks_repo,
        poll_interval_sec=0.01,
    )
    await dispatcher.start()
    await asyncio.sleep(0.05)
    await dispatcher.stop()

    mock_tasks_repo.requeue_running_generic_tasks_on_startup.assert_called_once()


# === _run_loop tests ===


@pytest.mark.asyncio
async def test_run_loop_processes_task(mock_collector, mock_channel_bundle, mock_tasks_repo):
    """_run_loop processes available tasks."""
    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.STATS_ALL,
        status=CollectionTaskStatus.PENDING,
        payload=StatsAllTaskPayload(channel_ids=[], next_index=0),
    )
    mock_tasks_repo.claim_next_due_generic_task.side_effect = [task, None, None]

    dispatcher = UnifiedDispatcher(
        mock_collector,
        mock_channel_bundle,
        mock_tasks_repo,
        poll_interval_sec=0.01,
    )

    await dispatcher.start()
    await asyncio.sleep(0.1)
    await dispatcher.stop()

    assert mock_tasks_repo.claim_next_due_generic_task.call_count >= 2


# === _handle_stats_all tests ===


@pytest.mark.asyncio
async def test_handle_stats_all_empty_channels(dispatcher, mock_tasks_repo):
    """_handle_stats_all completes immediately if next_index >= len(channel_ids)."""
    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.STATS_ALL,
        status=CollectionTaskStatus.RUNNING,
        payload=StatsAllTaskPayload(
            channel_ids=[123],
            next_index=1,  # Already past the end
            batch_size=10,
        ),
    )

    await dispatcher._handle_stats_all(task)

    mock_tasks_repo.update_collection_task.assert_called_once()
    args, kwargs = mock_tasks_repo.update_collection_task.call_args
    assert args[0] == 1
    assert args[1] == CollectionTaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_handle_stats_all_no_task_id(dispatcher, mock_tasks_repo):
    """_handle_stats_all returns early if task.id is None."""
    task = CollectionTask(
        id=None,
        task_type=CollectionTaskType.STATS_ALL,
        status=CollectionTaskStatus.RUNNING,
        payload=StatsAllTaskPayload(channel_ids=[123], next_index=0),
    )

    await dispatcher._handle_stats_all(task)

    mock_tasks_repo.update_collection_task.assert_not_called()


@pytest.mark.asyncio
async def test_handle_stats_all_invalid_payload(dispatcher, mock_tasks_repo):
    """_handle_stats_all fails if payload is wrong type."""
    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.STATS_ALL,
        status=CollectionTaskStatus.RUNNING,
        payload=SqStatsTaskPayload(sq_id=1),  # Wrong payload type
    )

    await dispatcher._handle_stats_all(task)

    mock_tasks_repo.update_collection_task.assert_called_once()
    args, kwargs = mock_tasks_repo.update_collection_task.call_args
    assert args[1] == CollectionTaskStatus.FAILED


@pytest.mark.asyncio
async def test_handle_stats_all_processes_batch(
    mock_collector, mock_channel_bundle, mock_tasks_repo
):
    """_handle_stats_all processes a batch of channels."""
    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.STATS_ALL,
        status=CollectionTaskStatus.RUNNING,
        payload=StatsAllTaskPayload(
            channel_ids=[100, 101, 102],
            next_index=0,
            batch_size=2,
        ),
    )

    dispatcher = UnifiedDispatcher(
        mock_collector,
        mock_channel_bundle,
        mock_tasks_repo,
        poll_interval_sec=0.01,
    )

    await dispatcher._handle_stats_all(task)

    # Should process 2 channels (batch_size=2), then create continuation
    mock_collector.collect_channel_stats.assert_called()


@pytest.mark.asyncio
async def test_handle_stats_all_channel_not_found(
    mock_collector, mock_channel_bundle, mock_tasks_repo
):
    """_handle_stats_all handles missing channel."""
    mock_channel_bundle.get_by_channel_id.return_value = None

    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.STATS_ALL,
        status=CollectionTaskStatus.RUNNING,
        payload=StatsAllTaskPayload(
            channel_ids=[999],
            next_index=0,
            batch_size=10,
        ),
    )

    dispatcher = UnifiedDispatcher(
        mock_collector,
        mock_channel_bundle,
        mock_tasks_repo,
        poll_interval_sec=0.01,
    )

    await dispatcher._handle_stats_all(task)

    # Should complete without calling collect_channel_stats
    mock_collector.collect_channel_stats.assert_not_called()


@pytest.mark.asyncio
async def test_handle_stats_all_timeout(
    mock_collector, mock_channel_bundle, mock_tasks_repo
):
    """_handle_stats_all handles timeout."""
    mock_collector.collect_channel_stats = AsyncMock(side_effect=asyncio.TimeoutError())

    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.STATS_ALL,
        status=CollectionTaskStatus.RUNNING,
        payload=StatsAllTaskPayload(
            channel_ids=[100],
            next_index=0,
            batch_size=10,
        ),
    )

    dispatcher = UnifiedDispatcher(
        mock_collector,
        mock_channel_bundle,
        mock_tasks_repo,
        poll_interval_sec=0.01,
    )

    await dispatcher._handle_stats_all(task)

    # Should mark task completed despite error
    mock_tasks_repo.update_collection_task.assert_called()


@pytest.mark.asyncio
async def test_handle_stats_all_stats_unavailable(
    mock_collector, mock_channel_bundle, mock_tasks_repo
):
    """_handle_stats_all handles case when stats collection returns None."""
    mock_collector.collect_channel_stats = AsyncMock(return_value=None)
    mock_collector.get_stats_availability.return_value = MagicMock(
        state="no_clients",
        next_available_at_utc=None,
    )

    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.STATS_ALL,
        status=CollectionTaskStatus.RUNNING,
        payload=StatsAllTaskPayload(
            channel_ids=[100],
            next_index=0,
            batch_size=10,
        ),
    )

    dispatcher = UnifiedDispatcher(
        mock_collector,
        mock_channel_bundle,
        mock_tasks_repo,
        poll_interval_sec=0.01,
    )

    await dispatcher._handle_stats_all(task)

    mock_tasks_repo.update_collection_task.assert_called()
    args, kwargs = mock_tasks_repo.update_collection_task.call_args
    assert args[1] == CollectionTaskStatus.FAILED


@pytest.mark.asyncio
async def test_handle_stats_all_flood_wait_creates_continuation(
    mock_collector, mock_channel_bundle, mock_tasks_repo
):
    """_handle_stats_all creates continuation task when all clients flooded."""
    mock_collector.collect_channel_stats = AsyncMock(return_value=None)
    mock_collector.get_stats_availability.return_value = MagicMock(
        state="all_flooded",
        next_available_at_utc=datetime.now(timezone.utc),
    )

    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.STATS_ALL,
        status=CollectionTaskStatus.RUNNING,
        payload=StatsAllTaskPayload(
            channel_ids=[100],
            next_index=0,
            batch_size=10,
        ),
    )

    dispatcher = UnifiedDispatcher(
        mock_collector,
        mock_channel_bundle,
        mock_tasks_repo,
        poll_interval_sec=0.01,
    )

    await dispatcher._handle_stats_all(task)

    # Should create continuation task
    mock_tasks_repo.create_stats_continuation_task.assert_called_once()


# === _handle_sq_stats tests ===


@pytest.mark.asyncio
async def test_handle_sq_stats_success(dispatcher, mock_tasks_repo, mock_sq_bundle):
    """_handle_sq_stats records stats successfully."""
    dispatcher._sq_bundle = mock_sq_bundle

    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.SQ_STATS,
        status=CollectionTaskStatus.RUNNING,
        payload=SqStatsTaskPayload(sq_id=7),
    )

    await dispatcher._handle_sq_stats(task)

    mock_sq_bundle.record_stat.assert_called_once_with(7, 42)
    mock_tasks_repo.update_collection_task.assert_called()
    args, kwargs = mock_tasks_repo.update_collection_task.call_args
    assert args[1] == CollectionTaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_handle_sq_stats_no_bundle(dispatcher, mock_tasks_repo):
    """_handle_sq_stats completes with note when no bundle configured."""
    dispatcher._sq_bundle = None

    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.SQ_STATS,
        status=CollectionTaskStatus.RUNNING,
        payload=SqStatsTaskPayload(sq_id=7),
    )

    await dispatcher._handle_sq_stats(task)

    mock_tasks_repo.update_collection_task.assert_called()
    args, kwargs = mock_tasks_repo.update_collection_task.call_args
    assert args[1] == CollectionTaskStatus.COMPLETED
    assert "note" in kwargs


@pytest.mark.asyncio
async def test_handle_sq_stats_invalid_payload(dispatcher, mock_tasks_repo):
    """_handle_sq_stats fails with invalid payload."""
    dispatcher._sq_bundle = MagicMock()

    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.SQ_STATS,
        status=CollectionTaskStatus.RUNNING,
        payload=StatsAllTaskPayload(channel_ids=[], next_index=0),  # Wrong type
    )

    await dispatcher._handle_sq_stats(task)

    mock_tasks_repo.update_collection_task.assert_called()
    args, kwargs = mock_tasks_repo.update_collection_task.call_args
    assert args[1] == CollectionTaskStatus.FAILED


@pytest.mark.asyncio
async def test_handle_sq_stats_query_not_found(dispatcher, mock_tasks_repo, mock_sq_bundle):
    """_handle_sq_stats completes when query not found."""
    dispatcher._sq_bundle = mock_sq_bundle
    mock_sq_bundle.get_by_id.return_value = None

    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.SQ_STATS,
        status=CollectionTaskStatus.RUNNING,
        payload=SqStatsTaskPayload(sq_id=999),
    )

    await dispatcher._handle_sq_stats(task)

    mock_tasks_repo.update_collection_task.assert_called()
    args, kwargs = mock_tasks_repo.update_collection_task.call_args
    assert args[1] == CollectionTaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_handle_sq_stats_exception(dispatcher, mock_tasks_repo, mock_sq_bundle):
    """_handle_sq_stats handles exceptions."""
    dispatcher._sq_bundle = mock_sq_bundle
    mock_sq_bundle.get_fts_daily_stats_for_query.side_effect = Exception("DB error")

    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.SQ_STATS,
        status=CollectionTaskStatus.RUNNING,
        payload=SqStatsTaskPayload(sq_id=7),
    )

    await dispatcher._handle_sq_stats(task)

    mock_tasks_repo.update_collection_task.assert_called()
    args, kwargs = mock_tasks_repo.update_collection_task.call_args
    assert args[1] == CollectionTaskStatus.FAILED


# === _handle_photo_due tests ===


@pytest.mark.asyncio
async def test_handle_photo_due_success(dispatcher, mock_tasks_repo, mock_photo_task_service):
    """_handle_photo_due processes due photos."""
    dispatcher._photo_task_service = mock_photo_task_service

    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.PHOTO_DUE,
        status=CollectionTaskStatus.RUNNING,
        payload=None,
    )

    await dispatcher._handle_photo_due(task)

    mock_photo_task_service.run_due.assert_called_once()
    mock_tasks_repo.update_collection_task.assert_called()
    args, kwargs = mock_tasks_repo.update_collection_task.call_args
    assert args[1] == CollectionTaskStatus.COMPLETED
    assert kwargs.get("messages_collected") == 5


@pytest.mark.asyncio
async def test_handle_photo_due_no_service(dispatcher, mock_tasks_repo):
    """_handle_photo_due completes with note when no service configured."""
    dispatcher._photo_task_service = None

    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.PHOTO_DUE,
        status=CollectionTaskStatus.RUNNING,
        payload=None,
    )

    await dispatcher._handle_photo_due(task)

    mock_tasks_repo.update_collection_task.assert_called()
    args, kwargs = mock_tasks_repo.update_collection_task.call_args
    assert args[1] == CollectionTaskStatus.COMPLETED


@pytest.mark.asyncio
async def test_handle_photo_due_exception(dispatcher, mock_tasks_repo, mock_photo_task_service):
    """_handle_photo_due handles exceptions."""
    dispatcher._photo_task_service = mock_photo_task_service
    mock_photo_task_service.run_due.side_effect = Exception("Photo error")

    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.PHOTO_DUE,
        status=CollectionTaskStatus.RUNNING,
        payload=None,
    )

    await dispatcher._handle_photo_due(task)

    mock_tasks_repo.update_collection_task.assert_called()
    args, kwargs = mock_tasks_repo.update_collection_task.call_args
    assert args[1] == CollectionTaskStatus.FAILED


# === _handle_photo_auto tests ===


@pytest.mark.asyncio
async def test_handle_photo_auto_success(dispatcher, mock_tasks_repo, mock_photo_auto_upload_service):
    """_handle_photo_auto processes auto jobs."""
    dispatcher._photo_auto_upload_service = mock_photo_auto_upload_service

    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.PHOTO_AUTO,
        status=CollectionTaskStatus.RUNNING,
        payload=None,
    )

    await dispatcher._handle_photo_auto(task)

    mock_photo_auto_upload_service.run_due.assert_called_once()
    mock_tasks_repo.update_collection_task.assert_called()
    args, kwargs = mock_tasks_repo.update_collection_task.call_args
    assert args[1] == CollectionTaskStatus.COMPLETED
    assert kwargs.get("messages_collected") == 3


@pytest.mark.asyncio
async def test_handle_photo_auto_no_service(dispatcher, mock_tasks_repo):
    """_handle_photo_auto completes with note when no service configured."""
    dispatcher._photo_auto_upload_service = None

    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.PHOTO_AUTO,
        status=CollectionTaskStatus.RUNNING,
        payload=None,
    )

    await dispatcher._handle_photo_auto(task)

    mock_tasks_repo.update_collection_task.assert_called()
    args, kwargs = mock_tasks_repo.update_collection_task.call_args
    assert args[1] == CollectionTaskStatus.COMPLETED


# === _handle_pipeline_run tests ===


@pytest.mark.asyncio
async def test_handle_pipeline_run_invalid_payload(dispatcher, mock_tasks_repo):
    """_handle_pipeline_run fails with invalid payload."""
    dispatcher._pipeline_bundle = MagicMock()
    dispatcher._search_engine = MagicMock()
    dispatcher._db = MagicMock()

    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.PIPELINE_RUN,
        status=CollectionTaskStatus.RUNNING,
        payload=SqStatsTaskPayload(sq_id=1),  # Wrong type
    )

    await dispatcher._handle_pipeline_run(task)

    mock_tasks_repo.update_collection_task.assert_called()
    args, kwargs = mock_tasks_repo.update_collection_task.call_args
    assert args[1] == CollectionTaskStatus.FAILED
    assert "Invalid PIPELINE_RUN payload" in kwargs.get("error", "")


@pytest.mark.asyncio
async def test_handle_pipeline_run_missing_deps(dispatcher, mock_tasks_repo):
    """_handle_pipeline_run fails when dependencies not configured."""
    dispatcher._pipeline_bundle = None
    dispatcher._search_engine = None
    dispatcher._db = None

    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.PIPELINE_RUN,
        status=CollectionTaskStatus.RUNNING,
        payload=PipelineRunTaskPayload(pipeline_id=1),
    )

    await dispatcher._handle_pipeline_run(task)

    mock_tasks_repo.update_collection_task.assert_called()
    args, kwargs = mock_tasks_repo.update_collection_task.call_args
    assert args[1] == CollectionTaskStatus.FAILED
    assert "environment not configured" in kwargs.get("error", "")


# === _dispatch tests ===


@pytest.mark.asyncio
async def test_dispatch_unknown_type(dispatcher, mock_tasks_repo):
    """_dispatch fails for unknown task type."""
    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.CHANNEL_COLLECT,  # Not in HANDLED_TYPES
        status=CollectionTaskStatus.RUNNING,
        payload=None,
    )

    await dispatcher._dispatch(task)

    mock_tasks_repo.update_collection_task.assert_called()
    args, kwargs = mock_tasks_repo.update_collection_task.call_args
    assert args[1] == CollectionTaskStatus.FAILED
    assert "Unknown task type" in kwargs.get("error", "")


# === Error recovery tests ===


@pytest.mark.asyncio
async def test_run_loop_handles_exception(
    mock_collector, mock_channel_bundle, mock_tasks_repo
):
    """_run_loop handles exceptions and continues."""
    task = CollectionTask(
        id=1,
        task_type=CollectionTaskType.STATS_ALL,
        status=CollectionTaskStatus.RUNNING,
        payload=StatsAllTaskPayload(channel_ids=[123], next_index=0),
    )

    call_count = [0]

    async def claim_side_effect(*args, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            raise Exception("DB connection lost")
        if call_count[0] == 2:
            return task
        return None

    mock_tasks_repo.claim_next_due_generic_task.side_effect = claim_side_effect
    mock_tasks_repo.get_collection_task.return_value = None  # No task to mark as failed

    dispatcher = UnifiedDispatcher(
        mock_collector,
        mock_channel_bundle,
        mock_tasks_repo,
        poll_interval_sec=0.01,
    )

    await dispatcher.start()
    await asyncio.sleep(0.1)
    await dispatcher.stop()

    # Should have recovered from exception and continued
    assert mock_tasks_repo.claim_next_due_generic_task.call_count >= 2
