"""Tests for CollectionTasksRepository."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.database.repositories.collection_tasks import CollectionTasksRepository
from src.models import CollectionTaskStatus, CollectionTaskType, StatsAllTaskPayload


@pytest.fixture
async def repo(db):
    """Create repository instance."""
    return CollectionTasksRepository(db.db)


# _deserialize_payload tests


def test_deserialize_payload_none():
    """Test deserializing None payload."""
    result = CollectionTasksRepository._deserialize_payload(None)
    assert result is None


def test_deserialize_payload_empty_string():
    """Test deserializing empty string payload."""
    result = CollectionTasksRepository._deserialize_payload("")
    assert result is None


def test_deserialize_payload_invalid_json():
    """Test deserializing invalid JSON."""
    result = CollectionTasksRepository._deserialize_payload("not json")
    assert result is None


def test_deserialize_payload_not_dict():
    """Test deserializing non-dict JSON."""
    result = CollectionTasksRepository._deserialize_payload("[1, 2, 3]")
    assert result is None


def test_deserialize_payload_plain_dict():
    """Test deserializing plain dict."""
    result = CollectionTasksRepository._deserialize_payload('{"key": "value"}')
    assert result == {"key": "value"}


def test_deserialize_payload_stats_all():
    """Test deserializing StatsAllTaskPayload."""
    payload = '{"task_kind": "stats_all", "channel_ids": [1, 2, 3], "next_index": 5}'
    result = CollectionTasksRepository._deserialize_payload(payload)
    assert isinstance(result, StatsAllTaskPayload)
    assert result.channel_ids == [1, 2, 3]
    assert result.next_index == 5


# _serialize_payload tests


def test_serialize_payload_none():
    """Test serializing None payload."""
    result = CollectionTasksRepository._serialize_payload(None)
    assert result is None


def test_serialize_payload_dict():
    """Test serializing dict payload."""
    result = CollectionTasksRepository._serialize_payload({"key": "value"})
    assert result == '{"key": "value"}'


def test_serialize_payload_stats_all():
    """Test serializing StatsAllTaskPayload."""
    payload = StatsAllTaskPayload(channel_ids=[1, 2, 3], next_index=5)
    result = CollectionTasksRepository._serialize_payload(payload)
    assert "channel_ids" in result
    assert "next_index" in result


# create_collection_task tests


async def test_create_collection_task_basic(repo):
    """Test creating a basic collection task."""
    task_id = await repo.create_collection_task(
        channel_id=12345,
        channel_title="Test Channel",
    )
    assert task_id > 0

    task = await repo.get_collection_task(task_id)
    assert task is not None
    assert task.channel_id == 12345
    assert task.channel_title == "Test Channel"
    assert task.task_type == CollectionTaskType.CHANNEL_COLLECT
    assert task.status == CollectionTaskStatus.PENDING


async def test_create_collection_task_with_all_fields(repo):
    """Test creating a task with all optional fields."""
    run_after = datetime(2026, 3, 16, 12, 0, 0, tzinfo=timezone.utc)
    task_id = await repo.create_collection_task(
        channel_id=12345,
        channel_title="Test",
        channel_username="testchannel",
        run_after=run_after,
        payload={"custom": "data"},
        parent_task_id=99,
    )

    task = await repo.get_collection_task(task_id)
    assert task.channel_username == "testchannel"
    assert task.run_after is not None
    assert task.payload == {"custom": "data"}
    assert task.parent_task_id == 99


async def test_create_collection_task_run_after_normalization(repo):
    """Test that run_after is normalized to UTC."""
    # Create datetime with different timezone
    local_tz = timezone(timedelta(hours=3))
    run_after = datetime(2026, 3, 16, 12, 0, 0, tzinfo=local_tz)

    task_id = await repo.create_collection_task(
        channel_id=1,
        channel_title="Test",
        run_after=run_after,
    )

    task = await repo.get_collection_task(task_id)
    # Should be stored as UTC (9:00 instead of 12:00 +03:00)
    assert task.run_after is not None
    assert task.run_after.hour == 9


# create_stats_task tests


async def test_create_stats_task_basic(repo):
    """Test creating a stats task."""
    payload = StatsAllTaskPayload(channel_ids=[1, 2, 3])
    task_id = await repo.create_stats_task(payload)

    task = await repo.get_collection_task(task_id)
    assert task.task_type == CollectionTaskType.STATS_ALL
    assert task.channel_title == "Обновление статистики"
    assert isinstance(task.payload, StatsAllTaskPayload)
    assert task.payload.channel_ids == [1, 2, 3]


async def test_create_stats_task_with_run_after(repo):
    """Test creating a stats task with run_after."""
    run_after = datetime(2026, 3, 16, 12, 0, 0, tzinfo=timezone.utc)
    payload = StatsAllTaskPayload(channel_ids=[1, 2, 3])
    task_id = await repo.create_stats_task(payload, run_after=run_after)

    task = await repo.get_collection_task(task_id)
    assert task.run_after is not None


# update_collection_task_progress tests


async def test_update_collection_task_progress(repo):
    """Test updating task progress."""
    task_id = await repo.create_collection_task(1, "Test")
    await repo.update_collection_task_progress(task_id, 100)

    task = await repo.get_collection_task(task_id)
    assert task.messages_collected == 100


# update_collection_task tests


async def test_update_collection_task_to_running(repo):
    """Test updating task to running status."""
    task_id = await repo.create_collection_task(1, "Test")
    await repo.update_collection_task(task_id, CollectionTaskStatus.RUNNING)

    task = await repo.get_collection_task(task_id)
    assert task.status == CollectionTaskStatus.RUNNING
    assert task.started_at is not None


async def test_update_collection_task_to_completed(repo):
    """Test updating task to completed status."""
    task_id = await repo.create_collection_task(1, "Test")
    await repo.update_collection_task(task_id, CollectionTaskStatus.RUNNING)
    await repo.update_collection_task(
        task_id,
        CollectionTaskStatus.COMPLETED,
        messages_collected=500,
        note="Done",
    )

    task = await repo.get_collection_task(task_id)
    assert task.status == CollectionTaskStatus.COMPLETED
    assert task.completed_at is not None
    assert task.messages_collected == 500
    assert task.note == "Done"


async def test_update_collection_task_to_failed(repo):
    """Test updating task to failed status."""
    task_id = await repo.create_collection_task(1, "Test")
    await repo.update_collection_task(
        task_id,
        CollectionTaskStatus.FAILED,
        error="Connection timeout",
    )

    task = await repo.get_collection_task(task_id)
    assert task.status == CollectionTaskStatus.FAILED
    assert task.error == "Connection timeout"
    assert task.completed_at is not None


async def test_update_collection_task_with_string_status(repo):
    """Test updating task with string status instead of enum."""
    task_id = await repo.create_collection_task(1, "Test")
    await repo.update_collection_task(task_id, "running")

    task = await repo.get_collection_task(task_id)
    assert task.status == CollectionTaskStatus.RUNNING


# get_collection_task tests


async def test_get_collection_task_not_found(repo):
    """Test getting non-existent task."""
    task = await repo.get_collection_task(999)
    assert task is None


# get_collection_tasks tests


async def test_get_collection_tasks_empty(repo):
    """Test getting tasks when none exist."""
    tasks = await repo.get_collection_tasks()
    assert tasks == []


async def test_get_collection_tasks_ordered(repo):
    """Test that tasks are ordered by id DESC."""
    id1 = await repo.create_collection_task(1, "First")
    id2 = await repo.create_collection_task(2, "Second")
    id3 = await repo.create_collection_task(3, "Third")

    tasks = await repo.get_collection_tasks(limit=10)
    assert len(tasks) == 3
    # Newest first (id DESC)
    assert tasks[0].id == id3
    assert tasks[1].id == id2
    assert tasks[2].id == id1


async def test_get_collection_tasks_limit(repo):
    """Test that limit is respected."""
    for i in range(10):
        await repo.create_collection_task(i, f"Channel {i}")

    tasks = await repo.get_collection_tasks(limit=5)
    assert len(tasks) == 5


# get_active_collection_tasks_for_channel tests


async def test_get_active_collection_tasks_for_channel_empty(repo):
    """Test getting active tasks when none exist."""
    tasks = await repo.get_active_collection_tasks_for_channel(12345)
    assert tasks == []


async def test_get_active_collection_tasks_for_channel(repo):
    """Test getting active tasks for a channel."""
    channel_id = 12345
    task_id1 = await repo.create_collection_task(channel_id, "Test")
    task_id2 = await repo.create_collection_task(channel_id, "Test")
    await repo.create_collection_task(99999, "Other")

    # Complete one task
    await repo.update_collection_task(task_id1, CollectionTaskStatus.COMPLETED)

    tasks = await repo.get_active_collection_tasks_for_channel(channel_id)
    assert len(tasks) == 1
    assert tasks[0].id == task_id2


async def test_get_active_collection_tasks_excludes_other_types(repo):
    """Test that stats tasks are excluded."""
    channel_id = 12345
    await repo.create_collection_task(channel_id, "Test")
    await repo.create_stats_task(StatsAllTaskPayload(channel_ids=[channel_id]))

    tasks = await repo.get_active_collection_tasks_for_channel(channel_id)
    assert len(tasks) == 1  # Only the channel_collect task


# get_channel_ids_with_active_tasks tests


async def test_get_channel_ids_with_active_tasks(repo):
    """Test getting channel IDs with active tasks."""
    await repo.create_collection_task(1, "Channel 1")
    await repo.create_collection_task(2, "Channel 2")
    await repo.create_collection_task(3, "Channel 3")

    # Complete task for channel 3
    task_id = (await repo.get_collection_tasks())[0].id
    await repo.update_collection_task(task_id, CollectionTaskStatus.COMPLETED)

    ids = await repo.get_channel_ids_with_active_tasks()
    assert ids == {1, 2}


async def test_get_channel_ids_with_active_tasks_empty(repo):
    """Test getting channel IDs when no active tasks."""
    ids = await repo.get_channel_ids_with_active_tasks()
    assert ids == set()


# get_active_stats_task tests


async def test_get_active_stats_task_none(repo):
    """Test getting active stats task when none exists."""
    task = await repo.get_active_stats_task()
    assert task is None


async def test_get_active_stats_task(repo):
    """Test getting active stats task."""
    payload = StatsAllTaskPayload(channel_ids=[1, 2])
    task_id = await repo.create_stats_task(payload)

    task = await repo.get_active_stats_task()
    assert task is not None
    assert task.id == task_id


async def test_get_active_stats_task_excludes_completed(repo):
    """Test that completed stats tasks are not returned."""
    payload = StatsAllTaskPayload(channel_ids=[1, 2])
    task_id = await repo.create_stats_task(payload)
    await repo.update_collection_task(task_id, CollectionTaskStatus.COMPLETED)

    task = await repo.get_active_stats_task()
    assert task is None


# claim_next_due_generic_task tests


async def test_claim_next_due_stats_task_none_available(repo):
    """Test claiming when no stats tasks available."""
    now = datetime.now(tz=timezone.utc)
    task = await repo.claim_next_due_generic_task(now, [CollectionTaskType.STATS_ALL.value])
    assert task is None


async def test_claim_next_due_stats_task_success(repo):
    """Test successfully claiming a stats task."""
    payload = StatsAllTaskPayload(channel_ids=[1, 2])
    task_id = await repo.create_stats_task(payload)

    now = datetime.now(tz=timezone.utc)
    claimed = await repo.claim_next_due_generic_task(now, [CollectionTaskType.STATS_ALL.value])

    assert claimed is not None
    assert claimed.id == task_id
    assert claimed.status == CollectionTaskStatus.RUNNING
    assert claimed.started_at is not None


async def test_claim_next_due_stats_task_respects_run_after(repo):
    """Test that run_after is respected."""
    now = datetime.now(tz=timezone.utc)
    future = now + timedelta(hours=1)

    payload = StatsAllTaskPayload(channel_ids=[1, 2])
    await repo.create_stats_task(payload, run_after=future)

    claimed = await repo.claim_next_due_generic_task(now, [CollectionTaskType.STATS_ALL.value])
    assert claimed is None


async def test_claim_next_due_stats_task_run_after_passed(repo):
    """Test claiming task when run_after has passed."""
    now = datetime.now(tz=timezone.utc)
    past = now - timedelta(hours=1)

    payload = StatsAllTaskPayload(channel_ids=[1, 2])
    task_id = await repo.create_stats_task(payload, run_after=past)

    claimed = await repo.claim_next_due_generic_task(now, [CollectionTaskType.STATS_ALL.value])
    assert claimed is not None
    assert claimed.id == task_id


async def test_claim_next_due_stats_task_skips_running(repo):
    """Test that running tasks are not claimed."""
    payload = StatsAllTaskPayload(channel_ids=[1, 2])
    task_id = await repo.create_stats_task(payload)
    await repo.update_collection_task(task_id, CollectionTaskStatus.RUNNING)

    now = datetime.now(tz=timezone.utc)
    claimed = await repo.claim_next_due_generic_task(now, [CollectionTaskType.STATS_ALL.value])
    assert claimed is None


# create_stats_continuation_task tests


async def test_create_stats_continuation_task(repo):
    """Test creating a continuation task."""
    parent_id = await repo.create_stats_task(StatsAllTaskPayload(channel_ids=[1, 2]))
    run_after = datetime.now(tz=timezone.utc) + timedelta(minutes=5)

    payload = StatsAllTaskPayload(channel_ids=[1, 2, 3], next_index=10)
    child_id = await repo.create_stats_continuation_task(
        payload=payload,
        run_after=run_after,
        parent_task_id=parent_id,
    )

    task = await repo.get_collection_task(child_id)
    assert task.parent_task_id == parent_id
    assert task.payload.next_index == 10


# get_pending_channel_tasks tests


async def test_get_pending_channel_tasks(repo):
    """Test getting pending channel tasks."""
    id1 = await repo.create_collection_task(1, "Channel 1")
    id2 = await repo.create_collection_task(2, "Channel 2")
    await repo.update_collection_task(id1, CollectionTaskStatus.RUNNING)
    await repo.create_stats_task(StatsAllTaskPayload(channel_ids=[1]))

    tasks = await repo.get_pending_channel_tasks()
    assert len(tasks) == 1
    assert tasks[0].id == id2


# fail_running_collection_tasks_on_startup tests


async def test_fail_running_collection_tasks_on_startup(repo):
    """Test failing running collection tasks."""
    id1 = await repo.create_collection_task(1, "Channel 1")
    id2 = await repo.create_collection_task(2, "Channel 2")
    await repo.update_collection_task(id1, CollectionTaskStatus.RUNNING)
    await repo.update_collection_task(id2, CollectionTaskStatus.PENDING)

    count = await repo.fail_running_collection_tasks_on_startup()
    assert count == 1

    task1 = await repo.get_collection_task(id1)
    assert task1.status == CollectionTaskStatus.FAILED
    assert task1.completed_at is not None

    task2 = await repo.get_collection_task(id2)
    assert task2.status == CollectionTaskStatus.PENDING


async def test_fail_running_collection_tasks_excludes_stats(repo):
    """Test that stats tasks are not affected."""
    stats_id = await repo.create_stats_task(StatsAllTaskPayload(channel_ids=[1]))
    await repo.update_collection_task(stats_id, CollectionTaskStatus.RUNNING)

    count = await repo.fail_running_collection_tasks_on_startup()
    assert count == 0

    task = await repo.get_collection_task(stats_id)
    assert task.status == CollectionTaskStatus.RUNNING


# requeue_running_generic_tasks_on_startup tests


async def test_requeue_running_stats_tasks_on_startup(repo):
    """Test requeueing running stats tasks."""
    stats_id = await repo.create_stats_task(StatsAllTaskPayload(channel_ids=[1]))
    await repo.update_collection_task(stats_id, CollectionTaskStatus.RUNNING)

    now = datetime.now(tz=timezone.utc)
    handled = [CollectionTaskType.STATS_ALL.value]
    count = await repo.requeue_running_generic_tasks_on_startup(now, handled)
    assert count == 1

    task = await repo.get_collection_task(stats_id)
    assert task.status == CollectionTaskStatus.PENDING
    assert task.started_at is None


async def test_requeue_running_stats_tasks_excludes_channel_collect(repo):
    """Test that channel collect tasks are not affected."""
    channel_id = await repo.create_collection_task(1, "Channel")
    await repo.update_collection_task(channel_id, CollectionTaskStatus.RUNNING)

    now = datetime.now(tz=timezone.utc)
    handled = [CollectionTaskType.STATS_ALL.value]
    count = await repo.requeue_running_generic_tasks_on_startup(now, handled)
    assert count == 0


async def test_requeue_running_stats_tasks_sets_run_after(repo):
    """Test that run_after is set if not already set."""
    stats_id = await repo.create_stats_task(StatsAllTaskPayload(channel_ids=[1]))
    await repo.update_collection_task(stats_id, CollectionTaskStatus.RUNNING)

    now = datetime.now(tz=timezone.utc)
    handled = [CollectionTaskType.STATS_ALL.value]
    await repo.requeue_running_generic_tasks_on_startup(now, handled)

    task = await repo.get_collection_task(stats_id)
    assert task.run_after is not None


# cancel_collection_task tests


async def test_cancel_collection_task_pending(repo):
    """Test cancelling a pending task."""
    task_id = await repo.create_collection_task(1, "Test")
    result = await repo.cancel_collection_task(task_id)
    assert result is True

    task = await repo.get_collection_task(task_id)
    assert task.status == CollectionTaskStatus.CANCELLED
    assert task.completed_at is not None


async def test_cancel_collection_task_running(repo):
    """Test cancelling a running task."""
    task_id = await repo.create_collection_task(1, "Test")
    await repo.update_collection_task(task_id, CollectionTaskStatus.RUNNING)
    result = await repo.cancel_collection_task(task_id)
    assert result is True

    task = await repo.get_collection_task(task_id)
    assert task.status == CollectionTaskStatus.CANCELLED


async def test_cancel_collection_task_completed(repo):
    """Test cancelling a completed task fails."""
    task_id = await repo.create_collection_task(1, "Test")
    await repo.update_collection_task(task_id, CollectionTaskStatus.COMPLETED)
    result = await repo.cancel_collection_task(task_id)
    assert result is False


async def test_cancel_collection_task_with_note(repo):
    """Test cancelling with a note."""
    task_id = await repo.create_collection_task(1, "Test")
    await repo.cancel_collection_task(task_id, note="User requested")

    task = await repo.get_collection_task(task_id)
    assert task.note == "User requested"


async def test_cancel_collection_task_not_found(repo):
    """Test cancelling non-existent task."""
    result = await repo.cancel_collection_task(999)
    assert result is False


# _to_task tests


async def test_to_task_deserializes_payload(repo):
    """Test that _to_task properly deserializes payload."""
    payload = StatsAllTaskPayload(channel_ids=[1, 2, 3], next_index=5)
    task_id = await repo.create_stats_task(payload)

    task = await repo.get_collection_task(task_id)
    assert isinstance(task.payload, StatsAllTaskPayload)
    assert task.payload.channel_ids == [1, 2, 3]
    assert task.payload.next_index == 5
