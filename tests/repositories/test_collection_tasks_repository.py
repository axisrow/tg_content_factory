"""Tests for CollectionTasksRepository."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from tg_agent.database import Database
from tg_agent.database.repositories.collection_tasks import CollectionTasksRepository
from tg_agent.models import (
    CollectionTaskStatus,
    CollectionTaskType,
    ContentPublishTaskPayload,
    StatsAllTaskPayload,
)

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
    assert result == '{"key":"value"}'  # orjson compact output


def test_serialize_payload_stats_all():
    """Test serializing StatsAllTaskPayload."""
    payload = StatsAllTaskPayload(channel_ids=[1, 2, 3], next_index=5)
    result = CollectionTasksRepository._serialize_payload(payload)
    assert "channel_ids" in result
    assert "next_index" in result


# create_collection_task tests


async def test_generic_task_keys_persist_and_dedupe_across_connections(tmp_path):
    path = str(tmp_path / "tasks.db")
    first = Database(path)
    second = Database(path)
    try:
        await first.initialize()
        task_id = await first.repos.tasks.create_generic_task("dm_reply", idempotency_key="send-1")
        await first.close()
        await first.initialize()
        assert await first.repos.tasks.create_generic_task("dm_reply", idempotency_key="send-1") == task_id

        await second.initialize()
        ids = await asyncio.gather(
            first.repos.tasks.create_generic_task("dm_reply", idempotency_key="send-2"),
            second.repos.tasks.create_generic_task("dm_reply", idempotency_key="send-2"),
        )
        assert ids[0] == ids[1] != task_id
        assert await first.repos.tasks.count_collection_tasks() == 2
    finally:
        await second.close()
        await first.close()


async def test_create_collection_task_basic(collection_tasks_repo):
    """Test creating a basic collection task."""
    task_id = await collection_tasks_repo.create_collection_task(
        channel_id=12345,
        channel_title="Test Channel",
    )
    assert task_id > 0

    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task is not None
    assert task.channel_id == 12345
    assert task.channel_title == "Test Channel"
    assert task.task_type == CollectionTaskType.CHANNEL_COLLECT
    assert task.status == CollectionTaskStatus.PENDING


async def test_create_collection_task_with_all_fields(collection_tasks_repo):
    """Test creating a task with all optional fields."""
    run_after = datetime(2026, 3, 16, 12, 0, 0, tzinfo=timezone.utc)
    task_id = await collection_tasks_repo.create_collection_task(
        channel_id=12345,
        channel_title="Test",
        channel_username="testchannel",
        run_after=run_after,
        payload={"custom": "data"},
        parent_task_id=99,
    )

    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task.channel_username == "testchannel"
    assert task.run_after is not None
    assert task.payload == {"custom": "data"}
    assert task.parent_task_id == 99


async def test_create_collection_task_run_after_normalization(collection_tasks_repo):
    """Test that run_after is normalized to UTC."""
    # Create datetime with different timezone
    local_tz = timezone(timedelta(hours=3))
    run_after = datetime(2026, 3, 16, 12, 0, 0, tzinfo=local_tz)

    task_id = await collection_tasks_repo.create_collection_task(
        channel_id=1,
        channel_title="Test",
        run_after=run_after,
    )

    task = await collection_tasks_repo.get_collection_task(task_id)
    # Should be stored as UTC (9:00 instead of 12:00 +03:00)
    assert task.run_after is not None
    assert task.run_after.hour == 9


# create_stats_task tests


async def test_create_stats_task_basic(collection_tasks_repo):
    """Test creating a stats task."""
    payload = StatsAllTaskPayload(channel_ids=[1, 2, 3])
    task_id = await collection_tasks_repo.create_stats_task(payload)

    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task.task_type == CollectionTaskType.STATS_ALL
    assert task.channel_title == "Обновление статистики"
    assert isinstance(task.payload, StatsAllTaskPayload)
    assert task.payload.channel_ids == [1, 2, 3]


async def test_create_stats_task_with_run_after(collection_tasks_repo):
    """Test creating a stats task with run_after."""
    run_after = datetime(2026, 3, 16, 12, 0, 0, tzinfo=timezone.utc)
    payload = StatsAllTaskPayload(channel_ids=[1, 2, 3])
    task_id = await collection_tasks_repo.create_stats_task(payload, run_after=run_after)

    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task.run_after is not None


# update_collection_task_progress tests


async def test_update_collection_task_progress(collection_tasks_repo):
    """Test updating task progress."""
    task_id = await collection_tasks_repo.create_collection_task(1, "Test")
    await collection_tasks_repo.update_collection_task_progress(task_id, 100)

    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task.messages_collected == 100


async def test_update_collection_task_progress_stamps_last_progress_at(collection_tasks_repo):
    """Progress updates must record *when* progress last moved.

    The scheduler health page uses this timestamp to tell a genuinely stuck
    collection ("no progress for a long time") apart from a worker that simply
    isn't running — so every progress write must refresh last_progress_at.
    """
    before = datetime.now(tz=timezone.utc)
    task_id = await collection_tasks_repo.create_collection_task(1, "Test")
    await collection_tasks_repo.update_collection_task_progress(task_id, 100)

    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task.last_progress_at is not None
    assert task.last_progress_at >= before - timedelta(seconds=5)


async def test_update_collection_task_to_running_seeds_last_progress_at(collection_tasks_repo):
    """A freshly RUNNING task must have a progress timestamp seeded.

    Without it, a task that hasn't flushed its first batch yet would look
    "stuck since NULL" instead of "just started".
    """
    task_id = await collection_tasks_repo.create_collection_task(1, "Test")
    await collection_tasks_repo.update_collection_task(task_id, CollectionTaskStatus.RUNNING)

    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task.last_progress_at is not None


# update_collection_task tests


async def test_update_collection_task_to_running(collection_tasks_repo):
    """Test updating task to running status."""
    task_id = await collection_tasks_repo.create_collection_task(1, "Test")
    await collection_tasks_repo.update_collection_task(task_id, CollectionTaskStatus.RUNNING)

    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task.status == CollectionTaskStatus.RUNNING
    assert task.started_at is not None


async def test_update_collection_task_to_completed(collection_tasks_repo):
    """Test updating task to completed status."""
    task_id = await collection_tasks_repo.create_collection_task(1, "Test")
    await collection_tasks_repo.update_collection_task(task_id, CollectionTaskStatus.RUNNING)
    await collection_tasks_repo.update_collection_task(
        task_id,
        CollectionTaskStatus.COMPLETED,
        messages_collected=500,
        note="Done",
    )

    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task.status == CollectionTaskStatus.COMPLETED
    assert task.completed_at is not None
    assert task.messages_collected == 500
    assert task.note == "Done"


async def test_update_collection_task_does_not_overwrite_cancelled(collection_tasks_repo):
    """#633 bug #30: a terminal CANCELLED must not be clobbered by a late update."""
    task_id = await collection_tasks_repo.create_collection_task(1, "Test")
    await collection_tasks_repo.update_collection_task(task_id, CollectionTaskStatus.RUNNING)
    await collection_tasks_repo.cancel_collection_task(task_id)

    # A worker that lost the in-memory cancel flag tries to mark it COMPLETED.
    await collection_tasks_repo.update_collection_task(
        task_id,
        CollectionTaskStatus.COMPLETED,
        messages_collected=99,
    )

    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task.status == CollectionTaskStatus.CANCELLED
    assert task.messages_collected != 99


async def test_update_collection_task_to_failed(collection_tasks_repo):
    """Test updating task to failed status."""
    task_id = await collection_tasks_repo.create_collection_task(1, "Test")
    await collection_tasks_repo.update_collection_task(
        task_id,
        CollectionTaskStatus.FAILED,
        error="Connection timeout",
    )

    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task.status == CollectionTaskStatus.FAILED
    assert task.error == "Connection timeout"
    assert task.completed_at is not None


async def test_update_collection_task_with_string_status(collection_tasks_repo):
    """Test updating task with string status instead of enum."""
    task_id = await collection_tasks_repo.create_collection_task(1, "Test")
    await collection_tasks_repo.update_collection_task(task_id, "running")

    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task.status == CollectionTaskStatus.RUNNING


# get_collection_task tests


async def test_get_collection_task_not_found(collection_tasks_repo):
    """Test getting non-existent task."""
    task = await collection_tasks_repo.get_collection_task(999)
    assert task is None


# get_collection_tasks tests


async def test_get_collection_tasks_empty(collection_tasks_repo):
    """Test getting tasks when none exist."""
    tasks = await collection_tasks_repo.get_collection_tasks()
    assert tasks == []


async def test_get_collection_tasks_ordered(collection_tasks_repo):
    """Test that tasks are ordered by id DESC."""
    id1 = await collection_tasks_repo.create_collection_task(1, "First")
    id2 = await collection_tasks_repo.create_collection_task(2, "Second")
    id3 = await collection_tasks_repo.create_collection_task(3, "Third")

    tasks = await collection_tasks_repo.get_collection_tasks(limit=10)
    assert len(tasks) == 3
    # Newest first (id DESC)
    assert tasks[0].id == id3
    assert tasks[1].id == id2
    assert tasks[2].id == id1


async def test_get_collection_tasks_limit(collection_tasks_repo):
    """Test that limit is respected."""
    for i in range(10):
        await collection_tasks_repo.create_collection_task(i, f"Channel {i}")

    tasks = await collection_tasks_repo.get_collection_tasks(limit=5)
    assert len(tasks) == 5


# get_active_collection_tasks_for_channel tests


async def test_get_active_collection_tasks_for_channel_empty(collection_tasks_repo):
    """Test getting active tasks when none exist."""
    tasks = await collection_tasks_repo.get_active_collection_tasks_for_channel(12345)
    assert tasks == []


async def test_get_active_collection_tasks_for_channel(collection_tasks_repo):
    """Test getting active tasks for a channel."""
    channel_id = 12345
    task_id1 = await collection_tasks_repo.create_collection_task(channel_id, "Test")
    task_id2 = await collection_tasks_repo.create_collection_task(channel_id, "Test")
    await collection_tasks_repo.create_collection_task(99999, "Other")

    # Complete one task
    await collection_tasks_repo.update_collection_task(task_id1, CollectionTaskStatus.COMPLETED)

    tasks = await collection_tasks_repo.get_active_collection_tasks_for_channel(channel_id)
    assert len(tasks) == 1
    assert tasks[0].id == task_id2


async def test_get_active_collection_tasks_excludes_other_types(collection_tasks_repo):
    """Test that stats tasks are excluded."""
    channel_id = 12345
    await collection_tasks_repo.create_collection_task(channel_id, "Test")
    await collection_tasks_repo.create_stats_task(StatsAllTaskPayload(channel_ids=[channel_id]))

    tasks = await collection_tasks_repo.get_active_collection_tasks_for_channel(channel_id)
    assert len(tasks) == 1  # Only the channel_collect task


# get_channel_ids_with_active_tasks tests


async def test_get_channel_ids_with_active_tasks(collection_tasks_repo):
    """Test getting channel IDs with active tasks."""
    await collection_tasks_repo.create_collection_task(1, "Channel 1")
    await collection_tasks_repo.create_collection_task(2, "Channel 2")
    await collection_tasks_repo.create_collection_task(3, "Channel 3")

    # Complete task for channel 3
    task_id = (await collection_tasks_repo.get_collection_tasks())[0].id
    await collection_tasks_repo.update_collection_task(task_id, CollectionTaskStatus.COMPLETED)

    ids = await collection_tasks_repo.get_channel_ids_with_active_tasks()
    assert ids == {1, 2}


async def test_get_channel_ids_with_active_tasks_empty(collection_tasks_repo):
    """Test getting channel IDs when no active tasks."""
    ids = await collection_tasks_repo.get_channel_ids_with_active_tasks()
    assert ids == set()


# get_active_stats_task tests


async def test_get_active_stats_task_none(collection_tasks_repo):
    """Test getting active stats task when none exists."""
    task = await collection_tasks_repo.get_active_stats_task()
    assert task is None


async def test_get_active_stats_task(collection_tasks_repo):
    """Test getting active stats task."""
    payload = StatsAllTaskPayload(channel_ids=[1, 2])
    task_id = await collection_tasks_repo.create_stats_task(payload)

    task = await collection_tasks_repo.get_active_stats_task()
    assert task is not None
    assert task.id == task_id


async def test_get_active_stats_task_excludes_completed(collection_tasks_repo):
    """Test that completed stats tasks are not returned."""
    payload = StatsAllTaskPayload(channel_ids=[1, 2])
    task_id = await collection_tasks_repo.create_stats_task(payload)
    await collection_tasks_repo.update_collection_task(task_id, CollectionTaskStatus.COMPLETED)

    task = await collection_tasks_repo.get_active_stats_task()
    assert task is None


# claim_next_due_generic_task tests


async def test_claim_next_due_stats_task_none_available(collection_tasks_repo):
    """Test claiming when no stats tasks available."""
    now = datetime.now(tz=timezone.utc)
    task = await collection_tasks_repo.claim_next_due_generic_task(now, [CollectionTaskType.STATS_ALL.value])
    assert task is None


async def test_claim_next_due_stats_task_success(collection_tasks_repo):
    """Test successfully claiming a stats task."""
    payload = StatsAllTaskPayload(channel_ids=[1, 2])
    task_id = await collection_tasks_repo.create_stats_task(payload)

    now = datetime.now(tz=timezone.utc)
    claimed = await collection_tasks_repo.claim_next_due_generic_task(now, [CollectionTaskType.STATS_ALL.value])

    assert claimed is not None
    assert claimed.id == task_id
    assert claimed.status == CollectionTaskStatus.RUNNING
    assert claimed.started_at is not None


async def test_claim_next_due_stats_task_only_claimed_once(collection_tasks_repo):
    """Two concurrent claim calls on the same task must result in exactly one winner."""
    payload = StatsAllTaskPayload(channel_ids=[1, 2])
    task_id = await collection_tasks_repo.create_stats_task(payload)

    now = datetime.now(tz=timezone.utc)
    results = await asyncio.gather(
        collection_tasks_repo.claim_next_due_generic_task(now, [CollectionTaskType.STATS_ALL.value]),
        collection_tasks_repo.claim_next_due_generic_task(now, [CollectionTaskType.STATS_ALL.value]),
    )
    claimed = [r for r in results if r is not None]
    assert len(claimed) == 1
    assert claimed[0].id == task_id
    assert claimed[0].status == CollectionTaskStatus.RUNNING


async def test_claim_next_due_stats_task_respects_run_after(collection_tasks_repo):
    """Test that run_after is respected."""
    now = datetime.now(tz=timezone.utc)
    future = now + timedelta(hours=1)

    payload = StatsAllTaskPayload(channel_ids=[1, 2])
    await collection_tasks_repo.create_stats_task(payload, run_after=future)

    claimed = await collection_tasks_repo.claim_next_due_generic_task(now, [CollectionTaskType.STATS_ALL.value])
    assert claimed is None


async def test_claim_next_due_stats_task_run_after_passed(collection_tasks_repo):
    """Test claiming task when run_after has passed."""
    now = datetime.now(tz=timezone.utc)
    past = now - timedelta(hours=1)

    payload = StatsAllTaskPayload(channel_ids=[1, 2])
    task_id = await collection_tasks_repo.create_stats_task(payload, run_after=past)

    claimed = await collection_tasks_repo.claim_next_due_generic_task(now, [CollectionTaskType.STATS_ALL.value])
    assert claimed is not None
    assert claimed.id == task_id


async def test_claim_next_due_stats_task_skips_running(collection_tasks_repo):
    """Test that running tasks are not claimed."""
    payload = StatsAllTaskPayload(channel_ids=[1, 2])
    task_id = await collection_tasks_repo.create_stats_task(payload)
    await collection_tasks_repo.update_collection_task(task_id, CollectionTaskStatus.RUNNING)

    now = datetime.now(tz=timezone.utc)
    claimed = await collection_tasks_repo.claim_next_due_generic_task(now, [CollectionTaskType.STATS_ALL.value])
    assert claimed is None


# reschedule_stats_task tests


async def test_reschedule_stats_task(collection_tasks_repo):
    """Test rescheduling a stats task back to pending."""
    task_id = await collection_tasks_repo.create_stats_task(StatsAllTaskPayload(channel_ids=[1, 2, 3]))
    await collection_tasks_repo.update_collection_task(task_id, CollectionTaskStatus.RUNNING)

    run_after = datetime.now(tz=timezone.utc) + timedelta(minutes=5)
    payload = StatsAllTaskPayload(channel_ids=[1, 2, 3], next_index=1, channels_ok=1)
    await collection_tasks_repo.reschedule_stats_task(
        task_id,
        payload=payload,
        run_after=run_after,
        messages_collected=1,
    )

    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task.status == CollectionTaskStatus.PENDING
    assert task.payload.next_index == 1
    assert task.payload.channels_ok == 1
    assert task.messages_collected == 1


# get_pending_channel_tasks tests


async def test_get_pending_channel_tasks(collection_tasks_repo):
    """Test getting pending channel tasks."""
    id1 = await collection_tasks_repo.create_collection_task(1, "Channel 1")
    id2 = await collection_tasks_repo.create_collection_task(2, "Channel 2")
    await collection_tasks_repo.update_collection_task(id1, CollectionTaskStatus.RUNNING)
    await collection_tasks_repo.create_stats_task(StatsAllTaskPayload(channel_ids=[1]))

    tasks = await collection_tasks_repo.get_pending_channel_tasks()
    assert len(tasks) == 1
    assert tasks[0].id == id2


async def test_count_active_channel_tasks(collection_tasks_repo):
    """Счётчик прогресса: только CHANNEL_COLLECT в PENDING/RUNNING."""
    id1 = await collection_tasks_repo.create_collection_task(1, "Channel 1")
    await collection_tasks_repo.create_collection_task(2, "Channel 2")
    await collection_tasks_repo.create_stats_task(StatsAllTaskPayload(channel_ids=[1]))
    await collection_tasks_repo.update_collection_task(id1, CollectionTaskStatus.RUNNING)

    assert await collection_tasks_repo.count_active_channel_tasks() == 2

    await collection_tasks_repo.update_collection_task(
        id1, CollectionTaskStatus.COMPLETED, messages_collected=5
    )
    assert await collection_tasks_repo.count_active_channel_tasks() == 1


async def test_queue_snapshot_counts_statuses_and_window(collection_tasks_repo):
    """Срез очереди: разбивка активных + завершения за окно; чужие task_type не считаются."""
    done_old = await collection_tasks_repo.create_collection_task(1, "Old")
    done_new = await collection_tasks_repo.create_collection_task(2, "New")
    failed = await collection_tasks_repo.create_collection_task(3, "Failed")
    await collection_tasks_repo.create_collection_task(4, "Pending")
    running = await collection_tasks_repo.create_collection_task(5, "Running")
    # STATS_ALL создаётся PENDING — без фильтра по task_type pending был бы 2.
    await collection_tasks_repo.create_stats_task(StatsAllTaskPayload(channel_ids=[1]))

    await collection_tasks_repo.update_collection_task(done_old, CollectionTaskStatus.COMPLETED)
    await collection_tasks_repo.update_collection_task(done_new, CollectionTaskStatus.COMPLETED)
    await collection_tasks_repo.update_collection_task(
        failed, CollectionTaskStatus.FAILED, error="boom"
    )
    await collection_tasks_repo.update_collection_task(running, CollectionTaskStatus.RUNNING)

    # Один completed сдвинуть за окно (10 минут назад).
    old_ts = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
    await collection_tasks_repo._database.execute_write(
        "UPDATE collection_tasks SET completed_at = ? WHERE id = ?", (old_ts, done_old)
    )

    snap = await collection_tasks_repo.queue_snapshot()
    assert snap.pending == 1
    assert snap.running == 1
    assert snap.remaining == 2
    assert snap.completed_window == 1
    assert snap.failed_window == 1
    # first_completion_at = самое раннее завершение в окне (для знаменателя rate).
    assert snap.first_completion_at is not None

    wide = await collection_tasks_repo.queue_snapshot(window_min=15)
    assert wide.completed_window == 2


async def test_queue_snapshot_empty(collection_tasks_repo):
    """Пустая таблица — нулевой срез, не NULL."""
    snap = await collection_tasks_repo.queue_snapshot()
    assert snap == (0, 0, 0, 0, None)
    assert snap.remaining == 0


async def test_reset_collection_task_to_pending(collection_tasks_repo):
    task_id = await collection_tasks_repo.create_collection_task(1, "Channel 1")
    await collection_tasks_repo.update_collection_task(task_id, CollectionTaskStatus.RUNNING)
    await collection_tasks_repo.update_collection_task(
        task_id,
        CollectionTaskStatus.FAILED,
        error="interrupted",
        note="old note",
    )

    await collection_tasks_repo.reset_collection_task_to_pending(task_id, note="shutdown")

    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task.status == CollectionTaskStatus.PENDING
    assert task.started_at is None
    assert task.completed_at is None
    assert task.last_progress_at is None
    assert task.error is None
    assert task.note == "shutdown"


async def test_reset_collection_task_to_pending_does_not_overwrite_cancelled(collection_tasks_repo):
    task_id = await collection_tasks_repo.create_collection_task(1, "Channel 1")
    await collection_tasks_repo.update_collection_task(task_id, CollectionTaskStatus.RUNNING)
    await collection_tasks_repo.cancel_collection_task(task_id, note="user cancel")

    await collection_tasks_repo.reset_collection_task_to_pending(task_id, note="shutdown")

    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task.status == CollectionTaskStatus.CANCELLED
    assert task.note == "user cancel"


async def test_reschedule_collection_task_does_not_overwrite_cancelled(collection_tasks_repo):
    task_id = await collection_tasks_repo.create_collection_task(1, "Channel 1")
    await collection_tasks_repo.update_collection_task(task_id, CollectionTaskStatus.RUNNING)
    await collection_tasks_repo.cancel_collection_task(task_id, note="user cancel")

    await collection_tasks_repo.reschedule_collection_task(
        task_id,
        run_after=datetime(2030, 1, 1, tzinfo=timezone.utc),
        note="flood wait",
        messages_collected=10,
    )

    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task.status == CollectionTaskStatus.CANCELLED
    assert task.note == "user cancel"
    assert task.messages_collected == 0


async def test_reschedule_collection_task_clears_last_progress_at(collection_tasks_repo):
    task_id = await collection_tasks_repo.create_collection_task(1, "Channel 1")
    await collection_tasks_repo.update_collection_task(task_id, CollectionTaskStatus.RUNNING)
    started = await collection_tasks_repo.get_collection_task(task_id)
    assert started is not None
    assert started.last_progress_at is not None

    await collection_tasks_repo.reschedule_collection_task(
        task_id,
        run_after=datetime(2030, 1, 1, tzinfo=timezone.utc),
        note="flood wait",
        messages_collected=10,
    )

    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task is not None
    assert task.status == CollectionTaskStatus.PENDING
    assert task.started_at is None
    assert task.completed_at is None
    assert task.last_progress_at is None
    assert task.messages_collected == 10


# fail_running_collection_tasks_on_startup tests


async def test_fail_running_collection_tasks_on_startup(collection_tasks_repo):
    """Test failing running collection tasks."""
    id1 = await collection_tasks_repo.create_collection_task(1, "Channel 1")
    id2 = await collection_tasks_repo.create_collection_task(2, "Channel 2")
    await collection_tasks_repo.update_collection_task(id1, CollectionTaskStatus.RUNNING)
    await collection_tasks_repo.update_collection_task(id2, CollectionTaskStatus.PENDING)

    count = await collection_tasks_repo.fail_running_collection_tasks_on_startup()
    assert count == 1

    task1 = await collection_tasks_repo.get_collection_task(id1)
    assert task1.status == CollectionTaskStatus.FAILED
    assert task1.completed_at is not None

    task2 = await collection_tasks_repo.get_collection_task(id2)
    assert task2.status == CollectionTaskStatus.PENDING


async def test_fail_running_collection_tasks_excludes_stats(collection_tasks_repo):
    """Test that stats tasks are not affected."""
    stats_id = await collection_tasks_repo.create_stats_task(StatsAllTaskPayload(channel_ids=[1]))
    await collection_tasks_repo.update_collection_task(stats_id, CollectionTaskStatus.RUNNING)

    count = await collection_tasks_repo.fail_running_collection_tasks_on_startup()
    assert count == 0

    task = await collection_tasks_repo.get_collection_task(stats_id)
    assert task.status == CollectionTaskStatus.RUNNING


# requeue_running_generic_tasks_on_startup tests


async def test_requeue_running_stats_tasks_on_startup(collection_tasks_repo):
    """Test requeueing running stats tasks."""
    stats_id = await collection_tasks_repo.create_stats_task(StatsAllTaskPayload(channel_ids=[1]))
    await collection_tasks_repo.update_collection_task(stats_id, CollectionTaskStatus.RUNNING)

    now = datetime.now(tz=timezone.utc)
    handled = [CollectionTaskType.STATS_ALL.value]
    count = await collection_tasks_repo.requeue_running_generic_tasks_on_startup(now, handled)
    assert count == 1

    task = await collection_tasks_repo.get_collection_task(stats_id)
    assert task.status == CollectionTaskStatus.PENDING
    assert task.started_at is None


async def test_requeue_running_stats_tasks_excludes_channel_collect(collection_tasks_repo):
    """Test that channel collect tasks are not affected."""
    channel_id = await collection_tasks_repo.create_collection_task(1, "Channel")
    await collection_tasks_repo.update_collection_task(channel_id, CollectionTaskStatus.RUNNING)

    now = datetime.now(tz=timezone.utc)
    handled = [CollectionTaskType.STATS_ALL.value]
    count = await collection_tasks_repo.requeue_running_generic_tasks_on_startup(now, handled)
    assert count == 0


async def test_requeue_running_stats_tasks_sets_run_after(collection_tasks_repo):
    """Test that run_after is set if not already set."""
    stats_id = await collection_tasks_repo.create_stats_task(StatsAllTaskPayload(channel_ids=[1]))
    await collection_tasks_repo.update_collection_task(stats_id, CollectionTaskStatus.RUNNING)

    now = datetime.now(tz=timezone.utc)
    handled = [CollectionTaskType.STATS_ALL.value]
    await collection_tasks_repo.requeue_running_generic_tasks_on_startup(now, handled)

    task = await collection_tasks_repo.get_collection_task(stats_id)
    assert task.run_after is not None


async def test_fail_running_side_effecting_tasks_on_startup(collection_tasks_repo):
    """Running side-effecting generic tasks fail on startup instead of retrying."""
    task_id = await collection_tasks_repo.create_generic_task(
        CollectionTaskType.CONTENT_PUBLISH,
        payload=ContentPublishTaskPayload(pipeline_id=None),
    )
    await collection_tasks_repo.update_collection_task(task_id, CollectionTaskStatus.RUNNING)

    count = await collection_tasks_repo.fail_running_generic_tasks_on_startup(
        datetime.now(tz=timezone.utc),
        [CollectionTaskType.CONTENT_PUBLISH.value],
        error="not retried",
        note="manual inspection",
    )
    assert count == 1

    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task.status == CollectionTaskStatus.FAILED
    assert task.completed_at is not None
    assert task.error == "not retried"
    assert task.note == "manual inspection"


# cancel_collection_task tests


async def test_cancel_collection_task_pending(collection_tasks_repo):
    """Test cancelling a pending task."""
    task_id = await collection_tasks_repo.create_collection_task(1, "Test")
    result = await collection_tasks_repo.cancel_collection_task(task_id)
    assert result is True

    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task.status == CollectionTaskStatus.CANCELLED
    assert task.completed_at is not None


async def test_cancel_collection_task_running(collection_tasks_repo):
    """Test cancelling a running task."""
    task_id = await collection_tasks_repo.create_collection_task(1, "Test")
    await collection_tasks_repo.update_collection_task(task_id, CollectionTaskStatus.RUNNING)
    result = await collection_tasks_repo.cancel_collection_task(task_id)
    assert result is True

    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task.status == CollectionTaskStatus.CANCELLED


async def test_cancel_collection_task_completed(collection_tasks_repo):
    """Test cancelling a completed task fails."""
    task_id = await collection_tasks_repo.create_collection_task(1, "Test")
    await collection_tasks_repo.update_collection_task(task_id, CollectionTaskStatus.COMPLETED)
    result = await collection_tasks_repo.cancel_collection_task(task_id)
    assert result is False


async def test_cancel_collection_task_with_note(collection_tasks_repo):
    """Test cancelling with a note."""
    task_id = await collection_tasks_repo.create_collection_task(1, "Test")
    await collection_tasks_repo.cancel_collection_task(task_id, note="User requested")

    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task.note == "User requested"


async def test_cancel_collection_task_not_found(collection_tasks_repo):
    """Test cancelling non-existent task."""
    result = await collection_tasks_repo.cancel_collection_task(999)
    assert result is False


async def test_delete_pending_channel_tasks_only_removes_pending_channel_collect(collection_tasks_repo):
    pending_id = await collection_tasks_repo.create_collection_task(1, "Pending")

    running_id = await collection_tasks_repo.create_collection_task(2, "Running")
    await collection_tasks_repo.update_collection_task(running_id, CollectionTaskStatus.RUNNING)

    completed_id = await collection_tasks_repo.create_collection_task(3, "Completed")
    await collection_tasks_repo.update_collection_task(completed_id, CollectionTaskStatus.COMPLETED)

    stats_id = await collection_tasks_repo.create_stats_task(StatsAllTaskPayload(channel_ids=[1, 2]))

    deleted = await collection_tasks_repo.delete_pending_channel_tasks()

    assert deleted == 1
    assert await collection_tasks_repo.get_collection_task(pending_id) is None
    assert (await collection_tasks_repo.get_collection_task(running_id)).status == CollectionTaskStatus.RUNNING
    assert (await collection_tasks_repo.get_collection_task(completed_id)).status == CollectionTaskStatus.COMPLETED
    assert (await collection_tasks_repo.get_collection_task(stats_id)).task_type == CollectionTaskType.STATS_ALL


# get_collection_tasks_paginated ordering tests (#1304)


async def test_get_collection_tasks_paginated_active_running_first(collection_tasks_repo):
    """#1304: for status=active, running tasks must sort before pending ones.

    Within each status group, order is ascending id (execution order) — not id DESC —
    so the currently-running/next-up work is at the top of page 1, not buried on the
    last page of a large batch.
    """
    pending1_id = await collection_tasks_repo.create_collection_task(1, "Pending1")
    pending2_id = await collection_tasks_repo.create_collection_task(2, "Pending2")
    running_id = await collection_tasks_repo.create_collection_task(3, "Running")
    await collection_tasks_repo.update_collection_task(running_id, CollectionTaskStatus.RUNNING)

    tasks, total = await collection_tasks_repo.get_collection_tasks_paginated(
        limit=20, offset=0, status_filter="active"
    )

    assert total == 3
    assert [t.id for t in tasks] == [running_id, pending1_id, pending2_id]


async def test_get_collection_tasks_paginated_completed_newest_first(collection_tasks_repo):
    """#1304: status=completed keeps the existing "newest first" (id DESC) ordering."""
    completed1_id = await collection_tasks_repo.create_collection_task(1, "Completed1")
    await collection_tasks_repo.update_collection_task(completed1_id, CollectionTaskStatus.COMPLETED)
    completed2_id = await collection_tasks_repo.create_collection_task(2, "Completed2")
    await collection_tasks_repo.update_collection_task(completed2_id, CollectionTaskStatus.COMPLETED)

    tasks, total = await collection_tasks_repo.get_collection_tasks_paginated(
        limit=20, offset=0, status_filter="completed"
    )

    assert total == 2
    assert [t.id for t in tasks] == [completed2_id, completed1_id]


# _to_task tests


async def test_to_task_deserializes_payload(collection_tasks_repo):
    """Test that _to_task properly deserializes payload."""
    payload = StatsAllTaskPayload(channel_ids=[1, 2, 3], next_index=5)
    task_id = await collection_tasks_repo.create_stats_task(payload)

    task = await collection_tasks_repo.get_collection_task(task_id)
    assert isinstance(task.payload, StatsAllTaskPayload)
    assert task.payload.channel_ids == [1, 2, 3]
    assert task.payload.next_index == 5


# claim_next_due_generic_task lease/TTL tests (#1468)


async def _create_interop_task(repo) -> int:
    return await repo.create_generic_task("dm_reply", payload={"text": "hi"})


async def test_lease_claim_records_owner(collection_tasks_repo):
    """A lease claim stamps claimed_by/claimed_at on the task."""
    task_id = await _create_interop_task(collection_tasks_repo)
    now = datetime.now(tz=timezone.utc)

    claimed = await collection_tasks_repo.claim_next_due_generic_task(
        now, ["dm_reply"], claimed_by="worker-a", lease_seconds=300
    )

    assert claimed is not None and claimed.id == task_id
    assert claimed.claimed_by == "worker-a"
    assert claimed.claimed_at is not None


async def test_internal_claim_leaves_lease_columns_null(collection_tasks_repo):
    """The dispatcher's no-lease claim keeps its old shape: no owner recorded."""
    task_id = await _create_interop_task(collection_tasks_repo)
    now = datetime.now(tz=timezone.utc)

    claimed = await collection_tasks_repo.claim_next_due_generic_task(now, ["dm_reply"])

    assert claimed is not None and claimed.id == task_id
    assert claimed.claimed_by is None
    assert claimed.claimed_at is None


async def test_live_lease_cannot_be_stolen(collection_tasks_repo):
    """Within the TTL a second worker's claim must not take the RUNNING task."""
    task_id = await _create_interop_task(collection_tasks_repo)
    t0 = datetime.now(tz=timezone.utc)

    await collection_tasks_repo.claim_next_due_generic_task(
        t0, ["dm_reply"], claimed_by="worker-a", lease_seconds=300
    )
    stolen = await collection_tasks_repo.claim_next_due_generic_task(
        t0 + timedelta(minutes=1), ["dm_reply"], claimed_by="worker-b", lease_seconds=300
    )

    assert stolen is None
    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task.claimed_by == "worker-a"


async def test_expired_lease_is_reclaimed(collection_tasks_repo):
    """After TTL expiry a claim re-claims the stranded task and transfers ownership."""
    task_id = await _create_interop_task(collection_tasks_repo)
    t0 = datetime.now(tz=timezone.utc)

    first = await collection_tasks_repo.claim_next_due_generic_task(
        t0, ["dm_reply"], claimed_by="worker-a", lease_seconds=300
    )
    assert first is not None
    reclaimed = await collection_tasks_repo.claim_next_due_generic_task(
        t0 + timedelta(seconds=301), ["dm_reply"], claimed_by="worker-b", lease_seconds=300
    )

    assert reclaimed is not None and reclaimed.id == task_id
    assert reclaimed.claimed_by == "worker-b"
    assert reclaimed.claimed_at is not None
    assert reclaimed.claimed_at > first.claimed_at


async def test_expired_lease_reclaim_keeps_id_ordering(collection_tasks_repo):
    """Ordering stays COALESCE(run_after,''), id: the older stranded task is
    re-claimed before newer pending work, both within one recovery flow."""
    first_id = await _create_interop_task(collection_tasks_repo)
    second_id = await _create_interop_task(collection_tasks_repo)
    t0 = datetime.now(tz=timezone.utc)

    stranded = await collection_tasks_repo.claim_next_due_generic_task(
        t0, ["dm_reply"], claimed_by="worker-a", lease_seconds=60
    )
    assert stranded is not None and stranded.id == first_id
    reclaim = await collection_tasks_repo.claim_next_due_generic_task(
        t0 + timedelta(seconds=61), ["dm_reply"], claimed_by="worker-b", lease_seconds=60
    )
    assert reclaim is not None and reclaim.id == first_id
    assert reclaim.claimed_by == "worker-b"
    # Only the stranded task was handed out; the pending one is next in line.
    still_pending = await collection_tasks_repo.get_collection_task(second_id)
    assert still_pending is not None and still_pending.status == CollectionTaskStatus.PENDING


async def test_expired_lease_invisible_to_no_lease_claim(collection_tasks_repo):
    """Without lease args the claim still sees PENDING only — dispatcher untouched."""
    task_id = await _create_interop_task(collection_tasks_repo)
    t0 = datetime.now(tz=timezone.utc)

    await collection_tasks_repo.claim_next_due_generic_task(
        t0, ["dm_reply"], claimed_by="worker-a", lease_seconds=60
    )
    plain = await collection_tasks_repo.claim_next_due_generic_task(
        t0 + timedelta(hours=1), ["dm_reply"]
    )

    assert plain is None
    task = await collection_tasks_repo.get_collection_task(task_id)
    assert task.status == CollectionTaskStatus.RUNNING
    assert task.claimed_by == "worker-a"


# Legacy interop rows after the /api/tasks removal (#1431)


async def test_legacy_interop_rows_stay_inert_after_contour_removal(collection_tasks_repo):
    """Rows of the removed interop types (dm_reply, chat_answer, fetch_dialogs,
    fetch_history) may still sit in deployed databases. Without the REST claim API
    nothing executes them: no internal pull or claim may pick them up, and reading
    them must not fail even though their enum values are gone."""
    from tg_agent.services.unified_dispatcher import HANDLED_TYPES

    legacy_types = ("dm_reply", "chat_answer", "fetch_dialogs", "fetch_history")
    assert not set(legacy_types) & {t.value for t in CollectionTaskType}
    ids = [
        await collection_tasks_repo.create_generic_task(task_type, payload={"text": "hi"})
        for task_type in legacy_types
    ]

    assert await collection_tasks_repo.get_pending_channel_tasks() == []
    claimed = await collection_tasks_repo.claim_next_due_generic_task(
        datetime.now(tz=timezone.utc), HANDLED_TYPES
    )
    assert claimed is None
    for task_id in ids:
        task = await collection_tasks_repo.get_collection_task(task_id)
        assert task is not None and task.status == CollectionTaskStatus.PENDING
