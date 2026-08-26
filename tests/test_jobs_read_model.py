"""Unit + integration tests for the unified jobs read-model (#963)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.models import (
    CollectionTask,
    CollectionTaskStatus,
    CollectionTaskType,
    JobRuntimeState,
    JobSource,
    PhotoAutoUploadJob,
    PhotoBatch,
    PhotoBatchItem,
    PhotoBatchStatus,
    RuntimeSnapshot,
    TelegramCommand,
    TelegramCommandStatus,
)
from src.services.jobs_read_model import JobsReadModel

NOW = datetime(2026, 6, 22, 12, 0, 0, tzinfo=timezone.utc)
FUTURE = NOW + timedelta(hours=1)


# --- pure normalization (no DB) -------------------------------------------


def _ct(status, *, run_after=None, task_id=1):
    return CollectionTask(
        id=task_id, task_type=CollectionTaskType.CHANNEL_COLLECT, status=status, run_after=run_after
    )


def test_collection_running():
    v = JobsReadModel._from_collection_task(_ct(CollectionTaskStatus.RUNNING), NOW, False, set())
    assert v.runtime_state == JobRuntimeState.RUNNING


def test_collection_active_id_overrides_to_running():
    v = JobsReadModel._from_collection_task(_ct(CollectionTaskStatus.PENDING), NOW, False, {1})
    assert v.runtime_state == JobRuntimeState.RUNNING


def test_collection_pending_paused_is_pause_gate():
    v = JobsReadModel._from_collection_task(_ct(CollectionTaskStatus.PENDING), NOW, True, set())
    assert v.runtime_state == JobRuntimeState.PAUSE_GATE


def test_collection_pending_future_run_after_is_scheduled():
    v = JobsReadModel._from_collection_task(
        _ct(CollectionTaskStatus.PENDING, run_after=FUTURE), NOW, False, set()
    )
    assert v.runtime_state == JobRuntimeState.SCHEDULED


def test_collection_pending_plain():
    v = JobsReadModel._from_collection_task(_ct(CollectionTaskStatus.PENDING), NOW, False, set())
    assert v.runtime_state == JobRuntimeState.PENDING
    assert v.id == "collection_task:1"


@pytest.mark.parametrize(
    "status, expected",
    [
        (CollectionTaskStatus.COMPLETED, JobRuntimeState.COMPLETED),
        (CollectionTaskStatus.FAILED, JobRuntimeState.FAILED),
        (CollectionTaskStatus.CANCELLED, JobRuntimeState.CANCELLED),
    ],
)
def test_collection_terminal(status, expected):
    v = JobsReadModel._from_collection_task(_ct(status), NOW, False, set())
    assert v.runtime_state == expected


def test_telegram_flood_wait():
    cmd = TelegramCommand(
        id=5,
        command_type="get_profile",
        status=TelegramCommandStatus.PENDING,
        result_payload={"state": "waiting_flood_wait"},
    )
    v = JobsReadModel._from_telegram_command(cmd, NOW)
    assert v.runtime_state == JobRuntimeState.FLOOD_WAIT
    assert v.source == JobSource.TELEGRAM_COMMAND


def test_telegram_scheduled_then_pending():
    sched = TelegramCommand(
        id=6, command_type="x", status=TelegramCommandStatus.PENDING, run_after=FUTURE
    )
    assert JobsReadModel._from_telegram_command(sched, NOW).runtime_state == JobRuntimeState.SCHEDULED
    plain = TelegramCommand(id=7, command_type="x", status=TelegramCommandStatus.PENDING)
    assert JobsReadModel._from_telegram_command(plain, NOW).runtime_state == JobRuntimeState.PENDING


def test_photo_auto_inactive_vs_scheduled():
    base = dict(phone="+1", target_dialog_id=10, folder_path="/p", interval_minutes=30)
    on = JobsReadModel._from_photo_auto(PhotoAutoUploadJob(id=1, is_active=True, **base))
    off = JobsReadModel._from_photo_auto(PhotoAutoUploadJob(id=2, is_active=False, **base))
    assert on.runtime_state == JobRuntimeState.SCHEDULED
    assert off.runtime_state == JobRuntimeState.INACTIVE


def test_photo_item_mapping():
    item = PhotoBatchItem(
        id=3, phone="+1", target_dialog_id=10, file_paths=["a.jpg", "b.jpg"],
        status=PhotoBatchStatus.SCHEDULED,
    )
    v = JobsReadModel._from_photo_item(item)
    assert v.runtime_state == JobRuntimeState.SCHEDULED
    assert "2 photo(s)" in v.summary


# --- integration over :memory: DB -----------------------------------------


async def test_list_jobs_aggregates_all_sources(db):
    await db.repos.tasks.create_collection_task(100, "Chan")
    await db.repos.telegram_commands.create_command(
        TelegramCommand(command_type="get_profile", payload={"phone": "+1"})
    )
    await db.repos.photo_loader.create_item(
        PhotoBatchItem(phone="+1", target_dialog_id=10, file_paths=["a.jpg"])
    )
    await db.repos.photo_loader.create_auto_job(
        PhotoAutoUploadJob(phone="+1", target_dialog_id=10, folder_path="/p")
    )
    await db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="scheduler_jobs",
            payload={"jobs": [{"job_id": "collect_all", "interval_minutes": 60}]},
        )
    )

    jobs = await JobsReadModel(db).list_jobs(now=NOW)
    by_source = {j.source for j in jobs}
    assert by_source == set(JobSource)
    sched = next(j for j in jobs if j.source == JobSource.SCHEDULER_JOB)
    assert sched.job_type == "collect_all"
    assert sched.summary == "every 60m"


async def test_list_jobs_paginated_sorts_mixed_sources_before_slicing(db):
    older_task_id = await db.repos.tasks.create_collection_task(101, "Older task")
    newer_task_id = await db.repos.tasks.create_collection_task(102, "Newest task")
    command_id = await db.repos.telegram_commands.create_command(
        TelegramCommand(command_type="middle", payload={"phone": "+1"})
    )
    await db.execute_write(
        "UPDATE collection_tasks SET created_at = ? WHERE id = ?",
        ("2026-06-22T10:00:00+00:00", older_task_id),
    )
    await db.execute_write(
        "UPDATE collection_tasks SET created_at = ? WHERE id = ?",
        ("2026-06-22T12:00:00+00:00", newer_task_id),
    )
    await db.execute_write(
        "UPDATE telegram_commands SET created_at = ? WHERE id = ?",
        ("2026-06-22T11:00:00+00:00", command_id),
    )

    model = JobsReadModel(db)
    first, first_total = await model.list_jobs_paginated(page=1, limit=2, now=NOW)
    second, second_total = await model.list_jobs_paginated(page=2, limit=2, now=NOW)

    assert [job.summary for job in first] == ["Newest task", "middle"]
    assert [job.summary for job in second] == ["Older task"]
    assert first_total == second_total == 3


async def test_list_jobs_paginated_handles_page_boundaries(db):
    for index in range(21):
        await db.repos.tasks.create_collection_task(200 + index, f"Task {index}")
    model = JobsReadModel(db)

    first, total = await model.list_jobs_paginated(page=0, limit=10, now=NOW)
    last, last_total = await model.list_jobs_paginated(page=3, limit=10, now=NOW)
    beyond, beyond_total = await model.list_jobs_paginated(page=4, limit=10, now=NOW)

    assert len(first) == 10
    assert len(last) == 1
    assert beyond == []
    assert total == last_total == beyond_total == 21


async def test_active_collection_feed_keeps_running_first_execution_order(db):
    pending_first = await db.repos.tasks.create_collection_task(401, "Pending first")
    running = await db.repos.tasks.create_collection_task(402, "Running")
    await db.repos.tasks.update_collection_task(running, CollectionTaskStatus.RUNNING)
    await db.repos.tasks.create_collection_task(403, "Pending second")
    await db.repos.tasks.create_collection_task(404, "Pending third")

    active_states = [
        JobRuntimeState.RUNNING,
        JobRuntimeState.PENDING,
        JobRuntimeState.SCHEDULED,
        JobRuntimeState.PAUSE_GATE,
        JobRuntimeState.FLOOD_WAIT,
    ]
    model = JobsReadModel(db)

    first_page, total = await model.list_jobs_paginated(
        sources=[JobSource.COLLECTION_TASK],
        statuses=active_states,
        page=1,
        limit=2,
        now=NOW,
    )
    legacy_list = await model.list_jobs(
        sources=[JobSource.COLLECTION_TASK],
        statuses=active_states,
        limit=2,
        now=NOW,
    )

    assert total == 4
    assert [job.raw_id for job in first_page] == [running, pending_first]
    assert [job.raw_id for job in legacy_list] == [running, pending_first]


async def test_list_jobs_paginated_keeps_source_fetches_bounded(db, monkeypatch):
    tasks = [
        CollectionTask(
            id=index + 1,
            task_type=CollectionTaskType.CHANNEL_COLLECT,
            status=CollectionTaskStatus.PENDING,
            channel_title=f"Task {index}",
            created_at=NOW - timedelta(seconds=index),
        )
        for index in range(501)
    ]
    fetches: list[tuple[int, tuple[str | None, int] | None]] = []

    async def fake_list_tasks(*, limit: int, cursor=None):
        fetches.append((limit, cursor))
        offset = 0 if cursor is None else 500
        rows = tasks[offset : offset + limit]
        next_cursor = (rows[-1].created_at.isoformat(), rows[-1].id) if len(rows) == limit else None
        return rows, next_cursor

    async def fake_count_tasks(status_filter=None):
        assert status_filter is None
        return len(tasks)

    monkeypatch.setattr(db.repos.tasks, "get_collection_tasks_for_jobs_page", fake_list_tasks)
    monkeypatch.setattr(db.repos.tasks, "count_collection_tasks", fake_count_tasks)
    model = JobsReadModel(db)

    page, total = await model.list_jobs_paginated(
        sources=[JobSource.COLLECTION_TASK], page=1, limit=1, now=NOW
    )
    assert len(page) == 1
    assert total == 501
    assert fetches == [(500, None)]

    fetches.clear()
    filtered, filtered_total = await model.list_jobs_paginated(
        sources=[JobSource.COLLECTION_TASK],
        statuses=[JobRuntimeState.PENDING],
        page=2,
        limit=10,
        now=NOW,
    )
    assert len(filtered) == 10
    assert filtered_total == 501
    assert fetches[0] == (500, None)
    assert fetches[1][0] == 500
    assert fetches[1][1] is not None


async def test_jobs_source_keyset_pages_are_ordered_and_complete(db):
    task_ids = [
        await db.repos.tasks.create_collection_task(300 + index, f"Task {index}")
        for index in range(3)
    ]
    command_ids = [
        await db.repos.telegram_commands.create_command(
            TelegramCommand(command_type=f"command_{index}", payload={})
        )
        for index in range(3)
    ]
    item_ids = [
        await db.repos.photo_loader.create_item(
            PhotoBatchItem(phone="+1", target_dialog_id=10, file_paths=[f"{index}.jpg"])
        )
        for index in range(3)
    ]
    for table, ids in (
        ("collection_tasks", task_ids),
        ("telegram_commands", command_ids),
        ("photo_batch_items", item_ids),
    ):
        await db.execute_write(
            f"UPDATE {table} SET created_at = ? WHERE id = ?",
            ("2026-06-22 10:00:00", ids[0]),
        )
        await db.execute_write(
            f"UPDATE {table} SET created_at = ? WHERE id = ?",
            ("2026-06-22 12:00:00", ids[1]),
        )
        await db.execute_write(f"UPDATE {table} SET created_at = NULL WHERE id = ?", (ids[2],))

    async def assert_two_pages(fetch_page, expected_ids):
        first, cursor = await fetch_page(limit=2)
        second, final_cursor = await fetch_page(limit=2, cursor=cursor)
        assert [row.id for row in [*first, *second]] == expected_ids
        assert final_cursor is None

    await assert_two_pages(
        db.repos.tasks.get_collection_tasks_for_jobs_page,
        [task_ids[1], task_ids[0], task_ids[2]],
    )
    await assert_two_pages(
        db.repos.telegram_commands.list_commands_for_jobs_page,
        [command_ids[1], command_ids[0], command_ids[2]],
    )
    await assert_two_pages(
        db.repos.photo_loader.list_items_for_jobs_page,
        [item_ids[1], item_ids[0], item_ids[2]],
    )

    cur = await db.execute(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND name LIKE 'idx_%_created_id'"
    )
    assert {row["name"] for row in await cur.fetchall()} >= {
        "idx_collection_tasks_created_id",
        "idx_telegram_commands_created_id",
        "idx_photo_batch_items_created_id",
    }


async def test_photo_batch_read_model_counts_progress(db):
    batch_id = await db.repos.photo_loader.create_batch(
        PhotoBatch(
            phone="+1",
            target_dialog_id=10,
            target_title="Chan",
            status=PhotoBatchStatus.RUNNING,
        )
    )
    await db.repos.photo_loader.create_item(
        PhotoBatchItem(
            batch_id=batch_id,
            phone="+1",
            target_dialog_id=10,
            file_paths=["a.jpg"],
            status=PhotoBatchStatus.COMPLETED,
        )
    )
    await db.repos.photo_loader.create_item(
        PhotoBatchItem(
            batch_id=batch_id,
            phone="+1",
            target_dialog_id=10,
            file_paths=["b.jpg"],
            status=PhotoBatchStatus.RUNNING,
        )
    )
    await db.repos.photo_loader.create_item(
        PhotoBatchItem(
            batch_id=batch_id,
            phone="+1",
            target_dialog_id=10,
            file_paths=["c.jpg"],
            status=PhotoBatchStatus.PENDING,
        )
    )

    batch = await JobsReadModel(db).get_photo_batch(batch_id)

    assert batch is not None
    assert batch.completed_items == 1
    assert batch.total_items == 3


async def test_photo_batch_progress_counts_all_terminal_items(db):
    """Progress counts items that reached ANY terminal state (completed/failed/
    cancelled), so the bar reaches N/N even when some items failed — counting only
    COMPLETED used to freeze the bar at e.g. 2/3 forever (#1152 follow-up)."""
    batch_id = await db.repos.photo_loader.create_batch(
        PhotoBatch(
            phone="+1",
            target_dialog_id=10,
            target_title="Chan",
            status=PhotoBatchStatus.FAILED,
        )
    )
    for fname, status in (
        ("a.jpg", PhotoBatchStatus.COMPLETED),
        ("b.jpg", PhotoBatchStatus.FAILED),
        ("c.jpg", PhotoBatchStatus.CANCELLED),
    ):
        await db.repos.photo_loader.create_item(
            PhotoBatchItem(
                batch_id=batch_id,
                phone="+1",
                target_dialog_id=10,
                file_paths=[fname],
                status=status,
            )
        )

    batch = await JobsReadModel(db).get_photo_batch(batch_id)

    assert batch is not None
    # all three are terminal → bar reaches N/N, not stuck at 1/3
    assert batch.completed_items == 3
    assert batch.total_items == 3


async def test_list_jobs_filters_by_source_and_state(db):
    await db.repos.tasks.create_collection_task(100, "Chan")  # pending collection task

    only_ct = await JobsReadModel(db).list_jobs(sources=[JobSource.COLLECTION_TASK], now=NOW)
    assert only_ct and all(j.source == JobSource.COLLECTION_TASK for j in only_ct)

    pending = await JobsReadModel(db).list_jobs(statuses=[JobRuntimeState.PENDING], now=NOW)
    assert all(j.runtime_state == JobRuntimeState.PENDING for j in pending)
    completed = await JobsReadModel(db).list_jobs(statuses=[JobRuntimeState.COMPLETED], now=NOW)
    assert completed == []


async def test_pause_gate_reflected_from_queue_snapshot(db):
    await db.repos.tasks.create_collection_task(100, "Chan")
    await db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(snapshot_type="collection_queue_status", payload={"paused": True, "active_task_ids": []})
    )
    jobs = await JobsReadModel(db).list_jobs(sources=[JobSource.COLLECTION_TASK], now=NOW)
    assert jobs[0].runtime_state == JobRuntimeState.PAUSE_GATE


def test_active_ids_do_not_override_terminal_status():
    # A COMPLETED task whose id lingers in the live active_ids snapshot must stay
    # COMPLETED, not flip to RUNNING (#963 review).
    v = JobsReadModel._from_collection_task(
        _ct(CollectionTaskStatus.COMPLETED, task_id=7), NOW, False, {7}
    )
    assert v.runtime_state == JobRuntimeState.COMPLETED
    # A PENDING task in active_ids still upgrades to RUNNING (legit in-flight case).
    p = JobsReadModel._from_collection_task(
        _ct(CollectionTaskStatus.PENDING, task_id=8), NOW, False, {8}
    )
    assert p.runtime_state == JobRuntimeState.RUNNING


async def test_disabled_scheduler_job_is_inactive(db):
    await db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="scheduler_jobs",
            payload={"jobs": [
                {"job_id": "collect_all", "interval_minutes": 60},
                {"job_id": "photo_due", "interval_minutes": 30},
            ]},
        )
    )
    await db.repos.settings.set_setting("scheduler_job_disabled:collect_all", "1")

    jobs = await JobsReadModel(db).list_jobs(sources=[JobSource.SCHEDULER_JOB], now=NOW)
    by_type = {j.job_type: j.runtime_state for j in jobs}
    assert by_type["collect_all"] == JobRuntimeState.INACTIVE
    assert by_type["photo_due"] == JobRuntimeState.SCHEDULED
