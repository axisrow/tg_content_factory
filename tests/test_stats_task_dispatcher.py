from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.models import (
    Channel,
    CollectionTaskStatus,
    CollectionTaskType,
    StatsAllTaskPayload,
)
from src.services.stats_task_dispatcher import StatsTaskDispatcher


def _stats_payload(channel_ids: list[int], *, next_index: int = 0) -> StatsAllTaskPayload:
    return StatsAllTaskPayload(channel_ids=channel_ids, next_index=next_index)


@pytest.mark.asyncio
async def test_dispatcher_requeues_running_on_start(db):
    tid = await db.create_stats_task(_stats_payload([]))
    await db.update_collection_task(tid, CollectionTaskStatus.RUNNING)

    collector = SimpleNamespace(is_running=True)
    dispatcher = StatsTaskDispatcher(collector, db, poll_interval_sec=0.01)
    await dispatcher.start()
    await dispatcher.stop()

    task = await db.get_collection_task(tid)
    assert task is not None
    assert task.status == CollectionTaskStatus.PENDING
    assert task.run_after is not None


@pytest.mark.asyncio
async def test_dispatcher_creates_batch_continuation(db):
    for idx in range(25):
        await db.add_channel(Channel(channel_id=-1000 - idx, title=f"Ch{idx}"))
    channels = await db.get_channels(active_only=True, include_filtered=False)
    channel_ids = [ch.channel_id for ch in channels]

    root_id = await db.create_stats_task(_stats_payload(channel_ids))
    root_task = await db.claim_next_due_stats_task(datetime.now(timezone.utc))
    assert root_task is not None

    async def _collect(_channel):
        return object()

    collector = SimpleNamespace(
        is_running=False,
        delay_between_channels_sec=0,
        collect_channel_stats=AsyncMock(side_effect=_collect),
        get_stats_availability=AsyncMock(),
    )
    dispatcher = StatsTaskDispatcher(collector, db)
    await dispatcher._run_stats_task(root_task)

    root = await db.get_collection_task(root_id)
    assert root is not None
    assert root.status == CollectionTaskStatus.COMPLETED
    assert root.messages_collected == 20

    active = await db.get_active_stats_task()
    assert active is not None
    assert active.parent_task_id == root_id
    assert active.task_type == CollectionTaskType.STATS_ALL
    assert isinstance(active.payload, StatsAllTaskPayload)
    assert active.payload.next_index == 20


@pytest.mark.asyncio
async def test_dispatcher_defers_when_all_clients_flooded(db):
    await db.add_channel(Channel(channel_id=-100123, title="Ch"))
    root_id = await db.create_stats_task(_stats_payload([-100123]))
    root_task = await db.claim_next_due_stats_task(datetime.now(timezone.utc))
    assert root_task is not None

    resume_at = datetime.now(timezone.utc) + timedelta(minutes=5)
    availability = SimpleNamespace(
        state="all_flooded",
        next_available_at_utc=resume_at,
    )
    collector = SimpleNamespace(
        is_running=False,
        delay_between_channels_sec=0,
        collect_channel_stats=AsyncMock(return_value=None),
        get_stats_availability=AsyncMock(return_value=availability),
    )
    dispatcher = StatsTaskDispatcher(collector, db)
    await dispatcher._run_stats_task(root_task)

    root = await db.get_collection_task(root_id)
    assert root is not None
    assert root.status == CollectionTaskStatus.FAILED
    assert root.error is not None
    assert "Deferred to task #" in root.error

    continuation = await db.get_active_stats_task()
    assert continuation is not None
    assert continuation.parent_task_id == root_id
    assert continuation.run_after is not None
    assert isinstance(continuation.payload, StatsAllTaskPayload)
    assert continuation.payload.next_index == 0


@pytest.mark.asyncio
async def test_dispatcher_fails_without_connected_clients(db):
    await db.add_channel(Channel(channel_id=-100555, title="Ch"))
    root_id = await db.create_stats_task(_stats_payload([-100555]))
    root_task = await db.claim_next_due_stats_task(datetime.now(timezone.utc))
    assert root_task is not None

    availability = SimpleNamespace(state="no_connected_active")
    collector = SimpleNamespace(
        is_running=False,
        delay_between_channels_sec=0,
        collect_channel_stats=AsyncMock(return_value=None),
        get_stats_availability=AsyncMock(return_value=availability),
    )
    dispatcher = StatsTaskDispatcher(collector, db)
    await dispatcher._run_stats_task(root_task)

    root = await db.get_collection_task(root_id)
    assert root is not None
    assert root.status == CollectionTaskStatus.FAILED
    assert root.error == "No active connected Telegram accounts"

    active = await db.get_active_stats_task()
    assert active is None


@pytest.mark.asyncio
async def test_dispatcher_marks_invalid_payload_failed(db):
    task_id = await db.create_stats_task(_stats_payload([-1001]))
    await db.execute(
        "UPDATE collection_tasks SET payload = ? WHERE id = ?",
        ('{"task_kind":"broken"}', task_id),
    )
    assert db.db is not None
    await db.db.commit()
    task = await db.claim_next_due_stats_task(datetime.now(timezone.utc))
    assert task is not None

    collector = SimpleNamespace(
        is_running=False,
        delay_between_channels_sec=0,
        collect_channel_stats=AsyncMock(),
        get_stats_availability=AsyncMock(),
    )
    dispatcher = StatsTaskDispatcher(collector, db)
    await dispatcher._run_stats_task(task)

    updated = await db.get_collection_task(task_id)
    assert updated is not None
    assert updated.status == CollectionTaskStatus.FAILED
    assert updated.error == "Unsupported stats task payload"


@pytest.mark.asyncio
async def test_dispatcher_completes_when_cursor_already_at_end(db):
    task_id = await db.create_stats_task(_stats_payload([-1001], next_index=1))
    task = await db.claim_next_due_stats_task(datetime.now(timezone.utc))
    assert task is not None

    collector = SimpleNamespace(
        is_running=False,
        delay_between_channels_sec=0,
        collect_channel_stats=AsyncMock(),
        get_stats_availability=AsyncMock(),
    )
    dispatcher = StatsTaskDispatcher(collector, db)
    await dispatcher._run_stats_task(task)

    updated = await db.get_collection_task(task_id)
    assert updated is not None
    assert updated.status == CollectionTaskStatus.COMPLETED
    assert updated.messages_collected == 0
    assert await db.get_active_stats_task() is None


@pytest.mark.asyncio
async def test_dispatcher_skips_missing_channel_and_completes_batch(db):
    task_id = await db.create_stats_task(_stats_payload([-100999]))
    task = await db.claim_next_due_stats_task(datetime.now(timezone.utc))
    assert task is not None

    collector = SimpleNamespace(
        is_running=False,
        delay_between_channels_sec=0,
        collect_channel_stats=AsyncMock(),
        get_stats_availability=AsyncMock(),
    )
    dispatcher = StatsTaskDispatcher(collector, db)
    await dispatcher._run_stats_task(task)

    updated = await db.get_collection_task(task_id)
    assert updated is not None
    assert updated.status == CollectionTaskStatus.COMPLETED
    assert updated.messages_collected == 0


@pytest.mark.asyncio
async def test_dispatcher_loop_marks_unexpected_failure(db):
    task_id = await db.create_stats_task(_stats_payload([]))
    await db.update_collection_task(task_id, CollectionTaskStatus.RUNNING)

    collector = SimpleNamespace(
        is_running=False,
        delay_between_channels_sec=0,
    )
    dispatcher = StatsTaskDispatcher(collector, db, poll_interval_sec=0.01)

    async def _broken(_task):
        raise RuntimeError("boom")

    dispatcher._run_stats_task = _broken  # type: ignore[method-assign]
    claimed = AsyncMock(side_effect=[await db.get_collection_task(task_id), None])
    db.claim_next_due_stats_task = claimed  # type: ignore[method-assign]

    run_task = asyncio.create_task(dispatcher._run_loop())
    await asyncio.sleep(0.03)
    await dispatcher.stop()
    if not run_task.done():
        run_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run_task

    updated = await db.get_collection_task(task_id)
    assert updated is not None
    assert updated.status == CollectionTaskStatus.FAILED
    assert updated.error == "Stats task failed with unexpected dispatcher error"
