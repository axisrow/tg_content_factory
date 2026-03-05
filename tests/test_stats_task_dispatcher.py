from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.models import Channel
from src.services.stats_task_dispatcher import StatsTaskDispatcher


def _stats_payload(channel_ids: list[int], *, next_index: int = 0) -> dict:
    return {
        "task_kind": "stats_all",
        "channel_ids": channel_ids,
        "next_index": next_index,
        "batch_size": 20,
        "channels_ok": 0,
        "channels_err": 0,
    }


@pytest.mark.asyncio
async def test_dispatcher_requeues_running_on_start(db):
    tid = await db.create_collection_task(
        0,
        "Обновление статистики",
        payload=_stats_payload([]),
    )
    await db.update_collection_task(tid, "running")

    collector = SimpleNamespace(is_running=True)
    dispatcher = StatsTaskDispatcher(collector, db, poll_interval_sec=0.01)
    await dispatcher.start()
    await dispatcher.stop()

    task = await db.get_collection_task(tid)
    assert task is not None
    assert task.status == "pending"
    assert task.run_after is not None


@pytest.mark.asyncio
async def test_dispatcher_creates_batch_continuation(db):
    for idx in range(25):
        await db.add_channel(Channel(channel_id=-1000 - idx, title=f"Ch{idx}"))
    channels = await db.get_channels(active_only=True, include_filtered=False)
    channel_ids = [ch.channel_id for ch in channels]

    root_id = await db.create_collection_task(
        0,
        "Обновление статистики",
        payload=_stats_payload(channel_ids),
    )
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
    assert root.status == "completed"
    assert root.messages_collected == 20

    active = await db.get_active_stats_task()
    assert active is not None
    assert active.parent_task_id == root_id
    assert active.payload is not None
    assert active.payload["next_index"] == 20


@pytest.mark.asyncio
async def test_dispatcher_defers_when_all_clients_flooded(db):
    await db.add_channel(Channel(channel_id=-100123, title="Ch"))
    root_id = await db.create_collection_task(
        0,
        "Обновление статистики",
        payload=_stats_payload([-100123]),
    )
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
    assert root.status == "failed"
    assert root.error is not None
    assert "Deferred to task #" in root.error

    continuation = await db.get_active_stats_task()
    assert continuation is not None
    assert continuation.parent_task_id == root_id
    assert continuation.run_after is not None
    assert continuation.payload is not None
    assert continuation.payload["next_index"] == 0


@pytest.mark.asyncio
async def test_dispatcher_fails_without_connected_clients(db):
    await db.add_channel(Channel(channel_id=-100555, title="Ch"))
    root_id = await db.create_collection_task(
        0,
        "Обновление статистики",
        payload=_stats_payload([-100555]),
    )
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
    assert root.status == "failed"
    assert root.error == "No active connected Telegram accounts"

    active = await db.get_active_stats_task()
    assert active is None
