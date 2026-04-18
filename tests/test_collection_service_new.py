"""Tests for CollectionService."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import Channel
from src.services.collection_service import CollectionService


def _make_channel(pk=1, channel_id=100, title="Test", is_filtered=False):
    return Channel(id=pk, channel_id=channel_id, title=title, is_filtered=is_filtered)


@pytest.mark.asyncio
async def test_enqueue_channel_by_pk_not_found():
    channels = MagicMock()
    channels.get_by_pk = AsyncMock(return_value=None)
    collector = MagicMock()

    svc = CollectionService(channels, collector)
    result = await svc.enqueue_channel_by_pk(pk=999)
    assert result == "not_found"


@pytest.mark.asyncio
async def test_enqueue_channel_by_pk_filtered():
    ch = _make_channel(is_filtered=True)
    channels = MagicMock()
    channels.get_by_pk = AsyncMock(return_value=ch)
    collector = MagicMock()

    svc = CollectionService(channels, collector)
    result = await svc.enqueue_channel_by_pk(pk=1)
    assert result == "filtered"


@pytest.mark.asyncio
async def test_enqueue_channel_by_pk_force_filtered():
    ch = _make_channel(is_filtered=True)
    channels = MagicMock()
    channels.get_by_pk = AsyncMock(return_value=ch)
    channels.create_collection_task_if_not_active = AsyncMock(return_value=42)
    collector = MagicMock()

    svc = CollectionService(channels, collector)
    result = await svc.enqueue_channel_by_pk(pk=1, force=True)
    assert result == "queued"


@pytest.mark.asyncio
async def test_enqueue_channel_by_pk_success():
    ch = _make_channel()
    channels = MagicMock()
    channels.get_by_pk = AsyncMock(return_value=ch)
    channels.create_collection_task_if_not_active = AsyncMock(return_value=42)
    collector = MagicMock()

    svc = CollectionService(channels, collector)
    result = await svc.enqueue_channel_by_pk(pk=1)
    assert result == "queued"


@pytest.mark.asyncio
async def test_enqueue_channel_by_pk_already_active():
    ch = _make_channel()
    channels = MagicMock()
    channels.get_by_pk = AsyncMock(return_value=ch)
    channels.create_collection_task_if_not_active = AsyncMock(return_value=None)
    collector = MagicMock()

    svc = CollectionService(channels, collector)
    result = await svc.enqueue_channel_by_pk(pk=1)
    assert result == "already_active"


@pytest.mark.asyncio
async def test_enqueue_all_channels_empty():
    channels = MagicMock()
    channels.list_channels = AsyncMock(return_value=[])
    collector = MagicMock()

    svc = CollectionService(channels, collector)
    result = await svc.enqueue_all_channels()
    assert result.total_candidates == 0
    assert result.queued_count == 0


@pytest.mark.asyncio
async def test_enqueue_all_channels_success():
    ch1 = _make_channel(pk=1, channel_id=100)
    ch2 = _make_channel(pk=2, channel_id=200)
    channels = MagicMock()
    channels.list_channels = AsyncMock(return_value=[ch1, ch2])
    channels.create_collection_task_if_not_active = AsyncMock(return_value=1)
    collector = MagicMock()

    svc = CollectionService(channels, collector)
    result = await svc.enqueue_all_channels()
    assert result.total_candidates == 2
    assert result.queued_count == 2
    assert result.skipped_existing_count == 0


@pytest.mark.asyncio
async def test_enqueue_all_channels_mixed():
    ch1 = _make_channel(pk=1, channel_id=100)
    ch2 = _make_channel(pk=2, channel_id=200)
    channels = MagicMock()
    channels.list_channels = AsyncMock(return_value=[ch1, ch2])
    channels.create_collection_task_if_not_active = AsyncMock(side_effect=[1, None])
    collector = MagicMock()

    svc = CollectionService(channels, collector)
    result = await svc.enqueue_all_channels()
    assert result.total_candidates == 2
    assert result.queued_count == 1
    assert result.skipped_existing_count == 1


@pytest.mark.asyncio
async def test_enqueue_with_queue():
    ch = _make_channel()
    queue = MagicMock()
    queue.enqueue = AsyncMock(return_value=7)

    channels = MagicMock()
    channels.get_by_pk = AsyncMock(return_value=ch)
    collector = MagicMock()

    svc = CollectionService(channels, collector, collection_queue=queue)
    result = await svc.enqueue_channel_by_pk(pk=1)
    assert result == "queued"
    queue.enqueue.assert_called_once()


@pytest.mark.asyncio
async def test_enqueue_with_queue_already_active():
    ch = _make_channel()
    queue = MagicMock()
    queue.enqueue = AsyncMock(return_value=None)

    channels = MagicMock()
    channels.get_by_pk = AsyncMock(return_value=ch)
    collector = MagicMock()

    svc = CollectionService(channels, collector, collection_queue=queue)
    result = await svc.enqueue_channel_by_pk(pk=1)
    assert result == "already_active"


@pytest.mark.asyncio
async def test_collect_channel_stats():
    ch = _make_channel()
    collector = MagicMock()
    collector.collect_channel_stats = AsyncMock(return_value=None)

    channels = MagicMock()
    svc = CollectionService(channels, collector)
    await svc.collect_channel_stats(ch)
    collector.collect_channel_stats.assert_called_once_with(ch)


@pytest.mark.asyncio
async def test_collect_single_channel_full():
    ch = _make_channel()
    collector = MagicMock()
    collector.collect_single_channel = AsyncMock(return_value=42)

    channels = MagicMock()
    svc = CollectionService(channels, collector)
    count = await svc.collect_single_channel_full(ch)
    assert count == 42
    collector.collect_single_channel.assert_called_once_with(ch, full=True)


# ── cancel_task / clear_pending_collect_tasks fallback (fix for #457) ─────


@pytest.mark.asyncio
async def test_cancel_task_with_live_queue_delegates():
    """With a live queue the service must delegate so RUNNING tasks get interrupted."""
    channels = MagicMock()
    channels.cancel_collection_task = AsyncMock(return_value=True)
    collector = MagicMock()
    queue = MagicMock()
    queue.cancel_task = AsyncMock(return_value=True)

    svc = CollectionService(channels, collector, collection_queue=queue)
    result = await svc.cancel_task(77, note="manual stop")
    assert result is True
    queue.cancel_task.assert_awaited_once_with(77, note="manual stop")
    channels.cancel_collection_task.assert_not_called()


@pytest.mark.asyncio
async def test_cancel_task_without_queue_falls_back_to_db():
    """In web-mode (queue=None) the DB path must keep the route from 500'ing (#457)."""
    channels = MagicMock()
    channels.cancel_collection_task = AsyncMock(return_value=True)
    collector = MagicMock()

    svc = CollectionService(channels, collector, collection_queue=None)
    result = await svc.cancel_task(77)
    assert result is True
    channels.cancel_collection_task.assert_awaited_once_with(77, note=None)


@pytest.mark.asyncio
async def test_clear_pending_collect_tasks_with_live_queue_delegates():
    """With a queue the service must delegate to drain memory + cancel requeue timers."""
    channels = MagicMock()
    collector = MagicMock()
    queue = MagicMock()
    queue.clear_pending_tasks = AsyncMock(return_value=12)

    svc = CollectionService(channels, collector, collection_queue=queue)
    result = await svc.clear_pending_collect_tasks()
    assert result == 12
    queue.clear_pending_tasks.assert_awaited_once()


@pytest.mark.asyncio
async def test_clear_pending_collect_tasks_without_queue_falls_back_to_db():
    """Without a queue the service just deletes PENDING rows — memory lives in the worker."""
    channels = MagicMock()
    channels.delete_pending_channel_tasks = AsyncMock(return_value=5)
    collector = MagicMock()

    svc = CollectionService(channels, collector, collection_queue=None)
    result = await svc.clear_pending_collect_tasks()
    assert result == 5
    channels.delete_pending_channel_tasks.assert_awaited_once()
