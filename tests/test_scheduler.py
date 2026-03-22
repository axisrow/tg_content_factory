from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.config import SchedulerConfig
from src.database.bundles import SchedulerBundle
from src.scheduler.manager import SchedulerManager


@pytest.mark.asyncio
async def test_run_collection_without_enqueuer():
    """Without task_enqueuer, _run_collection returns zero stats."""
    manager = SchedulerManager(SchedulerConfig())
    stats = await manager._run_collection()

    assert stats["enqueued"] == 0
    assert stats["errors"] == 0


@pytest.mark.asyncio
async def test_run_collection_with_enqueuer():
    """Successful collection returns enqueue stats."""
    result = MagicMock()
    result.queued_count = 2
    result.skipped_existing_count = 1
    result.total_candidates = 3

    enqueuer = MagicMock()
    enqueuer.enqueue_all_channels = AsyncMock(return_value=result)

    manager = SchedulerManager(SchedulerConfig(), task_enqueuer=enqueuer)
    stats = await manager._run_collection()

    assert stats["enqueued"] == 2
    assert stats["skipped"] == 1
    assert stats["total"] == 3
    assert stats["errors"] == 0


@pytest.mark.asyncio
async def test_load_settings_updates_interval():
    """load_settings() reads interval from DB and updates _current_interval_minutes."""
    bundle_mock = MagicMock(spec=SchedulerBundle)
    bundle_mock.get_setting = AsyncMock(return_value="15")

    manager = SchedulerManager(SchedulerConfig(), scheduler_bundle=bundle_mock)
    assert manager.interval_minutes == 60  # config default

    await manager.load_settings()

    assert manager.interval_minutes == 15


@pytest.mark.asyncio
async def test_load_settings_no_bundle():
    """load_settings() is a no-op when no scheduler_bundle is set."""
    manager = SchedulerManager(SchedulerConfig())
    await manager.load_settings()  # should not raise
    assert manager.interval_minutes == 60


@pytest.mark.asyncio
async def test_load_settings_missing_value_keeps_default():
    """load_settings() keeps config default when DB has no saved interval."""
    bundle_mock = MagicMock(spec=SchedulerBundle)
    bundle_mock.get_setting = AsyncMock(return_value=None)

    manager = SchedulerManager(SchedulerConfig(), scheduler_bundle=bundle_mock)
    await manager.load_settings()

    assert manager.interval_minutes == 60


@pytest.mark.asyncio
async def test_run_collection_enqueuer_error():
    """Enqueue error returns error stats without crashing."""
    enqueuer = MagicMock()
    enqueuer.enqueue_all_channels = AsyncMock(side_effect=RuntimeError("boom"))

    manager = SchedulerManager(SchedulerConfig(), task_enqueuer=enqueuer)
    stats = await manager._run_collection()

    assert stats["errors"] == 1
    assert stats["enqueued"] == 0
