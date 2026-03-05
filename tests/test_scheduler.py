from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.config import SchedulerConfig
from src.scheduler.manager import SchedulerManager


@pytest.mark.asyncio
async def test_run_collection_logs_exception():
    """Unhandled error in collector must not crash scheduler."""
    collector = MagicMock()
    collector.is_running = False
    collector.collect_all_channels = AsyncMock(side_effect=RuntimeError("boom"))

    manager = SchedulerManager(collector, SchedulerConfig())
    stats = await manager._run_collection()

    assert stats["errors"] == 1
    assert stats["channels"] == 0
    assert stats["messages"] == 0
    assert manager.last_run is None
    assert manager.last_stats is None


@pytest.mark.asyncio
async def test_run_collection_stores_stats_on_success():
    """Successful collection updates last_run and last_stats."""
    expected = {"channels": 2, "messages": 10, "errors": 0}
    collector = MagicMock()
    collector.collect_all_channels = AsyncMock(return_value=expected)

    manager = SchedulerManager(collector, SchedulerConfig())
    stats = await manager._run_collection()

    assert stats == expected
    assert manager.last_run is not None
    assert manager.last_stats == expected


@pytest.mark.asyncio
async def test_error_then_success_updates_stats():
    """After an error, the next successful run must update last_run and last_stats."""
    success_dict = {"channels": 3, "messages": 15, "errors": 0}
    collector = MagicMock()
    collector.collect_all_channels = AsyncMock(
        side_effect=[RuntimeError("boom"), success_dict]
    )

    manager = SchedulerManager(collector, SchedulerConfig())

    # First run — error
    await manager._run_collection()
    assert manager.last_run is None
    assert manager.last_stats is None

    # Second run — success
    stats = await manager._run_collection()
    assert manager.last_run is not None
    assert manager.last_stats == success_dict
    assert stats == success_dict
