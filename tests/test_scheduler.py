from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.config import SchedulerConfig
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
async def test_run_collection_enqueuer_error():
    """Enqueue error returns error stats without crashing."""
    enqueuer = MagicMock()
    enqueuer.enqueue_all_channels = AsyncMock(side_effect=RuntimeError("boom"))

    manager = SchedulerManager(SchedulerConfig(), task_enqueuer=enqueuer)
    stats = await manager._run_collection()

    assert stats["errors"] == 1
    assert stats["enqueued"] == 0
