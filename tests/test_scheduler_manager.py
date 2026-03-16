from unittest.mock import AsyncMock, MagicMock

import pytest

from src.config import SchedulerConfig
from src.models import SearchQuery
from src.scheduler.manager import SchedulerManager


@pytest.fixture
def mock_bundle():
    bundle = MagicMock()
    bundle.get_setting = AsyncMock(return_value=None)
    return bundle


@pytest.fixture
def mock_sq_bundle():
    bundle = MagicMock()
    bundle.get_all = AsyncMock(return_value=[])
    bundle.get_by_id = AsyncMock()
    bundle.get_fts_daily_stats_for_query = AsyncMock(return_value=[])
    bundle.record_stat = AsyncMock()
    return bundle


@pytest.fixture
def mock_task_enqueuer():
    enqueuer = MagicMock()
    result = MagicMock()
    result.queued_count = 3
    result.skipped_existing_count = 1
    result.total_candidates = 4
    enqueuer.enqueue_all_channels = AsyncMock(return_value=result)
    enqueuer.enqueue_sq_stats = AsyncMock()
    enqueuer.enqueue_photo_due = AsyncMock()
    enqueuer.enqueue_photo_auto = AsyncMock()
    return enqueuer


@pytest.fixture
def scheduler_config():
    return SchedulerConfig(collect_interval_minutes=60)


@pytest.mark.asyncio
async def test_scheduler_start_stop(scheduler_config, mock_bundle):
    mgr = SchedulerManager(scheduler_config, scheduler_bundle=mock_bundle)
    await mgr.start()
    assert mgr.is_running
    assert mgr.interval_minutes == 60

    await mgr.stop()
    assert not mgr.is_running


@pytest.mark.asyncio
async def test_scheduler_start_already_running(scheduler_config, mock_bundle):
    mgr = SchedulerManager(scheduler_config, scheduler_bundle=mock_bundle)
    await mgr.start()
    original_scheduler = mgr._scheduler
    await mgr.start()  # Should log warning and return early
    assert mgr.is_running
    assert mgr._scheduler is original_scheduler
    await mgr.stop()


@pytest.mark.asyncio
async def test_scheduler_start_cleans_stale_scheduler(scheduler_config, mock_bundle):
    """If a previous scheduler exists but is not running, start() should clean it up."""
    mgr = SchedulerManager(scheduler_config, scheduler_bundle=mock_bundle)
    await mgr.start()
    await mgr.stop()
    from apscheduler.schedulers.asyncio import AsyncIOScheduler

    stale = AsyncIOScheduler()
    mgr._scheduler = stale
    assert not stale.running

    await mgr.start()
    assert mgr.is_running
    assert mgr._scheduler is not stale
    await mgr.stop()


@pytest.mark.asyncio
async def test_scheduler_update_interval(scheduler_config, mock_bundle):
    mgr = SchedulerManager(scheduler_config, scheduler_bundle=mock_bundle)
    await mgr.start()
    mgr.update_interval(10)
    assert mgr._current_interval_minutes == 10
    await mgr.stop()


@pytest.mark.asyncio
async def test_scheduler_trigger_now_with_enqueuer(
    scheduler_config,
    mock_bundle,
    mock_task_enqueuer,
):
    mgr = SchedulerManager(
        scheduler_config,
        scheduler_bundle=mock_bundle,
        task_enqueuer=mock_task_enqueuer,
    )
    res = await mgr.trigger_now()
    assert res["enqueued"] == 3
    mock_task_enqueuer.enqueue_all_channels.assert_called_once()


@pytest.mark.asyncio
async def test_scheduler_trigger_now_without_enqueuer(scheduler_config, mock_bundle):
    mgr = SchedulerManager(scheduler_config, scheduler_bundle=mock_bundle)
    res = await mgr.trigger_now()
    assert res["enqueued"] == 0


@pytest.mark.asyncio
async def test_scheduler_trigger_background(
    scheduler_config,
    mock_bundle,
    mock_task_enqueuer,
):
    mgr = SchedulerManager(
        scheduler_config,
        scheduler_bundle=mock_bundle,
        task_enqueuer=mock_task_enqueuer,
    )
    await mgr.trigger_background()
    assert mgr._bg_task is not None
    await mgr._bg_task


@pytest.mark.asyncio
async def test_run_collection_failure(
    scheduler_config,
    mock_bundle,
    mock_task_enqueuer,
):
    mock_task_enqueuer.enqueue_all_channels.side_effect = Exception("boom")
    mgr = SchedulerManager(
        scheduler_config,
        scheduler_bundle=mock_bundle,
        task_enqueuer=mock_task_enqueuer,
    )
    res = await mgr._run_collection()
    assert res["errors"] == 1


@pytest.mark.asyncio
async def test_sync_search_query_jobs(
    scheduler_config,
    mock_bundle,
    mock_sq_bundle,
):
    sq1 = SearchQuery(
        id=1,
        name="q1",
        query="q1",
        track_stats=True,
        interval_minutes=10,
    )
    mock_sq_bundle.get_all.return_value = [sq1]

    mgr = SchedulerManager(
        scheduler_config,
        scheduler_bundle=mock_bundle,
        search_query_bundle=mock_sq_bundle,
    )
    await mgr.start()
    await mgr.sync_search_query_jobs()

    jobs = mgr._scheduler.get_jobs()
    assert any(j.id == "sq_1" for j in jobs)

    # Remove job
    mock_sq_bundle.get_all.return_value = []
    await mgr.sync_search_query_jobs()
    jobs = mgr._scheduler.get_jobs()
    assert not any(j.id == "sq_1" for j in jobs)

    await mgr.stop()


@pytest.mark.asyncio
async def test_run_search_query_with_enqueuer(
    scheduler_config,
    mock_bundle,
    mock_task_enqueuer,
):
    mgr = SchedulerManager(
        scheduler_config,
        scheduler_bundle=mock_bundle,
        task_enqueuer=mock_task_enqueuer,
    )
    await mgr._run_search_query(1)
    mock_task_enqueuer.enqueue_sq_stats.assert_called_once_with(1)


@pytest.mark.asyncio
async def test_run_search_query_no_enqueuer(scheduler_config, mock_bundle):
    mgr = SchedulerManager(scheduler_config, scheduler_bundle=mock_bundle)
    await mgr._run_search_query(1)  # Should return without error


@pytest.mark.asyncio
async def test_saved_interval_from_bundle(mock_bundle):
    mock_bundle.get_setting = AsyncMock(return_value="5")
    config = SchedulerConfig(collect_interval_minutes=60)
    mgr = SchedulerManager(config, scheduler_bundle=mock_bundle)
    await mgr.start()
    assert mgr.interval_minutes == 5
    await mgr.stop()
