from datetime import date
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.config import SchedulerConfig
from src.models import SearchQuery, SearchResult
from src.scheduler.manager import SchedulerManager


@pytest.fixture
def mock_collector():
    collector = MagicMock()
    collector.collect_all_channels = AsyncMock()
    collector.is_running = False
    return collector

@pytest.fixture
def mock_bundle():
    bundle = MagicMock()
    bundle.get_setting = AsyncMock(return_value=None)
    bundle.list_notification_queries = AsyncMock(return_value=[])
    bundle.get_notification_queries = AsyncMock(return_value=[])
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
def mock_search_engine():
    engine = MagicMock()
    engine.check_search_quota = AsyncMock()
    engine.search_telegram = AsyncMock()
    return engine

@pytest.fixture
def scheduler_config():
    return SchedulerConfig(
        collect_interval_minutes=60, search_interval_minutes=30,
    )

@pytest.mark.asyncio
async def test_scheduler_start_stop(
    mock_collector, scheduler_config, mock_bundle,
):
    mgr = SchedulerManager(mock_collector, scheduler_config, mock_bundle)
    await mgr.start()
    assert mgr.is_running
    assert mgr.interval_minutes == 60

    await mgr.stop()
    assert not mgr.is_running

@pytest.mark.asyncio
async def test_scheduler_start_already_running(
    mock_collector, scheduler_config, mock_bundle,
):
    mgr = SchedulerManager(mock_collector, scheduler_config, mock_bundle)
    await mgr.start()
    await mgr.start()  # Should log warning and return
    assert mgr.is_running
    await mgr.stop()

@pytest.mark.asyncio
async def test_scheduler_update_interval(
    mock_collector, scheduler_config, mock_bundle,
):
    mgr = SchedulerManager(mock_collector, scheduler_config, mock_bundle)
    await mgr.start()
    mgr.update_interval(10)
    assert mgr._current_interval_minutes == 10
    await mgr.stop()

@pytest.mark.asyncio
async def test_scheduler_trigger_now(
    mock_collector, scheduler_config, mock_bundle,
):
    mock_collector.collect_all_channels.return_value = {
        "channels": 1, "messages": 5,
    }
    mgr = SchedulerManager(mock_collector, scheduler_config, mock_bundle)
    res = await mgr.trigger_now()
    assert res["channels"] == 1
    assert mgr.last_run is not None

@pytest.mark.asyncio
async def test_scheduler_trigger_background(
    mock_collector, scheduler_config, mock_bundle,
):
    mock_collector.collect_all_channels.return_value = {"channels": 1}
    mgr = SchedulerManager(mock_collector, scheduler_config, mock_bundle)
    await mgr.trigger_background()
    assert mgr._bg_task is not None
    await mgr._bg_task
    assert mgr.last_stats["channels"] == 1

@pytest.mark.asyncio
async def test_run_collection_failure(
    mock_collector, scheduler_config, mock_bundle,
):
    mock_collector.collect_all_channels.side_effect = Exception("boom")
    mgr = SchedulerManager(mock_collector, scheduler_config, mock_bundle)
    res = await mgr._run_collection()
    assert res["errors"] == 1

@pytest.mark.asyncio
async def test_run_keyword_search_no_engine(
    mock_collector, scheduler_config, mock_bundle,
):
    mgr = SchedulerManager(mock_collector, scheduler_config, mock_bundle)
    res = await mgr._run_keyword_search()
    assert res["queries"] == 0

@pytest.mark.asyncio
async def test_run_keyword_search_success(
    mock_collector, scheduler_config, mock_bundle, mock_search_engine,
):
    sq = MagicMock()
    sq.query = "test"
    mock_bundle.get_notification_queries = AsyncMock(return_value=[sq])
    mock_search_engine.check_search_quota = AsyncMock(
        return_value={"remains": 10},
    )
    mock_search_engine.search_telegram.return_value = SearchResult(
        total=5, messages=[], query="test",
    )

    mgr = SchedulerManager(
        mock_collector, scheduler_config, mock_bundle,
        search_engine=mock_search_engine,
    )
    res = await mgr._run_keyword_search()
    assert res["queries"] == 1
    assert res["results"] == 5
    assert mgr.last_search_run is not None

@pytest.mark.asyncio
async def test_run_keyword_search_quota_exhausted(
    mock_collector, scheduler_config, mock_bundle, mock_search_engine,
):
    sq = MagicMock()
    sq.query = "test"
    mock_bundle.get_notification_queries = AsyncMock(return_value=[sq])
    mock_search_engine.check_search_quota.return_value = {
        "remains": 0, "query_is_free": False,
    }

    mgr = SchedulerManager(
        mock_collector, scheduler_config, mock_bundle,
        search_engine=mock_search_engine,
    )
    res = await mgr._run_keyword_search()
    assert res["queries"] == 0  # Stopped before search

@pytest.mark.asyncio
async def test_sync_search_query_jobs(
    mock_collector, scheduler_config, mock_bundle, mock_sq_bundle,
):
    sq1 = SearchQuery(
        id=1, name="q1", query="q1",
        track_stats=True, interval_minutes=10,
    )
    mock_sq_bundle.get_all.return_value = [sq1]

    mgr = SchedulerManager(
        mock_collector, scheduler_config, mock_bundle,
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
async def test_run_search_query_logic(
    mock_collector, scheduler_config, mock_bundle, mock_sq_bundle,
):
    sq = SearchQuery(id=1, name="q1", query="q1", track_stats=True)
    mock_sq_bundle.get_by_id.return_value = sq

    class Stat:
        def __init__(self, day, count):
            self.day = day
            self.count = count

    mock_sq_bundle.get_fts_daily_stats_for_query.return_value = [
        Stat(date.today().isoformat(), 10),
    ]

    mgr = SchedulerManager(
        mock_collector, scheduler_config, mock_bundle,
        search_query_bundle=mock_sq_bundle,
    )
    await mgr._run_search_query(1)
    mock_sq_bundle.record_stat.assert_called_once_with(1, 10)

@pytest.mark.asyncio
async def test_run_search_query_not_found(
    mock_collector, scheduler_config, mock_bundle, mock_sq_bundle,
):
    mock_sq_bundle.get_by_id.return_value = None
    mgr = SchedulerManager(
        mock_collector, scheduler_config, mock_bundle,
        search_query_bundle=mock_sq_bundle,
    )
    await mgr._run_search_query(1)
    mock_sq_bundle.record_stat.assert_not_called()

@pytest.mark.asyncio
async def test_legacy_bundle_fallback():
    store = MagicMock()
    store.get_setting = AsyncMock(return_value="5")
    store.get_notification_queries = AsyncMock(return_value=[])

    collector = MagicMock()
    config = SchedulerConfig(collect_interval_minutes=60)

    mgr = SchedulerManager(collector, config, scheduler_bundle=store)
    await mgr.start()
    assert mgr.interval_minutes == 5
    await mgr.stop()
