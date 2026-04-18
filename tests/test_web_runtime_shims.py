from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from src.web.runtime_shims import (
    SnapshotClientPool,
    SnapshotCollector,
    SnapshotSchedulerManager,
)


def _mock_db():
    db = MagicMock()
    db.repos = MagicMock()
    db.repos.runtime_snapshots = MagicMock()
    db.repos.runtime_snapshots.get_snapshot = AsyncMock(return_value=None)
    return db


async def test_snapshot_pool_clients_empty():
    db = _mock_db()
    pool = SnapshotClientPool(db)
    assert pool.clients == {}


async def test_snapshot_pool_refresh():
    db = _mock_db()
    snapshot = MagicMock(payload={"connected_phones": ["+1", "+2"]})
    db.repos.runtime_snapshots.get_snapshot.return_value = snapshot
    pool = SnapshotClientPool(db)
    await pool.refresh()
    assert set(pool.clients.keys()) == {"+1", "+2"}


async def test_snapshot_pool_refresh_no_snapshot():
    db = _mock_db()
    pool = SnapshotClientPool(db)
    await pool.refresh()
    assert pool.clients == {}


async def test_snapshot_pool_refresh_non_list_phones():
    db = _mock_db()
    snapshot = MagicMock(payload={"connected_phones": "not_a_list"})
    db.repos.runtime_snapshots.get_snapshot.return_value = snapshot
    pool = SnapshotClientPool(db)
    await pool.refresh()
    assert pool.clients == {}


async def test_snapshot_pool_initialize():
    db = _mock_db()
    pool = SnapshotClientPool(db)
    await pool.initialize()


async def test_snapshot_pool_warm_all():
    db = _mock_db()
    pool = SnapshotClientPool(db)
    assert await pool.warm_all_dialogs() is None


async def test_snapshot_pool_disconnect_all():
    db = _mock_db()
    pool = SnapshotClientPool(db)
    assert await pool.disconnect_all() is None


async def test_snapshot_pool_get_native_raises():
    db = _mock_db()
    pool = SnapshotClientPool(db)
    try:
        await pool.get_native_client_by_phone("+1")
        assert False, "Should have raised"
    except RuntimeError as e:
        assert "worker process" in str(e)


async def test_snapshot_pool_release_client():
    db = _mock_db()
    pool = SnapshotClientPool(db)
    await pool.release_client("+1")


async def test_snapshot_collector_default():
    db = _mock_db()
    collector = SnapshotCollector(db)
    assert collector.is_running is False


async def test_snapshot_collector_refresh():
    db = _mock_db()
    snapshot = MagicMock(payload={"is_running": True})
    db.repos.runtime_snapshots.get_snapshot.return_value = snapshot
    collector = SnapshotCollector(db)
    await collector.refresh()
    assert collector.is_running is True


async def test_snapshot_collector_availability_no_snapshot():
    db = _mock_db()
    collector = SnapshotCollector(db)
    avail = await collector.get_collection_availability()
    assert avail.state == "no_connected_active"
    assert avail.next_available_at_utc is None


async def test_snapshot_collector_availability_with_next():
    db = _mock_db()
    snapshot = MagicMock(payload={
        "state": "healthy",
        "next_available_at_utc": "2026-01-01T00:00:00",
        "retry_after_sec": 30,
    })
    db.repos.runtime_snapshots.get_snapshot.return_value = snapshot
    collector = SnapshotCollector(db)
    avail = await collector.get_collection_availability()
    assert avail.state == "healthy"
    assert avail.retry_after_sec == 30


async def test_snapshot_collector_cancel():
    db = _mock_db()
    collector = SnapshotCollector(db)
    await collector.cancel()


async def test_snapshot_scheduler_defaults():
    db = _mock_db()
    mgr = SnapshotSchedulerManager(db, default_interval_minutes=30)
    assert mgr.is_running is False
    assert mgr.interval_minutes == 30


async def test_snapshot_scheduler_load_settings():
    db = _mock_db()
    snapshot = MagicMock(payload={"is_running": True, "interval_minutes": 15})
    db.repos.runtime_snapshots.get_snapshot.return_value = snapshot
    mgr = SnapshotSchedulerManager(db, default_interval_minutes=30)
    await mgr.load_settings()
    assert mgr.is_running is True
    assert mgr.interval_minutes == 15


async def test_snapshot_scheduler_load_no_snapshot():
    db = _mock_db()
    mgr = SnapshotSchedulerManager(db, default_interval_minutes=60)
    await mgr.load_settings()
    assert mgr.is_running is False
    assert mgr.interval_minutes == 60


async def test_snapshot_scheduler_start():
    db = _mock_db()
    mgr = SnapshotSchedulerManager(db, 60)
    await mgr.start()


async def test_snapshot_scheduler_stop():
    db = _mock_db()
    mgr = SnapshotSchedulerManager(db, 60)
    await mgr.stop()


async def test_snapshot_scheduler_get_potential_jobs():
    db = _mock_db()
    snapshot = MagicMock(payload={"jobs": [{"name": "j1"}, {"name": "j2"}]})
    db.repos.runtime_snapshots.get_snapshot.return_value = snapshot
    mgr = SnapshotSchedulerManager(db, 60)
    jobs = await mgr.get_potential_jobs()
    assert len(jobs) == 2


async def test_snapshot_scheduler_get_potential_jobs_no_snapshot():
    db = _mock_db()
    mgr = SnapshotSchedulerManager(db, 60)
    jobs = await mgr.get_potential_jobs()
    assert jobs == []


async def test_snapshot_scheduler_noop_methods():
    db = _mock_db()
    mgr = SnapshotSchedulerManager(db, 60)
    assert mgr.get_all_jobs_next_run() == {}
    await mgr.trigger_warm_background()
    await mgr.sync_job_state("j1", enabled=True)
    await mgr.set_interval(30)
    mgr.update_interval(30)
    await mgr.sync_search_query_jobs()
    await mgr.sync_pipeline_jobs()
