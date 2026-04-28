from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.web.runtime_shims import (
    SnapshotClientPool,
    SnapshotCollector,
    SnapshotSchedulerManager,
)
from src.web.snapshot_refresher import SnapshotRefresher


def _mock_db():
    db = MagicMock()
    db.repos = MagicMock()
    db.repos.runtime_snapshots = MagicMock()
    db.repos.runtime_snapshots.get_snapshot = AsyncMock(return_value=None)
    return db


def _container_with_shims(db):
    return SimpleNamespace(
        pool=SnapshotClientPool(db),
        collector=SnapshotCollector(db),
        scheduler=SnapshotSchedulerManager(db, default_interval_minutes=60),
    )


async def _wait_for(predicate, *, timeout: float = 1.0, step: float = 0.02) -> bool:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(step)
    return predicate()


async def test_refresher_starts_and_stops_cleanly():
    db = _mock_db()
    container = _container_with_shims(db)
    refresher = SnapshotRefresher(container, interval=0.05)
    await refresher.start()
    await asyncio.sleep(0.1)
    await refresher.stop()


async def test_refresher_double_start_raises():
    db = _mock_db()
    refresher = SnapshotRefresher(_container_with_shims(db), interval=0.05)
    await refresher.start()
    try:
        with pytest.raises(RuntimeError):
            await refresher.start()
    finally:
        await refresher.stop()


async def test_refresher_picks_up_new_snapshots():
    db = _mock_db()
    container = _container_with_shims(db)

    # Initially: no snapshots → empty caches.
    await container.pool.refresh()
    await container.collector.refresh()
    await container.scheduler.load_settings()
    assert container.pool.clients == {}
    assert container.collector.is_running is False
    assert container.scheduler.interval_minutes == 60

    refresher = SnapshotRefresher(container, interval=0.02)
    await refresher.start()

    # Now publish fresh snapshots and let the refresher pick them up.
    accounts_snap = MagicMock(payload={"connected_phones": ["+79991", "+79992"]})
    collector_snap = MagicMock(payload={"is_running": True})
    scheduler_snap = MagicMock(payload={"is_running": True, "interval_minutes": 15})

    def _route(snapshot_type, scope="global"):
        if snapshot_type == "accounts_status":
            return accounts_snap
        if snapshot_type == "collector_status":
            return collector_snap
        if snapshot_type == "scheduler_status":
            return scheduler_snap
        return None

    db.repos.runtime_snapshots.get_snapshot = AsyncMock(side_effect=_route)

    try:
        assert await _wait_for(
            lambda: set(container.pool.clients.keys()) == {"+79991", "+79992"}, timeout=1.0
        )
        assert container.collector.is_running is True
        assert container.scheduler.is_running is True
        assert container.scheduler.interval_minutes == 15
    finally:
        await refresher.stop()


async def test_refresher_swallows_db_errors():
    db = _mock_db()
    container = _container_with_shims(db)

    snapshot = MagicMock(payload={"connected_phones": ["+79991"]})
    db.repos.runtime_snapshots.get_snapshot = AsyncMock(return_value=snapshot)

    calls = {"n": 0}
    real_refresh = container.pool.refresh

    async def flaky_refresh():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")
        await real_refresh()

    container.pool.refresh = flaky_refresh

    refresher = SnapshotRefresher(container, interval=0.02)
    await refresher.start()
    try:
        # Despite the first refresh raising, the loop survives and the
        # second tick populates the cache from the snapshot.
        assert await _wait_for(
            lambda: set(container.pool.clients.keys()) == {"+79991"}, timeout=1.0
        )
        assert calls["n"] >= 2
    finally:
        await refresher.stop()


async def test_refresher_noop_in_worker_mode():
    # Worker-mode containers hold the real ClientPool/Collector/SchedulerManager.
    # The isinstance guards must skip refresh on them — represented here by
    # plain objects without `refresh` / `load_settings` attributes.
    container = SimpleNamespace(
        pool=object(),
        collector=object(),
        scheduler=object(),
    )
    refresher = SnapshotRefresher(container, interval=0.02)
    await refresher.start()
    try:
        await asyncio.sleep(0.1)
    finally:
        await refresher.stop()


async def test_refresher_stop_without_start_is_noop():
    db = _mock_db()
    refresher = SnapshotRefresher(_container_with_shims(db), interval=0.05)
    await refresher.stop()  # must not raise


async def test_refresher_can_restart_after_stop():
    db = _mock_db()
    container = _container_with_shims(db)
    refresher = SnapshotRefresher(container, interval=0.02)

    await refresher.start()
    await refresher.stop()

    snapshot = MagicMock(payload={"connected_phones": ["+79991"]})
    db.repos.runtime_snapshots.get_snapshot = AsyncMock(return_value=snapshot)

    await refresher.start()
    try:
        assert await _wait_for(lambda: set(container.pool.clients.keys()) == {"+79991"}, timeout=1.0)
    finally:
        await refresher.stop()
