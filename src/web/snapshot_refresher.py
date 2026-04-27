"""Periodic reader of `runtime_snapshots` for the web container.

Mirror of `src/web/embedded_worker.py` from the *reader* side. The worker (either
embedded inside `serve` or a separate `python -m src.main worker` process)
publishes `accounts_status` / `collector_status` / `scheduler_status` snapshots
every ~5s; this task re-hydrates the read-only shims in
`src/web/runtime_shims.py` every `REFRESH_INTERVAL_SEC` seconds so pages like
`/scheduler/`, `/dashboard/` and `/health` reflect live state instead of the
single startup snapshot from `src/web/bootstrap.py`.
"""
from __future__ import annotations

import asyncio
import logging

from src.web.container import AppContainer
from src.web.runtime_shims import (
    SnapshotClientPool,
    SnapshotCollector,
    SnapshotSchedulerManager,
)

logger = logging.getLogger(__name__)

# Reader cadence. Worker writes every 5s; 3s gives worst-case staleness ≤ 8s
# without excess phase drift (5s reader would drift toward 10s lag).
REFRESH_INTERVAL_SEC = 3.0


class SnapshotRefresher:
    def __init__(self, container: AppContainer, *, interval: float = REFRESH_INTERVAL_SEC):
        self._container = container
        self._interval = interval
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("SnapshotRefresher already started")
        self._task = asyncio.create_task(self._run(), name="snapshot-refresher")

    async def stop(self, timeout: float = 5.0) -> None:
        if self._task is None:
            return
        self._stop.set()
        try:
            await asyncio.wait_for(self._task, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("[snapshot-refresher] did not stop within %.1fs; cancelling", timeout)
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._task = None

    async def _run(self) -> None:
        logger.debug("[snapshot-refresher] starting (interval=%.2fs)", self._interval)
        try:
            while not self._stop.is_set():
                try:
                    await self._refresh_once()
                except Exception:
                    logger.exception("[snapshot-refresher] refresh failed")
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=self._interval)
                except asyncio.TimeoutError:
                    continue
        finally:
            logger.debug("[snapshot-refresher] stopped")

    async def _refresh_once(self) -> None:
        pool = self._container.pool
        collector = self._container.collector
        scheduler = self._container.scheduler
        if isinstance(pool, SnapshotClientPool):
            await pool.refresh()
        if isinstance(collector, SnapshotCollector):
            await collector.refresh()
        if isinstance(scheduler, SnapshotSchedulerManager):
            await scheduler.load_settings()
