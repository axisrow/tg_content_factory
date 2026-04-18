"""Embedded Telegram worker that runs inside the `serve` process.

Background for #457: before round 4, `python -m src.main serve` started only
the FastAPI web app with snapshot shims. The real Telethon pool, collection
queue, dispatchers and scheduler lived in a separate `python -m src.main
worker` process. Users who did not know they had to run both processes saw
UI buttons accept clicks while nothing was collected, the scheduler stayed
"stopped", and tasks piled up in `collection_tasks` with status=pending
forever.

Round 4 attaches the worker runtime to the same process by default — the
lifespan hook spawns an asyncio task that is the exact moral equivalent of
`src.runtime.worker._run_worker_async`, just running inside the web loop
instead of its own `asyncio.run()`. The code path that production split
deployments take (dedicated worker container) stays available via
`python -m src.main serve --no-worker` plus a separately launched
`python -m src.main worker` — both processes keep sharing the SQLite file
and `runtime_snapshots` rows as before.

We deliberately do NOT reuse the web container's `Database` instance: each
AppContainer owns its own aiosqlite connection (`Database.close()` is called
in both `stop_container` paths), so sharing would double-close. Two
connections to the same SQLite file are fine because SQLite file-level
locking already serialises writes, which is exactly how the two-process
deployment has worked since #444.
"""
from __future__ import annotations

import asyncio
import logging

from src.config import AppConfig
from src.runtime.worker import _publish_snapshots
from src.web.bootstrap import build_worker_container, start_container, stop_container
from src.web.container import AppContainer
from src.web.log_handler import LogBuffer

logger = logging.getLogger(__name__)

# How often the embedded worker republishes `worker_heartbeat` and the other
# runtime snapshots. Matches `src/runtime/worker.py:_run_worker_async` and is
# what `_is_worker_alive` compares against (`WORKER_HEARTBEAT_STALE_AFTER_SEC`
# is 60s, so 5s gives 12 beats of slack before the UI marks the worker down).
HEARTBEAT_INTERVAL_SEC = 5.0


class EmbeddedWorker:
    """Lifecycle wrapper around an in-process worker container.

    Owns a separate `AppContainer` with runtime_mode="worker", its own
    aiosqlite connection, ClientPool, CollectionQueue, UnifiedDispatcher,
    TelegramCommandDispatcher and SchedulerManager — i.e. everything that
    `src/runtime/worker.py` bootstraps in its own process.
    """

    def __init__(self, config: AppConfig):
        self._config = config
        self._container: AppContainer | None = None
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()
        self._ready_event = asyncio.Event()

    @property
    def container(self) -> AppContainer | None:
        return self._container

    async def wait_ready(self, timeout: float | None = None) -> bool:
        """Wait until the worker has published its first heartbeat.

        Used by tests to know when it is safe to trigger a collection and
        expect the worker to pick it up.
        """
        try:
            await asyncio.wait_for(self._ready_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return False
        return True

    async def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("EmbeddedWorker already started")
        self._task = asyncio.create_task(self._run(), name="embedded-worker")

    async def stop(self, timeout: float = 10.0) -> None:
        if self._task is None:
            return
        self._stop_event.set()
        try:
            await asyncio.wait_for(self._task, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("[embedded-worker] did not stop within %.1fs; cancelling", timeout)
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        self._task = None

    async def _run(self) -> None:
        logger.info("[embedded-worker] starting")
        try:
            self._container = await build_worker_container(
                self._config, log_buffer=LogBuffer(maxlen=500)
            )
            await start_container(self._container)
        except Exception:
            logger.exception(
                "[embedded-worker] failed to start; UI will show worker_down banner"
            )
            if self._container is not None:
                try:
                    await stop_container(self._container)
                except Exception:
                    logger.exception("[embedded-worker] stop_container during startup-failure cleanup raised")
                self._container = None
            return

        try:
            while not self._stop_event.is_set():
                try:
                    await _publish_snapshots(self._container)
                    self._ready_event.set()
                except Exception:
                    # Snapshot publishing must never crash the loop — the worker
                    # is still processing queued tasks even if snapshots fail.
                    logger.exception("[embedded-worker] _publish_snapshots failed")
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=HEARTBEAT_INTERVAL_SEC
                    )
                except asyncio.TimeoutError:
                    continue
        finally:
            logger.info("[embedded-worker] stopping")
            try:
                await stop_container(self._container)
            except Exception:
                logger.exception("[embedded-worker] stop_container raised")
            self._container = None
            logger.info("[embedded-worker] stopped")
