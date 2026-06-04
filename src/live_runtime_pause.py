from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


class LiveRuntimePauseGate:
    """Shared pause flag for background work while an agent uses live runtime."""

    def __init__(self) -> None:
        self._active_agent_requests = 0
        self._resume_event = asyncio.Event()
        self._resume_event.set()
        self._lock = asyncio.Lock()

    @property
    def is_paused(self) -> bool:
        return not self._resume_event.is_set()

    @property
    def active_agent_requests(self) -> int:
        return self._active_agent_requests

    @asynccontextmanager
    async def agent_request(self):
        await self._acquire_agent_request()
        try:
            yield
        finally:
            await asyncio.shield(self._release_agent_request())

    async def _acquire_agent_request(self) -> None:
        async with self._lock:
            self._active_agent_requests += 1
            if self._active_agent_requests == 1:
                self._resume_event.clear()
                logger.info("Live runtime background work paused for agent request")

    async def _release_agent_request(self) -> None:
        async with self._lock:
            if self._active_agent_requests <= 0:
                self._active_agent_requests = 0
                self._resume_event.set()
                return
            self._active_agent_requests -= 1
            if self._active_agent_requests == 0:
                self._resume_event.set()
                logger.info("Live runtime background work resumed after agent request")

    async def wait_if_paused(self, *, stop_event: asyncio.Event | None = None) -> bool:
        """Wait until agent work releases the gate.

        Returns False when the optional stop_event is set before the gate opens.
        Wakes exactly once on whichever event fires first — no idle polling.
        """
        if self._resume_event.is_set():
            return True
        if stop_event is None:
            await self._resume_event.wait()
            return True
        resume_wait = asyncio.create_task(self._resume_event.wait())
        stop_wait = asyncio.create_task(stop_event.wait())
        try:
            await asyncio.wait({resume_wait, stop_wait}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (resume_wait, stop_wait):
                task.cancel()
            await asyncio.gather(resume_wait, stop_wait, return_exceptions=True)
        return not stop_event.is_set()
