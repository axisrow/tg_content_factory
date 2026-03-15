from __future__ import annotations

from src.scheduler.manager import SchedulerManager


class SchedulerService:
    def __init__(self, manager: SchedulerManager):
        self._manager = manager

    async def start(self) -> None:
        await self._manager.start()

    async def stop(self) -> None:
        await self._manager.stop()

    async def trigger_collection(self) -> None:
        await self._manager.trigger_now()

    async def trigger_search(self) -> None:
        await self._manager.trigger_search_now()
