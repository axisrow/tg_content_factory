"""Scheduler and collection-queue command handlers (#1047).

Domains: ``scheduler.*`` (reconcile / warm trigger) and ``collection.*``
(pause / resume) — process-control commands that don't touch Telegram.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.services.dispatcher._base import _DispatcherProtocol

    _Base = _DispatcherProtocol
else:
    _Base = object


class SchedulerCommandsMixin(_Base):
    """``scheduler.*`` and ``collection.*`` command handlers."""

    async def _handle_scheduler_reconcile(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._scheduler is None:
            raise RuntimeError("scheduler_unavailable")
        autostart = await self._db.get_setting("scheduler_autostart")
        desired_running = autostart == "1"
        if not desired_running:
            await self._scheduler.stop()
            await self._scheduler.load_settings()
            return {"running": False}
        if self._scheduler.is_running:
            await self._scheduler.stop()
        await self._scheduler.load_settings()
        await self._scheduler.start()
        return {"running": True, "interval_minutes": self._scheduler.interval_minutes}

    async def _handle_scheduler_trigger_warm(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._scheduler is None:
            raise RuntimeError("scheduler_unavailable")
        await self._scheduler.trigger_warm_background()
        return {"started": True}

    async def _handle_collection_pause(self, payload: dict[str, Any]) -> dict[str, Any]:
        await self._db.set_setting("collection_queue_paused", "1")
        if self._collection_queue is not None:
            self._collection_queue.pause()
        return {"paused": True}

    async def _handle_collection_resume(self, payload: dict[str, Any]) -> dict[str, Any]:
        await self._db.set_setting("collection_queue_paused", "0")
        if self._collection_queue is not None:
            self._collection_queue.resume()
        return {"paused": False}
