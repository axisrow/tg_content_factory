"""Collector cancellation, availability, and run-state helpers (#1137)."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from inspect import isawaitable

from src.telegram.collector_types import (
    AllCollectionClientsFloodedError,
    NoActiveCollectionClientsError,
)
from src.telegram.flood_wait import (
    is_transient_flood_wait_seconds,
    sleep_for_flood_wait_seconds,
)

logger = logging.getLogger("src.telegram.collector")


class CancellationMixin:
    def _get_resolve_username_backoff_remaining_sec(self, phone: str | None = None) -> int:
        # The pool owns the single source of truth for the resolve backoff
        # (#785). With ``phone`` — that account's window; without — the
        # pool-wide aggregate (0 while any connected account is free, #790).
        return self._pool.get_resolve_username_backoff_remaining_sec(phone)

    async def _can_rotate_resolve(self, attempted_phones: set[str]) -> bool:
        """True if another connected account outside ``attempted_phones`` can
        run a live username resolve right now (#790).

        Prefers the async ``has_rotatable_resolve_phone`` which also rejects
        accounts in a *generic* flood wait (only the DB knows that, so the
        check has to be async). Falls back to the sync resolve-backoff-only
        ``has_resolve_capable_phone`` for test doubles that lack the async
        method.
        """
        has_rotatable = getattr(self._pool, "has_rotatable_resolve_phone", None)
        if callable(has_rotatable):
            result = has_rotatable(exclude=attempted_phones)
            if isawaitable(result):
                result = await result
            return bool(result)
        has_capable = getattr(self._pool, "has_resolve_capable_phone", None)
        if not callable(has_capable):
            return False
        return bool(has_capable(exclude=attempted_phones))

    async def _next_resolve_capable_at(self) -> datetime | None:
        """Earliest moment any connected account can live-resolve again (#790).

        Prefers the async pool method, which also accounts for *generic*
        flood waits — in the mixed state (this phone resolve-blocked for
        hours, another phone generically flooded for minutes) the channel
        must retry when the generic flood clears. Falls back to the
        resolve-backoff-only aggregate for doubles lacking the async method.
        """
        getter = getattr(self._pool, "next_resolve_capable_at", None)
        if callable(getter):
            result = getter()
            if isawaitable(result):
                result = await result
            return result
        return self._pool.get_resolve_username_backoff_until()

    async def get_collection_availability(self):
        availability_fn = getattr(self._pool, "get_stats_availability", None)
        if callable(availability_fn):
            result = availability_fn()
            if asyncio.iscoroutine(result):
                return await result
        return await self._fallback_collection_availability()

    @property
    def is_running(self) -> bool:
        return (
            bool(getattr(self, "_running", False))
            or int(getattr(self, "_active_collection_count", 0) or 0) > 0
            or bool(getattr(self, "_stats_running", False))
            or bool(getattr(self, "_stats_all_running", False))
        )

    def _is_collection_cancelled(self, cancel_event: asyncio.Event | None = None) -> bool:
        if cancel_event is not None:
            return cancel_event.is_set() or self._cancel_event.is_set()
        return self._cancel_event.is_set()

    def _should_clear_collection_cancel_on_start(self, cancel_event: asyncio.Event | None) -> bool:
        return cancel_event is None or not self.is_running

    async def _wait_if_live_runtime_paused(
        self,
        *,
        stop_event: asyncio.Event | None = None,
    ) -> bool:
        if self._live_runtime_pause_gate is None:
            return True
        return await self._live_runtime_pause_gate.wait_if_paused(
            stop_event=stop_event or self._cancel_event
        )

    async def _fallback_collection_availability(self):
        connected = bool(getattr(self._pool, "clients", {}))
        if connected:
            return type(
                "Availability",
                (),
                {
                    "state": "available",
                    "retry_after_sec": None,
                    "next_available_at_utc": None,
                },
            )()
        return type(
            "Availability",
            (),
            {
                "state": "no_connected_active",
                "retry_after_sec": None,
                "next_available_at_utc": None,
            },
        )()

    def _log_collection_unavailability_once(
        self,
        *,
        state: str,
        retry_after_sec: int | None = None,
        next_available_at: datetime | None = None,
    ) -> None:
        signature = (state, retry_after_sec, next_available_at)
        if self._last_unavailability_log == signature:
            logger.debug(
                "Collection clients still unavailable: state=%s retry_after_sec=%s next_available_at=%s",
                state,
                retry_after_sec,
                next_available_at.isoformat() if next_available_at else None,
            )
            return
        self._last_unavailability_log = signature
        if state == "all_flooded" and retry_after_sec is not None and next_available_at is not None:
            logger.error(
                "No available clients for collection: all active clients are flood-waited until %s "
                "(retry in %ss)",
                next_available_at.isoformat(),
                retry_after_sec,
            )
            return
        logger.error("No available clients for collection: no active connected clients")

    def _reset_collection_unavailability_log(self) -> None:
        self._last_unavailability_log = None

    async def _wait_for_transient_collection_flood(self, availability) -> bool:
        retry_after_sec = getattr(availability, "retry_after_sec", None)
        if getattr(availability, "state", None) != "all_flooded":
            return False
        if not is_transient_flood_wait_seconds(retry_after_sec):
            return False
        await sleep_for_flood_wait_seconds(
            int(retry_after_sec),
            operation="collect_channel_all_clients_transient_flood_wait",
            logger_=logger,
        )
        self._reset_collection_unavailability_log()
        return True

    async def _raise_collection_unavailability(self, availability=None) -> None:
        availability = availability or await self.get_collection_availability()
        self._log_collection_unavailability_once(
            state=availability.state,
            retry_after_sec=availability.retry_after_sec,
            next_available_at=availability.next_available_at_utc,
        )
        if (
            availability.state == "all_flooded"
            and availability.retry_after_sec is not None
            and availability.next_available_at_utc is not None
        ):
            raise AllCollectionClientsFloodedError(
                retry_after_sec=availability.retry_after_sec,
                next_available_at=availability.next_available_at_utc,
            )
        raise NoActiveCollectionClientsError("No active connected clients")

    async def cancel(self) -> None:
        self._cancel_event.set()

    async def cancel_stats(self) -> None:
        """Cancel an in-flight STATS_ALL run without touching channel collection."""
        self._stats_cancel_event.set()

    def _is_stats_cancelled(self) -> bool:
        # Global shutdown (cancel()) also stops stats; a stats-only cancel does not
        # stop channel collection.
        return self._cancel_event.is_set() or self._stats_cancel_event.is_set()

    @property
    def is_cancelled(self) -> bool:
        return self._cancel_event.is_set()
