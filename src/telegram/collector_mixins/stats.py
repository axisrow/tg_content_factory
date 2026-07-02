"""Collector channel stats collection helpers (#1137)."""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from telethon.errors import UsernameInvalidError, UsernameNotOccupiedError
from telethon.tl.types import PeerChannel

from src.models import Channel, ChannelStats
from src.telegram.backends import adapt_transport_session
from src.telegram.collector_types import (
    AllStatsClientsFloodedError,
    NoActiveStatsClientsError,
)
from src.telegram.flood_wait import HandledFloodWaitError, run_with_flood_wait
from src.telegram.rate_limiter import (
    UsernameResolveFloodWaitDeferredError,
    UsernameResolveRateLimitedError,
)

if TYPE_CHECKING:
    from typing import Any, Protocol, TypeAlias

    from src.telegram.collector import Collector as _RuntimeCollector

    _RuntimeCollectorType: TypeAlias = type[_RuntimeCollector]

    class Collector(Protocol):
        def __getattribute__(self, name: str) -> Any: ...
        def __setattr__(self, name: str, value: Any) -> None: ...

logger = logging.getLogger("src.telegram.collector")


class StatsMixin:
    @property
    def is_stats_running(self: "Collector") -> bool:
        return self._stats_running or self._stats_all_running

    def stats_worker_count(self: "Collector") -> int:
        configured = max(1, int(getattr(self._config, "stats_worker_count", 3) or 1))
        connected = len(getattr(self._pool, "clients", {}) or {})
        if connected <= 0:
            return configured
        return max(1, min(configured, connected))

    def stats_all_worker_count(self: "Collector") -> int:
        configured = max(1, int(getattr(self._config, "stats_all_worker_count", 1) or 1))
        connected = len(getattr(self._pool, "clients", {}) or {})
        if connected <= 0:
            return configured
        return max(1, min(configured, connected))

    async def available_stats_worker_count(self: "Collector") -> int:
        configured = max(1, int(getattr(self._config, "stats_worker_count", 3) or 1))
        counter = getattr(self._pool, "available_stats_client_count", None)
        if callable(counter):
            try:
                count = counter()
                if asyncio.iscoroutine(count):
                    count = await count
                available = int(count)
                if available > 0:
                    return max(1, min(configured, available))
                return 1
            except Exception:
                logger.debug("Failed to read available stats client count", exc_info=True)
        return self.stats_worker_count()

    async def available_stats_all_worker_count(self: "Collector") -> int:
        configured = max(1, int(getattr(self._config, "stats_all_worker_count", 1) or 1))
        counter = getattr(self._pool, "available_stats_client_count", None)
        if callable(counter):
            try:
                count = counter()
                if asyncio.iscoroutine(count):
                    count = await count
                available = int(count)
                if available > 0:
                    return max(1, min(configured, available))
                return 1
            except Exception:
                logger.debug("Failed to read available stats-all client count", exc_info=True)
        return self.stats_all_worker_count()

    def set_stats_all_running(self: "Collector", running: bool) -> None:
        self._stats_all_running = running

    async def get_stats_availability(self: "Collector"):
        return await self.get_collection_availability()

    async def collect_channel_stats(self: "Collector", channel: Channel) -> ChannelStats | None:
        async with self._stats_lock:
            self._stats_running = True
            try:
                return await self._collect_channel_stats(channel)
            except (AllStatsClientsFloodedError, NoActiveStatsClientsError):
                logger.error("No available clients for stats collection")
                return None
            except (UsernameResolveRateLimitedError, UsernameResolveFloodWaitDeferredError) as exc:
                logger.warning("Stats collection deferred by username resolve guard: %s", exc)
                return None
            finally:
                self._stats_running = False

    async def collect_channel_stats_unlocked(self: "Collector", channel: Channel) -> ChannelStats | None:
        return await self._collect_channel_stats(channel)

    async def _resolve_stats_entity_or_deactivate(
        self: "Collector",
        session: object,
        phone: str,
        channel: Channel,
        operation: str,
    ) -> object | None:
        """Resolve a channel entity by numeric ID for the stats path.

        Returns the resolved entity, or ``None`` when the caller should stop
        and return ``None`` itself: either the channel was deactivated after a
        permanent lookup failure (``ValueError``/``TypeError``) or the run is
        skipped after a transient failure (timeout/connection drop, #815).
        Re-raises :class:`HandledFloodWaitError` so flood handling propagates.
        """
        try:
            return await self._pool.resolve_entity_with_warm(
                session,
                phone,
                PeerChannel(channel.channel_id),
                operation=operation,
            )
        except HandledFloodWaitError:
            raise
        except (ValueError, TypeError):
            logger.warning(
                "Stats: channel %d all entity lookups failed, deactivating",
                channel.channel_id,
            )
            if channel.id:
                try:
                    await self._db.set_channel_active(channel.id, False)
                except Exception:
                    logger.debug(
                        "Stats: failed to deactivate channel %d",
                        channel.channel_id,
                        exc_info=True,
                    )
            else:
                logger.warning(
                    "Stats: cannot deactivate channel %d -- no DB pk",
                    channel.channel_id,
                )
            return None
        except Exception:
            # Transient failure (timeout, connection drop, …): skip this run
            # WITHOUT deactivating the channel (#815 review follow-up).
            logger.warning(
                "Stats: channel %d entity lookup failed transiently, skipping",
                channel.channel_id,
                exc_info=True,
            )
            return None

    async def _collect_stats_metrics(
        self: "Collector", session, entity, phone: str, channel_id: int
    ) -> tuple[list[int], list[int], list[int]]:
        """Stream up to 50 recent messages and accumulate ``(views, reactions,
        forwards)`` totals for stats averaging. Extracted from
        ``_collect_channel_stats`` (#1045); the stats-cancel break, the 90s flood
        timeout, and the swallowed idle ``TimeoutError`` are preserved.
        """
        views_list: list[int] = []
        reactions_list: list[int] = []
        forwards_list: list[int] = []

        async def _collect_stats_messages() -> None:
            async for msg in session.stream_messages(
                entity,
                limit=50,
                wait_time=self._config.delay_between_requests_sec,
            ):
                if self._is_stats_cancelled():
                    break
                if getattr(msg, "views", None) is not None:
                    views_list.append(msg.views)
                if getattr(msg, "forwards", None) is not None:
                    forwards_list.append(msg.forwards)
                reactions = getattr(msg, "reactions", None)
                if reactions:
                    total = sum(
                        getattr(r, "count", 0) for r in getattr(reactions, "results", [])
                    )
                    reactions_list.append(total)

        try:
            await run_with_flood_wait(
                _collect_stats_messages(),
                operation="collect_channel_stats_stream_messages",
                phone=phone,
                pool=self._pool,
                logger_=logger,
                timeout=90.0,
            )
        except asyncio.TimeoutError:
            logger.warning("iter_messages timed out for stats on channel %d", channel_id)

        return views_list, reactions_list, forwards_list

    async def _collect_channel_stats(self: "Collector", channel: Channel) -> ChannelStats | None:
        while True:
            result = await self._pool.get_available_client()
            if result is None:
                availability_fn = getattr(self._pool, "get_stats_availability", None)
                if not callable(availability_fn):
                    raise NoActiveStatsClientsError("No active connected clients")
                availability_result = availability_fn()
                if not asyncio.iscoroutine(availability_result):
                    raise NoActiveStatsClientsError("No active connected clients")
                availability = await availability_result
                if (
                    availability.state == "all_flooded"
                    and availability.retry_after_sec is not None
                    and availability.next_available_at_utc is not None
                ):
                    raise AllStatsClientsFloodedError(
                        retry_after_sec=availability.retry_after_sec,
                        next_available_at=availability.next_available_at_utc,
                    )
                raise NoActiveStatsClientsError("No active connected clients")

            session, phone = result
            session = adapt_transport_session(session, disconnect_on_close=False)
            try:
                if channel.username:
                    try:
                        entity = await self._pool.run_live_username_resolve(
                            lambda session=session, username=channel.username: session.resolve_entity(username),
                            operation="collect_channel_stats_resolve_username",
                            phone=phone,
                            username=str(channel.username),
                            logger_=logger,
                            timeout=30.0,
                        )
                    except (ValueError, UsernameNotOccupiedError, UsernameInvalidError):
                        logger.warning(
                            "Stats: channel %d (%s) username not found, "
                            "trying numeric ID fallback",
                            channel.channel_id,
                            channel.username,
                        )
                        entity = await self._resolve_stats_entity_or_deactivate(
                            session,
                            phone,
                            channel,
                            operation="collect_channel_stats_resolve_channel_id_fallback",
                        )
                        if entity is None:
                            return None
                        new_username = getattr(entity, "username", None)
                        new_title = (
                            getattr(entity, "title", None)
                            or channel.title
                            or channel.username
                            or str(channel.channel_id)
                        )
                        await self._handle_meta_change_review(
                            channel,
                            new_username,
                            new_title,
                            log_prefix="Stats",
                        )
                        # Channel quarantined for rename review — stop here instead
                        # of writing stats/created_at/type for it, mirroring the
                        # collect path (audit #835/13).
                        return None
                else:
                    # Warm cache for numeric-id channels; bare resolve loops forever (#794).
                    entity = await self._resolve_stats_entity_or_deactivate(
                        session,
                        phone,
                        channel,
                        operation="collect_channel_stats_resolve_channel_id",
                    )
                    if entity is None:
                        return None

                # Guard: entity resolved as a User/Bot (no ``title``) — not a channel.
                # Reuse the pool's canonical classifier (bot/dm) so the channel
                # stops re-entering stats payloads forever instead of failing
                # silently on every run.
                if not hasattr(entity, "title"):
                    entity_type = self._pool._entity_to_dict(entity)["channel_type"]
                    logger.warning(
                        "Stats: channel %d resolved as non-channel entity (%s), "
                        "deactivating",
                        channel.channel_id,
                        entity_type,
                    )
                    await self._db.repos.channels.set_channel_type(channel.channel_id, entity_type)
                    await self._db.set_channel_active(channel.id, False)
                    return None

                full = await run_with_flood_wait(
                    session.fetch_full_channel(entity),
                    operation="collect_channel_stats_fetch_full_channel",
                    phone=phone,
                    pool=self._pool,
                    logger_=logger,
                    timeout=30.0,
                )
                subscriber_count = getattr(full.full_chat, "participants_count", None)

                views_list, reactions_list, forwards_list = await self._collect_stats_metrics(
                    session, entity, phone, channel.channel_id
                )

                stats = ChannelStats(
                    channel_id=channel.channel_id,
                    subscriber_count=subscriber_count,
                    avg_views=sum(views_list) / len(views_list) if views_list else None,
                    avg_reactions=(
                        sum(reactions_list) / len(reactions_list) if reactions_list else None
                    ),
                    avg_forwards=(
                        sum(forwards_list) / len(forwards_list) if forwards_list else None
                    ),
                )
                await self._db.save_channel_stats(stats)

                # Backfill channel creation date from entity if missing
                entity_created = getattr(entity, "date", None)
                if entity_created is not None:
                    await self._db.repos.channels.update_channel_created_at(
                        channel.channel_id, entity_created
                    )

                # Update channel_type if missing
                if channel.channel_type is None:
                    channel_type, _deactivate = self._pool._classify_entity(entity)
                    await self._db.set_channel_type(channel.channel_id, channel_type)

                return stats
            except HandledFloodWaitError:
                pass
            finally:
                await self._pool.release_client(phone)

    def _stats_all_channel_limit(self: "Collector", max_channels: int | None = None) -> int:
        configured = max_channels
        if configured is None:
            configured = int(getattr(self._config, "stats_all_max_channels_per_run", 10) or 10)
        return max(1, int(configured))

    async def _order_stats_all_channels(
        self: "Collector", channels: list[Channel], *, skip_fresh_hours: int = 0
    ) -> list[Channel]:
        try:
            latest_stats = await self._db.get_latest_stats_for_all()
        except Exception:
            logger.debug("Failed to load latest channel stats for stats-all ordering", exc_info=True)
            return channels

        def _collected_at(channel: Channel) -> datetime:
            """UTC-normalized last-stats timestamp; ``datetime.min`` if never collected."""
            latest = latest_stats.get(channel.channel_id)
            collected_at = (latest.collected_at if latest else None) or datetime.min.replace(tzinfo=timezone.utc)
            if collected_at.tzinfo is None:
                collected_at = collected_at.replace(tzinfo=timezone.utc)
            return collected_at

        if skip_fresh_hours > 0:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=skip_fresh_hours)
            channels = [
                ch for ch in channels
                if ch.channel_id not in latest_stats or _collected_at(ch) < cutoff
            ]

        def _sort_key(item: tuple[int, Channel]) -> tuple[int, datetime, int]:
            index, channel = item
            has_stats = channel.channel_id in latest_stats
            return (1 if has_stats else 0, _collected_at(channel), index)

        ordered = [channel for _index, channel in sorted(enumerate(channels), key=_sort_key)]

        if skip_fresh_hours > 0:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=skip_fresh_hours)
            result = []
            for ch in ordered:
                latest = latest_stats.get(ch.channel_id)
                if latest is None:
                    result.append(ch)
                    continue
                collected_at = latest.collected_at or datetime.min.replace(tzinfo=timezone.utc)
                if collected_at.tzinfo is None:
                    collected_at = collected_at.replace(tzinfo=timezone.utc)
                if collected_at < cutoff:
                    result.append(ch)
            ordered = result

        return ordered

    async def collect_all_stats(self: "Collector", *, max_channels: int | None = None) -> dict:
        async with self._stats_all_lock:
            self._stats_all_running = True
            # Fresh run — drop any stale stats-cancel from a previous STATS_ALL.
            self._stats_cancel_event.clear()
            try:
                channels = await self._db.get_channels(active_only=True, include_filtered=False)
                skip_fresh_hours = int(getattr(self._config, "stats_all_skip_fresh_hours", 24) or 0)
                channels = await self._order_stats_all_channels(channels, skip_fresh_hours=skip_fresh_hours)
                channel_limit = self._stats_all_channel_limit(max_channels)
                total_channels = len(channels)
                selected_channels = channels[:channel_limit]
                initial_remaining = max(0, total_channels - len(selected_channels))
                stats = {
                    "channels": 0,
                    "errors": 0,
                    "remaining": initial_remaining,
                    "limited": initial_remaining > 0,
                    "total": total_channels,
                    "max_channels": channel_limit,
                }
                if not channels:
                    return stats

                queue = deque(selected_channels)
                in_flight: list[Channel] = []
                deferred_front: deque[Channel] = deque()
                state_lock = asyncio.Lock()
                stop_workers = False

                def _remaining_count_unlocked() -> int:
                    seen: set[int] = set()
                    remaining = 0
                    for channel in [*deferred_front, *in_flight, *queue]:
                        if channel.channel_id in seen:
                            continue
                        seen.add(channel.channel_id)
                        remaining += 1
                    return remaining

                async def _defer_batch(channel: Channel, exc: AllStatsClientsFloodedError) -> None:
                    nonlocal stop_workers
                    async with state_lock:
                        stop_workers = True
                        if channel in in_flight:
                            in_flight.remove(channel)
                        deferred_front.appendleft(channel)
                        stats["flood_wait_until"] = exc.next_available_at.isoformat()
                        stats["flood_wait_retry_after_sec"] = exc.retry_after_sec
                        stats["limited"] = True

                async def _defer_resolve_batch(
                    channel: Channel,
                    *,
                    retry_after_sec: int,
                    next_available_at: datetime,
                ) -> None:
                    nonlocal stop_workers
                    async with state_lock:
                        stop_workers = True
                        if channel in in_flight:
                            in_flight.remove(channel)
                        deferred_front.appendleft(channel)
                        stats["resolve_username_until"] = next_available_at.isoformat()
                        stats["resolve_username_retry_after_sec"] = retry_after_sec
                        stats["limited"] = True

                async def _worker() -> None:
                    nonlocal stop_workers
                    while not self._is_stats_cancelled():
                        async with state_lock:
                            if stop_workers or not queue:
                                return
                            channel = queue.popleft()
                            in_flight.append(channel)
                        try:
                            result = await self._collect_channel_stats(channel)
                            async with state_lock:
                                if channel in in_flight:
                                    in_flight.remove(channel)
                                if result is None:
                                    stats["errors"] += 1
                                else:
                                    stats["channels"] += 1
                        except AllStatsClientsFloodedError as e:
                            logger.warning(
                                "All clients are flood-waited for stats. Deferring %d queued channels until %s",
                                1 + len(queue),
                                e.next_available_at.isoformat(),
                            )
                            await _defer_batch(channel, e)
                            return
                        except (UsernameResolveRateLimitedError, UsernameResolveFloodWaitDeferredError) as e:
                            retry_after_sec = int(
                                getattr(e, "retry_after_seconds", None)
                                or getattr(e, "wait_seconds", 0)
                                or 0
                            )
                            next_available_at = getattr(
                                e,
                                "next_available_at",
                                datetime.now(timezone.utc) + timedelta(seconds=retry_after_sec),
                            )
                            logger.warning(
                                "Username resolve budget is blocked for stats. "
                                "Deferring %d queued channels until %s",
                                1 + len(queue),
                                next_available_at.isoformat(),
                            )
                            await _defer_resolve_batch(
                                channel,
                                retry_after_sec=retry_after_sec,
                                next_available_at=next_available_at,
                            )
                            return
                        except NoActiveStatsClientsError:
                            logger.error("No active connected clients for stats collection")
                            async with state_lock:
                                if channel in in_flight:
                                    in_flight.remove(channel)
                                stats["errors"] += 1 + len(queue)
                                queue.clear()
                                stop_workers = True
                            return
                        except Exception as e:
                            logger.error("Stats error for %s: %s", channel.channel_id, e)
                            async with state_lock:
                                if channel in in_flight:
                                    in_flight.remove(channel)
                                stats["errors"] += 1
                        async with state_lock:
                            has_more = bool(queue) and not stop_workers
                        if has_more:
                            await asyncio.sleep(self._config.delay_between_channels_sec)

                workers = [
                    asyncio.create_task(_worker())
                    for _ in range(
                        min(await self.available_stats_all_worker_count(), len(selected_channels))
                    )
                ]
                if workers:
                    await asyncio.gather(*workers)
                async with state_lock:
                    stats["remaining"] = initial_remaining + _remaining_count_unlocked()
                    stats["limited"] = bool(stats["remaining"])
                return stats
            finally:
                self._stats_all_running = False
