from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from inspect import isawaitable

from telethon.errors import FloodWaitError, UsernameInvalidError, UsernameNotOccupiedError
from telethon.tl.types import (
    PeerChannel,
)

from src.config import SchedulerConfig
from src.database import Database, DatabaseBusyError
from src.filters.criteria import (
    LOW_SUBSCRIBER_RATIO_CHAT_THRESHOLD,
    LOW_SUBSCRIBER_RATIO_THRESHOLD,
    LOW_UNIQUENESS_THRESHOLD,
    PRECHECK_CROSS_DUPE_MIN_SAMPLE,
    PRECHECK_CROSS_DUPE_RATIO,
    PRECHECK_CROSS_DUPE_SAMPLE,
)
from src.live_runtime_pause import LiveRuntimePauseGate
from src.models import Channel, ChannelStats, Message
from src.settings_utils import parse_int_setting
from src.telegram.backends import adapt_transport_session
from src.telegram.client_pool import ClientPool
from src.telegram.collector_message_parse import (
    SERVICE_ACTION_SEMANTICS,
    build_message_from_telethon,
    extract_reactions,
    get_media_type_for,
    get_message_kind,
    get_sender_kind,
    get_sender_name,
    get_service_action_payload,
    get_service_action_raw,
    get_service_action_semantic,
)

# Re-exported (explicit `as` alias) from ``collector_resolve`` (#1045) — the
# resolve logic moved there, but ``collection_queue`` / CLI / tests still import
# this label from here.
from src.telegram.collector_resolve import (
    RESOLVE_USERNAME_OPERATION as RESOLVE_USERNAME_OPERATION,
)
from src.telegram.collector_resolve import (
    ResolveOutcome,
    resolve_channel_entity,
)
from src.telegram.flood_wait import (
    HandledFloodWaitError,
    coerce_flood_wait_seconds,
    is_transient_flood_wait_seconds,
    run_with_flood_wait,
    run_with_flood_wait_retry,
    sleep_for_flood_wait_seconds,
)
from src.telegram.notifier import Notifier
from src.telegram.rate_limiter import (
    GLOBAL_RESOLVE_BACKOFF_THRESHOLD_SEC,
    UsernameResolveFloodWaitDeferredError,
    UsernameResolveRateLimitedError,
)

# Re-exported for collection_queue / CLI consumers that import it from here.
from src.telegram.rate_limiter import (
    RESOLVE_USERNAME_BACKOFF_BUFFER_SEC as RESOLVE_USERNAME_BACKOFF_BUFFER_SEC,
)
from src.utils.safe_logging import mask_phone

logger = logging.getLogger(__name__)

# Global cross-account resolve backoff. Only a *long* resolve flood freezes
# live resolves for every account; short floods are left to normal rotation.
MESSAGE_FLUSH_BATCH_SIZE = 500
PERSISTED_ID_VERIFY_CHUNK_SIZE = 500
STREAM_CLEANUP_TIMEOUT_SEC = 10.0

# Sentinel returned by Collector._acquire_collection_client to tell the
# collection loop to retry (transient flood wait or dialog-prefetch flood) —
# the moral equivalent of the inline `continue` it replaced.
_ACQUIRE_RETRY = object()


class _StreamOutcome:
    """Mutable out-params for ``Collector._stream_channel_messages``.

    The streaming loop can be aborted mid-way by a FloodWait/idle-timeout raised
    from inside the Telethon iterator. These flags are written **in place** (not
    returned) so the caller's ``finally`` still sees them even when the streamer
    raised before returning — exactly the visibility the old ``nonlocal`` block
    had. ``messages_batch`` is shared the same way (the caller passes its list
    and the streamer mutates it in place via ``append``/``clear``).
    """

    __slots__ = ("retire_client", "stop_due_to_persistence_error")

    def __init__(self) -> None:
        self.retire_client = False
        self.stop_due_to_persistence_error = False


# Backward-compatible alias: the resolve outcome and its logic now live in
# ``collector_resolve`` (#1045). Kept under the historical private name so
# existing references and the rich docstring there remain the single source.
_ResolveOutcome = ResolveOutcome


# How far back the notification check re-scans persisted messages so a send that
# failed on an earlier pass is retried (the dedup ledger prevents duplicates).
NOTIFICATION_BACKLOG_LOOKBACK_HOURS = 24.0


def _format_channel_log_name(channel: Channel) -> str:
    username = (channel.username or "").strip().lstrip("@")
    if username:
        return f"@{username}"

    title = (channel.title or "").strip()
    return title or "no username"


class NoActiveStatsClientsError(RuntimeError):
    """Raised when there are no active connected clients for stats collection."""


class NoActiveCollectionClientsError(RuntimeError):
    """Raised when there are no active connected clients for message collection."""


class AllStatsClientsFloodedError(RuntimeError):
    """Raised when all active connected clients are in flood-wait."""

    def __init__(self, retry_after_sec: int, next_available_at: datetime):
        super().__init__(
            "All active clients are flood-waited until "
            f"{next_available_at.isoformat()} (retry in {retry_after_sec}s)"
        )
        self.retry_after_sec = retry_after_sec
        self.next_available_at = next_available_at


class AllCollectionClientsFloodedError(RuntimeError):
    """Raised when all active connected clients are in flood-wait."""

    def __init__(self, retry_after_sec: int, next_available_at: datetime):
        super().__init__(
            "All active clients are flood-waited until "
            f"{next_available_at.isoformat()} (retry in {retry_after_sec}s)"
        )
        self.retry_after_sec = retry_after_sec
        self.next_available_at = next_available_at


class Collector:
    # Kept as a class attribute for backward compatibility; the canonical
    # mapping now lives in ``collector_message_parse`` (#1045).
    _SERVICE_ACTION_SEMANTICS = SERVICE_ACTION_SEMANTICS

    def __init__(
        self,
        pool: ClientPool,
        db: Database,
        config: SchedulerConfig,
        notifier: Notifier | None = None,
        *,
        live_runtime_pause_gate: LiveRuntimePauseGate | None = None,
    ):
        self._pool = pool
        self._db = db
        self._config = config
        self._notifier = notifier
        self._live_runtime_pause_gate = live_runtime_pause_gate
        self._active_collection_count = 0
        self._stats_running = False
        self._stats_all_running = False
        self._cancel_event = asyncio.Event()
        # Stats-only stop signal. STATS_ALL cancellation must NOT use the global
        # _cancel_event, which channel-collect workers also watch — sharing it let
        # a STATS_ALL cancel abort unrelated in-flight collection (audit #835/6).
        self._stats_cancel_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._stats_lock = asyncio.Lock()
        self._stats_all_lock = asyncio.Lock()
        self._last_unavailability_log: tuple[str, str | int | None, datetime | None] | None = None

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

    @property
    def is_stats_running(self) -> bool:
        return self._stats_running or self._stats_all_running

    @property
    def delay_between_channels_sec(self) -> int:
        return self._config.delay_between_channels_sec

    def stats_worker_count(self) -> int:
        configured = max(1, int(getattr(self._config, "stats_worker_count", 3) or 1))
        connected = len(getattr(self._pool, "clients", {}) or {})
        if connected <= 0:
            return configured
        return max(1, min(configured, connected))

    def collection_worker_count(self) -> int:
        configured = int(getattr(self._config, "collection_worker_count", 0) or 0)
        connected = len(getattr(self._pool, "clients", {}) or {})
        if configured <= 0:
            return max(1, min(connected, 10)) if connected > 0 else 1
        if connected <= 0:
            return max(1, configured)
        return max(1, min(configured, connected))

    async def available_collection_slot_count(self) -> int:
        counter = getattr(self._pool, "available_collection_client_count", None)
        if callable(counter):
            try:
                count = counter()
                if asyncio.iscoroutine(count):
                    count = await count
                return max(0, int(count))
            except Exception:
                logger.debug("Failed to read available collection client slots", exc_info=True)
        return self.collection_worker_count()

    async def available_collection_worker_count(self) -> int:
        configured = int(getattr(self._config, "collection_worker_count", 0) or 0)
        connected = len(getattr(self._pool, "clients", {}) or {})
        available = await self.available_collection_slot_count()
        if available > 0:
            limit = configured if configured > 0 else 10
            return max(1, min(limit, available))
        if connected <= 0:
            return max(1, configured) if configured > 0 else 1
        return 1

    def stats_all_worker_count(self) -> int:
        configured = max(1, int(getattr(self._config, "stats_all_worker_count", 1) or 1))
        connected = len(getattr(self._pool, "clients", {}) or {})
        if connected <= 0:
            return configured
        return max(1, min(configured, connected))

    async def available_stats_worker_count(self) -> int:
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

    async def available_stats_all_worker_count(self) -> int:
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

    def set_stats_all_running(self, running: bool) -> None:
        self._stats_all_running = running

    async def get_stats_availability(self):
        return await self.get_collection_availability()

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

    async def _load_min_subscribers_filter(self) -> int:
        return parse_int_setting(
            await self._db.get_setting("min_subscribers_filter"),
            setting_name="min_subscribers_filter",
            default=0,
            logger=logger,
        )

    async def _is_auto_delete_enabled(self) -> bool:
        """Check if auto_delete_on_collect is enabled (cached per collection run)."""
        cached = getattr(self, "_auto_delete_cached", None)
        if cached is not None:
            return cached
        setting = await self._db.get_setting("auto_delete_on_collect")
        result = setting == "1"
        self._auto_delete_cached = result
        return result

    async def _handle_meta_change_review(
        self,
        channel: Channel,
        new_username: str | None,
        new_title: str | None,
        *,
        log_prefix: str,
    ) -> bool:
        """Apply meta-change detection: update channel meta, merge filter
        flags, create a pending rename event for user review.

        Returns True if a meta change was detected (caller should stop
        further processing for this channel in the collect path). Returns
        False if nothing changed.

        Does NOT call _maybe_auto_delete: messages are preserved until
        the user explicitly resolves the rename event on /channels/renames.
        """
        meta_flags: list[str] = []
        if new_username != channel.username:
            meta_flags.append("username_changed")
        if new_title != channel.title:
            meta_flags.append("title_changed")
        if not meta_flags:
            return False

        logger.warning(
            "%s: channel %d meta changed (%s → %s / %s → %s), "
            "marking filtered %s — awaiting user decision",
            log_prefix,
            channel.channel_id,
            channel.username,
            new_username,
            channel.title,
            new_title,
            meta_flags,
        )
        existing_flags = {
            f.strip()
            for f in (channel.filter_flags or "").split(",")
            if f.strip()
        }
        # Order: 1) create event (idempotent), 2) mark filtered,
        # 3) update meta LAST.  A crash before step 3 means the next
        # run re-detects the same diff — create_rename_event is
        # idempotent so no duplicate is created.
        await self._db.create_rename_event(
            channel_id=channel.channel_id,
            old_title=channel.title,
            new_title=new_title,
            old_username=channel.username,
            new_username=new_username,
        )
        await self._db.set_channels_filtered_bulk(
            [(channel.channel_id, ",".join(sorted(existing_flags | set(meta_flags))))]
        )
        await self._db.update_channel_meta(
            channel.channel_id, username=new_username, title=new_title
        )
        return True

    async def _maybe_auto_delete(self, channel_id: int) -> bool:
        """Purge messages from filtered channel if auto_delete_on_collect is enabled."""
        if not await self._is_auto_delete_enabled():
            return False
        try:
            deleted = await self._db.delete_messages_for_channel(channel_id)
            logger.info(
                "Auto-purged %d messages from filtered channel %d during collection",
                deleted,
                channel_id,
            )
            return True
        except Exception:
            logger.exception("Failed to auto-purge channel %d", channel_id)
            return False

    async def _resolve_channel_input_entity(
        self,
        session,
        *,
        channel_id: int,
        username: str,
        phone: str,
        cache_only: bool = False,
    ):
        try:
            return await session.resolve_cached_input_entity(PeerChannel(channel_id))
        except (AttributeError, ValueError, TypeError):
            pass

        try:
            cached = await session.resolve_cached_input_entity(username)
        except (AttributeError, ValueError, TypeError):
            cached = None
        if cached is not None and getattr(cached, "channel_id", None) == channel_id:
            return cached

        if cache_only:
            # Resolve backoff on this account (#552/#790): the cache missed and
            # we must not hit the live API on this phone. The caller decides
            # whether to rotate to another account or defer the channel.
            raise UsernameResolveRateLimitedError(
                phone, self._get_resolve_username_backoff_remaining_sec(phone)
            )

        raw_client = None
        if isinstance(getattr(type(session), "raw_client", None), property):
            raw_client = session.raw_client
        live_input_resolver = getattr(raw_client, "get_input_entity", None)
        def _session_resolver(name: str):
            if getattr(type(session), name, None) is not None:
                return getattr(session, name)
            return vars(session).get(name)

        if live_input_resolver is None:
            live_input_resolver = _session_resolver("get_input_entity")
        if live_input_resolver is None:
            live_input_resolver = _session_resolver("resolve_input_entity")
        if live_input_resolver is None:
            live_input_resolver = session.get_entity

        # Only the live API fallback is rate-limited (#551) — the cached
        # resolves above are free and must stay free. The shared pool guard
        # enforces the per-account budget and the global FloodWait backoff, so
        # a burned window defers the channel instead of firing the call that
        # would trigger a multi-hour flood wait.
        return await self._pool.run_live_username_resolve(
            lambda: live_input_resolver(username),
            phone=phone,
            username=username,
            operation=RESOLVE_USERNAME_OPERATION,
            logger_=logger,
            timeout=30.0,
        )

    async def collect_single_channel(
        self,
        channel: Channel,
        *,
        full: bool = False,
        progress_callback: Callable[[int], Awaitable[None]] | None = None,
        force: bool = False,
        cancel_event: asyncio.Event | None = None,
    ) -> int:
        """Collect messages from a single channel. If full=True, reset last_collected_id to 0.

        This is the canonical entry point for is_filtered checks in the
        collection path.  Other callers (CollectionQueue, CLI, web routes)
        may also guard against filtered channels earlier for better UX,
        but this check is the authoritative gate.
        """
        if channel.is_filtered and not force:
            logger.info(
                "Skipping collection for channel %d: channel is filtered",
                channel.channel_id,
            )
            return 0
        if self._should_clear_collection_cancel_on_start(cancel_event):
            self._cancel_event.clear()
        self._auto_delete_cached = None
        self._active_collection_count = int(getattr(self, "_active_collection_count", 0) or 0) + 1
        try:
            if full:
                channel = Channel(**{**channel.model_dump(), "last_collected_id": 0})

            min_subs = await self._load_min_subscribers_filter()
            return await self._collect_channel(
                channel,
                progress_callback=progress_callback,
                force=force,
                min_subs=min_subs,
                cancel_event=cancel_event,
            )
        finally:
            self._active_collection_count = max(0, self._active_collection_count - 1)

    async def collect_all_channels(self) -> dict:
        """Collect messages from all active channels. Returns stats."""
        async with self._lock:
            self._cancel_event.clear()
            self._auto_delete_cached = None
            self._active_collection_count += 1
            stats = {"channels": 0, "messages": 0, "errors": 0}

            try:
                channels = await self._db.get_channels(active_only=True, include_filtered=False)
                if not channels:
                    logger.info("No active unfiltered channels to collect")
                    return stats
                logger.info("Found %d active unfiltered channels to collect", len(channels))

                min_subs = await self._load_min_subscribers_filter()

                for channel in channels:
                    if self._cancel_event.is_set():
                        logger.info("Collection cancelled")
                        break
                    try:
                        collected = await self._collect_channel(channel, min_subs=min_subs)
                        stats["channels"] += 1
                        stats["messages"] += collected
                        await asyncio.sleep(self._config.delay_between_channels_sec)
                    except (AllCollectionClientsFloodedError, NoActiveCollectionClientsError) as e:
                        logger.error("Stopping collection: %s", e)
                        stats["errors"] += 1
                        break
                    except UsernameResolveFloodWaitDeferredError as e:
                        logger.warning(
                            "Channel %s deferred until %s (resolve flood backoff active); "
                            "continuing run cache-only",
                            channel.channel_id,
                            e.next_available_at.isoformat(),
                        )
                        stats["deferred"] = stats.get("deferred", 0) + 1
                        continue
                    except UsernameResolveRateLimitedError as e:
                        logger.warning(
                            "Channel %s deferred until %s: resolve_username rate-limited on %s",
                            channel.channel_id,
                            e.run_after_with_buffer().isoformat(),
                            e.phone,
                        )
                        stats["deferred"] = stats.get("deferred", 0) + 1
                        continue
                    except Exception as e:
                        logger.error("Error collecting channel %s: %s", channel.channel_id, e)
                        stats["errors"] += 1
            finally:
                self._active_collection_count = max(0, self._active_collection_count - 1)

        logger.info(
            "Collection done: %d channels, %d messages, %d errors",
            stats["channels"],
            stats["messages"],
            stats["errors"],
        )
        return stats

    @staticmethod
    def _get_media_type(msg) -> str | None:
        """Determine media type from a Telethon message."""
        return get_media_type_for(msg)

    async def _release_collection_client(
        self,
        phone: str,
        session,
        *,
        retire: bool = False,
    ) -> None:
        if not retire:
            await self._pool.release_client(phone)
            return

        logger.warning(
            "Retiring Telegram client for %s because a message stream read did not finish cancellation",
            phone,
        )
        pool_dict = getattr(self._pool, "__dict__", {})
        remove_client = None
        if "remove_client" in pool_dict or hasattr(type(self._pool), "remove_client"):
            remove_client = getattr(self._pool, "remove_client", None)
        if remove_client is not None:
            try:
                result = remove_client(phone)
                if isawaitable(result):
                    await asyncio.wait_for(result, timeout=STREAM_CLEANUP_TIMEOUT_SEC)
                return
            except asyncio.TimeoutError:
                logger.warning(
                    "Timed out removing dirty Telegram client for %s after %.1fs; "
                    "disconnecting and releasing the lease",
                    phone,
                    STREAM_CLEANUP_TIMEOUT_SEC,
                )
            except Exception:
                logger.debug("Failed to remove dirty Telegram client for %s", phone, exc_info=True)

        raw_client = getattr(session, "raw_client", None)
        disconnect = getattr(raw_client, "disconnect", None) if raw_client is not None else None
        if disconnect is None:
            disconnect = getattr(session, "disconnect", None)
        if disconnect is not None:
            try:
                result = disconnect()
                if isawaitable(result):
                    await asyncio.wait_for(result, timeout=STREAM_CLEANUP_TIMEOUT_SEC)
            except Exception:
                logger.debug("Failed to disconnect dirty Telegram client for %s", phone, exc_info=True)
        await self._pool.release_client(phone)

    async def _acquire_collection_client(self, channel: Channel, attempted_resolve_phones: set[str]):
        """Pick a client for `channel`, adapt its session, decide cache-only resolve
        mode, and warm the PeerChannel dialog cache once per phone.

        Returns ``(session, phone, resolve_cache_only)`` on success, or
        ``_ACQUIRE_RETRY`` to tell the collection loop to retry (transient flood
        wait or a dialog-prefetch flood). Raises the collection-unavailability
        error when no client can be used.
        """
        channel_id = channel.channel_id

        # For private groups (no username):
        #   1. preferred_phone from DB (persists across restarts)
        #   2. in-memory map built by warm_all_dialogs()
        #   3. if warming is still in progress — wait, then re-check map
        #   4. fall back to any available phone (new channel, no info yet)
        if not channel.username:
            preferred = channel.preferred_phone or self._pool.get_phone_for_channel(
                channel_id
            )
            if not preferred and self._pool.is_warming():
                await self._pool.wait_for_warm(timeout=30.0)
                preferred = self._pool.get_phone_for_channel(channel_id)
            if preferred:
                result = await self._pool.get_client_by_phone(preferred)
            else:
                result = await self._pool.get_available_client()
        elif attempted_resolve_phones:
            result = await self._pool.get_available_client(
                exclude_phones=set(attempted_resolve_phones)
            )
        else:
            result = await self._pool.get_available_client()

        if result is None:
            availability = await self.get_collection_availability()
            if await self._wait_for_transient_collection_flood(availability):
                return _ACQUIRE_RETRY
            await self._raise_collection_unavailability(availability)

        session, phone = result
        self._reset_collection_unavailability_log()
        session = adapt_transport_session(session, disconnect_on_close=False)

        # Per-account resolve backoff (#552/#790): while this phone is in a
        # flood backoff the channel runs in cache-only mode on it — a cached
        # InputPeer still collects for free, a cache miss rotates to another
        # account or defers the channel (handled at the resolve call site).
        resolve_cache_only = False
        if channel.username:
            backoff_remaining_sec = self._get_resolve_username_backoff_remaining_sec(
                phone
            )
            if backoff_remaining_sec > 0:
                resolve_cache_only = True
                logger.warning(
                    "Channel %d (%s): %s backoff active on %s for %ss — "
                    "cache-only resolve",
                    channel_id,
                    channel.username,
                    RESOLVE_USERNAME_OPERATION,
                    mask_phone(phone),
                    backoff_remaining_sec,
                )
        # Populate entity cache when using PeerChannel
        # (StringSession loses cache between restarts).
        # Only needed once per process lifetime per phone —
        # the in-memory cache persists.
        if not channel.username and not self._pool.is_dialogs_fetched(phone):
            try:
                await run_with_flood_wait_retry(
                    lambda: session.warm_dialog_cache(),
                    operation="collect_channel_warm_dialog_cache",
                    phone=phone,
                    pool=self._pool,
                    logger_=logger,
                    timeout=30.0,
                )
                self._pool.mark_dialogs_fetched(phone)
            except HandledFloodWaitError as exc:
                logger.warning("Failed to prefetch dialogs for %s: %s", phone, exc.info.detail)
                await self._pool.release_client(phone)
                return _ACQUIRE_RETRY
            except Exception as e:
                logger.warning("Failed to prefetch dialogs for %s: %s", phone, e)

        return session, phone, resolve_cache_only

    async def _handle_post_collection_flood(
        self,
        channel: Channel,
        phone: str,
        flood_wait_sec: int,
        flood_wait_operation: str | None,
        attempted_resolve_phones: set[str],
        total_collected: int,
        collected_count: int,
    ) -> tuple[str, int, Channel]:
        """Decide what to do after a collection pass ended on a FloodWait.

        Returns ``(kind, total_collected, channel)`` where kind is:
          * ``"continue"`` — retry the collection loop with the returned
            (possibly advanced) total_collected and (possibly re-read) channel;
          * ``"return"`` — stop; caller returns ``total_collected + collected_count``.
        Raises ``UsernameResolveFloodWaitDeferredError`` when a long resolve flood
        must defer the channel and no free account can take it.
        """
        channel_id = channel.channel_id
        if flood_wait_operation == RESOLVE_USERNAME_OPERATION:
            if is_transient_flood_wait_seconds(flood_wait_sec):
                await sleep_for_flood_wait_seconds(
                    flood_wait_sec,
                    operation=RESOLVE_USERNAME_OPERATION,
                    phone=phone,
                    logger_=logger,
                )
                return ("continue", total_collected, channel)
            # Only a *long* resolve flood freezes live resolves — and
            # only for the flooded account (#552/#790). A medium resolve
            # flood skips just this channel so one blip does not stall
            # the whole pool.
            if flood_wait_sec <= GLOBAL_RESOLVE_BACKOFF_THRESHOLD_SEC:
                logger.warning(
                    "%s short FloodWait %ss on %s; skipping channel %d "
                    "(no backoff)",
                    RESOLVE_USERNAME_OPERATION,
                    flood_wait_sec,
                    phone,
                    channel_id,
                )
                return ("return", total_collected, channel)
            # The pool already recorded this long flood for the phone
            # inside run_live_username_resolve (>300s threshold); read
            # back the active deadline rather than re-setting a second
            # window (#785).
            next_available_at = self._pool.get_resolve_username_backoff_until(phone)
            if next_available_at is None and flood_wait_sec > GLOBAL_RESOLVE_BACKOFF_THRESHOLD_SEC:
                next_available_at = self._pool.set_resolve_username_backoff(
                    flood_wait_sec, phone=phone
                )
                persist_backoff = getattr(self._pool, "persist_resolve_username_backoff", None)
                if callable(persist_backoff):
                    maybe_awaitable = persist_backoff()
                    if isawaitable(maybe_awaitable):
                        await maybe_awaitable
                logger.warning(
                    "%s: defensive backoff set to %s on %s (was None, flood_wait_sec=%d)",
                    RESOLVE_USERNAME_OPERATION,
                    next_available_at.isoformat(),
                    mask_phone(phone),
                    flood_wait_sec,
                )
            if next_available_at is None:
                next_available_at = datetime.now(timezone.utc) + timedelta(
                    seconds=flood_wait_sec
                )
            # Rotate the channel to a free account within the same pass
            # when one exists; defer only when every account is in
            # backoff (#790).
            attempted_resolve_phones.add(phone)
            if await self._can_rotate_resolve(attempted_resolve_phones):
                logger.warning(
                    "%s got FloodWait %ss on %s; backoff on that account "
                    "until %s — rotating channel %d to another account",
                    RESOLVE_USERNAME_OPERATION,
                    flood_wait_sec,
                    mask_phone(phone),
                    next_available_at.isoformat(),
                    channel_id,
                )
                return ("continue", total_collected + collected_count, channel)
            capable_at = await self._next_resolve_capable_at()
            if capable_at is not None:
                next_available_at = capable_at
            logger.warning(
                "%s got FloodWait %ss on %s; pausing username resolves on "
                "that account until %s (no free account to rotate to)",
                RESOLVE_USERNAME_OPERATION,
                flood_wait_sec,
                mask_phone(phone),
                next_available_at.isoformat(),
            )
            if self._notifier:
                await self._notifier.notify(
                    f"FloodWait {flood_wait_sec}s on {phone}, "
                    f"channel {channel_id} — pausing username resolves on "
                    f"this account until {next_available_at.isoformat()}"
                )
            # wait_seconds must describe the same deadline as
            # next_available_at (the aggregate min across accounts may
            # be earlier than this phone's own flood) — callers and the
            # stats path surface both, so they cannot diverge.
            defer_wait_sec = max(
                0,
                int(
                    (
                        next_available_at - datetime.now(timezone.utc)
                    ).total_seconds()
                ),
            )
            raise UsernameResolveFloodWaitDeferredError(
                wait_seconds=defer_wait_sec,
                next_available_at=next_available_at,
            )

        if self._notifier and flood_wait_sec > self._config.max_flood_wait_sec:
            await self._notifier.notify(
                f"FloodWait {flood_wait_sec}s on {phone}, "
                f"channel {channel_id} — rotating to another account"
            )
        # Re-read channel from DB to get updated last_collected_id.
        # Use get_channel_by_pk (no filtering) — collection
        # already started, so we must finish even if the
        # channel was filtered in the meantime.
        updated = None
        if channel.id is not None:
            updated = await self._db.get_channel_by_pk(channel.id)
        if updated:
            return ("continue", total_collected + collected_count, updated)
        return ("return", total_collected, channel)

    async def _stream_channel_messages(
        self,
        *,
        session,
        entity,
        min_id: int,
        limit,
        channel: Channel,
        channel_id: int,
        cancel_event: asyncio.Event | None,
        messages_batch: list[Message],
        flush_batch: Callable[[list[Message]], Awaitable[bool]],
        outcome: _StreamOutcome,
    ) -> None:
        """Stream messages from `entity` into `messages_batch`, flushing at the
        batch boundary via `flush_batch`. Mutates `messages_batch` in place and
        records abort flags on `outcome` so partial progress survives a FloodWait
        or idle-timeout raised mid-stream (see _StreamOutcome)."""
        # Idle timeout caps the wait for the *next* post, not the whole
        # channel: a healthy channel that streams post-by-post is never
        # aborted, only a stream gone silent (dead socket) is. 0/negative
        # disables the cap (wait_for(timeout=None) == a bare await).
        # asyncio.TimeoutError propagates to the outer handler, which
        # releases the client.
        configured = self._config.collection_stream_timeout_sec
        idle_timeout = configured if configured and configured > 0 else None
        stream = session.stream_messages(
            entity,
            min_id=min_id,
            limit=limit,
            reverse=True,
            wait_time=self._config.delay_between_requests_sec,
        )
        agen = stream.__aiter__()
        stream_close_allowed = True

        async def _next_message():
            nonlocal stream_close_allowed
            if idle_timeout is None:
                return await agen.__anext__()

            next_task = asyncio.create_task(agen.__anext__())

            def _consume_late_next_task(task: asyncio.Task) -> None:
                try:
                    exc = task.exception()
                except asyncio.CancelledError:
                    return
                if exc is not None:
                    logger.debug(
                        "Channel %d (%s): message stream next failed after timeout",
                        channel_id,
                        channel.username or channel.title or "",
                        exc_info=(type(exc), exc, exc.__traceback__),
                    )

            async def _cancel_next_task(reason: str) -> None:
                nonlocal stream_close_allowed
                if next_task.done():
                    return
                stream_close_allowed = False
                next_task.cancel()
                next_task.add_done_callback(_consume_late_next_task)
                done, _ = await asyncio.wait(
                    {next_task}, timeout=STREAM_CLEANUP_TIMEOUT_SEC
                )
                if next_task in done:
                    stream_close_allowed = True
                else:
                    outcome.retire_client = True
                    logger.warning(
                        "Channel %d (%s): message stream %s timed out after %.1fs",
                        channel_id,
                        channel.username or channel.title or "",
                        reason,
                        STREAM_CLEANUP_TIMEOUT_SEC,
                    )

            try:
                done, _ = await asyncio.wait({next_task}, timeout=idle_timeout)
            except asyncio.CancelledError:
                await _cancel_next_task("next-cancel on collector cancellation")
                raise
            if next_task in done:
                return await next_task

            await _cancel_next_task("next-cancel")
            raise asyncio.TimeoutError

        try:
            while True:
                msg = await _next_message()

                message = build_message_from_telethon(msg, channel_id)
                messages_batch.append(message)

                if (
                    len(messages_batch) % 10 == 0
                    and self._is_collection_cancelled(cancel_event)
                ):
                    logger.info("Channel %d collection interrupted", channel_id)
                    break

                if len(messages_batch) >= MESSAGE_FLUSH_BATCH_SIZE:
                    if not await self._channel_still_exists(channel_id):
                        messages_batch.clear()
                        break
                    if not await flush_batch(messages_batch):
                        outcome.stop_due_to_persistence_error = True
                        break
                    messages_batch.clear()
                    if self._is_collection_cancelled(cancel_event):
                        break
        except StopAsyncIteration:
            pass
        finally:
            aclose = getattr(agen, "aclose", None)
            if aclose is not None and stream_close_allowed:
                try:
                    close_result = aclose()
                    if isawaitable(close_result):
                        await asyncio.wait_for(
                            close_result, timeout=STREAM_CLEANUP_TIMEOUT_SEC
                        )
                except asyncio.TimeoutError:
                    logger.warning(
                        "Channel %d (%s): message stream close timed out after %.1fs",
                        channel_id,
                        channel.username or channel.title or "",
                        STREAM_CLEANUP_TIMEOUT_SEC,
                    )
                except Exception:
                    logger.debug(
                        "Channel %d (%s): message stream close failed",
                        channel_id,
                        channel.username or channel.title or "",
                        exc_info=True,
                    )
            elif aclose is not None:
                logger.debug(
                    "Channel %d (%s): skipping message stream close because "
                    "a pending next read did not finish cancellation",
                    channel_id,
                    channel.username or channel.title or "",
                )

    async def _verify_persisted_ids(self, channel_id: int, expected_ids: set[int]) -> set[int]:
        """Return which of ``expected_ids`` are actually present in ``messages``.

        Queried in ``PERSISTED_ID_VERIFY_CHUNK_SIZE``-sized ``IN (...)`` chunks to
        keep the SQL parameter count bounded. Extracted from ``_flush_batch``
        (#1045) so the flush path's control flow stays readable; behavior is
        unchanged.
        """
        persisted_ids: set[int] = set()
        expected_id_list = list(expected_ids)
        for start in range(0, len(expected_id_list), PERSISTED_ID_VERIFY_CHUNK_SIZE):
            chunk = expected_id_list[start : start + PERSISTED_ID_VERIFY_CHUNK_SIZE]
            placeholders = ",".join("?" for _ in chunk)
            cur = await self._db.execute(
                f"SELECT message_id FROM messages WHERE channel_id = ? "
                f"AND message_id IN ({placeholders})",
                (channel_id, *chunk),
            )
            rows = await cur.fetchall()
            persisted_ids.update(row["message_id"] for row in rows)
        return persisted_ids

    async def _finalize_collection_pass(
        self,
        *,
        channel_id: int,
        messages_batch: list[Message],
        flush_batch: Callable[[list[Message]], Awaitable[bool]],
        get_persisted_max_msg_id: Callable[[], int],
        min_id: int,
        phone: str,
        session,
        stream_outcome: _StreamOutcome,
    ) -> bool:
        """Run the per-pass cleanup that used to live in ``_collect_channel``'s
        ``finally`` (#1045): flush any leftover batch, advance
        ``last_collected_id``, and release/retire the client. Each step is guarded
        independently so a failure in one still runs the others — behavior is
        unchanged. Returns the resulting ``stop_due_to_persistence_error`` flag,
        seeded from a mid-stream failure recorded on ``stream_outcome``.

        ``get_persisted_max_msg_id`` is read **after** the leftover flush so it
        reflects the id the flush just advanced (the value lives in the caller's
        closure that ``flush_batch`` mutates), exactly as the old inline
        ``finally`` observed the ``nonlocal``.
        """
        # Carry over a mid-stream persistence failure (the streamer set it on
        # `stream_outcome`); the leftover-flush below may overwrite it.
        stop_due_to_persistence_error = stream_outcome.stop_due_to_persistence_error
        # Flush remaining messages — each operation is protected independently so
        # a failure in one doesn't prevent the other from executing.
        try:
            if messages_batch:
                if not await self._channel_still_exists(channel_id):
                    messages_batch = []
                else:
                    stop_due_to_persistence_error = not await flush_batch(messages_batch)
        except Exception as flush_err:
            logger.error(
                "Failed to flush %d messages for channel %d: %s",
                len(messages_batch),
                channel_id,
                flush_err,
            )
            stop_due_to_persistence_error = True
        persisted_max_msg_id = get_persisted_max_msg_id()
        try:
            if persisted_max_msg_id > min_id and await self._channel_still_exists(channel_id):
                await self._db.update_channel_last_id(channel_id, persisted_max_msg_id)
        except Exception as update_err:
            logger.error(
                "Failed to update last_collected_id for " "channel %d: %s",
                channel_id,
                update_err,
            )
        await self._release_collection_client(
            phone,
            session,
            retire=stream_outcome.retire_client,
        )
        return stop_due_to_persistence_error

    async def _collect_channel(
        self,
        channel: Channel,
        progress_callback: Callable[[int], Awaitable[None]] | None = None,
        force: bool = False,
        min_subs: int = 0,
        cancel_event: asyncio.Event | None = None,
    ) -> int:
        """Collect new messages from a single channel. Returns count."""
        total_collected = 0
        # Accounts that already failed a live resolve for this channel in this
        # pass (resolve backoff / limiter / fresh long flood). Used to rotate
        # the channel to a different account instead of deferring it while a
        # free account exists (#790).
        attempted_resolve_phones: set[str] = set()

        while True:
            if self._is_collection_cancelled(cancel_event):
                return total_collected

            channel_id = channel.channel_id
            min_id = channel.last_collected_id

            acquired = await self._acquire_collection_client(channel, attempted_resolve_phones)
            if acquired is _ACQUIRE_RETRY:
                continue
            session, phone, resolve_cache_only = acquired

            messages_batch: list[Message] = []
            # `all_messages` only retains message objects when notifications are
            # enabled (incremental runs, bounded by min_id). On first-run for huge
            # channels we keep just a count to avoid unbounded growth / OOM (#633).
            all_messages: list[Message] = []
            collected_count = 0
            saw_topic_message = False
            persisted_max_msg_id = min_id
            flood_wait_sec: int | None = None
            flood_wait_operation: str | None = None
            stop_due_to_persistence_error = False
            stream_idle_timeout = False
            stream_outcome = _StreamOutcome()

            is_first_run = channel.last_collected_id == 0
            should_notify = self._notifier is not None and not is_first_run

            async def _check_collected_notification_queries() -> None:
                nonlocal all_messages
                # Auto-translate runs regardless of notifications (audit #836/6).
                await self._maybe_enqueue_auto_translate()
                if not should_notify or not all_messages:
                    return
                for message in all_messages:
                    message.channel_username = channel.username
                await self._check_notification_queries(all_messages)
                all_messages = []

            limit = None
            channel_log_name = _format_channel_log_name(channel)
            logger.info(
                "Collecting channel %d (%s), first_run=%s, min_id=%d, limit=%s, account=%s",
                channel_id,
                channel_log_name,
                is_first_run,
                min_id,
                limit,
                mask_phone(phone),
            )

            async def _flush_batch(batch: list[Message]) -> bool:
                nonlocal persisted_max_msg_id, collected_count, saw_topic_message
                if not batch:
                    return True

                expected_ids = {message.message_id for message in batch}
                try:
                    await self._db.insert_messages_batch(batch)
                except DatabaseBusyError:
                    # Transient lock — not a real persistence failure. Stop this
                    # cycle cleanly; last_collected_id is untouched so the batch
                    # is re-collected next time. Avoids the misleading
                    # "Failed to persist N/N messages" error.
                    logger.warning(
                        "Channel %d (%s): DB busy during flush; will retry next cycle",
                        channel_id,
                        channel_log_name,
                    )
                    return False
                persisted_ids = await self._verify_persisted_ids(channel_id, expected_ids)
                missing_ids = expected_ids - persisted_ids
                if missing_ids:
                    logger.error(
                        "Failed to persist %d/%d messages for channel %d (%s); "
                        "last persisted id remains %d",
                        len(missing_ids),
                        len(expected_ids),
                        channel_id,
                        channel_log_name,
                        persisted_max_msg_id,
                    )
                    return False

                persisted_max_msg_id = max(persisted_max_msg_id, max(expected_ids))
                collected_count += len(batch)
                if not saw_topic_message and any(m.topic_id is not None for m in batch):
                    saw_topic_message = True
                # Only retain objects when they are needed downstream (notifications).
                if should_notify:
                    all_messages.extend(batch)
                logger.info(
                    "Channel %d (%s): persisted %d messages, total %d, account=%s",
                    channel_id,
                    channel_log_name,
                    len(batch),
                    collected_count,
                    mask_phone(phone),
                )
                if progress_callback:
                    await progress_callback(total_collected + collected_count)
                if not await self._wait_if_live_runtime_paused(stop_event=cancel_event):
                    return False
                return True

            try:
                outcome = await self._resolve_channel_entity(
                    channel,
                    session,
                    phone,
                    channel_id,
                    resolve_cache_only,
                    attempted_resolve_phones,
                )
                if outcome.channel is not None:
                    channel = outcome.channel
                if outcome.action == "retry":
                    continue
                if outcome.action == "stop":
                    return total_collected
                if outcome.flood_wait_sec is not None:
                    # FloodWait during resolve: record it and fall through to the
                    # finally + post-collection flood handler (skip pre-filters/stream).
                    flood_wait_sec = outcome.flood_wait_sec
                    flood_wait_operation = outcome.flood_wait_operation
                else:
                    entity = outcome.entity
                    if not await self._apply_pre_collection_filters(
                        channel,
                        channel_id,
                        session,
                        entity,
                        is_first_run=is_first_run,
                        force=force,
                        min_subs=min_subs,
                        cancel_event=cancel_event,
                        phone=phone,
                    ):
                        return total_collected

                    await run_with_flood_wait(
                        self._stream_channel_messages(
                            session=session,
                            entity=entity,
                            min_id=min_id,
                            limit=limit,
                            channel=channel,
                            channel_id=channel_id,
                            cancel_event=cancel_event,
                            messages_batch=messages_batch,
                            flush_batch=_flush_batch,
                            outcome=stream_outcome,
                        ),
                        operation="collect_channel_stream_messages",
                        phone=phone,
                        pool=self._pool,
                        logger_=logger,
                    )

            except (UsernameNotOccupiedError, UsernameInvalidError):
                logger.warning(
                    "Channel %d (%s): username not found, deactivating",
                    channel_id,
                    channel.username,
                )
                if channel.id:
                    await self._db.set_channel_active(channel.id, False)
                raise
            except HandledFloodWaitError as exc:
                flood_wait_sec = exc.info.wait_seconds
                flood_wait_operation = flood_wait_operation or exc.info.operation
            except asyncio.TimeoutError:
                logger.warning(
                    "Channel %d (%s): no new message for %.1fs (stream idle); "
                    "stopping this pass and releasing the client",
                    channel_id,
                    channel.username or channel.title or "",
                    self._config.collection_stream_timeout_sec,
                )
                stream_idle_timeout = True
            finally:
                stop_due_to_persistence_error = await self._finalize_collection_pass(
                    channel_id=channel_id,
                    messages_batch=messages_batch,
                    flush_batch=_flush_batch,
                    get_persisted_max_msg_id=lambda: persisted_max_msg_id,
                    min_id=min_id,
                    phone=phone,
                    session=session,
                    stream_outcome=stream_outcome,
                )

            if stop_due_to_persistence_error or stream_idle_timeout:
                # Idle timeout and persistence errors both stop this pass; the
                # finally block above already flushed any pending batch and
                # advanced last_collected_id, so message data is never lost.
                # Run the notification check for the messages that *did* persist
                # this pass — without it their search-query matches would be
                # lost, since last_collected_id has already advanced past them
                # (bug-hunt umbrella #1127). The closure drains its buffer on
                # success, so the call is idempotent and exactly-once here.
                await _check_collected_notification_queries()
                return total_collected + collected_count

            # Handle FloodWait AFTER finally has flushed progress.
            # Rotate regular collection FloodWaits to another account
            # regardless of wait duration — report_flood() was already
            # called, so the next get_available_client() call will skip
            # the flooded account. Username-resolve FloodWaits are handled
            # below with a process-local backoff instead.
            # Only skip the channel if the channel no longer exists in DB.
            if flood_wait_sec is not None:
                kind, total_collected, channel = await self._handle_post_collection_flood(
                    channel,
                    phone,
                    flood_wait_sec,
                    flood_wait_operation,
                    attempted_resolve_phones,
                    total_collected,
                    collected_count,
                )
                if kind == "continue":
                    continue
                return total_collected + collected_count

            await _check_collected_notification_queries()

            await self._post_collection_actions(
                channel_id,
                is_first_run=is_first_run,
                force=force,
                collected_count=collected_count,
                saw_topic_message=saw_topic_message,
            )

            return total_collected + collected_count

    async def _post_collection_actions(
        self,
        channel_id: int,
        *,
        is_first_run: bool,
        force: bool,
        collected_count: int,
        saw_topic_message: bool,
    ) -> None:
        """Post-collection side effects: refresh forum topics when topic messages
        were seen, and apply the first-run low-uniqueness filter. Split out of
        ``_collect_channel`` (#923); no control flow, pure side effects."""
        # Update forum topics in DB if messages with topic_id were collected
        if saw_topic_message:
            cached = await self._db.get_forum_topics(channel_id)
            if not cached:
                try:
                    topics = await self._pool.get_forum_topics(channel_id)
                    if topics:
                        await self._db.upsert_forum_topics(channel_id, topics)
                except Exception as e:
                    logger.warning(
                        "Failed to update forum topics for %d: %s",
                        channel_id,
                        e,
                    )

        if is_first_run and not force and collected_count >= 50:
            cur = await self._db.execute(
                "SELECT COUNT(*) as total,"
                " COUNT(DISTINCT substr(text,1,100)) as uniq"
                " FROM messages WHERE channel_id = ?"
                " AND text IS NOT NULL AND length(text) > 10",
                (channel_id,),
            )
            row = await cur.fetchone()
            if row and row["total"] >= 50:
                ratio = row["uniq"] / row["total"] * 100
                if ratio < LOW_UNIQUENESS_THRESHOLD:
                    await self._db.set_channels_filtered_bulk([(channel_id, "low_uniqueness")])
                    logger.warning(
                        "Post-collection: channel %d low_uniqueness" " %.1f%%, marked filtered",
                        channel_id,
                        ratio,
                    )
                    # Not auto-deleting here: messages were just collected,
                    # channel will be deleted on the next collection run.

    async def _resolve_channel_entity(
        self,
        channel: Channel,
        session,
        phone: str,
        channel_id: int,
        resolve_cache_only: bool,
        attempted_resolve_phones: set[str],
    ) -> ResolveOutcome:
        """Resolve a channel's Telegram entity for collection.

        Thin delegate to :func:`collector_resolve.resolve_channel_entity`; the
        logic (username→numeric fallback, resolve rate-limit rotation,
        preferred-phone rediscovery, flood-wait encoding) lives there (#1045).
        """
        return await resolve_channel_entity(
            self,
            channel,
            session,
            phone,
            channel_id,
            resolve_cache_only,
            attempted_resolve_phones,
        )

    async def _apply_pre_collection_filters(
        self,
        channel: Channel,
        channel_id: int,
        session,
        entity,
        *,
        is_first_run: bool,
        force: bool,
        min_subs: int,
        cancel_event: asyncio.Event | None,
        phone: str,
    ) -> bool:
        """Pre-collection filtering before any messages are streamed: manual
        min-subscriber, subscriber/message ratio, and first-run cross-channel
        duplicate precheck. Returns True to proceed, False when the channel was
        filtered (caller returns). A precheck ``HandledFloodWaitError`` propagates
        to ``_collect_channel``'s outer handler unchanged. Split out of
        ``_collect_channel`` (#923)."""
        # Превентивная фильтрация по subscriber_ratio до загрузки сообщений.
        # Пропускается при force=True (ручной запуск не должен менять фильтр-статус).
        if not force:
            stats_list = await self._db.get_channel_stats(channel_id, limit=1)
            subscriber_count = stats_list[0].subscriber_count if stats_list else None
            if subscriber_count is not None:
                if min_subs > 0 and subscriber_count < min_subs:
                    await self._db.set_channels_filtered_bulk(
                        [(channel_id, "low_subscriber_manual")]
                    )
                    logger.info(
                        "Pre-filter: channel %d subscribers %d < %d, skipping",
                        channel_id,
                        subscriber_count,
                        min_subs,
                    )
                    await self._maybe_auto_delete(channel_id)
                    return False
                cur = await self._db.execute(
                    "SELECT COUNT(*) FROM messages WHERE channel_id = ?",
                    (channel_id,),
                )
                row = await cur.fetchone()
                message_count = row[0] if row else 0
                if message_count > 0:
                    is_broadcast = channel.channel_type in (
                        "channel",
                        "monoforum",
                    )
                    threshold = (
                        LOW_SUBSCRIBER_RATIO_THRESHOLD
                        if is_broadcast
                        else LOW_SUBSCRIBER_RATIO_CHAT_THRESHOLD
                    )
                    ratio = subscriber_count / message_count
                    if ratio < threshold:
                        await self._db.set_channels_filtered_bulk(
                            [(channel_id, "low_subscriber_ratio")]
                        )
                        logger.info(
                            "Pre-filter: channel %d ratio %.4f < %.2f, skipping",
                            channel_id,
                            ratio,
                            threshold,
                        )
                        await self._maybe_auto_delete(channel_id)
                        return False

        # Pre-check: sample 10 posts to detect cross-channel duplicates.
        if is_first_run and not force:
            try:
                sample_prefixes = await run_with_flood_wait(
                    self._precheck_sample(
                        session,
                        entity,
                        PRECHECK_CROSS_DUPE_SAMPLE,
                        cancel_event=cancel_event,
                    ),
                    operation="collect_channel_precheck_sample",
                    phone=phone,
                    pool=self._pool,
                    logger_=logger,
                    timeout=60.0,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "Precheck timed out for channel %d, skipping precheck",
                    channel_id,
                )
                sample_prefixes = []
            # HandledFloodWaitError is intentionally NOT caught here: it propagates
            # to _collect_channel's outer `except HandledFloodWaitError`, which sets
            # flood_wait_sec/operation. operation resolves to exc.info.operation,
            # identical to the old inline assignment this block used to do.
            unique_prefixes = list(dict.fromkeys(sample_prefixes))
            if len(unique_prefixes) >= PRECHECK_CROSS_DUPE_MIN_SAMPLE:
                repo = self._db.filter_repo
                matches = await repo.count_matching_prefixes_in_other_channels(
                    channel_id, unique_prefixes
                )
                if matches / len(unique_prefixes) >= PRECHECK_CROSS_DUPE_RATIO:
                    await self._db.set_channels_filtered_bulk(
                        [(channel_id, "cross_channel_spam")]
                    )
                    logger.info(
                        "Pre-filter: channel %d has %d/%d cross-dupe messages, skipping",
                        channel_id,
                        matches,
                        len(unique_prefixes),
                    )
                    await self._maybe_auto_delete(channel_id)
                    return False
        return True

    async def _discover_phone_for_channel(
        self, channel_id: int, exclude: str
    ) -> str | None:
        """Try all connected phones (except `exclude`) to find one with access to channel_id.

        Used when a private group was added after startup warming and its phone is not
        yet in the pool's channel→phone map. Returns the first phone that can resolve
        the entity, or None if no account has access.
        """
        for candidate in self._pool.connected_phones() - {exclude}:
            result = await self._pool.get_client_by_phone(candidate)
            if result is None:
                continue
            session, p = result
            session = adapt_transport_session(session, disconnect_on_close=False)
            try:
                if not self._pool.is_dialogs_fetched(p):
                    await session.warm_dialog_cache()
                    self._pool.mark_dialogs_fetched(p)
                await session.resolve_entity(PeerChannel(channel_id))
                return p
            except HandledFloodWaitError as exc:
                # Transport already reported the flood (phone+pool were bound on the
                # session), so just log and move on.
                logger.warning(
                    "_discover_phone_for_channel: flood wait on %s: %s", mask_phone(p), exc.info.detail
                )
                continue
            except FloodWaitError as exc:
                # adapt_transport_session() binds neither phone nor pool here, so the
                # transport re-raises the raw FloodWaitError instead of reporting it
                # (handle_flood_wait short-circuits when phone is None). Report it
                # ourselves so the flooded account is marked and rotated out (#495);
                # dropping this — as the "dead branch" cleanup did — silently lost the
                # flood signal on private-group discovery (audit #835/16 regression).
                wait_seconds = coerce_flood_wait_seconds(getattr(exc, "seconds", 0))
                await self._pool.report_flood(p, wait_seconds)
                logger.warning(
                    "_discover_phone_for_channel: flood wait on %s: %ds", mask_phone(p), wait_seconds
                )
                continue
            except Exception:
                continue
            finally:
                await self._pool.release_client(p)
        return None

    async def _precheck_sample(
        self,
        session,
        entity,
        limit: int,
        *,
        cancel_event: asyncio.Event | None = None,
    ) -> list[str]:
        """Sample up to `limit` messages for cross-channel precheck."""
        prefixes: list[str] = []
        async for msg in session.stream_messages(
            entity,
            limit=limit,
            wait_time=self._config.delay_between_requests_sec,
        ):
            if self._is_collection_cancelled(cancel_event):
                break
            if msg.text and len(msg.text) > 10:
                prefixes.append(msg.text[:100])
        return prefixes

    async def _maybe_enqueue_auto_translate(self) -> None:
        """Enqueue a TRANSLATE_BATCH after collection if auto-translate is enabled.

        The settings toggle was previously inert — nothing read it (audit #836/6).
        Deduplicated so at most one TRANSLATE_BATCH is pending/running at a time.
        """
        try:
            from src.models import CollectionTaskType, TranslateBatchTaskPayload
            from src.services.translation_service import (
                TRANSLATION_AUTO_ON_COLLECT,
                TRANSLATION_SOURCE_FILTER,
                TRANSLATION_TARGET_LANG,
            )

            if (await self._db.get_setting(TRANSLATION_AUTO_ON_COLLECT)) != "1":
                return
            tasks = getattr(getattr(self._db, "repos", None), "tasks", None)
            if tasks is None:
                return
            if await tasks.has_active_task(CollectionTaskType.TRANSLATE_BATCH):
                return
            target = (await self._db.get_setting(TRANSLATION_TARGET_LANG)) or "en"
            source_raw = (await self._db.get_setting(TRANSLATION_SOURCE_FILTER)) or ""
            source_filter = [s.strip() for s in source_raw.split(",") if s.strip()]
            await tasks.create_generic_task(
                CollectionTaskType.TRANSLATE_BATCH,
                title="Auto-translate after collect",
                payload=TranslateBatchTaskPayload(target_lang=target, source_filter=source_filter),
            )
        except Exception:
            logger.warning("auto-translate enqueue failed", exc_info=True)

    async def _check_notification_queries(self, messages: list[Message]) -> None:
        """Check messages against active notification queries and send batched notifications."""
        if not self._notifier:
            return

        queries = await self._db.get_notification_queries(active_only=True)
        if not queries:
            return

        from src.services.notification_matcher import NotificationMatcher

        get_channels = getattr(self._db, "get_channels", None)
        channels = []
        if get_channels:
            import inspect

            maybe_channels = get_channels()
            channels = await maybe_channels if inspect.isawaitable(maybe_channels) else maybe_channels
            if not isinstance(channels, list):
                channels = []

        repos = getattr(self._db, "repos", None)
        notified_store = getattr(repos, "notified_messages", None)

        # Re-present recently persisted messages for the involved channels next to
        # the freshly-collected batch, so a notification that failed to send on an
        # earlier pass is retried — the dedup ledger prevents duplicates. This
        # decouples delivery from the forward-only collection cursor (audit #838/1).
        candidates = list(messages)
        if notified_store is not None:
            channel_ids = {m.channel_id for m in messages if m.channel_id is not None}
            get_recent = getattr(getattr(repos, "messages", None), "get_recent_for_channels", None)
            # Only replay the 24h backlog once the ledger already has rows for these channels.
            # On the very first pass after the table is created the ledger is empty, and the
            # backlog would otherwise re-present every already-delivered match as un-notified,
            # producing a duplicate-notification burst on upgrade (the ledger that is supposed to
            # prevent duplicates has nothing recorded yet). Empty ledger => fresh-only candidates,
            # i.e. the pre-#838/1 first-pass behavior; the rescan kicks in once delivery is tracked.
            ledger_seeded = False
            has_any = getattr(notified_store, "has_any", None)
            if channel_ids and callable(has_any):
                try:
                    ledger_seeded = await has_any(list(channel_ids))
                except Exception:
                    logger.warning("notification ledger has_any check failed", exc_info=True)
                    ledger_seeded = False
            if ledger_seeded and channel_ids and callable(get_recent):
                try:
                    backlog = await get_recent(list(channel_ids), NOTIFICATION_BACKLOG_LOOKBACK_HOURS)
                except Exception:
                    logger.warning("notification backlog rescan failed", exc_info=True)
                    backlog = []
                seen = {(m.channel_id, m.message_id) for m in candidates}
                for m in backlog:
                    key = (m.channel_id, m.message_id)
                    if key not in seen:
                        seen.add(key)
                        candidates.append(m)

        matcher = NotificationMatcher(
            self._notifier, channels=channels, notified_store=notified_store
        )
        await matcher.match_and_notify(candidates, queries)

    async def _channel_still_exists(self, channel_id: int) -> bool:
        return await self._db.get_channel_by_channel_id(channel_id) is not None

    async def sample_channel(self, channel_id: int, limit: int = 10) -> list[dict]:
        """Fetch the last `limit` messages from a channel for preview. No DB writes."""
        result = await self._pool.get_available_client()
        if result is None:
            logger.error("No available clients for sample collection")
            return []

        session, phone = result
        session = adapt_transport_session(session, disconnect_on_close=False)
        try:
            if not self._pool.is_dialogs_fetched(phone):
                try:
                    await run_with_flood_wait(
                        session.warm_dialog_cache(),
                        operation="sample_channel_warm_dialog_cache",
                        phone=phone,
                        pool=self._pool,
                        logger_=logger,
                        timeout=30.0,
                    )
                    self._pool.mark_dialogs_fetched(phone)
                except HandledFloodWaitError:
                    return []
                except Exception as e:
                    logger.warning("Failed to prefetch dialogs for %s: %s", phone, e)

            try:
                entity = await run_with_flood_wait(
                    session.resolve_entity(PeerChannel(channel_id)),
                    operation="sample_channel_resolve_entity",
                    phone=phone,
                    pool=self._pool,
                    logger_=logger,
                    timeout=30.0,
                )
            except (asyncio.TimeoutError, ValueError, LookupError):
                logger.warning("Could not resolve entity for channel %d", channel_id)
                return []
            except HandledFloodWaitError:
                return []

            previews: list[dict] = []
            async def _collect_previews() -> None:
                async for msg in session.stream_messages(
                    entity,
                    limit=limit,
                    wait_time=self._config.delay_between_requests_sec,
                ):
                    if self._cancel_event.is_set():
                        break
                    text = msg.text or ""
                    previews.append(
                        {
                            "message_id": msg.id,
                            "date": msg.date,
                            "text_preview": text[:100] if text else None,
                            "media_type": self._get_media_type(msg),
                        }
                    )

            await run_with_flood_wait(
                _collect_previews(),
                operation="sample_channel_stream_messages",
                phone=phone,
                pool=self._pool,
                logger_=logger,
            )

            return previews
        except HandledFloodWaitError:
            return []
        finally:
            await self._pool.release_client(phone)

    async def collect_channel_stats(self, channel: Channel) -> ChannelStats | None:
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

    async def collect_channel_stats_unlocked(self, channel: Channel) -> ChannelStats | None:
        return await self._collect_channel_stats(channel)

    async def _resolve_stats_entity_or_deactivate(
        self,
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
        self, session, entity, phone: str, channel_id: int
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

    async def _collect_channel_stats(self, channel: Channel) -> ChannelStats | None:
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
                            lambda: session.resolve_entity(channel.username),
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

    def _stats_all_channel_limit(self, max_channels: int | None = None) -> int:
        configured = max_channels
        if configured is None:
            configured = int(getattr(self._config, "stats_all_max_channels_per_run", 10) or 10)
        return max(1, int(configured))

    async def _order_stats_all_channels(
        self, channels: list[Channel], *, skip_fresh_hours: int = 0
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

    async def collect_all_stats(self, *, max_channels: int | None = None) -> dict:
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

    # --- Telethon ``msg`` → field helpers ---------------------------------
    # The conversion logic lives in ``collector_message_parse`` as stateless
    # functions (#1045). These thin delegates keep the historical
    # ``Collector._get_*`` / ``_build_message`` surface the test-suite and a few
    # other call sites depend on, without re-implementing anything here.

    @staticmethod
    def _build_message(msg, channel_id: int) -> Message:
        """Build a :class:`Message` from a Telethon message."""
        return build_message_from_telethon(msg, channel_id)

    @staticmethod
    def _extract_reactions(msg) -> str | None:
        """Extract reactions from a Telethon message as JSON string."""
        return extract_reactions(msg)

    @staticmethod
    def _get_sender_name(msg) -> str | None:
        return get_sender_name(msg)

    @classmethod
    def _get_message_kind(cls, msg) -> str:
        return get_message_kind(msg)

    @staticmethod
    def _get_service_action_raw(msg) -> str | None:
        return get_service_action_raw(msg)

    @classmethod
    def _get_service_action_semantic(cls, msg) -> str | None:
        return get_service_action_semantic(msg)

    @staticmethod
    def _get_service_action_payload(msg) -> str | None:
        return get_service_action_payload(msg)

    @staticmethod
    def _get_sender_kind(msg) -> str:
        return get_sender_kind(msg)
