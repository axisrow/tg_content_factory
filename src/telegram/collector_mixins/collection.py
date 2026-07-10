"""Collector message collection and channel filtering helpers (#1137)."""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from inspect import isawaitable
from typing import TYPE_CHECKING

from telethon.errors import FloodWaitError, UsernameInvalidError, UsernameNotOccupiedError
from telethon.tl.types import PeerChannel

from src.database import DatabaseBusyError
from src.filters.criteria import (
    LOW_SUBSCRIBER_RATIO_CHAT_THRESHOLD,
    LOW_SUBSCRIBER_RATIO_THRESHOLD,
    LOW_UNIQUENESS_THRESHOLD,
    PRECHECK_CROSS_DUPE_MIN_SAMPLE,
    PRECHECK_CROSS_DUPE_RATIO,
    PRECHECK_CROSS_DUPE_SAMPLE,
)
from src.models import Channel, Message
from src.settings_utils import parse_int_setting
from src.telegram.backends import adapt_transport_session
from src.telegram.collector_message_parse import get_media_type_for
from src.telegram.collector_resolve import (
    RESOLVE_USERNAME_OPERATION,
    ResolveOutcome,
    resolve_channel_entity,
)
from src.telegram.collector_types import (
    _ACQUIRE_RETRY,
    AllCollectionClientsFloodedError,
    NoActiveCollectionClientsError,
    _format_channel_log_name,
    _StreamOutcome,
)
from src.telegram.flood_wait import (
    HandledFloodWaitError,
    coerce_flood_wait_seconds,
    is_transient_flood_wait_seconds,
    run_with_flood_wait,
    run_with_flood_wait_retry,
    sleep_for_flood_wait_seconds,
)
from src.telegram.rate_limiter import (
    GLOBAL_RESOLVE_BACKOFF_THRESHOLD_SEC,
    UsernameResolveFloodWaitDeferredError,
    UsernameResolveRateLimitedError,
)
from src.utils.safe_logging import mask_phone

if TYPE_CHECKING:
    from typing import Any, Protocol, TypeAlias

    from src.telegram.collector import Collector as _RuntimeCollector

    _RuntimeCollectorType: TypeAlias = type[_RuntimeCollector]

    class Collector(Protocol):
        def __getattribute__(self, name: str) -> Any: ...
        def __setattr__(self, name: str, value: Any) -> None: ...

logger = logging.getLogger("src.telegram.collector")

PERSISTED_ID_VERIFY_CHUNK_SIZE = 500
NOTIFICATION_BACKLOG_LOOKBACK_HOURS = 24.0


class CollectionMixin:
    @property
    def delay_between_channels_sec(self: "Collector") -> int:
        return self._config.delay_between_channels_sec

    def collection_worker_count(self: "Collector") -> int:
        configured = int(getattr(self._config, "collection_worker_count", 0) or 0)
        connected = len(getattr(self._pool, "clients", {}) or {})
        if configured <= 0:
            return max(1, min(connected, 10)) if connected > 0 else 1
        if connected <= 0:
            return max(1, configured)
        return max(1, min(configured, connected))

    async def available_collection_slot_count(self: "Collector") -> int:
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

    async def available_collection_worker_count(self: "Collector") -> int:
        configured = int(getattr(self._config, "collection_worker_count", 0) or 0)
        connected = len(getattr(self._pool, "clients", {}) or {})
        available = await self.available_collection_slot_count()
        if available > 0:
            limit = configured if configured > 0 else 10
            return max(1, min(limit, available))
        if connected <= 0:
            return max(1, configured) if configured > 0 else 1
        return 1

    async def _load_min_subscribers_filter(self: "Collector") -> int:
        return parse_int_setting(
            await self._db.get_setting("min_subscribers_filter"),
            setting_name="min_subscribers_filter",
            default=0,
            logger=logger,
        )

    async def _is_auto_delete_enabled(self: "Collector") -> bool:
        """Check if auto_delete_on_collect is enabled (cached per collection run)."""
        cached = getattr(self, "_auto_delete_cached", None)
        if cached is not None:
            return cached
        setting = await self._db.get_setting("auto_delete_on_collect")
        result = setting == "1"
        self._auto_delete_cached = result
        return result

    async def _handle_meta_change_review(
        self: "Collector",
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

    async def _maybe_auto_delete(self: "Collector", channel_id: int) -> bool:
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
        self: "Collector",
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
        self: "Collector",
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

    async def collect_all_channels(self: "Collector") -> dict:
        """Collect messages from all active channels. Returns stats."""
        async with self._lock:
            self._cancel_event.clear()
            self._auto_delete_cached = None
            self._active_collection_count += 1
            stats: Counter[str] = Counter({"channels": 0, "messages": 0, "errors": 0})

            try:
                channels = await self._db.get_channels(active_only=True, include_filtered=False)
                if not channels:
                    logger.info("No active unfiltered channels to collect")
                    return dict(stats)
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
                        stats["deferred"] += 1
                        continue
                    except UsernameResolveRateLimitedError as e:
                        logger.warning(
                            "Channel %s deferred until %s: resolve_username rate-limited on %s",
                            channel.channel_id,
                            e.run_after_with_buffer().isoformat(),
                            e.phone,
                        )
                        stats["deferred"] += 1
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
        return dict(stats)

    @staticmethod
    def _get_media_type(msg) -> str | None:
        """Determine media type from a Telethon message."""
        return get_media_type_for(msg)

    async def _acquire_collection_client(self: "Collector", channel: Channel, attempted_resolve_phones: set[str]):
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
                if result is None:
                    # The preferred phone alone is unavailable (flood-waited,
                    # in-use, or gone) — rotate to any other available account
                    # rather than treating this as a global "no clients" outage.
                    # Raising here would surface NoActiveCollectionClientsError,
                    # which drains the entire in-memory collection queue in
                    # collection_queue.py — one busy preferred phone must not
                    # wipe every other channel's pending task (#1245).
                    result = await self._pool.get_available_client()
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
        self: "Collector",
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

    async def _verify_persisted_ids(self: "Collector", channel_id: int, expected_ids: set[int]) -> set[int]:
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
        self: "Collector",
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
        self: "Collector",
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

            async def _check_collected_notification_queries(
                channel_username: str | None,
                *,
                should_notify: bool = should_notify,
            ) -> None:
                nonlocal all_messages
                # Auto-translate runs regardless of notifications (audit #836/6).
                await self._maybe_enqueue_auto_translate()
                if not should_notify or not all_messages:
                    return
                for message in all_messages:
                    message.channel_username = channel_username
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

            async def _flush_batch(
                batch: list[Message],
                *,
                channel_id: int = channel_id,
                channel_log_name: str = channel_log_name,
                should_notify: bool = should_notify,
                all_messages: list[Message] = all_messages,
                phone: str = phone,
                total_collected: int = total_collected,
            ) -> bool:
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
                    get_persisted_max_msg_id=lambda: persisted_max_msg_id,  # noqa: B023 - must read post-flush value.
                    min_id=min_id,
                    phone=phone,
                    session=session,
                    stream_outcome=stream_outcome,
                )

            # The finally block above already flushed any pending batch and
            # advanced last_collected_id past the messages that persisted this
            # pass. Run the notification check ONCE for them before any exit
            # branch — the next pass's min_id filter will never re-stream them,
            # so without this their search-query matches would be lost forever.
            # The closure drains its buffer on success, so this is idempotent
            # and exactly-once across every exit path below: stop/idle return
            # (#1127/#1168), FloodWait rotation/return (#1169), normal completion.
            await _check_collected_notification_queries(channel.username)

            if stop_due_to_persistence_error or stream_idle_timeout:
                # Idle timeout and persistence errors both stop this pass;
                # message data is never lost (the finally flushed + advanced).
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

            await self._post_collection_actions(
                channel_id,
                is_first_run=is_first_run,
                force=force,
                collected_count=collected_count,
                saw_topic_message=saw_topic_message,
            )

            return total_collected + collected_count

    async def _post_collection_actions(
        self: "Collector",
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

    async def _resolve_channel_entity(
        self: "Collector",
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
        self: "Collector",
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
        self: "Collector", channel_id: int, exclude: str
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
        self: "Collector",
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

    async def _maybe_enqueue_auto_translate(self: "Collector") -> None:
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

    async def _check_notification_queries(self: "Collector", messages: list[Message]) -> None:
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

    async def _channel_still_exists(self: "Collector", channel_id: int) -> bool:
        return await self._db.get_channel_by_channel_id(channel_id) is not None

    async def sample_channel(self: "Collector", channel_id: int, limit: int = 10) -> list[dict]:
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
