"""Dialog cache, entity resolution, and channel routing for the pool (#1046).

Extracted from the ``ClientPool`` monolith as a composition mixin. This is the
largest connected cluster of the old class — the entity-cache machinery that the
flood-wait rotation and lifecycle mixins feed into:

* **channel ↔ phone routing** — the in-memory ``_channel_phone_map`` plus its
  persisted ``preferred_phone`` mirror (``remember_/forget_channel_phone``),
  used to route a (possibly private) channel to the account that can see it.
* **dialog cache** — the in-process TTL cache and the DB-backed cache, the
  background refresh tasks, and ``warm_all_dialogs`` (entity-cache preheating;
  StringSession loses the entity cache between restarts, see CLAUDE.md
  "Entity cache").
* **entity resolution** — ``resolve_entity_with_warm`` (the single warm-then-
  retry source of truth), ``resolve_channel`` / ``resolve_any_entity``
  (incl. the #858/#875 gone-vs-review detection), ``fetch_channel_meta``,
  ``get_dialogs_for_phone`` / ``get_dialogs``, ``get_forum_topics`` and
  ``leave_channels``.

Behaviour is unchanged by the split: the same methods run on the same single
``self`` and share the per-instance caches the rest of the pool reads.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from telethon.errors import (
    ChannelInvalidError,
    ChannelPrivateError,
    ChatAdminRequiredError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)
from telethon.tl.types import Channel as TLChannel
from telethon.tl.types import ChannelForbidden, Chat, PeerChannel, PeerChat, PeerUser

from src.database.live_accounts import load_live_usable_accounts
from src.parsers import bare_channel_id
from src.telegram.backends import (
    TelegramTransportSession,
    adapt_transport_session,
)
from src.telegram.flood_wait import (
    HandledFloodWaitError,
    is_blocking_flood_wait_until,
    run_with_flood_wait,
)
from src.telegram.utils import normalize_utc

if TYPE_CHECKING:
    from src.database import Database

logger = logging.getLogger(__name__)

WARM_SINGLE_PHONE_TIMEOUT_SEC = 30.0
WARM_ALL_PHONES_TOTAL_SEC = 150.0
WARM_STAGGER_DELAY_SEC = 1.0


@dataclass
class DialogFetchStats:
    raw_dialogs: int = 0
    channels: int = 0
    groups: int = 0
    dms: int = 0
    bots: int = 0
    partial: bool = False


@dataclass
class DialogCacheEntry:
    fetched_at_monotonic: float
    dialogs: list[dict]


class DialogsMixin:
    """Dialog cache, entity resolution, channel↔phone routing, dialog fetch.

    A composition mixin for ``ClientPool``; relies on state the concrete pool
    initialises in ``__init__`` and on acquisition helpers from the lifecycle
    mixin (``get_available_client`` / ``get_client_by_phone`` / ``release_client``
    / ``is_warming`` / ``wait_for_warm``). The annotations below declare that
    contract for the type checker (mirrors ``ResolveGuardMixin``).
    """

    # Provided by ClientPool.__init__.
    _db: Database
    _channel_phone_map: dict[int, str]
    _dialogs_fetched: set[str]
    _dialogs_fetched_at_monotonic: dict[str, float]
    _dialogs_warm_ttl_sec: float
    _monotonic: Callable[[], float]
    _dialogs_cache: dict[tuple[str, str], DialogCacheEntry]
    _dialogs_cache_ttl_sec: float
    _dialogs_db_cache_ttl_sec: float
    _dialog_refresh_tasks: dict[tuple[str, str], asyncio.Task[list[dict]]]
    _warming_task: asyncio.Task | None
    clients: dict[str, object]

    if TYPE_CHECKING:

        def _connected_phones(self) -> set[str]: ...

        async def get_available_client(
            self, *, exclude_phones: set[str] | frozenset[str] = frozenset()
        ) -> tuple[TelegramTransportSession, str] | None: ...

        async def get_client_by_phone(
            self, phone: str, *, wait_for_flood: bool = False
        ) -> tuple[TelegramTransportSession, str] | None: ...

        async def release_client(self, phone: str) -> None: ...

        async def run_live_username_resolve(self, *args: object, **kwargs: object) -> object: ...

        def _is_live_username_peer(self, peer: object) -> bool: ...

    # ------------------------------------------------------------------ #
    # channel ↔ phone routing
    # ------------------------------------------------------------------ #
    def _warm_timestamps(self) -> dict[str, float]:
        """The phone → monotonic-warm-time map, lazily created.

        getattr-tolerant so test doubles that build the pool via ``__new__``
        (and set only ``_dialogs_fetched``) still work — mirrors the
        ``getattr(self, "_mtproto_watchdog", None)`` pattern used elsewhere.
        """
        stamps = getattr(self, "_dialogs_fetched_at_monotonic", None)
        if stamps is None:
            stamps = {}
            self._dialogs_fetched_at_monotonic = stamps
        return stamps

    def is_dialogs_fetched(self, phone: str) -> bool:
        """Return True if the entity cache for this phone is warm and still fresh.

        The warm flag carries a TTL (``_dialogs_warm_ttl_sec``): once it expires
        the flag is treated as cold (and self-cleared) so the next collection
        pass re-warms a long-lived worker's stale entity cache (#1043). Within
        the TTL the answer stays True, so the hot path still warms at most once
        per phone per window — no perf regression. A flag set without a
        timestamp (legacy / direct ``_dialogs_fetched`` mutation) never expires.
        """
        if phone not in self._dialogs_fetched:
            return False
        warmed_at = self._warm_timestamps().get(phone)
        ttl = getattr(self, "_dialogs_warm_ttl_sec", None)
        clock = getattr(self, "_monotonic", time.monotonic)
        if warmed_at is not None and ttl is not None and (clock() - warmed_at) > ttl:
            self.reset_dialogs_warm(phone)
            return False
        return True

    def mark_dialogs_fetched(self, phone: str) -> None:
        """Mark that get_dialogs() has been called for this phone, stamping the
        warm time so the TTL in :meth:`is_dialogs_fetched` can age it out."""
        self._dialogs_fetched.add(phone)
        clock = getattr(self, "_monotonic", time.monotonic)
        self._warm_timestamps()[phone] = clock()

    def reset_dialogs_warm(self, phone: str) -> None:
        """Invalidate the per-phone entity-cache warm flag.

        Called on live re-auth (``add_client`` swaps in a *new* StringSession,
        whose in-memory entity cache is empty) and on teardown
        (``remove_client`` / ``disconnect_all``). A bare reconnect of the same
        session object does NOT call this: the StringSession entity cache
        (``session._entities``, the fallback that resolves numeric
        ``PeerChannel``) survives ``disconnect()`` + ``connect()``, so re-warming
        there would be a needless round-trip and a FloodWait risk (#1043).
        """
        self._dialogs_fetched.discard(phone)
        self._warm_timestamps().pop(phone, None)

    def connected_phones(self) -> set[str]:
        """Return the set of currently connected phone numbers."""
        return self._connected_phones()

    def get_phone_for_channel(self, channel_id: int) -> str | None:
        """Return the phone known to have channel_id in its dialogs, or None."""
        return self._channel_phone_map.get(channel_id)

    def register_channel_phone(self, channel_id: int, phone: str) -> None:
        """Cache the discovered phone for a channel (for post-startup additions)."""
        self._channel_phone_map[channel_id] = phone

    def clear_channel_phone(self, channel_id: int) -> None:
        """Remove cached phone mapping for a channel (used during error recovery)."""
        self._channel_phone_map.pop(channel_id, None)

    async def remember_channel_phone(
        self,
        channel_id: int,
        phone: str,
        *,
        known_preferred: str | None = None,
        force: bool = False,
    ) -> None:
        """Cache channel→phone in memory and persist to DB.

        By default persists only if no preferred is set yet (same gate as
        warm_all_dialogs), avoiding clobbering a valid value from previous error
        recovery. Pass ``known_preferred`` when the caller already read the DB
        value to skip a redundant SELECT. Pass ``force=True`` to overwrite a
        stale preferred with an account that just confirmably resolved the channel
        (e.g. a successful fallback when the stored preferred was unavailable).
        Best-effort: a failed DB write only means rediscovery repeats next pass.
        """
        self.register_channel_phone(channel_id, phone)
        if known_preferred and not force:
            return
        try:
            existing = known_preferred
            if existing is None:
                existing = await self._db.repos.channels.get_preferred_phone(channel_id)
            should_write = existing != phone if force else not existing
            if should_write:
                await self._db.repos.channels.update_channel_preferred_phone(
                    channel_id, phone
                )
        except Exception:
            # exc_info keeps the traceback for DB-lock diagnosis (#676).
            logger.debug(
                "remember_channel_phone: failed to read or persist preferred_phone "
                "for channel %d",
                channel_id,
                exc_info=True,
            )

    async def forget_channel_phone(
        self, channel_id: int, *, only_if_phone: str | None = None
    ) -> None:
        """Drop the channel→phone mapping in memory and clear preferred in DB.

        Used during error recovery when the routed account turned out to be stale
        (lost access / can no longer resolve), so the next pass rediscovers a
        working account. The in-memory map (which pointed at the failed account)
        is always cleared. Pass ``only_if_phone`` to clear the DB preferred_phone
        only when it still matches the account that just failed — avoids erasing a
        valid persisted mapping when the in-memory map was stale but the DB row
        points at a different, working account. Best-effort: a failed DB write
        only repeats next pass.
        """
        self.clear_channel_phone(channel_id)
        try:
            if only_if_phone is not None:
                existing = await self._db.repos.channels.get_preferred_phone(channel_id)
                if existing and existing != only_if_phone:
                    return
            await self._db.repos.channels.update_channel_preferred_phone(channel_id, None)
        except Exception:
            logger.debug(
                "forget_channel_phone: failed to clear stale preferred_phone "
                "for channel %d",
                channel_id,
                exc_info=True,
            )

    # ------------------------------------------------------------------ #
    # warm / dialog cache
    # ------------------------------------------------------------------ #
    def is_warming(self) -> bool:
        """True while warm_all_dialogs() is still running."""
        return self._warming_task is not None and not self._warming_task.done()

    async def wait_for_warm(self, timeout: float = 30.0) -> None:
        """Wait for warm_all_dialogs() to finish, up to `timeout` seconds."""
        if self._warming_task is None or self._warming_task.done():
            return
        try:
            await asyncio.wait_for(asyncio.shield(self._warming_task), timeout=timeout)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass

    async def warm_all_dialogs(self) -> None:
        """Warm entity cache for all connected phones and persist channel→phone to DB.

        Records which channels are accessible from which account so that
        private-group collection can target the right phone without guessing.
        Stores self._warming_task so the collector can wait during a race.

        Uses run_with_flood_wait (single-shot) instead of the retry variant —
        warm is a cache-preheating optimisation and should fail fast rather than
        blocking on FloodWait sleep/retry loops.
        """
        self._warming_task = asyncio.current_task()
        deadline = time.monotonic() + WARM_ALL_PHONES_TOTAL_SEC
        now = datetime.now(timezone.utc)
        flood_waited: set[str] = set()
        try:
            accounts = await load_live_usable_accounts(self._db, active_only=True)
            flood_waited = {
                account.phone
                for account in accounts
                if is_blocking_flood_wait_until(
                    normalize_utc(getattr(account, "flood_wait_until", None)),
                    now=now,
                )
            }
        except Exception:
            logger.debug("warm_all_dialogs: failed to load flood-wait status", exc_info=True)
        phones_list = list(self._connected_phones())
        for idx, phone in enumerate(phones_list):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                logger.info("warm_all_dialogs: budget exhausted after %d phones", idx)
                break
            if idx > 0:
                await asyncio.sleep(WARM_STAGGER_DELAY_SEC)
            if phone in flood_waited:
                logger.info("warm_all_dialogs: skip %s because active flood-wait is stored", phone)
                continue
            result = await self.get_client_by_phone(phone)
            if result is None:
                continue
            session, p = result
            try:
                dialogs = await run_with_flood_wait(
                    session.warm_dialog_cache(),
                    operation="telegram_warm_dialog_cache",
                    phone=p,
                    pool=self,
                    logger_=logger,
                    timeout=WARM_SINGLE_PHONE_TIMEOUT_SEC,
                )
                self.mark_dialogs_fetched(p)
                for dialog in dialogs or []:
                    entity = getattr(dialog, "entity", None)
                    if entity is None:
                        continue
                    if not isinstance(entity, (TLChannel, Chat)):
                        continue
                    eid = getattr(entity, "id", None)
                    if eid and eid not in self._channel_phone_map:
                        await self.remember_channel_phone(eid, p)
                logger.info(
                    "warm_all_dialogs: warmed %s (%d dialogs)", p, len(dialogs or [])
                )
            except Exception as e:
                logger.warning("warm_all_dialogs: failed for %s: %s", p, e)
            finally:
                await self.release_client(p)

    def invalidate_dialogs_cache(self, phone: str | None = None) -> None:
        if phone is None:
            self._dialogs_cache.clear()
            return
        keys = [key for key in self._dialogs_cache if key[0] == phone]
        for key in keys:
            del self._dialogs_cache[key]

    def _mark_degraded_cached_dialogs(
        self,
        dialogs: list[dict],
        *,
        source: str,
        age_sec: float | None = None,
    ) -> list[dict]:
        marked = [dict(dialog) for dialog in dialogs]
        for dialog in marked:
            dialog["_degraded"] = True
            dialog["_cache_source"] = source
            if age_sec is not None:
                dialog["_cache_age_sec"] = int(age_sec)
        return marked

    async def _get_db_cached_dialogs(
        self,
        phone: str,
        mode: str,
        *,
        allow_stale: bool = False,
        mark_degraded: bool = False,
    ) -> list[dict] | None:
        full_dialogs = await self._db.repos.dialog_cache.list_dialogs(phone)
        if not full_dialogs:
            return None
        cached_at = await self._db.repos.dialog_cache.get_cached_at(phone)
        age_sec = None
        if cached_at is not None:
            age_sec = (datetime.now(timezone.utc) - cached_at).total_seconds()
            if age_sec > self._dialogs_db_cache_ttl_sec and not allow_stale:
                logger.info(
                    "_get_db_cached_dialogs: stale cache for %s age=%.0fs > ttl=%.0fs, forcing refresh",
                    phone,
                    age_sec,
                    self._dialogs_db_cache_ttl_sec,
                )
                return None
        source = "stale_db_cache" if mark_degraded else "db_cache"
        if mode == "channels_only":
            filtered = [
                dict(dialog)
                for dialog in full_dialogs
                if dialog.get("channel_type") not in ("dm", "bot", "saved")
            ]
            if mark_degraded:
                filtered = self._mark_degraded_cached_dialogs(
                    filtered,
                    source=source,
                    age_sec=age_sec,
                )
            self._store_cached_dialogs(phone, mode, filtered)
            self._store_cached_dialogs(phone, "full", full_dialogs)
            return filtered
        self._store_cached_dialogs(phone, "full", full_dialogs)
        dialogs = [dict(dialog) for dialog in full_dialogs]
        if mark_degraded:
            dialogs = self._mark_degraded_cached_dialogs(
                dialogs,
                source=source,
                age_sec=age_sec,
            )
        return dialogs

    def _get_cached_dialogs(self, phone: str, mode: str) -> list[dict] | None:
        entry = self._dialogs_cache.get((phone, mode))
        if entry is not None:
            age = time.monotonic() - entry.fetched_at_monotonic
            if age <= self._dialogs_cache_ttl_sec:
                return [dict(dialog) for dialog in entry.dialogs]
            self._dialogs_cache.pop((phone, mode), None)

        if mode == "channels_only":
            full_entry = self._dialogs_cache.get((phone, "full"))
            if full_entry is not None:
                age = time.monotonic() - full_entry.fetched_at_monotonic
                if age > self._dialogs_cache_ttl_sec:
                    self._dialogs_cache.pop((phone, "full"), None)
                    return None
                filtered = [
                    dict(dialog)
                    for dialog in full_entry.dialogs
                    if dialog.get("channel_type") not in ("dm", "bot", "saved")
                ]
                self._store_cached_dialogs(phone, mode, filtered)
                return filtered
            return None
        return None

    def _store_cached_dialogs(self, phone: str, mode: str, dialogs: list[dict]) -> None:
        self._dialogs_cache[(phone, mode)] = DialogCacheEntry(
            fetched_at_monotonic=time.monotonic(),
            dialogs=[dict(dialog) for dialog in dialogs],
        )

    async def _get_cached_dialog(self, phone: str, dialog_id: int) -> dict | None:
        full_entry = self._dialogs_cache.get((phone, "full"))
        if full_entry is not None:
            age = time.monotonic() - full_entry.fetched_at_monotonic
            if age <= self._dialogs_cache_ttl_sec:
                for dialog in full_entry.dialogs:
                    if int(dialog.get("channel_id", 0)) == dialog_id:
                        return dict(dialog)
            else:
                self._dialogs_cache.pop((phone, "full"), None)
        return await self._db.repos.dialog_cache.get_dialog(phone, dialog_id)

    # ------------------------------------------------------------------ #
    # entity resolution
    # ------------------------------------------------------------------ #
    async def resolve_entity_with_warm(
        self,
        session: TelegramTransportSession | object,
        phone: str,
        peer: object,
        *,
        operation: str = "resolve_entity_with_warm",
        timeout: float = 30.0,
        warm_timeout: float = 60.0,
        use_input_entity: bool = False,
    ) -> object:
        """Resolve a Telegram entity, warming the dialog cache once on a cache miss.

        A peer referenced by numeric id may not be in the cold entity cache yet
        (e.g. a freshly created group not in get_dialogs). The single source of
        truth for the warm-then-retry pattern: resolve → on (ValueError, TypeError)
        warm_dialog_cache() + mark_dialogs_fetched() → retry once. Flood-aware via
        run_with_flood_wait. Set use_input_entity=True to return an InputPeer
        (resolve_input_entity) instead of a full entity.
        """
        session = adapt_transport_session(session, disconnect_on_close=False)
        resolver = session.resolve_input_entity if use_input_entity else session.resolve_entity
        is_live_username = self._is_live_username_peer(peer)

        async def _resolve(current_operation: str) -> object:
            if is_live_username:
                return await self.run_live_username_resolve(
                    lambda: resolver(peer),
                    phone=phone,
                    username=str(peer),
                    operation=current_operation,
                    logger_=logger,
                    timeout=timeout,
                )
            return await run_with_flood_wait(
                resolver(peer),
                operation=current_operation,
                phone=phone,
                pool=self,
                logger_=logger,
                timeout=timeout,
            )

        try:
            return await _resolve(operation)
        except (ValueError, TypeError):
            await run_with_flood_wait(
                session.warm_dialog_cache(),
                operation=f"{operation}_warm_dialog_cache",
                phone=phone,
                pool=self,
                logger_=logger,
                timeout=warm_timeout,
            )
            self.mark_dialogs_fetched(phone)
            return await _resolve(f"{operation}_after_warm")

    async def resolve_dialog_entity(
        self,
        session: TelegramTransportSession | object,
        phone: str,
        dialog_id: int,
        target_type: str | None = None,
    ):
        session = adapt_transport_session(session, disconnect_on_close=False)
        if target_type is None:
            dialog = await self._get_cached_dialog(phone, dialog_id)
            cached_type = str(dialog.get("channel_type") or "") if dialog else ""
            if cached_type in {"dm", "bot", "saved", "group"}:
                target_type = cached_type
        if target_type in ("dm", "bot", "saved"):
            peer = PeerUser(dialog_id)
        elif target_type == "group":
            # Legacy small groups are PeerChat, not PeerChannel.
            peer = PeerChat(abs(dialog_id))
        else:
            peer = PeerChannel(abs(dialog_id))
        try:
            return await self.resolve_entity_with_warm(
                session,
                phone,
                peer,
                operation="resolve_dialog_entity_peer",
                use_input_entity=True,
            )
        except (ValueError, TypeError):
            pass

        dialog = await self._get_cached_dialog(phone, dialog_id)
        username = dialog.get("username") if dialog else None
        if username:
            return await self.run_live_username_resolve(
                lambda: session.resolve_input_entity(username),
                operation="resolve_dialog_entity_username",
                phone=phone,
                username=str(username),
                logger_=logger,
                timeout=30.0,
            )

        return await run_with_flood_wait(
            session.resolve_input_entity(peer),
            operation="resolve_dialog_entity_peer_fallback",
            phone=phone,
            pool=self,
            logger_=logger,
            timeout=30.0,
        )

    async def resolve_channel(
        self,
        identifier: str,
        *,
        signal_gone: bool = False,
        numeric_fallback: str | None = None,
    ) -> dict | None:
        """Resolve channel by @username or t.me/ link. Returns dict with channel info.

        When ``signal_gone`` is True, a *definitive* not-found (username no longer
        occupied/invalid) returns the sentinel ``{"gone": True}`` instead of None,
        so callers like ``channel refresh-types`` can deactivate the channel — while
        a *transient* failure (timeout / flood / no client) still returns None and
        the channel is left active (audit #835/8).

        ``numeric_fallback`` guards against deactivating a *live* channel whose stored
        @username merely went stale: a ``UsernameNotOccupiedError`` only proves the
        username is no longer occupied, not that the channel row is dead — the channel
        keeps a stable numeric id and may have just renamed/dropped its @username. When
        the primary (username) resolution returns the gone sentinel and a distinct
        ``numeric_fallback`` is given, resolution is retried by that numeric id; only a
        *second* definitive not-found yields ``{"gone": True}``. A successful numeric
        resolution returns the live channel dict, so refresh-types leaves it active
        (#858 review).

        ChannelForbidden is NOT treated as gone: it is an access/permission error
        (the *resolving* account is not a member of a private/restricted channel),
        not a deletion — a genuinely deleted channel raises an exception. Resolution
        uses an arbitrary available account (no preferred-account routing), so a live
        private channel collectible via another account must NOT be deactivated just
        because this account can't see it (#858 review). It returns None -> SKIP.

        Raises:
            RuntimeError("no_client") — no connected/available Telegram accounts.
        """
        # A purely numeric identifier (channel has no @username) has no separate
        # username-gone signal to escalate from: route its first lookup through the
        # owning account directly, and treat an untrusted numeric miss as review
        # (uncertain → quarantine) rather than a silent skip (#875 redesign).
        primary_numeric = signal_gone and identifier.lstrip("-").isdigit()
        owner_phone = await self._owner_phone_for(identifier) if primary_numeric else None
        result = await self._resolve_channel_once(
            identifier,
            signal_gone=signal_gone,
            gone_phone=owner_phone,
            review_on_uncertain=primary_numeric,
        )
        # Stale-username guard: a gone-by-username verdict is not proof the channel is
        # dead — retry by the stable numeric id before signalling gone (#858 review).
        if (
            signal_gone
            and isinstance(result, dict)
            and result.get("gone")
            and numeric_fallback
            and numeric_fallback != identifier
        ):
            # The numeric retry may only CONFIRM gone when it runs on the account that
            # actually owns/collects the channel: a bare PeerChannel lookup on an
            # arbitrary account raises a *local* cache-miss ValueError (no cached
            # access_hash) that is indistinguishable from deletion, so confirming gone
            # from an arbitrary account would falsely deactivate a live channel that is
            # collected by a different account (#875 review). Route the retry through the
            # owning account; when the miss cannot be trusted as gone (owner unknown or
            # unavailable) the numeric lookup yields the review sentinel — the channel is
            # quarantined for human review instead of silently deactivated or skipped
            # (#875 redesign).
            retry_owner = await self._owner_phone_for(numeric_fallback)
            return await self._resolve_channel_once(
                numeric_fallback,
                signal_gone=signal_gone,
                gone_phone=retry_owner,
                review_on_uncertain=True,
            )
        return result

    async def _owner_phone_for(self, identifier: str) -> str | None:
        """Phone known to own/collect the numeric channel id, or None.

        Checks the in-memory channel→phone map first (populated by warm_all_dialogs),
        then the persisted preferred_phone. Returns None for a non-numeric identifier.
        """
        if not identifier.lstrip("-").isdigit():
            return None
        cid = bare_channel_id(int(identifier))
        phone = self.get_phone_for_channel(cid)
        if phone:
            return phone
        try:
            return await self._db.repos.channels.get_preferred_phone(cid)
        except Exception:
            logger.debug("resolve_channel: preferred_phone lookup failed for %d", cid, exc_info=True)
            return None

    async def _resolve_channel_once(
        self,
        identifier: str,
        *,
        signal_gone: bool = False,
        gone_phone: str | None = None,
        review_on_uncertain: bool = False,
    ) -> dict | None:
        gone: dict | None = {"gone": True} if signal_gone else None
        # When ``review_on_uncertain`` is set, an ambiguous numeric not-found (cache-miss
        # vs deletion) that cannot be trusted as gone is surfaced as a quarantine sentinel
        # instead of a silent None, so the channel is flagged for human review rather than
        # left invisibly active (#875 redesign). Only meaningful under signal_gone.
        review: dict | None = (
            {"review": True, "reason": "numeric_unresolved"}
            if signal_gone and review_on_uncertain
            else None
        )
        # Normalize post links: https://t.me/channel/123 → https://t.me/channel
        identifier = re.sub(r"(t\.me/[^/\s]+)/\d+$", r"\1", identifier)

        # Use PeerChannel for numeric IDs so Telethon treats them as channels, not users.
        # Strip the Bot-API -100 prefix for negative ids (mirror resolve_any_entity) so a
        # -100<bare> input resolves the correct peer, not PeerChannel(100<bare>). Stored
        # channel_id is bare-positive, but a caller may still pass a -100-style string.
        numeric_peer = identifier.lstrip("-").isdigit()
        if numeric_peer:
            peer: str | PeerChannel = PeerChannel(bare_channel_id(int(identifier)))
        else:
            peer = identifier

        # A numeric not-found may be trusted as "gone" ONLY when this lookup runs on the
        # account that owns the channel (gone_phone) — an arbitrary account's bare-peer
        # miss is just a local cache-miss, not a deletion (#875 review).
        gone_trusted = numeric_peer and gone_phone is not None

        last_flood_error: HandledFloodWaitError | None = None
        used_owner = False
        for _attempt in range(3):
            if gone_phone is not None:
                result = await self.get_client_by_phone(gone_phone)
                if not result:
                    # The owning account is unavailable (flood/disconnected): we cannot
                    # trustworthily confirm gone, so do NOT deactivate. Flag for review
                    # (uncertain) instead of a silent skip when asked (#875 redesign).
                    logger.info(
                        "resolve_channel: owner account for '%s' unavailable; not confirming gone",
                        identifier,
                    )
                    return review
                used_owner = True
            else:
                result = await self.get_available_client()
            if not result:
                if last_flood_error is not None:
                    raise last_flood_error
                logger.warning("resolve_channel: no available client for '%s'", identifier)
                raise RuntimeError("no_client")
            session, phone = result
            try:
                entity = await self.resolve_entity_with_warm(
                    session, phone, peer, operation="resolve_channel"
                )
                if isinstance(entity, ChannelForbidden):
                    # Access denied for THIS account, not a deletion — never deactivate
                    # (the channel may be live and collectible via another account). #858
                    return None
                if not hasattr(entity, "title"):
                    logger.info("resolve_channel: '%s' is a user, not a channel/group", identifier)
                    return None
                channel_type, deactivate = self._classify_entity(entity)
                # Self-heal the channel→phone map: remember the account that just
                # resolved this channel so the next pass routes there directly. Mirrors
                # fetch_channel_meta — only on the gone-detection path (signal_gone), where
                # owner routing matters; a plain lookup stays side-effect-free.
                if signal_gone and numeric_peer:
                    await self.remember_channel_phone(
                        bare_channel_id(int(identifier)), phone, force=not used_owner
                    )
                return {
                    "channel_id": entity.id,
                    "title": entity.title,
                    "username": getattr(entity, "username", None),
                    "channel_type": channel_type,
                    "deactivate": deactivate,
                    "created_at": getattr(entity, "date", None),
                }
            except asyncio.TimeoutError:
                logger.warning("resolve_channel: get_entity timed out for '%s'", identifier)
                return None
            except HandledFloodWaitError as exc:
                last_flood_error = exc
                logger.info(
                    "resolve_channel: rotating client after flood wait for '%s': %s",
                    identifier,
                    exc.info.detail,
                )
                continue
            except (UsernameNotOccupiedError, UsernameInvalidError) as e:
                logger.warning("resolve_channel: username not found '%s': %s", identifier, e)
                return gone
            except ChannelPrivateError as e:
                # Access denied for THIS account, not a deletion — never deactivate
                # (the channel may be live and collectible via another account). #858
                logger.info("resolve_channel: access denied for '%s': %s", identifier, e)
                # If the routed owner account lost access, its mapping is stale — drop it
                # so the next pass rediscovers a working account (mirror fetch_channel_meta).
                if used_owner and numeric_peer:
                    await self.forget_channel_phone(
                        bare_channel_id(int(identifier)), only_if_phone=phone
                    )
                return None
            except (ChannelInvalidError, ValueError, TypeError) as e:
                # A numeric peer that Telethon cannot resolve raises a plain
                # ValueError("Could not find the input entity ...") / ChannelInvalidError.
                # This confirms "gone" ONLY when the lookup ran on the OWNING account
                # (gone_trusted): there, a post-warm miss means the channel is really
                # deleted. On an arbitrary account the same ValueError is just a local
                # cache-miss (no cached access_hash) and must NOT deactivate a live
                # channel collected elsewhere (#875 review) — surface it for human review
                # instead of a silent skip. A username peer always keeps the old None
                # (transient/parse error).
                logger.warning("resolve_channel: could not resolve '%s': %s", identifier, e)
                if gone_trusted:
                    return gone
                return review if numeric_peer else None
            except Exception as e:
                logger.warning("resolve_channel: failed to resolve '%s': %s", identifier, e)
                return None
            finally:
                await self.release_client(phone)
        if last_flood_error is not None:
            logger.warning("resolve_channel: all clients flood-waited for '%s'", identifier)
            raise last_flood_error
        logger.warning("resolve_channel: all attempts failed for '%s' (no available clients)", identifier)
        return None

    async def resolve_any_entity(self, identifier: str, phone: str | None = None) -> dict | None:
        """Resolve any Telegram entity by @username, t.me/ link, or numeric ID.

        Unlike resolve_channel, this returns users, bots, channels, groups, and mini-apps.
        If phone is given, tries that account first, then falls back to any available client.
        Returns dict with channel_id, title, username, channel_type or None on failure.
        """
        identifier = re.sub(r"(t\.me/[^/\s]+)/\d+$", r"\1", identifier)
        if identifier.lstrip("-").isdigit():
            raw_id = int(identifier)
            if raw_id > 0:
                # Positive IDs are users/bots
                peer: str | PeerChannel | PeerUser = PeerUser(raw_id)
            else:
                # Negative IDs are groups/channels in Bot API format (-100XXXXXXXXX).
                # Strip the -100 prefix to get the Telethon MTProto channel ID.
                str_abs = str(abs(raw_id))
                channel_id = int(str_abs[3:]) if str_abs.startswith("100") else abs(raw_id)
                peer = PeerChannel(channel_id)
        else:
            peer = identifier

        async def _get_client() -> tuple[TelegramTransportSession, str] | None:
            if phone:
                result = await self.get_client_by_phone(phone)
                if result:
                    return result
            return await self.get_available_client()

        last_flood_error: HandledFloodWaitError | None = None
        for _attempt in range(3):
            result = await _get_client()
            if not result:
                if last_flood_error is not None:
                    raise last_flood_error
                raise RuntimeError("no_client")
            session, used_phone = result
            try:
                entity = await self.resolve_entity_with_warm(
                    session, used_phone, peer, operation="resolve_any_entity"
                )
                if isinstance(entity, ChannelForbidden):
                    return None
                return self._entity_to_dict(entity)
            except asyncio.TimeoutError:
                logger.warning("resolve_any_entity: timed out for '%s'", identifier)
                return None
            except HandledFloodWaitError as exc:
                last_flood_error = exc
                continue
            except (UsernameNotOccupiedError, UsernameInvalidError) as e:
                logger.warning("resolve_any_entity: username not found '%s': %s", identifier, e)
                return None
            except Exception as e:
                logger.warning("resolve_any_entity: failed to resolve '%s': %s", identifier, e)
                return None
            finally:
                await self.release_client(used_phone)
        if last_flood_error is not None:
            raise last_flood_error
        return None

    def _entity_to_dict(self, entity) -> dict:
        """Map a resolved Telegram entity to the resolve_any_entity result dict."""
        if hasattr(entity, "title"):
            # Channel or Chat
            channel_type, deactivate = self._classify_entity(entity)
            return {
                "channel_id": entity.id,
                "title": entity.title,
                "username": getattr(entity, "username", None),
                "channel_type": channel_type,
                "deactivate": deactivate,
            }
        # User or Bot
        first = getattr(entity, "first_name", "") or ""
        last = getattr(entity, "last_name", "") or ""
        title = (first + " " + last).strip() or str(entity.id)
        is_bot = getattr(entity, "bot", False)
        return {
            "channel_id": entity.id,
            "title": title,
            "username": getattr(entity, "username", None),
            "channel_type": "bot" if is_bot else "dm",
            "deactivate": False,
        }

    @staticmethod
    def _classify_entity(entity) -> tuple[str, bool]:
        """Return (channel_type, deactivate) for a Telegram channel/group entity."""
        if getattr(entity, "scam", False):
            channel_type = "scam"
        elif getattr(entity, "fake", False):
            channel_type = "fake"
        elif getattr(entity, "restricted", False):
            channel_type = "restricted"
        elif getattr(entity, "monoforum", False):
            channel_type = "monoforum"
        elif getattr(entity, "forum", False):
            channel_type = "forum"
        elif getattr(entity, "gigagroup", False):
            channel_type = "gigagroup"
        elif getattr(entity, "megagroup", False):
            channel_type = "supergroup"
        elif getattr(entity, "broadcast", False):
            channel_type = "channel"
        else:
            channel_type = "group"
        return channel_type, channel_type in ("scam", "fake", "restricted")

    async def fetch_channel_meta(
        self, channel_id: int, channel_type: str | None
    ) -> dict | None:
        """Fetch about, linked_chat_id, has_comments for a channel.

        Only works for channel/supergroup/gigagroup/forum/monoforum types.
        Returns dict with keys: about, linked_chat_id, has_comments.
        Returns None on failure.
        """
        # Group type has no linked_chat_id, so skip the expensive API call
        if channel_type == "group":
            return None

        # Route to the account that can actually see this (possibly private)
        # channel instead of a random available one (#808). Mirrors
        # Collector._collect_channel: in-memory map → DB preferred_phone →
        # wait_for_warm → fall back to any available client.
        db_preferred: str | None = None
        preferred = self.get_phone_for_channel(channel_id)
        if not preferred:
            try:
                db_preferred = await self._db.repos.channels.get_preferred_phone(channel_id)
            except Exception:
                logger.debug(
                    "fetch_channel_meta: failed to read preferred_phone for channel_id %s",
                    channel_id,
                    exc_info=True,
                )
            preferred = db_preferred
        if not preferred and self.is_warming():
            await self.wait_for_warm(timeout=30.0)
            preferred = self.get_phone_for_channel(channel_id)

        used_preferred = False
        result = None
        if preferred:
            result = await self.get_client_by_phone(preferred)
            used_preferred = result is not None
        if result is None:
            result = await self.get_available_client()
        if not result:
            logger.warning("fetch_channel_meta: no available client for channel_id %s", channel_id)
            return None

        session, phone = result
        session = adapt_transport_session(session, disconnect_on_close=False)
        try:
            # Pre-warm the entity cache when routing through a preferred account
            # whose dialogs haven't been fetched this process (StringSession loses
            # the cache between restarts). Mirrors Collector._collect_channel.
            if used_preferred and not self.is_dialogs_fetched(phone):
                try:
                    await run_with_flood_wait(
                        session.warm_dialog_cache(),
                        operation="fetch_channel_meta_warm_dialog_cache",
                        phone=phone,
                        pool=self,
                        logger_=logger,
                        timeout=30.0,
                    )
                    self.mark_dialogs_fetched(phone)
                except HandledFloodWaitError as exc:
                    logger.info(
                        "fetch_channel_meta: flood wait warming %s for channel_id %s: %s",
                        phone,
                        channel_id,
                        exc.info.detail,
                    )
                    return None

            entity = await self.resolve_entity_with_warm(
                session, phone, PeerChannel(channel_id), operation="fetch_channel_meta"
            )
            full = await run_with_flood_wait(
                session.fetch_full_channel(entity),
                operation="fetch_channel_meta_full",
                phone=phone,
                pool=self,
                logger_=logger,
                timeout=30.0,
            )
            about = full.full_chat.about if full and full.full_chat else None
            linked_chat_id = (
                full.full_chat.linked_chat_id if full and full.full_chat else None
            )
            has_comments = linked_chat_id is not None
            # Self-heal the channel→phone map on success: remember which account
            # resolved this channel so the next bulk pass routes there directly.
            # When we fell back to a different account than the stored preferred,
            # that preferred was stale/unavailable — force-update the DB to the
            # account that just confirmably worked. When we used the preferred
            # account, just fill the DB if it was empty (the warm_all_dialogs gate).
            await self.remember_channel_phone(
                channel_id,
                phone,
                known_preferred=db_preferred,
                force=not used_preferred,
            )
            return {
                "about": about,
                "linked_chat_id": linked_chat_id,
                "has_comments": has_comments,
            }
        except asyncio.TimeoutError:
            logger.warning(
                "fetch_channel_meta: get_entity timed out for channel_id %s", channel_id
            )
            return None
        except HandledFloodWaitError as exc:
            logger.info(
                "fetch_channel_meta: flood wait for channel_id %s: %s",
                channel_id,
                exc.info.detail,
            )
            return None
        except (ChannelPrivateError, ChatAdminRequiredError):
            logger.debug(
                "fetch_channel_meta: access denied for channel_id %s (expected for private channels)",
                channel_id,
            )
            # If the preferred account lost access, drop the stale mapping so the
            # next pass rediscovers. Leave it alone on the available-client path —
            # we'd be erasing a good record on a guess. Clear the DB only if it
            # still points at the account that just failed (the in-memory map may
            # be stale while the DB holds a different, valid account).
            if used_preferred:
                await self.forget_channel_phone(channel_id, only_if_phone=phone)
            return None
        except (ValueError, TypeError):
            # Telethon raises ValueError("Could not find the input entity ...")
            # when no warmed account can resolve the peer. Not unexpected during
            # bulk passes — keep it at DEBUG so it doesn't drown the logs (#808).
            logger.debug(
                "fetch_channel_meta: entity unresolved for channel_id %s "
                "(not in any warmed account)",
                channel_id,
            )
            # A stored preferred phone that can no longer resolve the entity is
            # stale (membership change) — clear it so the next pass rediscovers
            # instead of pinning the same dead account (Codex review on #809).
            # Clear the DB only if it still matches the failed account.
            if used_preferred:
                await self.forget_channel_phone(channel_id, only_if_phone=phone)
            return None
        except Exception as e:
            logger.warning(
                "fetch_channel_meta: failed to fetch full meta for channel_id %s: %s",
                channel_id,
                e,
            )
            return None
        finally:
            await self.release_client(phone)

    # ------------------------------------------------------------------ #
    # dialog fetching
    # ------------------------------------------------------------------ #
    async def get_dialogs_for_phone(
        self,
        phone: str,
        include_dm: bool = False,
        mode: str = "channels_only",
        refresh: bool = False,
    ) -> list[dict]:
        """Get all dialogs for a specific connected account."""
        cache_mode = "full" if include_dm or mode == "full" else "channels_only"
        if refresh:
            logger.info(
                "get_dialogs_for_phone: manual refresh for %s mode=%s",
                phone,
                cache_mode,
            )
        else:
            cached = self._get_cached_dialogs(phone, cache_mode)
            if cached is not None:
                logger.info(
                    "get_dialogs_for_phone: cache hit for %s mode=%s count=%d",
                    phone,
                    cache_mode,
                    len(cached),
                )
                return cached
            db_cached = await self._get_db_cached_dialogs(phone, cache_mode)
            if db_cached is not None:
                logger.info(
                    "get_dialogs_for_phone: db cache hit for %s mode=%s count=%d",
                    phone,
                    cache_mode,
                    len(db_cached),
                )
                return db_cached
            logger.info(
                "get_dialogs_for_phone: cache miss for %s mode=%s source=telegram",
                phone,
                cache_mode,
            )

        stale_cached = await self._get_db_cached_dialogs(
            phone,
            cache_mode,
            allow_stale=True,
            mark_degraded=True,
        )
        if not isinstance(getattr(self, "_dialog_refresh_tasks", None), dict):
            self._dialog_refresh_tasks = {}
        key = (phone, cache_mode)
        task = self._dialog_refresh_tasks.get(key)
        if task is None or task.done():
            task = asyncio.create_task(
                self._fetch_dialogs_for_phone(phone, include_dm, mode, cache_mode)
            )
            self._dialog_refresh_tasks[key] = task
        else:
            logger.info("get_dialogs_for_phone: joining in-flight refresh for %s mode=%s", phone, cache_mode)
        try:
            return await task
        except Exception as exc:
            if stale_cached is not None:
                logger.warning(
                    "get_dialogs_for_phone: degraded stale cache for %s mode=%s after %s",
                    phone,
                    cache_mode,
                    type(exc).__name__,
                )
                return stale_cached
            if isinstance(exc, RuntimeError) and str(exc) == "no_client":
                return []
            raise
        finally:
            if self._dialog_refresh_tasks.get(key) is task and task.done():
                self._dialog_refresh_tasks.pop(key, None)

    async def _fetch_dialogs_for_phone(
        self,
        phone: str,
        include_dm: bool,
        mode: str,
        cache_mode: str,
    ) -> list[dict]:
        result = await self.get_client_by_phone(phone)
        if not result:
            raise RuntimeError("no_client")
        session, acquired_phone = result
        session = adapt_transport_session(session, disconnect_on_close=False)
        started_at = time.perf_counter()
        try:
            items: list[dict] = []
            stats = DialogFetchStats()
            try:
                me = await session.fetch_me()
                my_id: int | None = me.id
            except Exception:
                my_id = None

            async def _iter() -> None:
                async for dialog in session.stream_dialogs():
                    stats.raw_dialogs += 1
                    entity = dialog.entity
                    if dialog.is_channel or dialog.is_group:
                        channel_type, deactivate = self._classify_entity(entity)
                        if channel_type in (
                            "channel",
                            "monoforum",
                            "scam",
                            "fake",
                            "restricted",
                        ):
                            stats.channels += 1
                        if channel_type in ("supergroup", "group", "gigagroup", "forum"):
                            stats.groups += 1
                        items.append(
                            {
                                "channel_id": entity.id,
                                "title": dialog.title,
                                "username": getattr(entity, "username", None),
                                "channel_type": channel_type,
                                "deactivate": deactivate,
                                "is_own": getattr(entity, "creator", False),
                                "created_at": getattr(entity, "date", None),
                            }
                        )
                    elif include_dm or mode == "full":
                        is_bot = getattr(entity, "bot", False)
                        is_saved = my_id is not None and entity.id == my_id
                        if is_bot:
                            stats.bots += 1
                        else:
                            stats.dms += 1
                        items.append(
                            {
                                "channel_id": entity.id,
                                "title": "Избранное (Saved Messages)" if is_saved else dialog.title,
                                "username": getattr(entity, "username", None),
                                "channel_type": "saved" if is_saved else ("bot" if is_bot else "dm"),
                                "deactivate": False,
                                "is_own": False,
                                "created_at": getattr(entity, "date", None),
                            }
                        )

            iter_coro = _iter()
            try:
                await run_with_flood_wait(
                    iter_coro,
                    operation="get_dialogs_for_phone",
                    phone=acquired_phone,
                    pool=self,
                    logger_=logger,
                    timeout=60.0,
                )
            except asyncio.TimeoutError:
                iter_coro.close()
                stats.partial = True
                logger.warning(
                    "get_dialogs_for_phone: timed out for %s mode=%s, returning %d partial results",
                    acquired_phone,
                    cache_mode,
                    len(items),
                )
            elapsed_ms = int((time.perf_counter() - started_at) * 1000)
            logger.info(
                "get_dialogs_for_phone: phone=%s mode=%s duration_ms=%d "
                "raw=%d channels=%d groups=%d dms=%d bots=%d "
                "partial=%s result=%d",
                acquired_phone,
                cache_mode,
                elapsed_ms,
                stats.raw_dialogs,
                stats.channels,
                stats.groups,
                stats.dms,
                stats.bots,
                stats.partial,
                len(items),
            )
            if not stats.partial:
                self.invalidate_dialogs_cache(acquired_phone)
                self._store_cached_dialogs(acquired_phone, cache_mode, items)
                if cache_mode == "full":
                    self.mark_dialogs_fetched(acquired_phone)
                    await self._db.repos.dialog_cache.replace_dialogs(acquired_phone, items)
            return items
        finally:
            await self.release_client(acquired_phone)

    async def leave_channels(self, phone: str, dialogs: list[tuple[int, str]]) -> dict[int, bool]:
        """Leave/unsubscribe from a list of dialogs for the given account.

        dialogs: list of (channel_id, channel_type) where channel_type comes from
        get_dialogs_for_phone (e.g. "channel", "supergroup", "dm", "bot").
        """
        result = await self.get_client_by_phone(phone)
        if not result:
            return {cid: False for cid, _ in dialogs}
        session, phone = result
        session = adapt_transport_session(session, disconnect_on_close=False)
        outcomes: dict[int, bool] = {}
        try:
            for cid, ctype in dialogs:
                try:
                    if ctype in ("dm", "bot", "saved"):
                        peer = PeerUser(cid)
                    elif ctype == "group":
                        # Legacy small groups are PeerChat, not PeerChannel.
                        peer = PeerChat(abs(cid))
                    else:
                        peer = PeerChannel(abs(cid))
                    async def _remove_dialog() -> None:
                        entity = await session.resolve_entity(peer)
                        await session.remove_dialog(entity)

                    await run_with_flood_wait(
                        _remove_dialog(),
                        operation=f"leave_channels:{cid}",
                        phone=phone,
                        pool=self,
                        logger_=logger,
                    )
                    outcomes[cid] = True
                    await asyncio.sleep(0.3)
                except HandledFloodWaitError:
                    outcomes[cid] = False
                    for remaining_cid, _ in dialogs:
                        if remaining_cid not in outcomes:
                            outcomes[remaining_cid] = False
                    break
                except Exception as e:
                    logger.warning("leave_channels: failed for %d: %s", cid, e)
                    outcomes[cid] = False
        finally:
            await self.release_client(phone)
        self.invalidate_dialogs_cache(phone)
        await self._db.repos.dialog_cache.clear_dialogs(phone)
        return outcomes

    async def delete_dialogs(self, phone: str, dialogs: list[tuple[int, str]]) -> dict[int, bool]:
        """Fully delete a list of dialogs for the given account.

        Unlike ``leave_channels`` (which only drops the dialog from the account's
        list — a no-op for the *owner* of a channel/supergroup, the entity keeps
        existing), this deletes whatever Telethon allows for each type:

        * channel/supergroup/gigagroup/forum → ``channels.DeleteChannelRequest``
          (full destruction; only the creator may do this)
        * legacy small group → ``messages.DeleteChatRequest``
        * dm/bot/saved → ``delete_dialog`` (clears the conversation history)

        dialogs: list of (channel_id, channel_type) as produced by
        ``get_dialogs_for_phone``.
        """
        result = await self.get_client_by_phone(phone)
        if not result:
            return {cid: False for cid, _ in dialogs}
        session, phone = result
        session = adapt_transport_session(session, disconnect_on_close=False)
        outcomes: dict[int, bool] = {}
        try:
            for cid, ctype in dialogs:
                try:
                    if ctype in ("dm", "bot", "saved"):
                        peer: object = PeerUser(cid)
                    elif ctype == "group":
                        # Legacy small groups are PeerChat, not PeerChannel.
                        peer = PeerChat(abs(cid))
                    else:
                        peer = PeerChannel(abs(cid))

                    async def _delete(peer: object = peer, ctype: str = ctype, cid: int = cid) -> None:
                        # Resolve the entity only where it is actually needed. A legacy
                        # group is deleted by bare chat_id (DeleteChatRequest), so a
                        # needless resolve_entity here would fail deletion for a group
                        # whose entity can't be resolved (migrated to supergroup, stale
                        # entity cache).
                        if ctype == "group":
                            await session.delete_chat(abs(cid))
                            return
                        entity = await session.resolve_entity(peer)
                        if ctype in ("dm", "bot", "saved"):
                            await session.remove_dialog(entity)
                        else:
                            await session.delete_channel(entity)

                    await run_with_flood_wait(
                        _delete(),
                        operation=f"delete_dialogs:{cid}",
                        phone=phone,
                        pool=self,
                        logger_=logger,
                    )
                    outcomes[cid] = True
                    await asyncio.sleep(0.3)
                except HandledFloodWaitError:
                    outcomes[cid] = False
                    for remaining_cid, _ in dialogs:
                        if remaining_cid not in outcomes:
                            outcomes[remaining_cid] = False
                    break
                except Exception as e:
                    logger.warning("delete_dialogs: failed for %d: %s", cid, e)
                    outcomes[cid] = False
        finally:
            await self.release_client(phone)
        self.invalidate_dialogs_cache(phone)
        await self._db.repos.dialog_cache.clear_dialogs(phone)
        return outcomes

    async def get_forum_topics(self, channel_id: int) -> list[dict]:
        """Fetch forum topics for a forum-type channel.

        Uses entity-first approach: tries get_entity(PeerChannel) directly (fast, 10s timeout).
        Falls back to get_dialogs() only if entity lookup fails with ValueError (cache miss).
        """
        result = await self.get_available_client()
        if result is None:
            logger.warning("get_forum_topics: no available client for channel %d", channel_id)
            return []
        session, phone = result
        session = adapt_transport_session(session, disconnect_on_close=False)
        try:
            # Try direct entity lookup first (works when entity is already cached)
            try:
                entity = await run_with_flood_wait(
                    session.resolve_entity(PeerChannel(channel_id)),
                    operation="get_forum_topics_resolve_channel_id",
                    phone=phone,
                    pool=self,
                    logger_=logger,
                    timeout=10.0,
                )
            except (ValueError, asyncio.TimeoutError):
                # Cache miss — populate cache via get_dialogs and retry
                if not self.is_dialogs_fetched(phone):
                    await run_with_flood_wait(
                        session.warm_dialog_cache(),
                        operation="get_forum_topics_warm_dialog_cache",
                        phone=phone,
                        pool=self,
                        logger_=logger,
                        timeout=60.0,
                    )
                    self.mark_dialogs_fetched(phone)
                    try:
                        entity = await run_with_flood_wait(
                            session.resolve_entity(PeerChannel(channel_id)),
                            operation="get_forum_topics_resolve_channel_id_after_warm",
                            phone=phone,
                            pool=self,
                            logger_=logger,
                            timeout=10.0,
                        )
                    except (ValueError, asyncio.TimeoutError):
                        entity = None
                else:
                    entity = None

                # Last resort — resolve by username from DB
                if entity is None:
                    channel = await self._db.get_channel_by_channel_id(channel_id)
                    if channel and channel.username:
                        entity = await self.run_live_username_resolve(
                            lambda: session.resolve_entity(channel.username),
                            operation="get_forum_topics_resolve_username",
                            phone=phone,
                            username=str(channel.username),
                            logger_=logger,
                            timeout=10.0,
                        )
                    else:
                        raise LookupError(
                            f"Cannot resolve entity for channel {channel_id}: "
                            "not in cache and no username in DB"
                        )
            response = await run_with_flood_wait(
                session.fetch_forum_topics(entity, limit=100),
                operation="get_forum_topics_fetch_topics",
                phone=phone,
                pool=self,
                logger_=logger,
                timeout=30.0,
            )
            return [
                {
                    "id": t.id,
                    "title": t.title,
                    "icon_emoji_id": getattr(t, "icon_emoji_id", None),
                    "date": (t.date.isoformat() if getattr(t, "date", None) else None),
                }
                for t in response.topics
                if hasattr(t, "title")
            ]
        except ChannelInvalidError as e:
            logger.info("get_forum_topics unavailable for channel %d: %s", channel_id, e)
            return []
        except Exception as e:
            logger.warning(
                "get_forum_topics failed for channel %d: %s", channel_id, e, exc_info=True
            )
            return []
        finally:
            await self.release_client(phone)

    async def get_dialogs(self) -> list[dict]:
        """Get list of subscribed channels and groups."""
        accounts = await load_live_usable_accounts(self._db, active_only=True)
        now = datetime.now(timezone.utc)
        for acc in accounts:
            flood_until = normalize_utc(acc.flood_wait_until)
            if flood_until and flood_until > now:
                continue
            if acc.phone in self.clients:
                return await self.get_dialogs_for_phone(acc.phone, mode="channels_only")
        return []
