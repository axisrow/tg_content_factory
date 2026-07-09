from __future__ import annotations

import json
import logging
from collections.abc import Awaitable, Callable, Iterable
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, cast

from src.telegram.flood_wait import HandledFloodWaitError, run_with_flood_wait
from src.telegram.rate_limiter import (
    GLOBAL_RESOLVE_BACKOFF_THRESHOLD_SEC,
    ResolveRateLimiter,
    UsernameResolveRateLimitedError,
)

if TYPE_CHECKING:
    from typing import Protocol

    class _SettingsStore(Protocol):
        """Structural view of the ``db`` argument: only the settings get/set pair."""

        async def get_setting(self, key: str) -> str | None: ...
        async def set_setting(self, key: str, value: str) -> None: ...


logger = logging.getLogger(__name__)

RESOLVE_BACKOFF_BY_PHONE_SETTING = "resolve_username_backoff_by_phone"
RESOLVE_BACKOFF_LEGACY_SETTING = "resolve_username_backoff_until_utc"


def parse_resolve_backoff_setting(
    raw: str | None, *, now: datetime | None = None
) -> dict[str, datetime]:
    """Parse the per-phone backoff DB setting, keeping only active deadlines.

    Shared by CLI ``account flood-status`` and the web settings page, which
    read the persisted state directly instead of holding a live pool (#790).
    """
    if not raw:
        return {}
    try:
        entries = json.loads(raw)
    except (ValueError, TypeError):
        return {}
    if not isinstance(entries, dict):
        return {}
    now = now or datetime.now(timezone.utc)
    result: dict[str, datetime] = {}
    for phone, raw_until in entries.items():
        try:
            until = datetime.fromisoformat(str(raw_until))
        except (ValueError, TypeError):
            continue
        if until.tzinfo is None:
            until = until.replace(tzinfo=timezone.utc)
        if until > now:
            result[str(phone)] = until
    return result


class ResolveGuardMixin:
    """Shared live-username-resolve FloodWait guard (#785, #790).

    Owns the single source of truth for the per-account resolve rate limiter
    and the per-account FloodWait backoff windows. Every runtime path that
    performs a live ``auth.resolveUsername`` (collection, stats, search
    fallback, agent tools) routes through :meth:`run_live_username_resolve`
    so they share one budget and one flood-wait policy.

    Since #790 the backoff is **per phone**: a long resolve flood on one
    account freezes live resolves only for that account, while the other
    connected accounts keep resolving. Callers that need the old pool-wide
    view (collection queue deferral, CLI status) use the no-argument
    aggregate getters, which report "blocked" only when *every* connected
    account is in backoff.

    Concrete pools must initialise ``self._resolve_rate_limiter`` and the
    per-phone dicts in their ``__init__`` (the mixin lazily repairs missing
    or legacy-scalar attributes for test doubles).
    """

    _resolve_rate_limiter: ResolveRateLimiter
    _resolve_username_backoff_until_utc: dict[str, datetime]
    _resolve_ramp_up_until_utc: dict[str, datetime]
    _resolve_ramp_up_last_call_utc: dict[str, datetime]
    _resolve_ramp_up_min_interval_sec: float

    def _get_resolve_rate_limiter(self) -> ResolveRateLimiter:
        limiter = getattr(self, "_resolve_rate_limiter", None)
        if not isinstance(limiter, ResolveRateLimiter):
            limiter = ResolveRateLimiter()
            self._resolve_rate_limiter = limiter
        return limiter

    def _resolve_guard_dict(self, attr: str) -> dict[str, datetime]:
        """Return the per-phone state dict, repairing non-dict legacy values."""
        value = getattr(self, attr, None)
        if not isinstance(value, dict):
            value = {}
            setattr(self, attr, value)
        return value

    def _resolve_backoff_map(self) -> dict[str, datetime]:
        return self._resolve_guard_dict("_resolve_username_backoff_until_utc")

    def _resolve_capable_phones(self) -> set[str]:
        """Phones eligible for live resolves — the pool's connected clients."""
        clients = getattr(self, "clients", None) or {}
        return {str(phone) for phone in clients}

    def _prune_resolve_backoff(self) -> dict[str, datetime]:
        backoff = self._resolve_backoff_map()
        now = datetime.now(timezone.utc)
        for phone in [p for p, until in backoff.items() if until <= now]:
            del backoff[phone]
        return backoff

    def get_resolve_username_backoff_remaining_sec(self, phone: str | None = None) -> int:
        """Remaining live-resolve backoff seconds.

        With ``phone`` — the remaining window for that account only. Without —
        the pool-wide aggregate: ``0`` while at least one connected account is
        free, otherwise the smallest remaining window (the moment the first
        account becomes usable again).
        """
        backoff = self._prune_resolve_backoff()
        now = datetime.now(timezone.utc)
        if phone is not None:
            until = backoff.get(str(phone))
            if until is None:
                return 0
            return int((until - now).total_seconds())
        if not backoff:
            return 0
        phones = self._resolve_capable_phones()
        if phones:
            if any(p not in backoff for p in phones):
                return 0
            candidates = [backoff[p] for p in phones]
        else:
            candidates = list(backoff.values())
        return int((min(candidates) - now).total_seconds())

    def get_resolve_username_backoff_until(self, phone: str | None = None) -> datetime | None:
        """Backoff deadline — per-phone, or the aggregate min across the pool."""
        backoff = self._prune_resolve_backoff()
        if phone is not None:
            return backoff.get(str(phone))
        if not backoff:
            return None
        phones = self._resolve_capable_phones()
        if phones:
            if any(p not in backoff for p in phones):
                return None
            return min(backoff[p] for p in phones)
        return min(backoff.values())

    def has_resolve_capable_phone(self, exclude: set[str] | None = None) -> bool:
        """True if a connected account outside ``exclude`` can resolve live now."""
        excluded = {str(p) for p in (exclude or set())}
        return any(
            phone not in excluded
            and self.get_resolve_username_backoff_remaining_sec(phone) == 0
            for phone in self._resolve_capable_phones()
        )

    def is_resolve_ramp_up_active(self, phone: str) -> bool:
        ramp = self._resolve_guard_dict("_resolve_ramp_up_until_utc")
        until = ramp.get(str(phone))
        if until is None:
            return False
        if (until - datetime.now(timezone.utc)).total_seconds() <= 0:
            del ramp[str(phone)]
            return False
        return True

    def set_resolve_username_backoff(self, wait_seconds: int, *, phone: str) -> datetime:
        phone = str(phone)
        new_until = datetime.now(timezone.utc) + timedelta(
            seconds=max(0, int(wait_seconds))
        )
        backoff = self._resolve_backoff_map()
        existing = backoff.get(phone)
        if existing is not None and existing > new_until:
            logger.warning(
                "resolve backoff [%s]: keeping existing %s (new would be %s for wait_seconds=%d)",
                phone,
                existing.isoformat(),
                new_until.isoformat(),
                wait_seconds,
            )
            return existing
        backoff[phone] = new_until
        # Ramp-up: 10% of flood wait, max 1h
        ramp_duration = min(wait_seconds * 0.1, 3600)
        ramp = self._resolve_guard_dict("_resolve_ramp_up_until_utc")
        ramp[phone] = new_until + timedelta(seconds=ramp_duration)
        logger.warning(
            "resolve backoff [%s]: set until %s (wait_seconds=%d, ramp-up until %s)",
            phone,
            new_until.isoformat(),
            wait_seconds,
            ramp[phone].isoformat(),
        )
        return new_until

    async def persist_resolve_username_backoff(self) -> None:
        """Persist the per-phone backoff deadlines to DB settings."""
        db = getattr(self, "_db", None)
        if db is None:
            return
        backoff = self._prune_resolve_backoff()
        payload = json.dumps(
            {phone: until.isoformat() for phone, until in backoff.items()}
        )
        try:
            await db.set_setting(RESOLVE_BACKOFF_BY_PHONE_SETTING, payload)
        except Exception:
            logger.debug("Failed to persist resolve_username backoff", exc_info=True)

    def _restore_resolve_backoff_entry(self, phone: str, until: datetime) -> None:
        remaining = (until - datetime.now(timezone.utc)).total_seconds()
        if remaining <= 0:
            return
        self._resolve_backoff_map()[phone] = until
        # Also restore ramp-up: 10% of remaining, max 1h
        ramp_duration = min(remaining * 0.1, 3600)
        self._resolve_guard_dict("_resolve_ramp_up_until_utc")[phone] = until + timedelta(
            seconds=ramp_duration
        )
        logger.warning(
            "resolve backoff [%s]: restored from DB until %s (%.0fs remaining)",
            phone,
            until.isoformat(),
            remaining,
        )

    @staticmethod
    def _parse_backoff_deadline(value: object) -> datetime | None:
        try:
            restored = datetime.fromisoformat(str(value))
        except (ValueError, TypeError):
            return None
        if restored.tzinfo is None:
            restored = restored.replace(tzinfo=timezone.utc)
        return restored

    async def restore_resolve_username_backoff(
        self, db: object, *, phones: Iterable[str] | None = None
    ) -> None:
        """Restore backoff from DB on startup. Call once during pool init.

        ``phones`` (known account phones) is only needed for the one-time
        migration of the legacy single-deadline setting: an active legacy
        backoff is conservatively applied to every known phone, re-persisted
        in the per-phone format, and the legacy key is cleared.
        """
        # ``db`` stays ``object`` (pool_lifecycle mirrors this signature); the cast
        # only gives mypy the settings get/set pair actually used here.
        store = cast("_SettingsStore", db)
        try:
            value = await store.get_setting(RESOLVE_BACKOFF_BY_PHONE_SETTING)
        except Exception:
            return
        if value:
            try:
                entries = json.loads(value)
            except (ValueError, TypeError):
                entries = None
            if isinstance(entries, dict):
                for phone, raw_until in entries.items():
                    until = self._parse_backoff_deadline(raw_until)
                    if until is not None:
                        self._restore_resolve_backoff_entry(str(phone), until)
                return
        # Legacy single-deadline migration (pre-#790).
        try:
            legacy_value = await store.get_setting(RESOLVE_BACKOFF_LEGACY_SETTING)
        except Exception:
            return
        if not legacy_value:
            return
        until = self._parse_backoff_deadline(legacy_value)
        if until is None or (until - datetime.now(timezone.utc)).total_seconds() <= 0:
            return
        phone_list = [str(p) for p in (phones or []) if str(p)]
        if not phone_list:
            # No known accounts yet — leave the legacy key for the next start.
            return
        for phone in phone_list:
            self._restore_resolve_backoff_entry(phone, until)
        try:
            await store.set_setting(
                RESOLVE_BACKOFF_BY_PHONE_SETTING,
                json.dumps({phone: until.isoformat() for phone in phone_list}),
            )
            await store.set_setting(RESOLVE_BACKOFF_LEGACY_SETTING, "")
        except Exception:
            logger.debug("Failed to migrate legacy resolve backoff", exc_info=True)

    def reserve_resolve_username_call(self, phone: str) -> float:
        phone = str(phone or "unknown")
        remaining = self.get_resolve_username_backoff_remaining_sec(phone)
        if remaining > 0:
            return float(remaining)
        if self.is_resolve_ramp_up_active(phone):
            min_interval = getattr(self, "_resolve_ramp_up_min_interval_sec", 5.0)
            last_map = self._resolve_guard_dict("_resolve_ramp_up_last_call_utc")
            last = last_map.get(phone)
            now = datetime.now(timezone.utc)
            if last is not None:
                elapsed = (now - last).total_seconds()
                if elapsed < min_interval:
                    return min_interval - elapsed
            last_map[phone] = now
            return 0.0
        return self._get_resolve_rate_limiter().try_acquire(phone)

    @staticmethod
    def _is_live_username_peer(peer: object) -> bool:
        if not isinstance(peer, str):
            return False
        value = peer.strip()
        if not value:
            return False
        return not value.lstrip("-").isdigit()

    def _record_resolve_username_flood(self, wait_seconds: int, *, phone: str) -> datetime | None:
        if wait_seconds <= GLOBAL_RESOLVE_BACKOFF_THRESHOLD_SEC:
            return None
        return self.set_resolve_username_backoff(wait_seconds, phone=phone)

    async def run_live_username_resolve(
        self,
        awaitable_factory: Callable[[], Awaitable[object]],
        *,
        phone: str,
        username: str,
        operation: str,
        logger_: logging.Logger | None = None,
        timeout: float | None = 30.0,
    ) -> object:
        """Run a live username resolve behind the shared FloodWait guard."""
        retry_after = self.reserve_resolve_username_call(phone)
        if retry_after > 0:
            raise UsernameResolveRateLimitedError(phone, retry_after)
        try:
            return await run_with_flood_wait(
                awaitable_factory(),
                operation=operation,
                phone=phone,
                pool=self,
                logger_=logger_ or logger,
                timeout=timeout,
            )
        except HandledFloodWaitError as exc:
            next_available_at = self._record_resolve_username_flood(
                exc.info.wait_seconds, phone=phone
            )
            if next_available_at is not None:
                await self.persist_resolve_username_backoff()
                (logger_ or logger).warning(
                    "%s got FloodWait %ss on %s while resolving %s; "
                    "pausing live username resolves on %s until %s",
                    operation,
                    exc.info.wait_seconds,
                    phone,
                    username,
                    phone,
                    next_available_at.isoformat(),
                )
            raise
