"""Flood-wait rotation and availability for the Telegram client pool (#1046).

Extracted from the ``ClientPool`` monolith as a composition mixin. Owns the two
flood-wait surfaces the pool exposes:

* **generic account flood-wait** — persisted in ``accounts.flood_wait_until`` and
  honoured by :class:`~src.telegram.account_lease_pool.AccountLeasePool` during
  account selection; reported via :meth:`FloodRotationMixin.report_flood`.
* **premium-only flood-wait** — kept in-memory (``_premium_flood_wait_until``)
  so a premium-search flood on one account does not freeze its generic use.

The mixin also answers the "can anyone rotate / when is the next account free"
questions the collector and scheduler ask. It relies on attributes the concrete
``ClientPool`` initialises (``clients``, ``_lease_pool``,
``_premium_flood_wait_until``, ``_db``) and on helpers provided by the lifecycle
mixin / resolve guard (``_connected_phones``, ``_acquire_from_lease``,
``get_resolve_username_backoff_*``). Splitting it out does NOT change behaviour —
the same methods run on the same single ``self``.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from src.database.live_accounts import load_live_usable_accounts
from src.telegram.backends import TelegramTransportSession
from src.telegram.flood_wait import (
    FLOOD_WAIT_RETRY_BUFFER_SEC,
    TRANSIENT_FLOOD_WAIT_MAX_SEC,
    is_transient_flood_wait_seconds,
)
from src.telegram.utils import normalize_utc

if TYPE_CHECKING:
    from src.database import Database
    from src.models import Account
    from src.telegram.account_lease_pool import AccountLease, AccountLeasePool

logger = logging.getLogger(__name__)


class FloodRotationMixin:
    """Flood-wait reporting, premium-flood tracking, and availability queries.

    A composition mixin for ``ClientPool``; it depends on state the concrete
    pool initialises in ``__init__`` and on helpers provided by the lifecycle
    mixin / resolve guard. The annotations below declare that cross-mixin
    contract for the type checker (mirrors ``ResolveGuardMixin``).
    """

    # Provided by ClientPool.__init__ / ClientLifecycleMixin / ResolveGuardMixin.
    _db: Database
    _lease_pool: AccountLeasePool
    _premium_flood_wait_until: dict[str, datetime]
    clients: dict[str, object]

    if TYPE_CHECKING:

        def _connected_phones(self) -> set[str]: ...

        async def _acquire_from_lease(
            self,
            account_lease: AccountLease,
            *,
            force_native: bool = False,
            report_generic_flood: bool = True,
        ) -> tuple[TelegramTransportSession, str] | None: ...

        async def _get_account_for_phone(
            self, phone: str, *, active_only: bool = True
        ) -> Account | None: ...

        # Signatures mirror ResolveGuardMixin exactly: ClientPool inherits all
        # four mixins, so a divergent stub here would make mypy flag the method
        # as incompatibly redefined across base classes (#1046).
        def get_resolve_username_backoff_remaining_sec(
            self, phone: str | None = None
        ) -> int: ...

        def get_resolve_username_backoff_until(
            self, phone: str | None = None
        ) -> datetime | None: ...

    async def has_rotatable_resolve_phone(
        self, exclude: set[str] | frozenset[str] = frozenset()
    ) -> bool:
        """True if some connected account outside ``exclude`` can run a live
        username resolve right now (#790).

        Stricter than the sync :meth:`has_resolve_capable_phone`, which only
        knows the resolve-backoff map: this also rejects accounts in a *generic*
        flood wait (``accounts.flood_wait_until``, known only to the DB) and
        accounts already leased out. The collector uses it to decide whether
        rotating a channel to another account can actually succeed — avoiding a
        rotate-into-dead-end that would otherwise abort the whole run.
        """
        excluded = {str(p) for p in exclude}
        # First narrow to phones that are not in resolve backoff (sync), then
        # let the lease pool reject generic-flooded / in-use ones (async).
        candidates = {
            phone
            for phone in self._connected_phones() - excluded
            if self.get_resolve_username_backoff_remaining_sec(phone) == 0
        }
        if not candidates:
            return False
        return await self._lease_pool.available_exclusive_count(candidates) > 0

    async def next_resolve_capable_at(self) -> datetime | None:
        """Earliest UTC moment any connected account can run a live username
        resolve again (#790).

        Per phone the readiness is ``max(resolve backoff, generic
        accounts.flood_wait_until)``; the result is the minimum across
        connected phones. Returns ``None`` when some account is capable right
        now (even if transiently leased) or when nothing is connected — the
        caller then falls back to its own deadline.
        """
        connected = self._connected_phones()
        if not connected:
            return None
        now = datetime.now(timezone.utc)
        accounts = await load_live_usable_accounts(self._db, active_only=True)
        generic: dict[str, datetime] = {}
        for account in accounts:
            until = normalize_utc(getattr(account, "flood_wait_until", None))
            if until is not None:
                generic[account.phone] = until
        earliest: datetime | None = None
        for phone in connected:
            deadlines = [
                until
                for until in (
                    self.get_resolve_username_backoff_until(phone),
                    generic.get(phone),
                )
                if until is not None and until > now
            ]
            if not deadlines:
                return None
            ready = max(deadlines)
            if earliest is None or ready < earliest:
                earliest = ready
        return earliest

    async def available_stats_client_count(self) -> int:
        return await self._lease_pool.available_exclusive_count(self._connected_phones())

    async def available_collection_client_count(self) -> int:
        return await self._lease_pool.available_exclusive_count(self._connected_phones())

    async def get_premium_client(self) -> tuple[TelegramTransportSession, str] | None:
        """Get first available premium client.

        Premium-only flood waits are tracked separately from generic account flood waits.
        """
        blocked_phones = self._premium_flooded_phones()
        for _ in range(max(1, len(self.clients))):
            lease = await self._lease_pool.acquire_premium(
                self._connected_phones(),
                blocked_phones=blocked_phones,
            )
            if lease is None:
                return None
            result = await self._acquire_from_lease(
                lease,
                report_generic_flood=False,
            )
            if result is not None:
                return result
        return None

    async def get_premium_unavailability_reason(self) -> str:
        accounts = await load_live_usable_accounts(self._db, active_only=True)
        premium = [acc for acc in accounts if acc.is_premium]
        if not premium:
            return "Нет аккаунтов с Telegram Premium. Добавьте Premium-аккаунт в настройках."
        connected = [acc for acc in premium if acc.phone in self.clients]
        if not connected:
            return "Premium-аккаунт не подключён. Перезапустите сервер."
        now = datetime.now(timezone.utc)
        blocked = [
            phone
            for phone, until in self._premium_flood_wait_until.items()
            if phone in {acc.phone for acc in connected} and until > now
        ]
        if blocked and len(blocked) == len(connected):
            return "Premium-аккаунты временно недоступны из-за Flood Wait."
        return "Premium-аккаунт недоступен."

    async def get_stats_availability(self):
        """Describe stats client availability for batch scheduling decisions."""
        # Imported lazily-by-name from the host module to avoid a circular import
        # of the StatsClientAvailability dataclass at module load time.
        from src.telegram.client_pool import StatsClientAvailability

        state, retry_after_sec, next_available_at_utc = (
            await self._lease_pool.snapshot_stats_availability(self._connected_phones())
        )
        return StatsClientAvailability(
            state=state,
            retry_after_sec=retry_after_sec,
            next_available_at_utc=next_available_at_utc,
        )

    async def get_premium_stats_availability(self):
        """Describe premium client availability including premium-only flood waits."""
        from src.telegram.client_pool import StatsClientAvailability

        accounts = await load_live_usable_accounts(self._db, active_only=True)
        now = datetime.now(timezone.utc)
        connected_premium = [
            acc
            for acc in accounts
            if acc.is_premium and acc.phone in self.clients
        ]
        if not connected_premium:
            return StatsClientAvailability(state="no_connected_active")

        earliest: datetime | None = None
        for account in connected_premium:
            premium_until = self._premium_flood_wait_until.get(account.phone)
            if premium_until is None or premium_until <= now:
                return StatsClientAvailability(state="available")
            if earliest is None or premium_until < earliest:
                earliest = premium_until

        if earliest is None:
            return StatsClientAvailability(state="no_connected_active")

        retry_after_sec = max(1, int((earliest - now).total_seconds()))
        return StatsClientAvailability(
            state="all_flooded",
            retry_after_sec=retry_after_sec,
            next_available_at_utc=earliest,
        )

    async def report_flood(self, phone: str, wait_seconds: int) -> None:
        """Mark account as flood-waited."""
        until = datetime.now(timezone.utc) + timedelta(seconds=wait_seconds)
        await self._db.update_account_flood(phone, until)
        level = logging.INFO if is_transient_flood_wait_seconds(wait_seconds) else logging.WARNING
        logger.log(level, "Flood wait for %s: %d seconds (until %s)", phone, wait_seconds, until)

    async def report_premium_flood(self, phone: str, wait_seconds: int) -> None:
        """Mark account as premium-search flood-waited without touching generic account state."""
        now = datetime.now(timezone.utc)
        until = now + timedelta(seconds=wait_seconds)
        self._premium_flood_wait_until[phone] = until
        # Eager cleanup of expired entries
        expired = [p for p, u in self._premium_flood_wait_until.items() if u <= now and p != phone]
        for p in expired:
            del self._premium_flood_wait_until[p]
        logger.warning(
            "Premium flood wait for %s: %d seconds (until %s)",
            phone,
            wait_seconds,
            until,
        )

    async def clear_flood(self, phone: str) -> None:
        await self._db.update_account_flood(phone, None)

    def clear_premium_flood(self, phone: str) -> None:
        self._premium_flood_wait_until.pop(phone, None)

    def _premium_flooded_phones(self) -> set[str]:
        now = datetime.now(timezone.utc)
        expired = [
            phone
            for phone, until in self._premium_flood_wait_until.items()
            if until <= now
        ]
        for phone in expired:
            self._premium_flood_wait_until.pop(phone, None)
        return set(self._premium_flood_wait_until)

    async def _await_transient_flood(self, phone: str) -> None:
        """Sleep out a transient (<=60s) flood-wait on *phone* before acquiring.

        Centralizes "flood before the operation" handling for the by-phone write
        path: get_available_client rotates past flood, but a pinned phone cannot,
        so without this it fails outright. Only waits transient floods (matching
        run_with_flood_wait_retry's threshold); longer floods fall through and the
        caller still gets None. Re-reads the account after sleeping in case the
        flood was cleared meanwhile.
        """
        account = await self._get_account_for_phone(phone)
        if account is None:
            return
        flood_until = normalize_utc(account.flood_wait_until)
        now = datetime.now(timezone.utc)
        if not flood_until or flood_until <= now:
            return
        remaining = (flood_until - now).total_seconds()
        if remaining > TRANSIENT_FLOOD_WAIT_MAX_SEC:
            return  # too long to wait inline; let the caller get None as before
        logger.info("Waiting %.1fs for transient flood-wait on %s", remaining, phone)
        await asyncio.sleep(remaining + FLOOD_WAIT_RETRY_BUFFER_SEC)
