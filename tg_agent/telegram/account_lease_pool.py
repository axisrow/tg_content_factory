from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone

from tg_agent.database import Database
from tg_agent.database.live_accounts import load_live_usable_accounts
from tg_agent.models import Account
from tg_agent.telegram.flood_scope import family_floods_repo
from tg_agent.telegram.utils import normalize_utc


@dataclass(frozen=True)
class AccountLease:
    account: Account
    shared: bool


class AccountLeasePool:
    """Own account selection and in-use tracking independently from the client backend.

    ``_lock`` serializes the at-most-one exclusive reservation; shared leases
    are allowed concurrently for pinned read paths. Callers must release every
    lease; ``ClientPool`` holds its lock before calling :meth:`release` so the
    exclusive marker is never half-released.
    """

    def __init__(self, db: Database, in_use: set[str]):
        self._db = db
        self._in_use = in_use
        self._lock = asyncio.Lock()
        self._last_phone: str | None = None

    @staticmethod
    async def _family_floods(db: Database, now: datetime) -> dict[str, dict[str, datetime]]:
        """Active per-family floods, empty on stores without the family slice.

        A store without the slice (see ``family_floods_repo``) selects exactly
        as before (#1832).
        """
        repo = family_floods_repo(db)
        if repo is None:
            return {}
        return await repo.list_active_family_floods(now=now)

    @staticmethod
    def _account_block_sources(
        account: Account,
        family_floods: dict[str, dict[str, datetime]],
        now: datetime,
        allow_families: frozenset[str],
        block_families: frozenset[str] | None = None,
    ) -> tuple[datetime | None, str]:
        """The rule of :meth:`_blocked_until` plus WHAT blocks, for diagnostics (#1847).

        One computation for both the pick/verdict and the per-account detail
        rows: the ruler cannot drift between what blocks an account and what
        the diagnostics say blocks it (#1879 lesson).
        """
        if block_families is None:
            block_families = frozenset(family_floods.get(account.phone, {})) - allow_families
        deadlines: list[datetime] = []
        sources: list[str] = []
        generic = normalize_utc(account.flood_wait_until)
        if generic is not None and generic > now:
            deadlines.append(generic)
            sources.append("account-wide")
        for family, until in sorted((family_floods.get(account.phone, {})).items()):
            if family not in block_families:
                continue
            deadline = normalize_utc(until)
            if deadline is not None and deadline > now:
                deadlines.append(deadline)
                sources.append(f"family: {family}")
        blocked_until = max((until for until in deadlines if until is not None), default=None)
        return blocked_until, ", ".join(sources) or "unknown"

    @classmethod
    def _blocked_until(
        cls,
        account: Account,
        family_floods: dict[str, dict[str, datetime]],
        now: datetime,
        allow_families: frozenset[str],
        block_families: frozenset[str] | None = None,
    ) -> datetime | None:
        """Until when ``account`` stays out of a pick; None while it is free.

        The account-wide flood plus every per-family flood (#1832) outside
        ``allow_families``. The pick and the availability report share this
        rule (#1879): when they disagreed, collection looped on "no active
        connected clients" while every account sat under a family flood.

        With ``block_families`` the ruler narrows to what the pick needs
        (#1889): only the account-wide flood and those families block — the
        lookup-retry pick passes the lookup's own families (a flood on an
        unrelated read family does not refuse a lookup), mirroring
        ``lookup_capable_phone_count``. ``block_families`` wins when both are
        given.
        """
        return cls._account_block_sources(
            account, family_floods, now, allow_families, block_families
        )[0]

    async def acquire_available(
        self,
        connected_phones: set[str],
        *,
        prefer: str | None = None,
        allow_families: frozenset[str] = frozenset(),
        block_families: frozenset[str] | None = None,
    ) -> AccountLease | None:
        """Acquire an account with at most one exclusive reservation per phone.

        ``prefer`` goes first while it is free and unflooded (#1877): a username
        channel's access hash lives on the account that resolved it. A flood in
        one of ``allow_families`` does not take an account out of this pick
        (#1879). ``block_families`` narrows the ruler to those families only
        (the lookup-retry pick, #1889).
        """
        async with self._lock:
            now = datetime.now(timezone.utc)
            accounts = await load_live_usable_accounts(self._db, active_only=True)

            # Proactively clear flood_wait_until values that have already expired.
            for account in accounts:
                flood_until = normalize_utc(account.flood_wait_until)
                if flood_until is not None and flood_until <= now:
                    await self._db.update_account_flood(account.phone, None)
                    account.flood_wait_until = None

            family_floods = await self._family_floods(self._db, now)
            ordered_accounts = self._round_robin_accounts(accounts)
            if prefer is not None:
                ordered_accounts = sorted(ordered_accounts, key=lambda account: account.phone != prefer)
            free = [
                account
                for account in ordered_accounts
                if account.phone in connected_phones
                and self._blocked_until(account, family_floods, now, allow_families, block_families) is None
            ]

            for account in free:
                if account.phone in self._in_use:
                    continue
                self._in_use.add(account.phone)
                self._last_phone = account.phone
                return AccountLease(account=account, shared=False)

            if free:
                self._last_phone = free[0].phone
                return AccountLease(account=free[0], shared=True)
            return None

    async def available_exclusive_count(
        self,
        connected_phones: set[str],
        *,
        allow_families: frozenset[str] = frozenset(),
        block_families: frozenset[str] | None = None,
    ) -> int:
        async with self._lock:
            now = datetime.now(timezone.utc)
            accounts = await load_live_usable_accounts(self._db, active_only=True)
            family_floods = await self._family_floods(self._db, now)
            count = 0
            for account in accounts:
                flood_until = normalize_utc(account.flood_wait_until)
                if flood_until is not None and flood_until <= now:
                    await self._db.update_account_flood(account.phone, None)
                    account.flood_wait_until = None
                if account.phone not in connected_phones or account.phone in self._in_use:
                    continue
                if self._blocked_until(account, family_floods, now, allow_families, block_families) is None:
                    count += 1
            return count

    async def acquire_by_phone(self, phone: str, connected_phones: set[str]) -> AccountLease | None:
        """Acquire ``phone`` atomically; an existing marker yields shared."""
        async with self._lock:
            account = await self._get_account(phone)
            if account is None or account.phone not in connected_phones:
                return None
            if self._is_flood_waited(account):
                return None
            shared = phone in self._in_use
            if not shared:
                self._in_use.add(phone)
            return AccountLease(account=account, shared=shared)

    async def acquire_premium(
        self,
        connected_phones: set[str],
        *,
        blocked_phones: set[str] | None = None,
    ) -> AccountLease | None:
        async with self._lock:
            accounts = await load_live_usable_accounts(self._db, active_only=True)
            blocked_phones = blocked_phones or set()

            for account in accounts:
                if not account.is_premium or account.phone not in connected_phones:
                    continue
                if account.phone in blocked_phones:
                    continue
                if account.phone in self._in_use:
                    continue
                self._in_use.add(account.phone)
                return AccountLease(account=account, shared=False)

            for account in accounts:
                if not account.is_premium or account.phone not in connected_phones:
                    continue
                if account.phone in blocked_phones:
                    continue
                return AccountLease(account=account, shared=True)

            return None

    async def get_connected_accounts(self, connected_phones: set[str]) -> list[Account]:
        accounts = await load_live_usable_accounts(self._db, active_only=True)
        return [account for account in accounts if account.phone in connected_phones]

    async def get_account(self, phone: str, *, active_only: bool = True) -> Account | None:
        accounts = await load_live_usable_accounts(self._db, active_only=active_only)
        for account in accounts:
            if account.phone == phone:
                return account
        return None

    async def release(self, phone: str) -> None:
        """Clear the exclusive marker for ``phone`` under the lease-pool lock."""
        async with self._lock:
            self._in_use.discard(phone)

    async def snapshot_stats_availability(
        self,
        connected_phones: set[str],
        *,
        allow_families: frozenset[str] = frozenset(),
        block_families: frozenset[str] | None = None,
    ) -> tuple[str, int | None, datetime | None, tuple[tuple[str, datetime, str], ...]]:
        """Whether some connected account is free now, else when the first frees up.

        Judged by the pick's own rule (:meth:`_blocked_until`), with the same
        ``allow_families`` the matching pick uses (#1879) — or the same
        ``block_families`` for the lookup-retry pick and its report (#1889).
        The fourth element carries per-account detail rows
        (phone, blocked_until, sources) for diagnostics (#1847).
        """
        now = datetime.now(timezone.utc)
        accounts = await self.get_connected_accounts(connected_phones)
        if not accounts:
            return "no_connected_active", None, None, ()

        family_floods = await self._family_floods(self._db, now)
        deadlines: list[datetime] = []
        rows: list[tuple[str, datetime, str]] = []
        for account in accounts:
            until, sources = self._account_block_sources(
                account, family_floods, now, allow_families, block_families
            )
            if until is None:
                return "available", None, None, ()
            deadlines.append(until)
            rows.append((account.phone, until, sources))

        earliest = min(deadlines)
        retry_after_sec = max(1, int((earliest - now).total_seconds()))
        return "all_flooded", retry_after_sec, earliest, tuple(rows)

    async def _get_account(self, phone: str) -> Account | None:
        return await self.get_account(phone, active_only=True)

    def _round_robin_accounts(self, accounts: list[Account]) -> list[Account]:
        if not accounts:
            return []
        if self._last_phone is None:
            return accounts
        for i, account in enumerate(accounts):
            if account.phone == self._last_phone:
                start = (i + 1) % len(accounts)
                return accounts[start:] + accounts[:start]
        return accounts

    @classmethod
    def _is_flood_waited(cls, account: Account, now: datetime | None = None) -> bool:
        now = now or datetime.now(timezone.utc)
        flood_until = normalize_utc(account.flood_wait_until)
        return flood_until is not None and flood_until > now
