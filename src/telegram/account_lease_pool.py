from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone

from src.database import Database
from src.database.live_accounts import load_live_usable_accounts
from src.models import Account
from src.telegram.utils import normalize_utc


@dataclass(frozen=True)
class AccountLease:
    account: Account
    shared: bool


class AccountLeasePool:
    """Own account selection and in-use tracking independently from the client backend."""

    def __init__(self, db: Database, in_use: set[str]):
        self._db = db
        self._in_use = in_use
        self._lock = asyncio.Lock()
        self._last_phone: str | None = None

    async def acquire_available(self, connected_phones: set[str]) -> AccountLease | None:
        async with self._lock:
            now = datetime.now(timezone.utc)
            accounts = await load_live_usable_accounts(self._db, active_only=True)

            # Proactively clear flood_wait_until values that have already expired.
            for account in accounts:
                flood_until = normalize_utc(account.flood_wait_until)
                if flood_until is not None and flood_until <= now:
                    await self._db.update_account_flood(account.phone, None)
                    account.flood_wait_until = None

            ordered_accounts = self._round_robin_accounts(accounts)
            if not ordered_accounts:
                return None

            for account in ordered_accounts:
                if account.phone not in connected_phones:
                    continue
                if account.phone in self._in_use:
                    continue
                if self._is_flood_waited(account, now):
                    continue
                self._in_use.add(account.phone)
                self._last_phone = account.phone
                return AccountLease(account=account, shared=False)

            for account in ordered_accounts:
                if account.phone not in connected_phones:
                    continue
                if self._is_flood_waited(account, now):
                    continue
                self._last_phone = account.phone
                return AccountLease(account=account, shared=True)

            return None

    async def acquire_by_phone(self, phone: str, connected_phones: set[str]) -> AccountLease | None:
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
        async with self._lock:
            self._in_use.discard(phone)

    async def snapshot_stats_availability(
        self,
        connected_phones: set[str],
    ) -> tuple[str, int | None, datetime | None]:
        now = datetime.now(timezone.utc)
        accounts = await self.get_connected_accounts(connected_phones)
        if not accounts:
            return "no_connected_active", None, None

        earliest: datetime | None = None
        for account in accounts:
            flood_until = normalize_utc(account.flood_wait_until)
            if flood_until is None or flood_until <= now:
                return "available", None, None
            if earliest is None or flood_until < earliest:
                earliest = flood_until

        if earliest is None:
            return "no_connected_active", None, None

        retry_after_sec = max(1, int((earliest - now).total_seconds()))
        return "all_flooded", retry_after_sec, earliest

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
