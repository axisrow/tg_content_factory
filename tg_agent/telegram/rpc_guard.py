"""Bind the upstream dispatch guard to pool-owned Telethon clients.

No limiter lives here: the adapters compose the pool's existing gate,
username limiter, breaker and process ceiling. Correlation records curated
dispatch facts, never request arguments or exception text.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from telethon import TelegramClient, errors
from telethon_floodgate import (
    FloodCircuitBreaker,
    ResolveRateLimiter,
    RpcDispatchAdmission,
    RpcDispatchBudget,
    RpcDispatchDecision,
    TelegramRateLimitGate,
    attach_rpc_gate,
)

from tg_agent.telegram.outgoing_ceiling import OutgoingRateCeiling

logger = logging.getLogger(__name__)
FLOOD_ERRORS = (errors.FloodWaitError, getattr(errors, "FloodPremiumWaitError", errors.FloodWaitError),
                errors.SlowModeWaitError, errors.FloodTestPhoneWaitError)


@dataclass
class RpcOperation:
    operation: str
    writes: int = 0
    confirmed: int = 0
    active: bool = True
    tracked: bool = False

    @property
    def delivery_state(self) -> str:
        if not self.tracked:
            return "uncertain"
        return "not_sent" if self.writes == 0 else "partial" if self.confirmed else "uncertain"


_operation: ContextVar[RpcOperation | None] = ContextVar("telegram_rpc_operation", default=None)


@contextmanager
def rpc_operation(operation: str) -> Iterator[RpcOperation]:
    current = _operation.get()
    if current is not None and current.active:
        yield current
        return
    current = RpcOperation(operation)
    token = _operation.set(current)
    try:
        yield current
    except BaseException as exc:
        exc.rpc_diagnostics = diagnostic_fields(exc, current=current)
        raise
    finally:
        current.active = False
        _operation.reset(token)


def diagnostic_fields(exc: BaseException, *, current: RpcOperation | None = None) -> dict:
    """Only curated metadata is allowed into receipts, including old errors."""
    saved = getattr(exc, "rpc_diagnostics", None)
    if saved is None and current is None and _operation.get() is None and exc.__cause__ is not None:
        return diagnostic_fields(exc.__cause__)
    if isinstance(saved, dict):
        return {k: v for k, v in saved.items()
                if k in {"outer_operation", "rpc", "category", "source", "delivery_state"}
                and isinstance(v, str)}
    current = current or _operation.get()
    result = {}
    for source, target in (("rpc", "rpc"), ("rpc_category", "category"), ("source", "source")):
        value = getattr(exc, source, None)
        if isinstance(value, str):
            result[target] = value
    if current is not None and (current.tracked or result):
        result.update(outer_operation=current.operation, delivery_state=current.delivery_state)
    return result


def has_rpc_guard(client: Any) -> bool:
    return isinstance(getattr(client, "_tg_owned_rpc_guard", None), OwnedRpcGuard)


def bind_rpc_guard(client: Any, pool: Any, phone: str | None) -> None:
    """Idempotent binding before connect and on adapter acquisition.

    Non-Telethon transport doubles retain adapter-level gating. A replaced
    __call__ that bypasses Telethon's sender is not a supported live backend.
    """
    if isinstance(client, TelegramClient) and type(client).__call__ is not TelegramClient.__call__:
        if getattr(pool, "_require_rpc_guard", False) is True:
            raise ValueError("Owned Telethon clients must preserve the native dispatch contract")
        return  # non-native iterator/transport doubles retain outer gating
    if (phone is None or pool is None or not isinstance(client, TelegramClient)
            or not isinstance(getattr(pool, "_rate_limit_gate", None), TelegramRateLimitGate)):
        return
    existing = getattr(client, "_tg_owned_rpc_guard", None)
    if isinstance(existing, OwnedRpcGuard):
        if existing.pool is not pool or existing.phone != phone:
            raise ValueError("A guarded Telegram client cannot change account or pool")
        return
    client.flood_sleep_threshold = 0
    guard = OwnedRpcGuard(client, pool, phone)
    client._tg_owned_rpc_guard = guard
    pool._rpc_guard_enabled = True


async def dispose_rpc_guard(client: Any) -> None:
    guard = getattr(client, "_tg_owned_rpc_guard", None)
    if isinstance(guard, OwnedRpcGuard):
        guard.handle.detach()
        if guard.tasks:
            await asyncio.gather(*tuple(guard.tasks), return_exceptions=True)
        del client._tg_owned_rpc_guard


async def wait_rpc_report(exc: BaseException) -> bool:
    task = getattr(exc, "flood_reporting", None)
    if isinstance(task, asyncio.Task):
        try:
            await asyncio.shield(task)
        except Exception:
            logger.warning("RPC flood persistence failed; retaining conservative fallback", exc_info=True)
    return getattr(exc, "flood_reported", False) is True


def _family(member: RpcDispatchAdmission) -> str:
    if member.rpc == "ResolveUsernameRequest":
        return "resolve"
    if member.category is None:
        return "history"
    return member.category


class OwnedRpcGuard:
    def __init__(self, client: TelegramClient, pool: Any, phone: str):
        self.pool, self.phone = pool, phone
        self.tasks: set[asyncio.Task] = set()
        self.deadlines: dict[str, datetime | None] = {}
        budgets = [RpcDispatchBudget(self._peek_state, self._claim_probes, name="flood_backoff",
                                      prepare=self._prepare)]
        limiter = getattr(pool, "_resolve_rate_limiter", None)
        # A relayed pool's owner gates every resolve of the same account (#1877): a
        # second username budget here refused first and never got its slot back.
        if isinstance(limiter, ResolveRateLimiter) and vars(pool).get("_relay") is None:
            budgets.append(RpcDispatchBudget(
                self._peek_resolves, self._commit_resolves, name="resolve_username",
                applies=lambda member: member.rpc == "ResolveUsernameRequest",
            ))
        ceiling = getattr(pool, "_outgoing_ceiling", None)
        if isinstance(ceiling, OutgoingRateCeiling):
            budgets.append(RpcDispatchBudget(ceiling.peek_batch, ceiling.commit_batch,
                                              name="process_ceiling", mode="wait",
                                              max_batch_size=ceiling.max_calls))
        self.handle = attach_rpc_gate(client, pool._rate_limit_gate, phone, budgets=budgets,
                                      on_decision=self._decision, on_result=self._result, gate_wait_max_sec=5.0)

    def _spawn(self, coroutine) -> asyncio.Task:
        task = asyncio.create_task(coroutine, name="telegram-rpc-state")
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        # Native-object/event calls may have no adapter waiter. Their state
        # reporting must still finish and exceptions must be observed.
        task.add_done_callback(lambda done: None if done.cancelled() else done.exception())
        return task

    async def _prepare(self, members: Sequence[RpcDispatchAdmission]) -> None:
        current = _operation.get()
        if current is not None:
            current.tracked = True
        now = time.monotonic()
        loaded = getattr(self.pool, "_rpc_accounts_loaded_at", None)
        getter = getattr(getattr(self.pool, "_db", None), "get_accounts", None)
        if callable(getter) and (loaded is None or now - loaded >= 60.0):
            generation = getattr(self.pool, "_rpc_account_generation", 0)
            accounts = await getter()
            # A report concurrent with this DB read takes precedence over a
            # stale snapshot. Periodic refresh also honors operator clears.
            if generation == getattr(self.pool, "_rpc_account_generation", 0):
                deadlines = getattr(self.pool, "_rpc_account_deadlines", None)
                if not isinstance(deadlines, dict):
                    deadlines = self.pool._rpc_account_deadlines = {}
                for account in accounts:
                    deadlines[account.phone] = account.flood_wait_until
                self.pool._rpc_accounts_loaded_at = now
        getter = getattr(self.pool, "family_flood_deadline", None)
        if callable(getter):
            for family in {_family(m) for m in members if m.read_only}:
                deadline = await getter(self.phone, family)
                pending = getattr(self.pool, "_rpc_pending_family_deadlines", {}).get((self.phone, family))
                self.deadlines[family] = max(deadline or pending, pending) if pending else deadline

    def _peek_state(self, members: Sequence[RpcDispatchAdmission]) -> float:
        now = datetime.now(timezone.utc)
        delay = 0.0
        accounts = getattr(self.pool, "_rpc_account_deadlines", {})
        account_until = accounts.get(self.phone) if isinstance(accounts, dict) else None
        pending = getattr(self.pool, "_rpc_pending_account_deadlines", {}).get(self.phone)
        if isinstance(pending, datetime):
            account_until = max(account_until or pending, pending)
        if isinstance(account_until, datetime):
            account_until = account_until.replace(tzinfo=account_until.tzinfo or timezone.utc)
            delay = max(delay, (account_until - now).total_seconds())
        for member in members:
            deadline = self.deadlines.get(_family(member)) if member.read_only else None
            if isinstance(deadline, datetime):
                delay = max(delay, (deadline.replace(tzinfo=deadline.tzinfo or timezone.utc) - now).total_seconds())
        breaker = getattr(self.pool, "_flood_breaker", None)
        if isinstance(breaker, FloodCircuitBreaker):
            for rpc, slots in Counter(m.rpc for m in members).items():
                delay = max(delay, breaker.peek(f"rpc:{rpc}", self.phone, slots=slots))
        return max(0.0, delay)

    def _claim_probes(self, members: Sequence[RpcDispatchAdmission]) -> None:
        breaker = getattr(self.pool, "_flood_breaker", None)
        if isinstance(breaker, FloodCircuitBreaker):
            for rpc in {m.rpc for m in members}:
                breaker.check(f"rpc:{rpc}", self.phone)

    def _peek_resolves(self, members: Sequence[RpcDispatchAdmission]) -> float:
        pool = self.pool
        remaining = pool.get_resolve_username_backoff_remaining_sec(self.phone)
        if remaining > 0:
            return float(remaining)
        delay = pool._resolve_rate_limiter.peek_many(self.phone, len(members))
        if pool.is_resolve_ramp_up_active(self.phone):
            last = getattr(pool, "_resolve_ramp_up_last_call_utc", {}).get(self.phone)
            if isinstance(last, datetime):
                interval = getattr(pool, "_resolve_ramp_up_min_interval_sec", 5.0)
                delay = max(delay, interval - (datetime.now(timezone.utc) - last).total_seconds())
        return max(0.0, delay)

    def _commit_resolves(self, members: Sequence[RpcDispatchAdmission]) -> None:
        pool = self.pool
        pool._resolve_rate_limiter.record_many(self.phone, len(members))
        if pool.is_resolve_ramp_up_active(self.phone):
            pool._resolve_ramp_up_last_call_utc[self.phone] = datetime.now(timezone.utc)
        pool._rpc_snapshot_dirty = True
        task = getattr(pool, "_rpc_snapshot_task", None)
        if not isinstance(task, asyncio.Task) or task.done():
            async def persist():
                while pool._rpc_snapshot_dirty:
                    pool._rpc_snapshot_dirty = False
                    await pool.persist_resolve_limiter_snapshot()
            pool._rpc_snapshot_task = self._spawn(persist())

    def _decision(self, decision: RpcDispatchDecision) -> None:
        current = _operation.get()
        if current is not None:
            current.tracked = True
        if current is not None and current.active and decision.dispatched and not decision.read_only:
            current.writes += 1

    def _result(self, member: RpcDispatchAdmission, exception: BaseException | None) -> None:
        breaker = getattr(self.pool, "_flood_breaker", None)
        if isinstance(breaker, FloodCircuitBreaker):
            if isinstance(exception, FLOOD_ERRORS):
                breaker.record_flood(f"rpc:{member.rpc}", self.phone)
            elif exception is None:
                breaker.record_success(f"rpc:{member.rpc}", self.phone)
            else:
                breaker.release_probe(f"rpc:{member.rpc}", self.phone)
        current = _operation.get()
        if exception is None and current is not None and current.active and not member.read_only:
            current.confirmed += 1
        if not isinstance(exception, FLOOD_ERRORS):
            return
        seconds = max(1, int(exception.seconds))
        deadline = datetime.now(timezone.utc) + timedelta(seconds=seconds)
        if member.read_only:
            self.deadlines[_family(member)] = deadline
            pending = getattr(self.pool, "_rpc_pending_family_deadlines", None)
            if not isinstance(pending, dict):
                pending = self.pool._rpc_pending_family_deadlines = {}
            key = (self.phone, _family(member))
            pending[key] = max(pending.get(key) or deadline, deadline)
        else:
            accounts = getattr(self.pool, "_rpc_account_deadlines", None)
            if not isinstance(accounts, dict):
                accounts = self.pool._rpc_account_deadlines = {}
            accounts[self.phone] = max(accounts.get(self.phone) or deadline, deadline)
            pending = getattr(self.pool, "_rpc_pending_account_deadlines", None)
            if not isinstance(pending, dict):
                pending = self.pool._rpc_pending_account_deadlines = {}
            pending[self.phone] = max(pending.get(self.phone) or deadline, deadline)
            self.pool._rpc_account_generation = getattr(self.pool, "_rpc_account_generation", 0) + 1
        if member.rpc == "ResolveUsernameRequest":
            setter = getattr(self.pool, "set_resolve_username_backoff", None)
            if callable(setter):
                setter(seconds, phone=self.phone)
            limiter = getattr(self.pool, "_resolve_rate_limiter", None)
            if isinstance(limiter, ResolveRateLimiter):
                limiter.tighten(self.phone)
        exception.flood_reporting = self._spawn(self._report(member, exception, deadline))

    async def _report(self, member: RpcDispatchAdmission, exception: BaseException, deadline: datetime) -> None:
        seconds = max(1, int(exception.seconds))
        reporter = getattr(self.pool, "report_family_flood", None)
        if member.read_only and callable(reporter):
            await reporter(self.phone, seconds, _family(member), operation=member.operation)
            # The durable reporter has updated the shared family cache. Never
            # clear a later concurrent signal while completing this report.
            pending = getattr(self.pool, "_rpc_pending_family_deadlines", {})
            key = (self.phone, _family(member))
            if pending.get(key) == deadline:
                pending.pop(key, None)
        else:
            await self.pool.report_flood(self.phone, seconds)
            accounts = self.pool._rpc_account_deadlines
            accounts[self.phone] = max(accounts.get(self.phone) or deadline, deadline)
            pending = getattr(self.pool, "_rpc_pending_account_deadlines", {})
            if pending.get(self.phone) == deadline:
                pending.pop(self.phone, None)
            self.pool._rpc_account_generation = getattr(self.pool, "_rpc_account_generation", 0) + 1
        if member.rpc == "ResolveUsernameRequest":
            await self.pool.persist_resolve_username_backoff()
            await self.pool.persist_resolve_limiter_snapshot()
        exception.flood_reported = True
