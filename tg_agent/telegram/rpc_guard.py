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
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from telethon import TelegramClient, errors
from telethon_floodgate import (
    DEFAULT_RPC_OPERATIONS,
    FloodCircuitBreaker,
    HandledFloodWaitError,
    ResolveRateLimiter,
    RpcDispatchAdmission,
    RpcDispatchBudget,
    RpcDispatchDecision,
    RpcDispatchPolicy,
    TelegramRateLimitedError,
    TelegramRateLimitGate,
    TelegramRpcDeferredError,
    attach_rpc_gate,
)

from tg_agent.telegram.outgoing_ceiling import OutgoingRateCeiling

logger = logging.getLogger(__name__)
FLOOD_ERRORS = (errors.FloodWaitError, getattr(errors, "FloodPremiumWaitError", errors.FloodWaitError),
                errors.SlowModeWaitError, errors.FloodTestPhoneWaitError)

# RPC constructors floodgate's DEFAULT_RPC_OPERATIONS has no row for: without
# one, the dispatch boundary derives the tag "rpc:<ClassName>" and the gate
# warns into the permissive "default" bucket (#1903). Same gap-fill policy as
# _GATE_OPERATION_CATEGORIES in backends.py — floodgate stays authoritative
# (merged UNDER its table, so any future floodgate row wins the lookup), and
# a row here is dropped once a floodgate release registers the constructor.
# test_relayed_comment_send_costs_a_deliberate_gate_category (test_relay_comment_to.py)
# pins both the row and its pricing.
_RPC_OPERATION_TAGS: dict[str, str] = {
    # Resolves the linked discussion group before a comment send: a one-shot
    # lookup of the same shape as floodgate's registered ReadDiscussionRequest
    # ("rpc_default" — floodgate normalizes its own "default" rows to this
    # registered boundary tag).
    "GetDiscussionMessageRequest": "rpc_default",
}

#: The policy every owned guard runs: floodgate's table plus the gap rows.
GAP_RPC_POLICY = RpcDispatchPolicy(operations={**_RPC_OPERATION_TAGS, **DEFAULT_RPC_OPERATIONS})


@dataclass
class RpcOperation:
    operation: str
    writes: int = 0
    confirmed: int = 0
    active: bool = True
    tracked: bool = False
    # RPC constructor names actually dispatched under this operation — the
    # only attribution a cancelled/timeout exception still carries (#1828).
    dispatched_rpcs: list[str] = field(default_factory=list)

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
        cast(Any, exc).rpc_diagnostics = diagnostic_fields(exc, current=current)
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
        # A cancellation or timeout carries no RPC of its own: name the one
        # this operation dispatched, so "delivery uncertain" stays attributable.
        if current.dispatched_rpcs and "rpc" not in result:
            result["rpc"] = current.dispatched_rpcs[-1]
        result.update(outer_operation=current.operation, delivery_state=current.delivery_state)
    return result


_DELIVERY_TEXT = {
    "not_sent": "not dispatched",
    "partial": "partially dispatched",
    "uncertain": "delivery uncertain",
}


def describe_rpc_failure(exc: BaseException) -> str:
    """One curated receipt line for a throttled or flood-refused RPC (#1828).

    Composed only from whitelisted dispatch facts — the refusing RPC, the
    outer operation, the delivery verdict and the retry deadline; never
    request arguments or exception text. Errors without guard metadata
    (old receipts, foreign call sites) still render a plain reason.
    """
    fields = diagnostic_fields(exc)
    if isinstance(exc, HandledFloodWaitError):
        # The wrap carries its own curated copy: a receipt saved without the
        # context stamp still renders the RPC and the delivery verdict.
        for name in ("rpc", "delivery_state"):
            value = getattr(exc.info, name, None)
            if isinstance(value, str) and value and name not in fields:
                fields[name] = value
    if isinstance(exc, TelegramRpcDeferredError):
        # The guard refuses the whole dispatch batch before sending it: that
        # verdict is structural — a context stamp (partial batch) still wins.
        fields.setdefault("delivery_state", "not_sent")
        reason = f"local throttling by {exc.budget}"
        deadline = f"; retry in {max(0, int(exc.retry_after_sec))}s"
    elif isinstance(exc, TelegramRateLimitedError):
        reason = "refused by the local flood guard"
        deadline = f"; retry in {max(0, int(exc.retry_after_sec))}s"
    elif isinstance(exc, HandledFloodWaitError):
        reason = "Telegram FloodWait"
        deadline = (f"; retry in {exc.info.wait_seconds}s "
                    f"(at {exc.info.next_available_at_utc.isoformat()} UTC)")
    else:
        # Foreign exception types: name the class, never the message text.
        reason, deadline = type(exc).__name__, ""
    parts = [reason]
    rpc = fields.get("rpc") or getattr(exc, "rpc", None)
    if isinstance(rpc, str):
        parts.append(f"rpc={rpc}")
    if fields.get("outer_operation"):
        parts.append(f"operation={fields['outer_operation']}")
    delivery = fields.get("delivery_state")
    if isinstance(delivery, str) and delivery:
        parts.append(_DELIVERY_TEXT.get(delivery, delivery))
    return ", ".join(parts) + deadline


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
    cast(Any, client)._tg_owned_rpc_guard = guard
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
        self.handle = attach_rpc_gate(client, pool._rate_limit_gate, phone, policy=GAP_RPC_POLICY,
                                      budgets=budgets, on_decision=self._decision, on_result=self._result,
                                      gate_wait_max_sec=5.0)

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
            accounts = await cast(Any, getter)()
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
                deadline = await cast(Any, getter)(self.phone, family)
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
            current.dispatched_rpcs.append(decision.rpc)

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
        flood = cast(Any, exception)
        seconds = max(1, int(flood.seconds))
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
        flood.flood_reporting = self._spawn(self._report(member, flood, deadline))

    async def _report(self, member: RpcDispatchAdmission, exception: BaseException, deadline: datetime) -> None:
        seconds = max(1, int(cast(Any, exception).seconds))
        reporter = getattr(self.pool, "report_family_flood", None)
        if member.read_only and callable(reporter):
            await cast(Any, reporter)(self.phone, seconds, _family(member), operation=member.operation)
            # The durable reporter has updated the shared family cache. Never
            # clear a later concurrent signal while completing this report.
            pending = getattr(self.pool, "_rpc_pending_family_deadlines", {})
            key = (self.phone, _family(member))
            if pending.get(key) == deadline:
                pending.pop(key, None)
        else:
            # Account-wide (write-family) flood: name the request that earned
            # it — the in-memory cause feeds the diagnostics (#1847).
            await self.pool.report_flood(
                self.phone, seconds, rpc=member.rpc, operation=member.operation
            )
            accounts = self.pool._rpc_account_deadlines
            accounts[self.phone] = max(accounts.get(self.phone) or deadline, deadline)
            pending = getattr(self.pool, "_rpc_pending_account_deadlines", {})
            if pending.get(self.phone) == deadline:
                pending.pop(self.phone, None)
            self.pool._rpc_account_generation = getattr(self.pool, "_rpc_account_generation", 0) + 1
        if member.rpc == "ResolveUsernameRequest":
            await self.pool.persist_resolve_username_backoff()
            await self.pool.persist_resolve_limiter_snapshot()
        cast(Any, exception).flood_reported = True
