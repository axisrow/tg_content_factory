"""Behavioral spec for the agent tool floodgate boundary (#1851).

Port of the upstream chigwell/telegram-mcp#363 runtime tests (Apache-2.0,
see THIRD_PARTY_NOTICES.md), adapted to this repo's composition: the
boundary peeks the pool-owned gate (charging stays at the physical
dispatch boundary, #1840) and feeds the tool-keyed breaker through the
pool's flood reporters.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from claude_agent_sdk import SdkMcpTool
from telethon_floodgate import (
    FloodCircuitBreaker,
    TelegramOperationSuspendedError,
    TelegramRateLimitGate,
)

from tg_agent.agent.tools import floodgate
from tg_agent.agent.tools.floodgate import wrap_tool_with_floodgate
from tg_agent.telegram.tool_flood_scope import current_tool_flood_scope, tool_flood_scope


class _FakeGate(TelegramRateLimitGate):
    """peek_batch defers phones in ``deferred``; records what it saw.

    Subclasses the real gate only so the boundary's isinstance activation
    guard accepts it; the fake overrides every method it exercises.
    """

    def __init__(self, deferred=()):
        self.deferred = set(deferred)
        self.seen: list[tuple[str, tuple]] = []
        self.floods: list[tuple[str, float | None]] = []

    def peek_batch(self, phone, requests):
        self.seen.append((phone, tuple(requests)))
        return 25.0 if phone in self.deferred else 0.0

    def note_flood(self, phone, seconds=None):
        self.floods.append((phone, seconds))
        return 1.0


class _FakeBreaker(FloodCircuitBreaker):
    def __init__(self, suspended=()):
        self.suspended = set(suspended)
        self.floods: list[tuple[str, str | None]] = []
        self.successes: list[tuple[str, str | None]] = []

    def check(self, operation, phone):
        if phone is not None and (operation, phone) in self.suspended:
            raise TelegramOperationSuspendedError(operation, phone, 300.0)

    def record_flood(self, operation, phone):
        self.floods.append((operation, phone))

    def record_success(self, operation, phone):
        self.successes.append((operation, phone))


def _pool(gate=None, breaker=None):
    return SimpleNamespace(
        _rate_limit_gate=gate if gate is not None else _FakeGate(),
        _flood_breaker=breaker if breaker is not None else _FakeBreaker(),
    )


def _db_with_phone(phone: str = "+70000000001"):
    db = MagicMock()
    db.get_account_summaries = AsyncMock(
        return_value=[
            SimpleNamespace(
                phone=phone, is_active=True, is_primary=True, session_status="ok"
            )
        ]
    )
    db.get_setting = AsyncMock(return_value=None)
    return db


def _tool(name: str, calls: list, *, body="ok", raise_exc=None):
    async def handler(args):
        calls.append(dict(args))
        if raise_exc is not None:
            raise raise_exc
        return {"content": [{"type": "text", "text": body}]}

    return SdkMcpTool(name=name, description="test tool", input_schema={}, handler=handler)


def _wrapped(calls, *, name="send_message", pool=None, db=None, **tool_kwargs):
    tool = _tool(name, calls, **tool_kwargs)
    return wrap_tool_with_floodgate(
        tool,
        db=db if db is not None else _db_with_phone(),
        client_pool=pool if pool is not None else _pool(),
    )


@pytest.fixture(autouse=True)
def _phone_bound_tools():
    """Treat the spec's tool names as phone-bound regardless of the real registry.

    mock.patch, not monkeypatch.setattr: PHONE_BINDED_TOOLS is served by the
    permissions module __getattr__, and monkeypatch's undo would pin the
    current snapshot as a real attribute for every later test in the worker.
    """
    with patch(
        "tg_agent.agent.tools.permissions.PHONE_BINDED_TOOLS",
        frozenset({"send_message", "create_telegram_channel", "mark_read"}),
    ):
        yield


async def test_gate_allow_runs_the_tool():
    calls = []
    wrapped = _wrapped(calls)

    result = await wrapped.handler({"phone": "+70000000001"})

    assert result["content"][0]["text"] == "ok"
    assert len(calls) == 1


async def test_gate_defer_returns_rate_limit_reply_without_handler():
    calls = []
    pool = _pool(gate=_FakeGate(deferred={"+70000000001"}))
    wrapped = _wrapped(calls, pool=pool)

    result = await wrapped.handler({"phone": "+70000000001"})

    text = result["content"][0]["text"]
    assert "FLOODGATE-ERR-" in text
    assert "НЕ повторяйте вызов немедленно" in text
    assert "25 сек" in text
    assert calls == []  # the handler was never reached


async def test_gate_defer_releases_a_pinned_trial():
    """check() pins a half-open trial before the gate can defer: no call
    happens, so the deferral must release the trial, not wedge it."""
    calls = []
    breaker = _FakeBreaker()
    pool = _pool(gate=_FakeGate(deferred={"+70000000001"}), breaker=breaker)
    wrapped = _wrapped(calls, pool=pool)

    await wrapped.handler({"phone": "+70000000001"})

    assert breaker.successes == [("send_message", "+70000000001")]


async def test_kill_switch_env_runs_the_tool_despite_full_gate(monkeypatch):
    monkeypatch.setenv("TG_AGENT_FLOODGATE", "0")
    calls = []
    pool = _pool(gate=_FakeGate(deferred={"+70000000001"}))
    wrapped = _wrapped(calls, pool=pool)

    result = await wrapped.handler({"phone": "+70000000001"})

    assert result["content"][0]["text"] == "ok"
    assert len(calls) == 1


async def test_open_breaker_defers_the_tool_without_settling():
    calls = []
    breaker = _FakeBreaker(suspended={("send_message", "+70000000001")})
    pool = _pool(breaker=breaker)
    wrapped = _wrapped(calls, pool=pool)

    first = await wrapped.handler({"phone": "+70000000001"})
    second = await wrapped.handler({"phone": "+70000000001"})

    assert calls == []
    # A suspension grants nothing, so neither completion may settle the pair
    # as a success — the breaker must stay open past the first refusal.
    assert "5.0 мин" in first["content"][0]["text"]
    assert breaker.successes == []
    assert "мин" in second["content"][0]["text"]


async def test_unknown_tool_takes_the_default_category():
    calls = []
    gate = _FakeGate()
    pool = _pool(gate=gate)
    wrapped = _wrapped(calls, name="mark_read", pool=pool)  # phone-bound, unannotated

    await wrapped.handler({"phone": "+70000000001"})

    assert gate.seen == [("+70000000001", (("default", None),))]


async def test_missing_phone_resolves_the_default_account():
    calls = []
    gate = _FakeGate()
    pool = _pool(gate=gate)
    wrapped = _wrapped(calls, pool=pool)

    await wrapped.handler({})

    assert gate.seen == [("+70000000001", (("send", None),))]


async def test_explicit_phone_wins_over_the_default_account():
    """The SDK passes the argument dict positionally (args[0]); an explicit
    phone must gate the explicit account, not the DB default (#1861 review)."""
    calls = []
    gate = _FakeGate(deferred={"+79990000002"})
    pool = _pool(gate=gate)
    wrapped = _wrapped(calls, pool=pool)

    result = await wrapped.handler({"phone": "+79990000002"})

    assert "FLOODGATE-ERR-" in result["content"][0]["text"]
    assert gate.seen == [("+79990000002", (("send", None),))]


async def test_successful_call_releases_the_breaker_trial():
    """A granted half-open trial must not wedge when the call simply works."""

    class _TrialBreaker(_FakeBreaker):
        def __init__(self):
            super().__init__()
            self.trial_open = False

        def check(self, operation, phone):
            if self.trial_open:
                raise TelegramOperationSuspendedError(operation, phone, 1.0)
            self.trial_open = True  # grant one trial, pinned until record_success

        def record_success(self, operation, phone):
            super().record_success(operation, phone)
            self.trial_open = False

    calls = []
    breaker = _TrialBreaker()
    wrapped = _wrapped(calls, pool=_pool(breaker=breaker))

    assert (await wrapped.handler({"phone": "+70000000001"}))["content"][0]["text"] == "ok"
    assert (await wrapped.handler({"phone": "+70000000001"}))["content"][0]["text"] == "ok"
    assert breaker.successes == [("send_message", "+70000000001")] * 2


async def test_flood_reported_during_the_call_keeps_the_flood():
    """A flood the pool funnel reported mid-call (text result, no exception)
    must NOT settle the breaker as a success — upstream's _FLOOD_SEEN rule."""
    calls = []
    breaker = _FakeBreaker()

    async def handler(args):
        calls.append(dict(args))
        # Simulate the pool funnel attributing a real flood to this call.
        scope = current_tool_flood_scope()
        assert scope is not None
        scope.flooded = True
        return {"content": [{"type": "text", "text": "Flood wait активен"}]}

    tool = SdkMcpTool(name="send_message", description="t", input_schema={}, handler=handler)
    wrapped = wrap_tool_with_floodgate(
        tool, db=_db_with_phone(), client_pool=_pool(breaker=breaker)
    )

    await wrapped.handler({"phone": "+70000000001"})

    assert breaker.successes == []


async def test_non_flood_error_releases_the_trial():
    calls = []
    breaker = _FakeBreaker()
    wrapped = _wrapped(calls, pool=_pool(breaker=breaker), raise_exc=ValueError("no"))

    with pytest.raises(ValueError):
        await wrapped.handler({"phone": "+70000000001"})

    assert breaker.successes == [("send_message", "+70000000001")]


async def test_result_quoting_flood_text_does_not_wedge_the_trial():
    """A result that merely quotes flood text (chat content, stored errors)
    feeds nothing: flood detection rides the pool funnel, not output strings."""
    calls = []
    breaker = _FakeBreaker()
    wrapped = _wrapped(
        calls,
        pool=_pool(breaker=breaker),
        body="FloodWait 300 — повторите позже",
    )

    first = await wrapped.handler({"phone": "+70000000001"})
    second = await wrapped.handler({"phone": "+70000000001"})

    assert "FloodWait 300" in first["content"][0]["text"]
    assert breaker.floods == []
    # The second call reached the handler again — no wedge, no suspension.
    assert "FloodWait 300" in second["content"][0]["text"]
    assert len(calls) == 2


async def test_non_phone_bound_tool_skips_the_boundary():
    calls = []
    gate = _FakeGate()
    wrapped = _wrapped(calls, name="list_pipelines", pool=_pool(gate=gate))
    await wrapped.handler({})

    assert gate.seen == []


async def test_pool_without_floodgate_primitives_skips_the_boundary():
    calls = []
    wrapped = _wrapped(calls, pool=SimpleNamespace())

    result = await wrapped.handler({"phone": "+70000000001"})

    assert result["content"][0]["text"] == "ok"


async def test_no_client_pool_skips_the_boundary():
    calls = []
    wrapped = wrap_tool_with_floodgate(
        _tool("send_message", calls), db=_db_with_phone(), client_pool=None
    )

    result = await wrapped.handler({"phone": "+70000000001"})

    assert result["content"][0]["text"] == "ok"


async def test_floodgate_disabled_by_default_env_truth_table(monkeypatch):
    monkeypatch.delenv("TG_AGENT_FLOODGATE", raising=False)
    assert floodgate.floodgate_enabled() is True
    monkeypatch.setenv("TG_AGENT_FLOODGATE", "false")
    assert floodgate.floodgate_enabled() is False
    monkeypatch.setenv("TG_AGENT_FLOODGATE", "banana")
    assert floodgate.floodgate_enabled() is True  # invalid value keeps it on


async def test_real_gate_and_breaker_composed_with_the_wrapper():
    """Real package primitives behind the wrapper: the handler simulates the
    physical boundary's charge; the second send within the window defers with
    no extra handler execution."""
    from telethon_floodgate import FloodCircuitBreaker, RateLimitSpec, TelegramRateLimitGate

    gate = TelegramRateLimitGate(
        category_limits={"send": RateLimitSpec(max_calls=1, window_sec=60.0)},
        jitter_func=lambda low, high: 0.0,
    )
    pool = _pool(gate=gate, breaker=FloodCircuitBreaker())
    calls = []

    async def handler(args):
        calls.append(dict(args))
        gate.try_acquire("+70000000001", "send")  # the physical boundary charges here
        return {"content": [{"type": "text", "text": "ok"}]}

    tool = SdkMcpTool(name="send_message", description="t", input_schema={}, handler=handler)
    wrapped = wrap_tool_with_floodgate(tool, db=_db_with_phone(), client_pool=pool)

    first = await wrapped.handler({"phone": "+70000000001"})
    second = await wrapped.handler({"phone": "+70000000001"})

    assert first["content"][0]["text"] == "ok"
    assert "FLOODGATE-ERR-" in second["content"][0]["text"]
    assert len(calls) == 1  # exactly one real execution


# ---------------------------------------------------------------------------
# Pool funnel attribution: real floods land in the tool-keyed breaker even
# when the handler converts them into text results.
# ---------------------------------------------------------------------------


def _funnel_pool(breaker, gate=None):
    from tg_agent.telegram.pool_flood import FloodRotationMixin

    class _FunnelPool(FloodRotationMixin):
        def __init__(self):
            self._flood_breaker = breaker
            self._rate_limit_gate = gate
            self._db = MagicMock()
            self._db.update_account_flood = AsyncMock()
            self._rpc_account_deadlines = {}

    return _FunnelPool()


async def test_pool_flood_report_attributes_to_the_tool_breaker():
    breaker = _FakeBreaker()
    gate = _FakeGate()
    pool = _funnel_pool(breaker, gate)

    with tool_flood_scope("send_message", "+70000000001"):
        await pool.report_flood("+70000000001", 42)

    assert breaker.floods == [("send_message", "+70000000001")]
    assert gate.floods == [("+70000000001", 42.0)]


async def test_pool_flood_report_without_tool_scope_is_inert():
    breaker = _FakeBreaker()
    pool = _funnel_pool(breaker)

    await pool.report_flood("+70000000001", 42)

    assert breaker.floods == []


async def test_pool_flood_report_other_phone_is_inert():
    breaker = _FakeBreaker()
    pool = _funnel_pool(breaker)

    with tool_flood_scope("send_message", "+70000000001"):
        await pool.report_flood("+70000000002", 42)

    assert breaker.floods == []


async def test_family_flood_report_attributes_once(monkeypatch):
    """Read-family reports attribute exactly once — no double count through
    the report_flood fallback."""
    import tg_agent.telegram.pool_flood as pool_flood

    monkeypatch.setattr(pool_flood, "family_floods_repo", lambda db: None)
    breaker = _FakeBreaker()
    pool = _funnel_pool(breaker)

    with tool_flood_scope("read_messages", "+70000000001"):
        await pool.report_family_flood("+70000000001", 42, "history", operation="telegram_stream_messages")

    assert breaker.floods == [("read_messages", "+70000000001")]


# ---------------------------------------------------------------------------
# Category registry derived from TOOL_GROUPS (#245 convention).
# ---------------------------------------------------------------------------


def test_flood_category_registry_covers_the_hot_tools():
    from tg_agent.agent.tools.permissions import TOOL_FLOOD_CATEGORIES

    assert TOOL_FLOOD_CATEGORIES["send_message"] == "send"
    assert TOOL_FLOOD_CATEGORIES["edit_message"] == "send"
    assert TOOL_FLOOD_CATEGORIES["forward_messages"] == "send"
    assert TOOL_FLOOD_CATEGORIES["read_messages"] == "history"
    assert TOOL_FLOOD_CATEGORIES["edit_admin"] == "admin_action"
    assert TOOL_FLOOD_CATEGORIES["kick_participant"] == "admin_action"
    assert TOOL_FLOOD_CATEGORIES["create_telegram_channel"] == "channel_lifecycle"
    assert TOOL_FLOOD_CATEGORIES["join_channel"] == "channel_lifecycle"
    assert TOOL_FLOOD_CATEGORIES["send_photos_now"] == "send"


def test_flood_category_unknown_tool_takes_default():
    from tg_agent.agent.tools.permissions import TOOL_FLOOD_CATEGORIES

    assert TOOL_FLOOD_CATEGORIES.get("not_a_tool", "default") == "default"


def test_flood_categories_are_gate_buckets():
    """Every declared flood category must be a bucket the pool gate actually
    configures — a typo must fail here, not degrade into the default bucket."""
    from telethon_floodgate import TelegramRateLimitGate

    from tg_agent.agent.tools._categories import FLOOD_TOOL_CATEGORIES
    from tg_agent.agent.tools.permissions import TOOL_FLOOD_CATEGORIES

    gate = TelegramRateLimitGate()
    configured = set(gate._category_specs)
    assert set(FLOOD_TOOL_CATEGORIES) <= configured
    assert set(TOOL_FLOOD_CATEGORIES.values()) <= set(FLOOD_TOOL_CATEGORIES)


def test_tool_meta_rejects_unknown_flood_category():
    from tg_agent.agent.tools._categories import FLOOD_TOOL_CATEGORIES, ToolCategory, ToolMeta

    with pytest.raises(ValueError):
        ToolMeta(ToolCategory.WRITE, flood_category="snd")
    assert "snd" not in FLOOD_TOOL_CATEGORIES


# ---------------------------------------------------------------------------
# Registry wiring: every registered tool passes through the boundary.
# ---------------------------------------------------------------------------


async def test_registry_wraps_phone_bound_tools_with_the_boundary(monkeypatch):
    """build_agent_tool_registry wraps handlers: a deferred phone returns the
    floodgate reply before any live-runtime work."""
    gate = _FakeGate(deferred={"+70000000001"})
    pool = _pool(gate=gate)
    db = _db_with_phone("+70000000001")

    captured = []
    monkeypatch.setattr(
        "tg_agent.agent.tools.create_sdk_mcp_server",
        lambda **kw: captured.extend(kw.get("tools", [])),
    )
    from tg_agent.agent.tools import make_mcp_server

    make_mcp_server(db, client_pool=pool)
    handlers = {t.name: t.handler for t in captured}

    result = await handlers["send_message"](
        {"phone": "+70000000001", "recipient": "@x", "text": "hi", "confirm": True}
    )
    assert "FLOODGATE-ERR-" in result["content"][0]["text"]
