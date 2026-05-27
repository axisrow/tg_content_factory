"""Tests for PermissionGate — session overrides, interactive check, resolve, and module-level helpers."""

from __future__ import annotations

import asyncio
import json

import pytest

from src.agent.permission_gate import (
    AgentRequestContext,
    PermissionGate,
    _text_response,
    get_gate,
    get_request_context,
    reset_request_context,
    set_gate,
    set_request_context,
)

# ── _text_response ──────────────────────────────────────────────────


def test_text_response_shape():
    result = _text_response("some error")
    assert result == {"content": [{"type": "text", "text": "some error"}]}


# ── PermissionGate basics ───────────────────────────────────────────


def test_is_session_approved_empty():
    gate = PermissionGate()
    assert gate.is_session_approved("tool_a", "session-1") is False


def test_is_session_approved_after_manual_set():
    gate = PermissionGate()
    gate._session_overrides["session-1"] = {("tool_a", "+111")}
    assert gate.is_session_approved("tool_a", "session-1", "+111") is True
    assert gate.is_session_approved("tool_a", "session-1", "+222") is False
    assert gate.is_session_approved("tool_b", "session-1", "+111") is False


def test_is_session_approved_missing_session():
    gate = PermissionGate()
    assert gate.is_session_approved("tool_a", "nonexistent") is False


def test_is_session_approved_global_grant_does_not_leak_to_phone():
    """A session-level (phone="") grant must not satisfy a phone-specific check."""
    gate = PermissionGate()
    gate._session_overrides["session-1"] = {("send_reaction", "")}
    # The global grant exists, but a phone-specific check must still be denied.
    assert gate.is_session_approved("send_reaction", "session-1", "+1") is False
    assert gate.is_session_approved("send_reaction", "session-1", "") is True


def test_is_session_approved_phone_specific_grant_isolated():
    """Granting a tool for one phone must not allow it for another phone."""
    gate = PermissionGate()
    gate._session_overrides["session-1"] = {("send_reaction", "+111")}
    assert gate.is_session_approved("send_reaction", "session-1", "+111") is True
    assert gate.is_session_approved("send_reaction", "session-1", "+222") is False


# ── resolve() ───────────────────────────────────────────────────────


@pytest.mark.anyio
async def test_resolve_sets_future_result():
    gate = PermissionGate()
    request_id = "test-req-1"
    future = asyncio.get_running_loop().create_future()
    gate._pending[request_id] = future

    assert gate.resolve(request_id, "once") is True
    assert future.done()
    assert future.result() == "once"


@pytest.mark.anyio
async def test_resolve_unknown_request_returns_false():
    gate = PermissionGate()
    assert gate.resolve("nonexistent-id", "once") is False


@pytest.mark.anyio
async def test_resolve_already_done_future_still_returns_true():
    gate = PermissionGate()
    request_id = "test-req-2"
    future = asyncio.get_running_loop().create_future()
    future.set_result("deny")
    gate._pending[request_id] = future

    # Should return True even though future is already done
    assert gate.resolve(request_id, "once") is True


# ── clear_session() ────────────────────────────────────────────────


def test_clear_session():
    gate = PermissionGate()
    gate._session_overrides["session-1"] = {("tool_a", "+1"), ("tool_b", "")}
    gate.clear_session("session-1")
    assert "session-1" not in gate._session_overrides


@pytest.mark.anyio
async def test_clear_session_does_not_cancel_pending_prompts():
    gate = PermissionGate()
    future = asyncio.get_running_loop().create_future()
    gate._session_overrides["session-1"] = {("tool_a", "+1")}
    gate._pending["request-1"] = future
    gate._pending_sessions["request-1"] = "session-1"
    gate._pending_threads["request-1"] = 10

    gate.clear_session("session-1")

    assert "session-1" not in gate._session_overrides
    assert not future.cancelled()
    assert gate._pending["request-1"] is future
    assert gate._pending_sessions["request-1"] == "session-1"
    assert gate._pending_threads["request-1"] == 10


def test_clear_session_nonexistent():
    gate = PermissionGate()
    gate.clear_session("nonexistent")  # should not raise


# ── clear_thread() ─────────────────────────────────────────────────


@pytest.mark.anyio
async def test_clear_thread_cancels_only_matching_thread():
    gate = PermissionGate()
    matching = asyncio.get_running_loop().create_future()
    other_thread = asyncio.get_running_loop().create_future()
    other_session = asyncio.get_running_loop().create_future()
    gate._pending.update(
        {
            "matching": matching,
            "other-thread": other_thread,
            "other-session": other_session,
        }
    )
    gate._pending_sessions.update(
        {
            "matching": "session-1",
            "other-thread": "session-1",
            "other-session": "session-2",
        }
    )
    gate._pending_threads.update(
        {
            "matching": 10,
            "other-thread": 11,
            "other-session": 10,
        }
    )

    gate.clear_thread("session-1", 10)

    assert matching.cancelled()
    assert not other_thread.cancelled()
    assert not other_session.cancelled()
    assert "matching" not in gate._pending
    assert "other-thread" in gate._pending
    assert "other-session" in gate._pending
    assert gate._pending_threads["other-thread"] == 11
    assert gate._pending_threads["other-session"] == 10


# ── check() with no context ────────────────────────────────────────


@pytest.mark.anyio
async def test_check_returns_none_without_context():
    """When no request context is set, check returns None (fall through)."""
    gate = PermissionGate()
    result = await gate.check("some_tool", "")
    assert result is None


# ── check() with session already approved ──────────────────────────


@pytest.mark.anyio
async def test_check_returns_none_when_session_approved():
    gate = PermissionGate()
    gate._session_overrides["session-1"] = {("my_tool", "+1234567890")}

    ctx = AgentRequestContext(
        session_id="session-1",
        thread_id=1,
        queue=asyncio.Queue(),
        db_permissions={},
        permission_timeout=5,
    )
    token = set_request_context(ctx)
    try:
        result = await gate.check("my_tool", "+1234567890")
        assert result is None
    finally:
        reset_request_context(token)


# ── check() — user grants "once" ───────────────────────────────────


@pytest.mark.anyio
async def test_check_user_grants_once():
    gate = PermissionGate()
    ctx = AgentRequestContext(
        session_id="session-2",
        thread_id=2,
        queue=asyncio.Queue(),
        db_permissions={},
        permission_timeout=5,
    )
    token = set_request_context(ctx)
    try:
        # Run check in background so we can resolve concurrently
        task = asyncio.create_task(gate.check("my_tool", "+1234567890"))

        # Wait for the SSE event in the queue
        event = await asyncio.wait_for(ctx.queue.get(), timeout=2.0)
        assert "permission_request" in event
        event_data = json.loads(event.removeprefix("data: ").strip())
        assert event_data["tool"] == "my_tool"
        request_id = event_data["request_id"]

        # Resolve with "once"
        gate.resolve(request_id, "once")

        result = await asyncio.wait_for(task, timeout=2.0)
        assert result is None
        # "once" should NOT store in session overrides
        assert not gate.is_session_approved("my_tool", "session-2", "+1234567890")
    finally:
        reset_request_context(token)


# ── check() — user grants "session" ────────────────────────────────


@pytest.mark.anyio
async def test_check_user_grants_session():
    gate = PermissionGate()
    ctx = AgentRequestContext(
        session_id="session-3",
        thread_id=3,
        queue=asyncio.Queue(),
        db_permissions={},
        permission_timeout=5,
    )
    token = set_request_context(ctx)
    try:
        task = asyncio.create_task(gate.check("my_tool", ""))

        event = await asyncio.wait_for(ctx.queue.get(), timeout=2.0)
        event_data = json.loads(event.removeprefix("data: ").strip())
        request_id = event_data["request_id"]

        gate.resolve(request_id, "session")

        result = await asyncio.wait_for(task, timeout=2.0)
        assert result is None
        # "session" should store in session overrides keyed on (tool, phone="")
        assert gate.is_session_approved("my_tool", "session-3", "")
        # A phone-specific check for the same tool must NOT inherit the global grant
        assert not gate.is_session_approved("my_tool", "session-3", "+999")
    finally:
        reset_request_context(token)


# ── check() — user denies ──────────────────────────────────────────


@pytest.mark.anyio
async def test_check_user_denies():
    gate = PermissionGate()
    ctx = AgentRequestContext(
        session_id="session-4",
        thread_id=4,
        queue=asyncio.Queue(),
        db_permissions={},
        permission_timeout=5,
    )
    token = set_request_context(ctx)
    try:
        task = asyncio.create_task(gate.check("my_tool", "+1234567890"))

        event = await asyncio.wait_for(ctx.queue.get(), timeout=2.0)
        event_data = json.loads(event.removeprefix("data: ").strip())
        request_id = event_data["request_id"]

        gate.resolve(request_id, "deny")

        result = await asyncio.wait_for(task, timeout=2.0)
        assert result is not None
        assert result["content"][0]["type"] == "text"
        assert "запрещ" in result["content"][0]["text"]
    finally:
        reset_request_context(token)


# ── check() — no timeout ───────────────────────────────────────────


@pytest.mark.anyio
async def test_check_waits_without_permission_timeout():
    gate = PermissionGate()
    ctx = AgentRequestContext(
        session_id="session-5",
        thread_id=5,
        queue=asyncio.Queue(),
        db_permissions={},
        permission_timeout=0,
    )
    token = set_request_context(ctx)
    try:
        task = asyncio.create_task(gate.check("my_tool", ""))
        event = await asyncio.wait_for(ctx.queue.get(), timeout=2.0)
        event_data = json.loads(event.removeprefix("data: ").strip())
        assert event_data["timeout"] is None

        done, _pending = await asyncio.wait({task}, timeout=0.05)
        assert not done

        gate.resolve(event_data["request_id"], "once")
        assert await asyncio.wait_for(task, timeout=2.0) is None
    finally:
        reset_request_context(token)


# ── check() — cancellation ─────────────────────────────────────────


@pytest.mark.anyio
async def test_check_cancelled_error_propagates():
    gate = PermissionGate()
    ctx = AgentRequestContext(
        session_id="session-6",
        thread_id=6,
        queue=asyncio.Queue(),
        db_permissions={},
        permission_timeout=10,
    )
    token = set_request_context(ctx)
    try:
        task = asyncio.create_task(gate.check("my_tool", ""))

        # Wait for the SSE event
        await asyncio.wait_for(ctx.queue.get(), timeout=2.0)

        # Cancel the task
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        reset_request_context(token)


# ── check() — serialized prompts ───────────────────────────────────


@pytest.mark.anyio
async def test_concurrent_session_grant_serializes_prompts_and_releases_all():
    gate = PermissionGate()
    ctx = AgentRequestContext(
        session_id="session-queue-session",
        thread_id=7,
        queue=asyncio.Queue(),
        db_permissions={},
    )
    token = set_request_context(ctx)
    try:
        tasks = [
            asyncio.create_task(gate.check("send_reaction", "+1234567890"))
            for _ in range(10)
        ]

        event = await asyncio.wait_for(ctx.queue.get(), timeout=2.0)
        event_data = json.loads(event.removeprefix("data: ").strip())
        assert event_data["tool"] == "send_reaction"
        assert event_data["phone"] == "+1234567890"
        await asyncio.sleep(0.05)
        assert ctx.queue.empty()

        gate.resolve(event_data["request_id"], "session")
        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=2.0)

        assert results == [None] * 10
        assert ctx.queue.empty()
        assert gate.is_session_approved("send_reaction", ctx.session_id, "+1234567890")
    finally:
        reset_request_context(token)


@pytest.mark.anyio
async def test_concurrent_once_grant_keeps_following_requests_queued():
    gate = PermissionGate()
    ctx = AgentRequestContext(
        session_id="session-queue-once",
        thread_id=8,
        queue=asyncio.Queue(),
        db_permissions={},
    )
    token = set_request_context(ctx)
    try:
        tasks = [
            asyncio.create_task(gate.check("send_reaction", "+1234567890"))
            for _ in range(3)
        ]

        first_event = await asyncio.wait_for(ctx.queue.get(), timeout=2.0)
        first_data = json.loads(first_event.removeprefix("data: ").strip())
        await asyncio.sleep(0.05)
        assert ctx.queue.empty()

        gate.resolve(first_data["request_id"], "once")
        second_event = await asyncio.wait_for(ctx.queue.get(), timeout=2.0)
        second_data = json.loads(second_event.removeprefix("data: ").strip())

        assert second_data["request_id"] != first_data["request_id"]
        assert not gate.is_session_approved("send_reaction", ctx.session_id, "+1234567890")

        gate.resolve(second_data["request_id"], "session")
        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=2.0)

        assert results == [None] * 3
        assert ctx.queue.empty()
        assert gate.is_session_approved("send_reaction", ctx.session_id, "+1234567890")
    finally:
        reset_request_context(token)


@pytest.mark.anyio
async def test_concurrent_deny_does_not_store_session_grant():
    gate = PermissionGate()
    ctx = AgentRequestContext(
        session_id="session-queue-deny",
        thread_id=9,
        queue=asyncio.Queue(),
        db_permissions={},
    )
    token = set_request_context(ctx)
    try:
        tasks = [
            asyncio.create_task(gate.check("send_reaction", "+1234567890"))
            for _ in range(2)
        ]

        first_event = await asyncio.wait_for(ctx.queue.get(), timeout=2.0)
        first_data = json.loads(first_event.removeprefix("data: ").strip())
        gate.resolve(first_data["request_id"], "deny")

        second_event = await asyncio.wait_for(ctx.queue.get(), timeout=2.0)
        second_data = json.loads(second_event.removeprefix("data: ").strip())
        assert not gate.is_session_approved("send_reaction", ctx.session_id, "+1234567890")

        gate.resolve(second_data["request_id"], "session")
        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=2.0)

        assert any(result is not None for result in results)
        assert any(result is None for result in results)
        assert gate.is_session_approved("send_reaction", ctx.session_id, "+1234567890")
    finally:
        reset_request_context(token)


# ── Module-level gate accessor ──────────────────────────────────────


def test_set_and_get_gate():
    gate = PermissionGate()
    set_gate(gate)
    assert get_gate() is gate

    set_gate(None)
    assert get_gate() is None


def test_get_gate_returns_request_scoped_gate_without_global_gate():
    request_gate = PermissionGate()
    ctx = AgentRequestContext(
        session_id="request-scoped",
        thread_id=1,
        queue=asyncio.Queue(),
        permission_gate=request_gate,
        permission_timeout=10,
    )
    set_gate(None)
    token = set_request_context(ctx)
    try:
        assert get_gate() is request_gate
    finally:
        reset_request_context(token)


def test_get_gate_prefers_request_scoped_gate_over_global_gate():
    request_gate = PermissionGate()
    global_gate = PermissionGate()
    ctx = AgentRequestContext(
        session_id="request-scoped",
        thread_id=1,
        queue=asyncio.Queue(),
        permission_gate=request_gate,
        permission_timeout=10,
    )
    set_gate(global_gate)
    token = set_request_context(ctx)
    try:
        assert get_gate() is request_gate
    finally:
        reset_request_context(token)
        set_gate(None)


def test_get_gate_falls_back_to_global_without_request_context():
    gate = PermissionGate()
    set_gate(gate)
    try:
        assert get_request_context() is None
        assert get_gate() is gate
    finally:
        set_gate(None)


# ── ContextVar get/set/reset ────────────────────────────────────────


def test_get_request_context_default_none():
    # Outside of any context, should return None
    assert get_request_context() is None


def test_set_and_get_request_context():
    ctx = AgentRequestContext(
        session_id="test-session",
        thread_id=99,
        queue=asyncio.Queue(),
        db_permissions={"tool_a": True},
        permission_timeout=30,
    )
    token = set_request_context(ctx)
    try:
        retrieved = get_request_context()
        assert retrieved is ctx
        assert retrieved.session_id == "test-session"
        assert retrieved.thread_id == 99
        assert retrieved.db_permissions == {"tool_a": True}
    finally:
        reset_request_context(token)


def test_reset_request_context_restores_none():
    ctx = AgentRequestContext(
        session_id="temp",
        thread_id=1,
        queue=asyncio.Queue(),
        db_permissions={},
        permission_timeout=10,
    )
    token = set_request_context(ctx)
    reset_request_context(token)
    assert get_request_context() is None
