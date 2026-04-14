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
    gate._session_overrides["session-1"] = {"tool_a"}
    assert gate.is_session_approved("tool_a", "session-1") is True
    assert gate.is_session_approved("tool_b", "session-1") is False


def test_is_session_approved_missing_session():
    gate = PermissionGate()
    assert gate.is_session_approved("tool_a", "nonexistent") is False


# ── resolve() ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_resolve_sets_future_result():
    gate = PermissionGate()
    request_id = "test-req-1"
    future = asyncio.get_running_loop().create_future()
    gate._pending[request_id] = future

    assert gate.resolve(request_id, "once") is True
    assert future.done()
    assert future.result() == "once"


@pytest.mark.asyncio
async def test_resolve_unknown_request_returns_false():
    gate = PermissionGate()
    assert gate.resolve("nonexistent-id", "once") is False


@pytest.mark.asyncio
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
    gate._session_overrides["session-1"] = {"tool_a", "tool_b"}
    gate.clear_session("session-1")
    assert "session-1" not in gate._session_overrides


def test_clear_session_nonexistent():
    gate = PermissionGate()
    gate.clear_session("nonexistent")  # should not raise


# ── check() with no context ────────────────────────────────────────


@pytest.mark.asyncio
async def test_check_returns_none_without_context():
    """When no request context is set, check returns None (fall through)."""
    gate = PermissionGate()
    result = await gate.check("some_tool", "")
    assert result is None


# ── check() with session already approved ──────────────────────────


@pytest.mark.asyncio
async def test_check_returns_none_when_session_approved():
    gate = PermissionGate()
    gate._session_overrides["session-1"] = {"my_tool"}

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


@pytest.mark.asyncio
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
        assert not gate.is_session_approved("my_tool", "session-2")
    finally:
        reset_request_context(token)


# ── check() — user grants "session" ────────────────────────────────


@pytest.mark.asyncio
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
        # "session" should store in session overrides
        assert gate.is_session_approved("my_tool", "session-3")
    finally:
        reset_request_context(token)


# ── check() — user denies ──────────────────────────────────────────


@pytest.mark.asyncio
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


# ── check() — timeout ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_check_timeout():
    gate = PermissionGate()
    ctx = AgentRequestContext(
        session_id="session-5",
        thread_id=5,
        queue=asyncio.Queue(),
        db_permissions={},
        permission_timeout=1,  # 1 second timeout
    )
    token = set_request_context(ctx)
    try:
        result = await gate.check("my_tool", "")
        assert result is not None
        assert "Таймаут" in result["content"][0]["text"]
    finally:
        reset_request_context(token)


# ── check() — cancellation ─────────────────────────────────────────


@pytest.mark.asyncio
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
        event = await asyncio.wait_for(ctx.queue.get(), timeout=2.0)

        # Cancel the task
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        reset_request_context(token)


# ── Module-level gate accessor ──────────────────────────────────────


def test_set_and_get_gate():
    gate = PermissionGate()
    set_gate(gate)
    assert get_gate() is gate

    set_gate(None)
    assert get_gate() is None


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
