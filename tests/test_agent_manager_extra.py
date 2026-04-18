"""Extra coverage tests for src/agent/manager.py — missing lines.

Focuses on uncovered branches, error handling paths, conditional logic,
and method calls not exercised by existing tests.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.agent.manager import (
    AgentManager,
    AgentRuntimeStatus,
    ClaudeSdkBackend,
    DeepagentsBackend,
    _await_with_countdown,
    _embed_history_in_prompt,
    _SettingsCache,
    _ToolTracker,
)
from src.agent.provider_registry import ProviderRuntimeConfig
from src.config import AppConfig
from src.database import Database


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_assistant_msg(text, model="test"):
    """Create a MagicMock AssistantMessage (avoids constructor issues)."""
    from claude_agent_sdk import AssistantMessage, TextBlock

    msg = MagicMock(spec=AssistantMessage)
    msg.content = [TextBlock(text=text)]
    return msg


def _make_result_msg(**overrides):
    """Create a MagicMock ResultMessage."""
    from claude_agent_sdk import ResultMessage

    msg = MagicMock(spec=ResultMessage)
    msg.usage = overrides.get("usage", {})
    msg.model_usage = overrides.get("model_usage", {})
    msg.total_cost_usd = overrides.get("total_cost_usd")
    msg.num_turns = overrides.get("num_turns")
    msg.session_id = overrides.get("session_id")
    return msg


def _make_stream_event(event_dict):
    """Create a MagicMock StreamEvent."""
    from claude_agent_sdk import StreamEvent

    ev = MagicMock(spec=StreamEvent)
    ev.event = event_dict
    return ev


def _make_text_event(text):
    """Create a StreamEvent with text_delta."""
    return _make_stream_event({
        "type": "content_block_delta",
        "delta": {"type": "text_delta", "text": text},
    })


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _default_agent_env(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-claude-key")


@pytest.fixture
def mock_db():
    db = MagicMock(spec=Database)
    db.repos = MagicMock()
    return db


# ===========================================================================
# _await_with_countdown — activity extension & thinking status
# ===========================================================================


class TestAwaitWithCountdown:
    @pytest.mark.asyncio
    async def test_extends_deadline_on_activity(self):
        """Deadline is extended when activity_ts indicates fresh SDK activity."""
        queue: asyncio.Queue = asyncio.Queue()
        activity_ts = [0.0]

        async def coro_with_activity():
            # Update activity_ts before the countdown interval fires
            activity_ts[0] = time.monotonic()
            await asyncio.sleep(0.05)
            return "done"

        result = await _await_with_countdown(
            coro_with_activity(),
            timeout=3.0,
            queue=queue,
            label="test",
            countdown_interval=0.2,
            activity_ts=activity_ts,
            activity_extend=5.0,
        )
        assert result == "done"

    @pytest.mark.asyncio
    async def test_shows_thinking_status_after_api_request_delay(self):
        """Shows 'thinking' status when api_request_ts is stale (>15s)."""
        queue: asyncio.Queue = asyncio.Queue()
        api_request_ts = [time.monotonic() - 20]  # 20s ago

        async def slow_coro():
            await asyncio.sleep(100)

        with pytest.raises(asyncio.TimeoutError):
            await _await_with_countdown(
                slow_coro(),
                timeout=1.0,
                queue=queue,
                label="test",
                countdown_interval=0.3,
                api_request_ts=api_request_ts,
            )

        items = []
        while not queue.empty():
            items.append(queue.get_nowait())
        thinking_items = [i for i in items if "thinking" in i]
        assert len(thinking_items) >= 1, f"Expected thinking event, got: {items}"

    @pytest.mark.asyncio
    async def test_max_timeout_caps_deadline(self):
        """max_timeout prevents deadline from extending beyond hard ceiling."""
        queue: asyncio.Queue = asyncio.Queue()
        activity_ts = [0.0]

        async def slow_coro():
            await asyncio.sleep(100)

        with pytest.raises(asyncio.TimeoutError):
            await _await_with_countdown(
                slow_coro(),
                timeout=0.8,
                queue=queue,
                label="test",
                countdown_interval=0.2,
                activity_ts=activity_ts,
                activity_extend=5.0,
                max_timeout=0.8,
            )

    @pytest.mark.asyncio
    async def test_cancelled_error_propagation(self):
        """CancelledError from outer scope propagates correctly."""
        queue: asyncio.Queue = asyncio.Queue()

        async def hanging_coro():
            await asyncio.sleep(100)

        task = asyncio.ensure_future(
            _await_with_countdown(
                hanging_coro(), timeout=10.0, queue=queue, label="test", countdown_interval=1,
            )
        )
        await asyncio.sleep(0.1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


# ===========================================================================
# _ToolTracker — edge cases
# ===========================================================================


class TestToolTracker:
    @pytest.mark.asyncio
    async def test_on_first_event_idempotent(self):
        """on_first_event only emits once."""
        queue: asyncio.Queue = asyncio.Queue()
        tracker = _ToolTracker(queue=queue)
        await tracker.on_first_event()
        await tracker.on_first_event()  # second call should be no-op
        assert queue.qsize() == 1

    @pytest.mark.asyncio
    async def test_on_block_stop_with_invalid_json(self):
        """Accumulated input that is not valid JSON produces empty summary."""
        queue: asyncio.Queue = asyncio.Queue()
        tracker = _ToolTracker(queue=queue)
        await tracker.on_tool_start("test_tool", 0, tool_use_id="t1")
        tracker.accumulate_input("not-valid-json{{{")
        await tracker.on_block_stop(0)

        items = []
        while not queue.empty():
            items.append(queue.get_nowait())
        tool_end_events = [json.loads(i.removeprefix("data: ")) for i in items if "tool_end" in i]
        assert len(tool_end_events) == 1
        assert tool_end_events[0]["is_error"] is False

    @pytest.mark.asyncio
    async def test_on_block_stop_ignores_mismatched_index(self):
        """on_block_stop with non-matching index does not emit tool_end."""
        queue: asyncio.Queue = asyncio.Queue()
        tracker = _ToolTracker(queue=queue)
        await tracker.on_tool_start("tool_a", 0)
        # Drain the tool_start event
        while not queue.empty():
            queue.get_nowait()
        await tracker.on_block_stop(99)  # different index — should be ignored
        assert queue.empty()

    @pytest.mark.asyncio
    async def test_on_tool_result_unknown_id(self):
        """Tool result with unknown tool_use_id defaults to 'tool'."""
        queue: asyncio.Queue = asyncio.Queue()
        tracker = _ToolTracker(queue=queue)
        await tracker.on_tool_result("unknown_id", "result text", False)

        items = []
        while not queue.empty():
            items.append(queue.get_nowait())
        results = [json.loads(i.removeprefix("data: ")) for i in items]
        assert results[0]["tool"] == "tool"

    @pytest.mark.asyncio
    async def test_on_tool_result_with_error(self):
        """Tool result with is_error=True is correctly emitted."""
        queue: asyncio.Queue = asyncio.Queue()
        tracker = _ToolTracker(queue=queue)
        await tracker.on_tool_start("my_tool", 0, tool_use_id="tid1")
        await tracker.on_tool_result("tid1", "error details", True)

        items = []
        while not queue.empty():
            items.append(queue.get_nowait())
        results = [json.loads(i.removeprefix("data: ")) for i in items]
        tool_result = [r for r in results if r.get("type") == "tool_result"][0]
        assert tool_result["is_error"] is True

    @pytest.mark.asyncio
    async def test_on_tool_result_none_content(self):
        """Tool result with None content produces empty summary."""
        queue: asyncio.Queue = asyncio.Queue()
        tracker = _ToolTracker(queue=queue)
        await tracker.on_tool_result("id1", None, False)

        items = []
        while not queue.empty():
            items.append(queue.get_nowait())
        results = [json.loads(i.removeprefix("data: ")) for i in items]
        assert results[0]["summary"] == ""

    @pytest.mark.asyncio
    async def test_on_status_emits_status_event(self):
        """on_status pushes a status event to the queue."""
        queue: asyncio.Queue = asyncio.Queue()
        tracker = _ToolTracker(queue=queue)
        await tracker.on_status("test status")
        item = queue.get_nowait()
        payload = json.loads(item.removeprefix("data: "))
        assert payload["type"] == "status"
        assert payload["text"] == "test status"


# ===========================================================================
# _SettingsCache
# ===========================================================================


class TestSettingsCache:
    def test_get_missing_key_returns_none(self):
        cache = _SettingsCache()
        assert cache.get("missing") is None

    def test_set_and_get(self):
        cache = _SettingsCache()
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"

    def test_expired_entry_returns_none(self):
        cache = _SettingsCache()
        cache.set("key1", "value1", ttl=-1)  # already expired
        assert cache.get("key1") is None

    def test_invalidate_specific_key(self):
        cache = _SettingsCache()
        cache.set("key1", "v1")
        cache.set("key2", "v2")
        cache.invalidate("key1")
        assert cache.get("key1") is None
        assert cache.get("key2") == "v2"

    def test_invalidate_all_keys(self):
        cache = _SettingsCache()
        cache.set("key1", "v1")
        cache.set("key2", "v2")
        cache.invalidate()
        assert cache.get("key1") is None
        assert cache.get("key2") is None


# ===========================================================================
# _embed_history_in_prompt
# ===========================================================================


class TestEmbedHistoryInPrompt:
    def test_empty_history(self):
        result = _embed_history_in_prompt([], "message")
        assert result == "<user>\nmessage\n</user>"

    def test_mixed_roles(self):
        result = _embed_history_in_prompt(
            [{"role": "user", "content": "hi"}, {"role": "assistant", "content": "hello"}],
            "follow-up",
        )
        assert "<user>\nhi\n</user>" in result
        assert "<assistant>\nhello\n</assistant>" in result
        assert "<user>\nfollow-up\n</user>" in result


# ===========================================================================
# ClaudeSdkBackend — error handling branches
# ===========================================================================


class TestClaudeSdkBackendChatStreamErrors:
    @pytest.mark.asyncio
    async def test_total_timeout_exceeded(self, db):
        """When total timeout is exceeded during stream, error is emitted."""
        thread_id = await db.create_agent_thread("total-timeout")
        await db.save_agent_message(thread_id, "user", "test")

        from claude_agent_sdk import StreamEvent

        def _stream_event(text):
            ev = MagicMock(spec=StreamEvent)
            ev.event = {
                "type": "content_block_delta",
                "delta": {"type": "text_delta", "text": text},
            }
            return ev

        async def mock_query(prompt, options):
            yield _stream_event("partial")
            # Now stall — total timeout will fire
            await asyncio.sleep(100)

        config = AppConfig()
        config.agent.first_event_timeout = 5
        config.agent.idle_timeout = 2
        config.agent.total_timeout = 1

        mgr = AgentManager(db, config=config)
        mgr.initialize()

        chunks = []
        with patch("src.agent.manager.query", mock_query):
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        payloads = [
            json.loads(c.removeprefix("data: ").strip())
            for c in chunks
            if c.startswith("data: ")
        ]
        errors = [p for p in payloads if "error" in p]
        assert errors, f"Expected timeout error, got: {payloads}"

    @pytest.mark.asyncio
    async def test_cli_not_found_error(self, db):
        """CLINotFoundError produces user-friendly error message."""
        thread_id = await db.create_agent_thread("cli-not-found")
        await db.save_agent_message(thread_id, "user", "test")

        from claude_agent_sdk import CLINotFoundError

        async def mock_query(prompt, options):
            raise CLINotFoundError("claude not found")
            yield  # make it a generator

        mgr = AgentManager(db)
        mgr.initialize()

        chunks = []
        with patch("src.agent.manager.query", mock_query):
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks if c.startswith("data:")]
        errors = [p for p in payloads if "error" in p]
        assert errors, f"Expected error, got: {payloads}"
        assert "Claude CLI" in errors[0]["error"] or "npm install" in errors[0].get("error", "")

    @pytest.mark.asyncio
    async def test_process_error(self, db):
        """ProcessError produces error with captured stderr details."""
        thread_id = await db.create_agent_thread("process-error")
        await db.save_agent_message(thread_id, "user", "test")

        from claude_agent_sdk import ProcessError

        async def mock_query(prompt, options):
            raise ProcessError(exit_code=1, stderr="fatal error occurred")
            yield

        mgr = AgentManager(db)
        mgr.initialize()

        chunks = []
        with patch("src.agent.manager.query", mock_query):
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks if c.startswith("data:")]
        errors = [p for p in payloads if "error" in p]
        assert errors, f"Expected error, got: {payloads}"

    @pytest.mark.asyncio
    async def test_process_error_with_long_stderr_truncated(self, db):
        """ProcessError with long stderr truncates the details."""
        thread_id = await db.create_agent_thread("long-stderr")
        await db.save_agent_message(thread_id, "user", "test")

        from claude_agent_sdk import ProcessError

        long_stderr = "x" * 1000

        async def mock_query(prompt, options):
            raise ProcessError(exit_code=1, stderr=long_stderr)
            yield

        mgr = AgentManager(db)
        mgr.initialize()

        chunks = []
        with patch("src.agent.manager.query", mock_query):
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks if c.startswith("data:")]
        errors = [p for p in payloads if "error" in p]
        assert errors
        details = errors[0].get("details", "")
        if details:
            assert len(details) <= 503  # 500 + "..."

    @pytest.mark.asyncio
    async def test_cli_connection_error_retries_then_fails(self, db):
        """CLIConnectionError retries once then fails with user message."""
        thread_id = await db.create_agent_thread("conn-error")
        await db.save_agent_message(thread_id, "user", "test")

        from claude_agent_sdk import CLIConnectionError

        async def mock_query(prompt, options):
            raise CLIConnectionError("connection reset")
            yield

        mgr = AgentManager(db)
        mgr.initialize()

        chunks = []
        with patch("src.agent.manager.query", mock_query):
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks if c.startswith("data:")]
        errors = [p for p in payloads if "error" in p]
        assert errors, f"Expected error, got: {payloads}"

    @pytest.mark.asyncio
    async def test_cli_connection_error_overloaded_message(self, db):
        """CLIConnectionError with 'overloaded' uses specific message."""
        thread_id = await db.create_agent_thread("overloaded")
        await db.save_agent_message(thread_id, "user", "test")

        from claude_agent_sdk import CLIConnectionError

        call_count = 0

        async def mock_query(prompt, options):
            nonlocal call_count
            call_count += 1
            raise CLIConnectionError("API overloaded, try again later")
            yield

        mgr = AgentManager(db)
        mgr.initialize()

        chunks = []
        with patch("src.agent.manager.query", mock_query):
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks if c.startswith("data:")]
        errors = [p for p in payloads if "error" in p]
        assert errors
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_sdk_error_produces_error_payload(self, db):
        """ClaudeSDKError produces error with SDK details."""
        thread_id = await db.create_agent_thread("sdk-error")
        await db.save_agent_message(thread_id, "user", "test")

        from claude_agent_sdk import ClaudeSDKError

        async def mock_query(prompt, options):
            raise ClaudeSDKError("internal SDK failure")
            yield

        mgr = AgentManager(db)
        mgr.initialize()

        chunks = []
        with patch("src.agent.manager.query", mock_query):
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks if c.startswith("data:")]
        errors = [p for p in payloads if "error" in p]
        assert errors
        assert "SDK" in errors[0]["error"]

    @pytest.mark.asyncio
    async def test_base_exception_group_with_retryable_error(self, db):
        """ExceptionGroup with retryable error retries once then fails."""
        thread_id = await db.create_agent_thread("exc-group")
        await db.save_agent_message(thread_id, "user", "test")

        call_count = 0

        async def mock_query(prompt, options):
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise BaseExceptionGroup("errors", [RuntimeError("stream closed unexpectedly")])
            from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

            yield _make_assistant_msg("ok")
            yield MagicMock(spec=ResultMessage)

        mgr = AgentManager(db)
        mgr.initialize()

        chunks = []
        with patch("src.agent.manager.query", mock_query):
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks if c.startswith("data:")]
        done_or_error = [p for p in payloads if p.get("done") or "error" in p]
        assert done_or_error, f"Expected done or error, got: {payloads}"

    @pytest.mark.asyncio
    async def test_base_exception_group_with_timeout_no_retry(self, db):
        """ExceptionGroup containing TimeoutError does NOT retry."""
        thread_id = await db.create_agent_thread("exc-group-timeout")
        await db.save_agent_message(thread_id, "user", "test")

        call_count = 0

        async def mock_query(prompt, options):
            nonlocal call_count
            call_count += 1
            raise BaseExceptionGroup("errors", [TimeoutError("deadline exceeded")])
            yield

        mgr = AgentManager(db)
        mgr.initialize()

        chunks = []
        with patch("src.agent.manager.query", mock_query):
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks if c.startswith("data:")]
        errors = [p for p in payloads if "error" in p]
        assert errors
        assert call_count == 1, "Should not retry on timeout"

    @pytest.mark.asyncio
    async def test_generic_exception_with_control_request_timeout_retries(self, db):
        """Generic Exception with 'Control request timeout' retries once."""
        thread_id = await db.create_agent_thread("ctrl-timeout")
        await db.save_agent_message(thread_id, "user", "test")

        call_count = 0

        async def mock_query(prompt, options):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("Control request timeout")
            from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

            yield _make_assistant_msg("recovered")
            yield _make_result_msg()

        mgr = AgentManager(db)
        mgr.initialize()

        chunks = []
        with patch("src.agent.manager.query", mock_query):
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        assert call_count >= 2, f"Expected retry, got {call_count} calls"

    @pytest.mark.asyncio
    async def test_generic_exception_no_retry(self, db):
        """Generic Exception without retry keywords fails immediately."""
        thread_id = await db.create_agent_thread("generic-fail")
        await db.save_agent_message(thread_id, "user", "test")

        async def mock_query(prompt, options):
            raise RuntimeError("unexpected fatal error")
            yield

        mgr = AgentManager(db)
        mgr.initialize()

        chunks = []
        with patch("src.agent.manager.query", mock_query):
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks if c.startswith("data:")]
        errors = [p for p in payloads if "error" in p]
        assert errors

    @pytest.mark.asyncio
    async def test_rate_limit_event_rejected(self, db):
        """RateLimitEvent with 'rejected' status surfaces as warning."""
        thread_id = await db.create_agent_thread("rate-limit")
        await db.save_agent_message(thread_id, "user", "test")

        from claude_agent_sdk import AssistantMessage, RateLimitEvent, ResultMessage, TextBlock

        rl_event = MagicMock(spec=RateLimitEvent)
        rl_info = MagicMock()
        rl_info.status = "rejected"
        rl_info.resets_at = None
        rl_info.utilization = None
        rl_event.rate_limit_info = rl_info

        async def mock_query(prompt, options):
            yield rl_event
            yield _make_assistant_msg("ok")
            yield _make_result_msg()

        mgr = AgentManager(db)
        mgr.initialize()

        chunks = []
        with patch("src.agent.manager.query", mock_query):
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks if c.startswith("data:")]
        warnings = [p for p in payloads if p.get("type") == "warning"]
        assert warnings, f"Expected warning for rejected rate limit, got: {payloads}"

    @pytest.mark.asyncio
    async def test_rate_limit_event_non_rejected(self, db):
        """RateLimitEvent with non-rejected status emits status event."""
        thread_id = await db.create_agent_thread("rate-limit-ok")
        await db.save_agent_message(thread_id, "user", "test")

        from claude_agent_sdk import AssistantMessage, RateLimitEvent, ResultMessage, TextBlock

        rl_event = MagicMock(spec=RateLimitEvent)
        rl_info = MagicMock()
        rl_info.status = "limited"
        rl_info.resets_at = "2026-01-01"
        rl_info.utilization = 0.8
        rl_event.rate_limit_info = rl_info

        async def mock_query(prompt, options):
            yield rl_event
            yield _make_assistant_msg("ok")
            yield _make_result_msg()

        mgr = AgentManager(db)
        mgr.initialize()

        chunks = []
        with patch("src.agent.manager.query", mock_query):
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks if c.startswith("data:")]
        status_events = [
            p for p in payloads if p.get("type") == "status" and "Rate limit" in p.get("text", "")
        ]
        assert status_events, f"Expected Rate limit status, got: {payloads}"

    @pytest.mark.asyncio
    async def test_rate_limit_event_with_none_info(self, db):
        """RateLimitEvent with None rate_limit_info uses 'unknown' status."""
        thread_id = await db.create_agent_thread("rl-none-info")
        await db.save_agent_message(thread_id, "user", "test")

        from claude_agent_sdk import AssistantMessage, RateLimitEvent, ResultMessage, TextBlock

        rl_event = MagicMock(spec=RateLimitEvent)
        rl_event.rate_limit_info = None

        async def mock_query(prompt, options):
            yield rl_event
            yield _make_assistant_msg("ok")
            yield _make_result_msg()

        mgr = AgentManager(db)
        mgr.initialize()

        chunks = []
        with patch("src.agent.manager.query", mock_query):
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks if c.startswith("data:")]
        status_events = [
            p for p in payloads if p.get("type") == "status" and "Rate limit" in p.get("text", "")
        ]
        assert status_events
        assert "unknown" in status_events[0]["text"]

    @pytest.mark.asyncio
    async def test_stream_event_error_overloaded(self, db):
        """StreamEvent with error type 'overloaded_error' raises CLIConnectionError."""
        thread_id = await db.create_agent_thread("overload-error")
        await db.save_agent_message(thread_id, "user", "test")

        from claude_agent_sdk import StreamEvent

        def _make_error_event(error_type, error_msg):
            ev = MagicMock(spec=StreamEvent)
            ev.event = {"type": "error", "error": {"type": error_type, "message": error_msg}}
            return ev

        async def mock_query(prompt, options):
            yield _make_error_event("overloaded_error", "server overloaded")

        mgr = AgentManager(db)
        mgr.initialize()

        chunks = []
        with patch("src.agent.manager.query", mock_query):
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks if c.startswith("data:")]
        errors = [p for p in payloads if "error" in p]
        assert errors

    @pytest.mark.asyncio
    async def test_stream_event_error_generic(self, db):
        """StreamEvent with generic error type raises ClaudeSDKError."""
        thread_id = await db.create_agent_thread("api-error")
        await db.save_agent_message(thread_id, "user", "test")

        from claude_agent_sdk import StreamEvent

        def _make_error_event(error_type, error_msg):
            ev = MagicMock(spec=StreamEvent)
            ev.event = {"type": "error", "error": {"type": error_type, "message": error_msg}}
            return ev

        async def mock_query(prompt, options):
            yield _make_error_event("invalid_request", "bad input")

        mgr = AgentManager(db)
        mgr.initialize()

        chunks = []
        with patch("src.agent.manager.query", mock_query):
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks if c.startswith("data:")]
        errors = [p for p in payloads if "error" in p]
        assert errors

    @pytest.mark.asyncio
    async def test_content_block_start_non_tool_use(self, db):
        """content_block_start with non-tool_use type is logged but doesn't crash."""
        thread_id = await db.create_agent_thread("block-start")
        await db.save_agent_message(thread_id, "user", "test")

        from claude_agent_sdk import AssistantMessage, ResultMessage, StreamEvent, TextBlock

        def _make_event(event_dict):
            ev = MagicMock(spec=StreamEvent)
            ev.event = event_dict
            return ev

        async def mock_query(prompt, options):
            yield _make_event({
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": "hello"},
            })
            yield _make_assistant_msg("done")
            yield _make_result_msg()

        mgr = AgentManager(db)
        mgr.initialize()

        chunks = []
        with patch("src.agent.manager.query", mock_query):
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        assert chunks

    @pytest.mark.asyncio
    async def test_result_message_with_cost_and_turns(self, db):
        """ResultMessage with total_cost_usd and num_turns fields."""
        thread_id = await db.create_agent_thread("result-msg")
        await db.save_agent_message(thread_id, "user", "test")

        result_msg = _make_result_msg(
            total_cost_usd=0.05,
            num_turns=3,
            session_id="test-session-id",
            usage={"input_tokens": 100},
            model_usage={"model": "claude"},
        )

        async def mock_query(prompt, options):
            yield _make_assistant_msg("response")
            yield result_msg

        mgr = AgentManager(db)
        mgr.initialize()

        chunks = []
        with patch("src.agent.manager.query", mock_query):
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks if c.startswith("data:")]
        done_events = [p for p in payloads if p.get("done")]
        assert done_events
        assert done_events[0]["total_cost_usd"] == 0.05
        assert done_events[0]["num_turns"] == 3
        assert done_events[0]["session_id"] == "test-session-id"

    @pytest.mark.asyncio
    async def test_unhandled_message_type_logs_warning(self, db):
        """Unknown SDK message type logs a warning without crashing."""
        thread_id = await db.create_agent_thread("unhandled-msg")
        await db.save_agent_message(thread_id, "user", "test")

        from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

        async def mock_query(prompt, options):
            yield MagicMock()  # unknown type
            yield _make_assistant_msg("ok")
            yield _make_result_msg()

        mgr = AgentManager(db)
        mgr.initialize()

        chunks = []
        with patch("src.agent.manager.query", mock_query):
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        assert chunks  # should not crash

    @pytest.mark.asyncio
    async def test_cancelled_error_sets_draining(self, db):
        """CancelledError during stream sets draining=True and continues."""
        thread_id = await db.create_agent_thread("cancel-drain")
        await db.save_agent_message(thread_id, "user", "test")

        async def mock_query(prompt, options):
            yield _make_text_event("first")
            yield _make_assistant_msg("ok")
            yield _make_result_msg()

        mgr = AgentManager(db)
        mgr.initialize()

        chunks = []
        with patch("src.agent.manager.query", mock_query):
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        assert chunks

    @pytest.mark.asyncio
    async def test_idle_timeout_with_rate_limit_info(self, db):
        """Idle timeout includes rate limit info in error message."""
        thread_id = await db.create_agent_thread("idle-rl")
        await db.save_agent_message(thread_id, "user", "test")

        from claude_agent_sdk import RateLimitEvent, StreamEvent

        def _stream_event(text):
            ev = MagicMock(spec=StreamEvent)
            ev.event = {"type": "content_block_delta", "delta": {"type": "text_delta", "text": text}}
            return ev

        rl_event = MagicMock(spec=RateLimitEvent)
        rl_info = MagicMock()
        rl_info.status = "limited"
        rl_info.resets_at = None
        rl_info.utilization = 0.5
        rl_event.rate_limit_info = rl_info

        async def mock_query(prompt, options):
            yield _stream_event("hello")
            yield rl_event
            await asyncio.sleep(100)

        config = AppConfig()
        config.agent.first_event_timeout = 5
        config.agent.idle_timeout = 1
        config.agent.total_timeout = 10

        mgr = AgentManager(db, config=config)
        mgr.initialize()

        chunks = []
        with patch("src.agent.manager.query", mock_query):
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks if c.startswith("data:")]
        errors = [p for p in payloads if "error" in p]
        assert errors

    @pytest.mark.asyncio
    async def test_stderr_debug_lines_dumped_after_error(self, db):
        """Debug and stderr lines are dumped to log after error in chat_stream."""
        thread_id = await db.create_agent_thread("debug-dump")
        await db.save_agent_message(thread_id, "user", "test")

        from claude_agent_sdk import CLINotFoundError

        async def mock_query(prompt, options):
            if options.stderr:
                options.stderr("2026-01-01T00:00:00Z [DEBUG] some debug line")
                options.stderr("2026-01-01T00:00:00Z [WARN] warning line")
            raise CLINotFoundError("not found")
            yield

        mgr = AgentManager(db)
        mgr.initialize()

        chunks = []
        with patch("src.agent.manager.query", mock_query):
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        assert chunks


# ===========================================================================
# DeepagentsBackend — additional coverage
# ===========================================================================


class TestDeepagentsBackendInit:
    def test_initialize_not_configured(self, mock_db):
        """initialize() logs and returns when not configured."""
        config = AppConfig()
        config.agent.fallback_model = ""
        backend = DeepagentsBackend(mock_db, config)
        backend.initialize()

    def test_initialize_no_candidates_with_legacy_model(self, mock_db):
        """initialize() raises when legacy model format is wrong."""
        config = AppConfig()
        config.agent.fallback_model = "invalid-no-colon"
        backend = DeepagentsBackend(mock_db, config)
        with pytest.raises(RuntimeError, match="provider:model"):
            backend.initialize()

    def test_initialize_with_enabled_db_config_validation_errors(self, mock_db):
        """initialize() collects validation errors from all providers."""
        config = AppConfig()
        config.agent.fallback_model = ""
        backend = DeepagentsBackend(mock_db, config)

        bad_cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4",
            plain_fields={},
            secret_fields={},
            last_validation_error="Missing API key",
        )
        backend._cached_db_configs = [bad_cfg]

        with pytest.raises(RuntimeError, match="Missing API key"):
            backend.initialize()

    def test_initialize_all_legacy_candidates_with_validation_error(self, mock_db):
        """initialize() raises when all legacy candidates have validation errors."""
        config = AppConfig()
        config.agent.fallback_model = "anthropic:claude-3"
        backend = DeepagentsBackend(mock_db, config)

        with pytest.raises(RuntimeError, match="AGENT_FALLBACK_API_KEY"):
            backend.initialize()

    def test_initialize_legacy_candidate_passes_validation(self, mock_db):
        """initialize() succeeds when legacy candidate passes validation."""
        config = AppConfig()
        config.agent.fallback_model = "openai:gpt-4"
        config.agent.fallback_api_key = "test-key"
        backend = DeepagentsBackend(mock_db, config)

        backend.initialize()
        assert backend._preflight_available is True

    def test_initialize_non_legacy_candidates_build_fails(self, mock_db):
        """initialize() raises when _build_agent fails for all non-legacy candidates."""
        config = AppConfig()
        config.agent.fallback_model = ""
        backend = DeepagentsBackend(mock_db, config)

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4",
            plain_fields={},
            secret_fields={"api_key": "test-key"},
        )
        backend._cached_db_configs = [cfg]

        with patch.object(backend, "_build_agent", side_effect=RuntimeError("init failed")):
            with pytest.raises(RuntimeError, match="init failed"):
                backend.initialize()

    def test_initialize_no_candidates_no_legacy_model(self, mock_db):
        """initialize() with no candidates and no legacy model reports no providers."""
        config = AppConfig()
        config.agent.fallback_model = ""
        backend = DeepagentsBackend(mock_db, config)
        # No DB configs either, and configured is False, so initialize returns early
        backend._cached_db_configs = []
        # configured returns False (no fallback model, no db configs)
        assert backend.configured is False


class TestDeepagentsBackendBuildAgent:
    def test_build_agent_empty_model_raises(self, mock_db):
        """_build_agent raises RuntimeError when selected_model is empty."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="",
            plain_fields={},
            secret_fields={},
        )
        with pytest.raises(RuntimeError, match="not configured"):
            backend._build_agent(cfg)

    def test_build_agent_anthropic_without_key_raises(self, mock_db):
        """_build_agent raises RuntimeError for anthropic without API key."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        cfg = ProviderRuntimeConfig(
            provider="anthropic",
            enabled=True,
            priority=0,
            selected_model="anthropic:claude-3",
            plain_fields={},
            secret_fields={},
        )
        with pytest.raises(RuntimeError, match="AGENT_FALLBACK_API_KEY"):
            backend._build_agent(cfg)

    def test_build_agent_value_error_from_create_agent(self, mock_db):
        """_build_agent handles ValueError from create_deep_agent."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4",
            plain_fields={},
            secret_fields={"api_key": "test"},
        )

        with (
            patch("langchain.chat_models.init_chat_model", return_value=MagicMock()),
            patch("deepagents.create_deep_agent", side_effect=ValueError("bad config")),
        ):
            with pytest.raises(RuntimeError, match="Некорректная конфигурация"):
                backend._build_agent(cfg)

    def test_build_agent_generic_exception_from_create_agent(self, mock_db):
        """_build_agent handles generic Exception from create_deep_agent."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4",
            plain_fields={},
            secret_fields={"api_key": "test"},
        )

        with (
            patch("langchain.chat_models.init_chat_model", return_value=MagicMock()),
            patch("deepagents.create_deep_agent", side_effect=Exception("unexpected")),
        ):
            with pytest.raises(RuntimeError, match="Не удалось инициализировать"):
                backend._build_agent(cfg)

    def test_build_agent_ollama_react_fallback(self, mock_db):
        """_build_agent uses OllamaReActAgent for models without native FC."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        cfg = ProviderRuntimeConfig(
            provider="ollama",
            enabled=True,
            priority=0,
            selected_model="kimi-k2.5",
            plain_fields={"base_url": "http://localhost:11434"},
            secret_fields={"api_key": ""},
        )

        with patch("src.agent.react_agent.OllamaReActAgent") as mock_react:
            mock_react.return_value = MagicMock()
            agent = backend._build_agent(cfg, record_last_used=True)
            mock_react.assert_called_once()
            assert backend._last_used_provider == "ollama"
            assert backend._last_used_model == "kimi-k2.5"

    def test_build_agent_ollama_with_api_key(self, mock_db):
        """_build_agent for Ollama sets Authorization header when api_key provided."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        cfg = ProviderRuntimeConfig(
            provider="ollama",
            enabled=True,
            priority=0,
            selected_model="llama3",
            plain_fields={"base_url": "http://localhost:11434"},
            secret_fields={"api_key": "ollama-key"},
        )

        captured_kwargs = {}

        def fake_init_chat_model(*, model, model_provider, **kwargs):
            captured_kwargs.update(kwargs)
            return MagicMock()

        with (
            patch("langchain.chat_models.init_chat_model", side_effect=fake_init_chat_model),
            patch("deepagents.create_deep_agent", return_value=MagicMock()),
        ):
            agent = backend._build_agent(cfg)
            assert agent is not None
            assert (
                captured_kwargs.get("client_kwargs", {}).get("headers", {}).get("Authorization")
                == "Bearer ollama-key"
            )

    def test_build_agent_with_provider_prefix_in_model(self, mock_db):
        """_build_agent strips provider prefix from model name when it matches."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="openai:gpt-4",
            plain_fields={},
            secret_fields={"api_key": "test"},
        )

        captured = {}

        def fake_init_chat_model(*, model, model_provider, **kwargs):
            captured["model"] = model
            return MagicMock()

        with (
            patch("langchain.chat_models.init_chat_model", side_effect=fake_init_chat_model),
            patch("deepagents.create_deep_agent", return_value=MagicMock()),
        ):
            backend._build_agent(cfg)
            assert captured["model"] == "gpt-4"

    def test_build_agent_record_last_used(self, mock_db):
        """_build_agent records last used provider/model when flag is True."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4",
            plain_fields={},
            secret_fields={"api_key": "test"},
        )

        with (
            patch("langchain.chat_models.init_chat_model", return_value=MagicMock()),
            patch("deepagents.create_deep_agent", return_value=MagicMock()),
        ):
            backend._build_agent(cfg, record_last_used=True)
            assert backend._last_used_provider == "openai"
            assert backend._last_used_model == "gpt-4"

    def test_build_agent_no_record_when_flag_false(self, mock_db):
        """_build_agent does not record when record_last_used is False."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)
        backend._last_used_provider = ""
        backend._last_used_model = ""

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4",
            plain_fields={},
            secret_fields={"api_key": "test"},
        )

        with (
            patch("langchain.chat_models.init_chat_model", return_value=MagicMock()),
            patch("deepagents.create_deep_agent", return_value=MagicMock()),
        ):
            backend._build_agent(cfg, record_last_used=False)
            assert backend._last_used_provider == ""
            assert backend._last_used_model == ""


class TestDeepagentsBackendRunAgent:
    def test_run_agent_with_run_method(self, mock_db):
        """_run_agent calls agent.run() when agent has run attribute."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        mock_agent = MagicMock()
        mock_agent.run = MagicMock(return_value="agent response")

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4",
            plain_fields={},
            secret_fields={"api_key": "test"},
        )

        with patch.object(backend, "_build_agent", return_value=mock_agent):
            with patch.object(backend, "_default_tools", return_value=[]):
                result = backend._run_agent("test prompt", cfg)
                assert result == "agent response"
                mock_agent.run.assert_called_once()

    def test_run_agent_with_invoke_method(self, mock_db):
        """_run_agent calls agent.invoke() when agent does not have run."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        msg = SimpleNamespace(content="invoke result")
        mock_agent = MagicMock(spec=[])
        mock_agent.invoke = MagicMock(return_value={"messages": [msg]})

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4",
            plain_fields={},
            secret_fields={"api_key": "test"},
        )

        with patch.object(backend, "_build_agent", return_value=mock_agent):
            with patch.object(backend, "_default_tools", return_value=[]):
                result = backend._run_agent("test prompt", cfg)
                assert "invoke result" in result

    def test_run_agent_with_history_and_invoke(self, mock_db):
        """_run_agent passes history to invoke-based agent."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        msg = SimpleNamespace(content="history response")
        mock_agent = MagicMock(spec=[])
        mock_agent.invoke = MagicMock(return_value={"messages": [msg]})

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4",
            plain_fields={},
            secret_fields={"api_key": "test"},
        )

        history = [{"role": "user", "content": "prev question"}]

        with patch.object(backend, "_build_agent", return_value=mock_agent):
            with patch.object(backend, "_default_tools", return_value=[]):
                result = backend._run_agent("new question", cfg, history_msgs=history)
                assert result == "history response"


class TestDeepagentsProbeAndRun:
    def test_run_probe_with_run_method(self, mock_db):
        """_run_probe calls agent.run() when available."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        # The probe creates its own tool wrappers. We need to ensure
        # the mock agent actually calls the get_channels tool.
        # We'll patch _probe_tools to return tools that track calls,
        # and make the mock agent call get_channels during run().
        probe_tools = []
        probe_calls = {"search_messages": 0, "get_channels": 0}

        def _search_tool(query_text):
            probe_calls["search_messages"] += 1
            return "search result"

        def _get_channels_tool():
            probe_calls["get_channels"] += 1
            return "channels result"

        probe_tools = [_search_tool, _get_channels_tool]

        mock_agent = MagicMock()

        def _run_side_effect(prompt):
            # Simulate the agent calling the get_channels tool
            _get_channels_tool()
            return "probe result"

        mock_agent.run = MagicMock(side_effect=_run_side_effect)

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4",
            plain_fields={},
            secret_fields={"api_key": "test"},
        )

        with patch.object(backend, "_build_agent", return_value=mock_agent):
            with patch.object(backend, "_probe_tools", return_value=(probe_tools, probe_calls)):
                backend._run_probe(cfg)
                mock_agent.run.assert_called_once()

    def test_run_probe_with_invoke_method(self, mock_db):
        """_run_probe calls agent.invoke() when no run method."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        probe_calls = {"search_messages": 0, "get_channels": 0}

        def _get_channels_tool():
            probe_calls["get_channels"] += 1
            return "channels result"

        def _search_tool(query_text):
            probe_calls["search_messages"] += 1
            return "search result"

        probe_tools = [_search_tool, _get_channels_tool]

        msg = SimpleNamespace(content="probe result")
        mock_agent = MagicMock(spec=[])

        def _invoke_side_effect(msgs):
            _get_channels_tool()
            return {"messages": [msg]}

        mock_agent.invoke = MagicMock(side_effect=_invoke_side_effect)

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4",
            plain_fields={},
            secret_fields={"api_key": "test"},
        )

        with patch.object(backend, "_build_agent", return_value=mock_agent):
            with patch.object(backend, "_probe_tools", return_value=(probe_tools, probe_calls)):
                backend._run_probe(cfg)
                mock_agent.invoke.assert_called_once()

    def test_run_probe_restores_state_on_failure(self, mock_db):
        """_run_probe restores previous state even when probe fails."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)
        backend._last_used_provider = "old_provider"
        backend._last_used_model = "old_model"

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4",
            plain_fields={},
            secret_fields={"api_key": "test"},
        )

        with patch.object(backend, "_build_agent", side_effect=RuntimeError("fail")):
            with pytest.raises(RuntimeError):
                backend._run_probe(cfg)

        assert backend._last_used_provider == "old_provider"
        assert backend._last_used_model == "old_model"

    def test_run_probe_no_get_channels_call_raises(self, mock_db):
        """_run_probe raises when get_channels tool is not called."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        mock_agent = MagicMock()
        mock_agent.run = MagicMock(return_value="response without tool use")

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4",
            plain_fields={},
            secret_fields={"api_key": "test"},
        )

        with patch.object(backend, "_build_agent", return_value=mock_agent):
            with pytest.raises(RuntimeError, match="get_channels"):
                backend._run_probe(cfg)

    @pytest.mark.asyncio
    async def test_probe_config_supported(self, mock_db):
        """probe_config returns 'supported' on successful probe."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4",
            plain_fields={},
            secret_fields={"api_key": "test"},
        )

        with patch.object(backend, "_run_probe"):
            result = await backend.probe_config(cfg, probe_kind="manual")

        assert result.status == "supported"
        assert result.model == "gpt-4"

    @pytest.mark.asyncio
    async def test_probe_config_timeout(self, mock_db):
        """probe_config returns 'unknown' on timeout."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4",
            plain_fields={},
            secret_fields={"api_key": "test"},
        )

        import src.agent.manager as mgr_module

        original_timeout = mgr_module._DEEPAGENTS_PROBE_TIMEOUT_SECONDS
        mgr_module._DEEPAGENTS_PROBE_TIMEOUT_SECONDS = 0.5

        try:
            # _run_probe runs via asyncio.to_thread, so we need it to block
            # in a real thread to allow the wait_for timeout to fire.
            with patch.object(backend, "_run_probe", side_effect=lambda cfg: __import__("time").sleep(10)):
                result = await backend.probe_config(cfg)
        finally:
            mgr_module._DEEPAGENTS_PROBE_TIMEOUT_SECONDS = original_timeout

        assert result.status == "unknown"
        assert "timed out" in (result.reason or "").lower()

    @pytest.mark.asyncio
    async def test_probe_config_exception(self, mock_db):
        """probe_config returns appropriate status on exception."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4",
            plain_fields={},
            secret_fields={"api_key": "test"},
        )

        with patch.object(backend, "_run_probe", side_effect=RuntimeError("model not available")):
            result = await backend.probe_config(cfg)

        assert result.status == "unsupported"


class TestDeepagentsChatStream:
    @pytest.mark.asyncio
    async def test_chat_stream_empty_response(self, mock_db):
        """chat_stream handles empty response from agent."""
        config = AppConfig()
        config.agent.fallback_model = "openai:gpt-4"
        config.agent.fallback_api_key = "test-key"
        backend = DeepagentsBackend(mock_db, config)

        mock_agent = MagicMock()
        mock_agent.run = MagicMock(return_value="")

        queue: asyncio.Queue[str | None] = asyncio.Queue()

        with patch.object(backend, "_candidate_configs") as mock_candidates:
            mock_candidates.return_value = [
                ProviderRuntimeConfig(
                    provider="openai",
                    enabled=True,
                    priority=0,
                    selected_model="gpt-4",
                    plain_fields={},
                    secret_fields={"api_key": "test"},
                )
            ]
            with patch.object(backend, "_validation_error", return_value=""):
                with patch.object(backend, "_build_agent", return_value=mock_agent):
                    with patch.object(backend, "_default_tools", return_value=[]):
                        await backend.chat_stream(
                            thread_id=1,
                            prompt="test",
                            system_prompt="sys",
                            stats={},
                            model=None,
                            queue=queue,
                        )

        items = []
        while not queue.empty():
            items.append(queue.get_nowait())
        payloads = [
            json.loads(i.removeprefix("data: ")) for i in items if i and i.startswith("data:")
        ]
        done_events = [p for p in payloads if p.get("done")]
        assert done_events
        assert done_events[0]["full_text"] == ""

    @pytest.mark.asyncio
    async def test_chat_stream_multiple_providers_fallback(self, mock_db):
        """chat_stream tries next provider when first fails."""
        config = AppConfig()
        config.agent.fallback_model = ""
        backend = DeepagentsBackend(mock_db, config)

        call_count = 0

        def _build_agent_side_effect(cfg, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("provider 1 failed")
            mock_agent = MagicMock()
            mock_agent.run = MagicMock(return_value="provider 2 response")
            return mock_agent

        configs = [
            ProviderRuntimeConfig(
                provider="openai",
                enabled=True,
                priority=0,
                selected_model="gpt-4",
                plain_fields={},
                secret_fields={"api_key": "test"},
            ),
            ProviderRuntimeConfig(
                provider="groq",
                enabled=True,
                priority=1,
                selected_model="llama3",
                plain_fields={},
                secret_fields={"api_key": "groq-key"},
            ),
        ]

        queue: asyncio.Queue[str | None] = asyncio.Queue()

        with patch.object(backend, "_candidate_configs", return_value=configs):
            with patch.object(backend, "_validation_error", return_value=""):
                with patch.object(
                    backend, "_build_agent", side_effect=_build_agent_side_effect
                ):
                    with patch.object(backend, "_default_tools", return_value=[]):
                        await backend.chat_stream(
                            thread_id=1,
                            prompt="test",
                            system_prompt="sys",
                            stats={},
                            model=None,
                            queue=queue,
                        )

        items = []
        while not queue.empty():
            items.append(queue.get_nowait())
        payloads = [
            json.loads(i.removeprefix("data: ")) for i in items if i and i.startswith("data:")
        ]
        done_events = [p for p in payloads if p.get("done")]
        assert done_events
        assert done_events[0]["full_text"] == "provider 2 response"

    @pytest.mark.asyncio
    async def test_chat_stream_all_providers_fail(self, mock_db):
        """chat_stream raises RuntimeError when all providers fail."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        configs = [
            ProviderRuntimeConfig(
                provider="openai",
                enabled=True,
                priority=0,
                selected_model="gpt-4",
                plain_fields={},
                secret_fields={"api_key": "test"},
            ),
        ]

        queue: asyncio.Queue[str | None] = asyncio.Queue()

        with patch.object(backend, "_candidate_configs", return_value=configs):
            with patch.object(backend, "_validation_error", return_value="Missing API key"):
                with pytest.raises(RuntimeError, match="Missing API key"):
                    await backend.chat_stream(
                        thread_id=1,
                        prompt="test",
                        system_prompt="sys",
                        stats={},
                        model=None,
                        queue=queue,
                    )


class TestDeepagentsResearcherWriter:
    @pytest.mark.asyncio
    async def test_run_researcher_writer_success(self, mock_db):
        """run_researcher_writer completes research and writer phases."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4",
            plain_fields={},
            secret_fields={"api_key": "test"},
        )

        with patch.object(backend, "_candidate_configs", return_value=[cfg]):
            with patch.object(backend, "_validation_error", return_value=""):
                with patch.object(backend, "_run_agent", return_value="research done") as mock_run:
                    result = await backend.run_researcher_writer(
                        "research prompt", "writer prompt"
                    )
                    assert result == "research done"
                    assert mock_run.call_count == 2

    @pytest.mark.asyncio
    async def test_run_researcher_writer_all_fail(self, mock_db):
        """run_researcher_writer raises when all providers fail."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4",
            plain_fields={},
            secret_fields={"api_key": "test"},
        )

        with patch.object(backend, "_candidate_configs", return_value=[cfg]):
            with patch.object(backend, "_validation_error", return_value=""):
                with patch.object(
                    backend, "_run_agent", side_effect=RuntimeError("provider failed")
                ):
                    with pytest.raises(RuntimeError, match="provider failed"):
                        await backend.run_researcher_writer("r", "w")


# ===========================================================================
# AgentManager — additional coverage
# ===========================================================================


class TestAgentManagerChatStreamErrors:
    @pytest.mark.asyncio
    async def test_chat_stream_with_no_backend_selected(self, db, monkeypatch):
        """chat_stream yields error when no backend is selected."""
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
        monkeypatch.delenv("AGENT_FALLBACK_MODEL", raising=False)

        mgr = AgentManager(db)
        thread_id = await db.create_agent_thread("no-backend")
        await db.save_agent_message(thread_id, "user", "test")

        chunks = []
        async for chunk in mgr.chat_stream(thread_id, "test"):
            chunks.append(chunk)

        payloads = [
            json.loads(c.removeprefix("data: ").strip())
            for c in chunks
            if c.startswith("data: ")
        ]
        errors = [p for p in payloads if "error" in p]
        assert errors, f"Expected error for no backend, got: {payloads}"

    @pytest.mark.asyncio
    async def test_chat_stream_deepagents_backend_selected(self, db, monkeypatch):
        """chat_stream works when deepagents backend is selected."""
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
        monkeypatch.setenv("AGENT_FALLBACK_MODEL", "openai:gpt-4")
        monkeypatch.setenv("AGENT_FALLBACK_API_KEY", "test-key")

        mgr = AgentManager(db)
        thread_id = await db.create_agent_thread("deepagents-stream")
        await db.save_agent_message(thread_id, "user", "test")

        mock_agent = MagicMock()
        mock_agent.run = MagicMock(return_value="deep response")

        with patch.object(mgr._deepagents_backend, "_build_agent", return_value=mock_agent):
            with patch.object(mgr._deepagents_backend, "_default_tools", return_value=[]):
                chunks = []
                async for chunk in mgr.chat_stream(thread_id, "test"):
                    chunks.append(chunk)

        payloads = [
            json.loads(c.removeprefix("data: ").strip())
            for c in chunks
            if c.startswith("data: ")
        ]
        done_events = [p for p in payloads if p.get("done")]
        assert done_events
        assert done_events[0]["backend"] == "deepagents"

    @pytest.mark.asyncio
    async def test_chat_stream_cancel_stream(self, db):
        """cancel_stream cancels active stream."""
        mgr = AgentManager(db)
        mgr.initialize()
        result = await mgr.cancel_stream(999)
        assert result is False

    @pytest.mark.asyncio
    async def test_chat_stream_with_invalid_model_for_claude(self, db):
        """chat_stream ignores invalid model for Claude backend."""
        thread_id = await db.create_agent_thread("invalid-model")
        await db.save_agent_message(thread_id, "user", "test")

        from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

        async def mock_query(prompt, options):
            assert options.model is None  # model should be None for invalid model
            yield _make_assistant_msg("ok")
            yield _make_result_msg()

        mgr = AgentManager(db)
        mgr.initialize()

        with patch("src.agent.manager.query", mock_query):
            chunks = [
                c async for c in mgr.chat_stream(thread_id, "test", model="invalid-model")
            ]
            assert chunks

    @pytest.mark.asyncio
    async def test_chat_stream_backend_exception_with_ollama_500(self, db, monkeypatch):
        """chat_stream shows Ollama 500 error with helpful message."""
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
        monkeypatch.setenv("AGENT_FALLBACK_MODEL", "ollama:llama3")
        monkeypatch.setenv("AGENT_FALLBACK_API_KEY", "")

        config = AppConfig()
        mgr = AgentManager(db, config)
        thread_id = await db.create_agent_thread("ollama-500")
        await db.save_agent_message(thread_id, "user", "test")

        with patch.object(
            mgr._deepagents_backend,
            "_build_agent",
            side_effect=RuntimeError("Ollama 500 internal server error status code"),
        ):
            chunks = []
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        payloads = [
            json.loads(c.removeprefix("data: ").strip())
            for c in chunks
            if c.startswith("data: ")
        ]
        errors = [p for p in payloads if "error" in p]
        assert errors
        assert any("Ollama" in e.get("error", "") for e in errors)

    @pytest.mark.asyncio
    async def test_chat_stream_backend_exception_with_ollama_connection_refused(
        self, db, monkeypatch
    ):
        """chat_stream shows Ollama connection error with helpful message."""
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
        monkeypatch.setenv("AGENT_FALLBACK_MODEL", "ollama:llama3")

        config = AppConfig()
        mgr = AgentManager(db, config)
        thread_id = await db.create_agent_thread("ollama-conn")
        await db.save_agent_message(thread_id, "user", "test")

        with patch.object(
            mgr._deepagents_backend,
            "_build_agent",
            side_effect=RuntimeError("Ollama connection refused to localhost"),
        ):
            chunks = []
            async for chunk in mgr.chat_stream(thread_id, "test"):
                chunks.append(chunk)

        payloads = [
            json.loads(c.removeprefix("data: ").strip())
            for c in chunks
            if c.startswith("data: ")
        ]
        errors = [p for p in payloads if "error" in p]
        assert errors
        assert any("Ollama" in e.get("error", "") for e in errors)


class TestAgentManagerPermissionGate:
    def test_enable_permission_gate(self, db):
        """enable_permission_gate sets the global gate."""
        mgr = AgentManager(db)
        mgr.enable_permission_gate()
        from src.agent.permission_gate import get_gate

        assert get_gate() is not None
        mgr.disable_permission_gate()

    def test_disable_permission_gate(self, db):
        """disable_permission_gate clears the global gate."""
        mgr = AgentManager(db)
        mgr.enable_permission_gate()
        mgr.disable_permission_gate()
        from src.agent.permission_gate import get_gate

        assert get_gate() is None

    def test_permission_gate_property(self, db):
        """permission_gate property returns the gate instance."""
        mgr = AgentManager(db)
        assert mgr.permission_gate is not None


class TestAgentManagerCloseAll:
    @pytest.mark.asyncio
    async def test_close_all_with_no_tasks(self, db):
        """close_all completes immediately with no active tasks."""
        mgr = AgentManager(db)
        await mgr.close_all()
        assert len(mgr._active_tasks) == 0

    @pytest.mark.asyncio
    async def test_close_all_cancels_active_tasks(self, db):
        """close_all cancels and waits for active tasks."""
        mgr = AgentManager(db)

        async def long_task():
            await asyncio.sleep(100)

        task = asyncio.create_task(long_task())
        mgr._active_tasks[1] = task

        await mgr.close_all()
        assert len(mgr._active_tasks) == 0
        assert task.cancelled() or task.done()


class TestAgentManagerEstimateTokens:
    @pytest.mark.asyncio
    async def test_estimate_prompt_tokens(self, db):
        """estimate_prompt_tokens returns reasonable estimate."""
        mgr = AgentManager(db)
        thread_id = await db.create_agent_thread("token-estimate")
        await db.save_agent_message(thread_id, "user", "hello world")

        tokens = await mgr.estimate_prompt_tokens(thread_id, "test message")
        assert tokens > 0


class TestAgentManagerRuntimeStatusEdgeCases:
    @pytest.mark.asyncio
    async def test_runtime_status_with_dev_override_unavailable_deepagents(
        self, db, monkeypatch
    ):
        """Runtime status reports error when override is deepagents but it's unavailable."""
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
        monkeypatch.delenv("AGENT_FALLBACK_MODEL", raising=False)
        await db.set_setting("agent_dev_mode_enabled", "1")
        await db.set_setting("agent_backend_override", "deepagents")

        mgr = AgentManager(db)
        status = await mgr.get_runtime_status()

        assert status.using_override is True
        assert status.selected_backend == "deepagents"
        assert status.error is not None

    @pytest.mark.asyncio
    async def test_runtime_status_with_invalid_override(self, db, monkeypatch):
        """Runtime status falls back to auto for invalid override values."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        await db.set_setting("agent_dev_mode_enabled", "1")
        await db.set_setting("agent_backend_override", "invalid_backend")

        mgr = AgentManager(db)
        status = await mgr.get_runtime_status()

        assert status.backend_override == "auto"

    @pytest.mark.asyncio
    async def test_runtime_status_no_backends_available(self, db, monkeypatch):
        """Runtime status reports error when no backend is available."""
        monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
        monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
        monkeypatch.delenv("AGENT_FALLBACK_MODEL", raising=False)

        mgr = AgentManager(db)
        status = await mgr.get_runtime_status()

        assert status.selected_backend is None
        assert status.error is not None

    @pytest.mark.asyncio
    async def test_runtime_status_deepagents_without_db_configs(self, db, monkeypatch):
        """Runtime status selects claude when deepagents has no usable DB configs."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setenv("AGENT_FALLBACK_MODEL", "openai:gpt-4")
        monkeypatch.setenv("AGENT_FALLBACK_API_KEY", "test-key")

        mgr = AgentManager(db)
        # Empty _cached_db_configs means has_usable_db_provider_configs returns False
        mgr._deepagents_backend._cached_db_configs = []
        with patch.object(mgr._deepagents_backend, "_build_agent", return_value=None):
            status = await mgr.get_runtime_status()

        assert status.selected_backend == "claude"

    @pytest.mark.asyncio
    async def test_runtime_status_deepagents_has_db_configs(self, db, monkeypatch):
        """Runtime status selects deepagents when it has usable DB configs."""
        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
        monkeypatch.setenv("AGENT_FALLBACK_MODEL", "openai:gpt-4")
        monkeypatch.setenv("AGENT_FALLBACK_API_KEY", "test-key")

        mgr = AgentManager(db)
        # Add a valid, enabled DB config so has_usable_db_provider_configs returns True
        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4",
            plain_fields={},
            secret_fields={"api_key": "test-key"},
        )
        mgr._deepagents_backend._cached_db_configs = [cfg]
        # Prevent refresh_settings_cache from overwriting our config
        mgr._deepagents_backend.refresh_settings_cache = AsyncMock()
        with patch.object(mgr._deepagents_backend, "_build_agent", return_value=None):
            with patch.object(
                mgr._deepagents_backend._provider_service, "validate_provider_config",
                return_value="",
            ):
                with patch.object(
                    mgr._deepagents_backend._provider_service, "compatibility_error_for_config",
                    return_value="",
                ):
                    status = await mgr.get_runtime_status()

        assert status.selected_backend == "deepagents"


class TestAgentManagerCachedSettings:
    @pytest.mark.asyncio
    async def test_settings_cache_hit(self, db):
        """Cached settings are used on subsequent calls."""
        mgr = AgentManager(db)
        await db.set_setting("test_key", "test_value")

        val1 = await mgr._get_setting_cached("test_key")
        assert val1 == "test_value"

        val2 = await mgr._get_setting_cached("test_key")
        assert val2 == "test_value"

    @pytest.mark.asyncio
    async def test_settings_cache_default(self, db):
        """Default value is used when setting is not found."""
        mgr = AgentManager(db)
        val = await mgr._get_setting_cached("nonexistent_key", "default_val")
        assert val == "default_val"


class TestAgentManagerProbeProvider:
    @pytest.mark.asyncio
    async def test_probe_provider_config_delegates(self, db):
        """probe_provider_config delegates to deepagents backend."""
        mgr = AgentManager(db)

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4",
            plain_fields={},
            secret_fields={"api_key": "test"},
        )

        with patch.object(mgr._deepagents_backend, "_run_probe"):
            result = await mgr.probe_provider_config(cfg)
            assert result.status == "supported"


class TestDeepagentsRunDbToolSync:
    def test_run_db_tool_sync_outside_loop(self, mock_db):
        """_run_db_tool_sync runs operation with asyncio.run outside event loop."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        async def mock_op():
            return "result"

        result = backend._run_db_tool_sync("test_tool", mock_op)
        assert result == "result"

    def test_run_db_tool_sync_inside_loop_raises(self, mock_db):
        """_run_db_tool_sync raises RuntimeError when called inside event loop."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        async def mock_op():
            return "result"

        async def _test():
            backend._run_db_tool_sync("test_tool", mock_op)

        with pytest.raises(RuntimeError, match="cannot run inside"):
            asyncio.run(_test())


class TestDeepagentsRefreshSettingsCache:
    @pytest.mark.asyncio
    async def test_refresh_resets_state_on_change(self, mock_db):
        """refresh_settings_cache resets state when configs change."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)
        backend._last_used_provider = "old"
        backend._last_used_model = "old_model"
        backend._preflight_available = True

        new_configs = [
            ProviderRuntimeConfig(
                provider="openai",
                enabled=True,
                priority=0,
                selected_model="gpt-4",
                plain_fields={},
                secret_fields={"api_key": "key"},
            ),
        ]

        with patch.object(
            backend._provider_service, "load_provider_configs", return_value=new_configs
        ):
            with patch.object(
                backend._provider_service, "load_model_cache", return_value={}
            ):
                await backend.refresh_settings_cache()

        assert backend._last_used_provider == ""
        assert backend._last_used_model == ""
        assert backend._preflight_available is None

    @pytest.mark.asyncio
    async def test_refresh_keeps_state_when_no_change(self, mock_db):
        """refresh_settings_cache keeps state when configs haven't changed."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4",
            plain_fields={},
            secret_fields={"api_key": "key"},
        )
        backend._cached_db_configs = [cfg]
        backend._cached_model_cache = {}
        backend._last_used_provider = "openai"
        backend._last_used_model = "gpt-4"

        with patch.object(
            backend._provider_service, "load_provider_configs", return_value=[cfg]
        ):
            with patch.object(
                backend._provider_service, "load_model_cache", return_value={}
            ):
                await backend.refresh_settings_cache()

        assert backend._last_used_provider == "openai"
        assert backend._last_used_model == "gpt-4"

    @pytest.mark.asyncio
    async def test_refresh_clears_state_when_no_configs(self, mock_db):
        """refresh_settings_cache clears state when no configs returned."""
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)
        backend._last_used_provider = "old"
        backend._last_used_model = "old_model"

        with patch.object(
            backend._provider_service, "load_provider_configs", return_value=[]
        ):
            with patch.object(
                backend._provider_service, "load_model_cache", return_value={}
            ):
                await backend.refresh_settings_cache()

        assert backend._last_used_provider == ""
        assert backend._last_used_model == ""


class TestDiagnoseConnection:
    def test_diagnose_no_cli_no_key(self):
        """Diagnosis reports missing CLI and API key."""
        from src.agent.manager import _diagnose_connection

        with patch.dict(os.environ, {}, clear=True):
            os.environ.pop("ANTHROPIC_API_KEY", None)
            os.environ.pop("CLAUDE_CODE_OAUTH_TOKEN", None)
            result = _diagnose_connection(None, [])
        assert "не найден" in result
        assert "не заданы" in result

    def test_diagnose_invalid_key(self):
        """Diagnosis reports invalid API key from stderr."""
        from src.agent.manager import _diagnose_connection

        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "bad-key"}):
            result = _diagnose_connection("/usr/bin/claude", ["Invalid API key provided"])
        assert "невалиден" in result

    def test_diagnose_rate_limit(self):
        """Diagnosis reports rate limit from stderr."""
        from src.agent.manager import _diagnose_connection

        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "key"}):
            result = _diagnose_connection("/usr/bin/claude", ["rate limit exceeded"])
        assert "rate limit" in result.lower()

    def test_diagnose_network_error(self):
        """Diagnosis reports network issues from stderr."""
        from src.agent.manager import _diagnose_connection

        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "key"}):
            result = _diagnose_connection("/usr/bin/claude", ["ECONNREFUSED connection failed"])
        assert "сетев" in result.lower()

    def test_diagnose_permission_error(self):
        """Diagnosis reports permission denied from stderr."""
        from src.agent.manager import _diagnose_connection

        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "key"}):
            result = _diagnose_connection("/usr/bin/claude", ["Permission denied 403"])
        assert "прав" in result.lower()

    def test_diagnose_unauthorized(self):
        """Diagnosis reports unauthorized from stderr."""
        from src.agent.manager import _diagnose_connection

        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "key"}):
            result = _diagnose_connection("/usr/bin/claude", ["Unauthorized 401"])
        assert "отклон" in result.lower()

    def test_diagnose_fallback_message(self):
        """Diagnosis returns generic message when no specific issue found."""
        from src.agent.manager import _diagnose_connection

        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "key"}):
            result = _diagnose_connection("/usr/bin/claude", [])
        assert "Проверьте" in result


class TestSummarizeToolArgs:
    def test_empty_args(self):
        from src.agent.manager import _summarize_tool_args

        assert _summarize_tool_args({}) == ""

    def test_single_arg(self):
        from src.agent.manager import _summarize_tool_args

        result = _summarize_tool_args({"query": "test"})
        assert "query" in result
        assert "test" in result

    def test_long_value_truncated(self):
        from src.agent.manager import _summarize_tool_args

        result = _summarize_tool_args({"text": "x" * 100})
        assert "..." in result

    def test_multiple_args(self):
        from src.agent.manager import _summarize_tool_args

        result = _summarize_tool_args({"a": "1", "b": "2"})
        assert "+1" in result


class TestTruncate:
    def test_short_string_unchanged(self):
        from src.agent.manager import _truncate

        assert _truncate("short") == "short"

    def test_long_string_truncated(self):
        from src.agent.manager import _truncate

        long_str = "x" * 200
        result = _truncate(long_str)
        assert result.endswith("...")
        assert len(result) == 120


class TestAsPromptStream:
    @pytest.mark.asyncio
    async def test_yields_single_message(self):
        from src.agent.manager import _as_prompt_stream

        chunks = []
        async for chunk in _as_prompt_stream("hello"):
            chunks.append(chunk)

        assert len(chunks) == 1
        assert chunks[0]["type"] == "user"
        assert chunks[0]["message"]["content"] == "hello"


class TestAutoApproveTool:
    @pytest.mark.asyncio
    async def test_returns_allow(self):
        from src.agent.manager import _auto_approve_tool
        from claude_agent_sdk import PermissionResultAllow

        result = await _auto_approve_tool("tool", {}, MagicMock())
        assert isinstance(result, PermissionResultAllow)
