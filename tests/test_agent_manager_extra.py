"""Extra tests for agent/manager.py — focusing on uncovered helper functions and DeepagentsBackend internals."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.agent.manager import (
    AgentManager,
    DeepagentsBackend,
    _SettingsCache,
    _ToolTracker,
    _await_with_countdown,
    _diagnose_connection,
    _embed_history_in_prompt,
    _summarize_tool_args,
    _truncate,
)
from src.config import AppConfig


# ── _embed_history_in_prompt ────────────────────────────────────────


def test_embed_history_empty():
    result = _embed_history_in_prompt([], "hello")
    assert "<user>" in result
    assert "hello" in result


def test_embed_history_with_messages():
    history = [
        {"role": "user", "content": "question"},
        {"role": "assistant", "content": "answer"},
    ]
    result = _embed_history_in_prompt(history, "follow-up")
    assert "<user>" in result
    assert "<assistant>" in result
    assert "question" in result
    assert "answer" in result
    assert "follow-up" in result


# ── _summarize_tool_args ────────────────────────────────────────────


def test_summarize_empty_args():
    assert _summarize_tool_args({}) == ""


def test_summarize_single_arg():
    assert _summarize_tool_args({"query": "test"}) == "query='test'"


def test_summarize_multiple_args():
    result = _summarize_tool_args({"query": "test", "limit": 10})
    assert "query=" in result
    assert "+1" in result


def test_summarize_long_value_truncated():
    result = _summarize_tool_args({"text": "x" * 100})
    assert "..." in result
    assert len(result) < 100


# ── _truncate ───────────────────────────────────────────────────────


def test_truncate_short():
    assert _truncate("hello", 120) == "hello"


def test_truncate_long():
    result = _truncate("x" * 200, 120)
    assert result.endswith("...")
    assert len(result) == 120


# ── _diagnose_connection ────────────────────────────────────────────


def test_diagnose_no_cli_no_keys():
    result = _diagnose_connection(None, [])
    assert "не найден" in result
    assert "ANTHROPIC_API_KEY" in result


def test_diagnose_invalid_key():
    result = _diagnose_connection(
        "/usr/bin/claude",
        ["error: Invalid API key provided"],
    )
    assert "невалиден" in result


def test_diagnose_rate_limit():
    result = _diagnose_connection(
        "/usr/bin/claude",
        ["rate limit exceeded 429"],
    )
    assert "rate limit" in result.lower()


def test_diagnose_network_error():
    result = _diagnose_connection(
        "/usr/bin/claude",
        ["network error: dns resolution failed"],
    )
    # Both network issue and no API key are reported
    assert "Проблема с сетевым подключением" in result


def test_diagnose_403():
    result = _diagnose_connection(
        "/usr/bin/claude",
        ["Permission denied 403"],
    )
    assert "403" in result


def test_diagnose_401():
    result = _diagnose_connection(
        "/usr/bin/claude",
        ["unauthorized 401"],
    )
    assert "401" in result


def test_diagnose_generic():
    result = _diagnose_connection("/usr/bin/claude", [])
    assert "Подключение" in result or "сети" in result or "API" in result


# ── _SettingsCache ──────────────────────────────────────────────────


def test_settings_cache_get_miss():
    cache = _SettingsCache()
    assert cache.get("nonexistent") is None


def test_settings_cache_set_and_get():
    cache = _SettingsCache()
    cache.set("key1", "value1")
    assert cache.get("key1") == "value1"


def test_settings_cache_expiry():
    cache = _SettingsCache()
    cache.set("key1", "value1", ttl=-1)  # already expired
    assert cache.get("key1") is None


def test_settings_cache_invalidate_specific_key():
    cache = _SettingsCache()
    cache.set("key1", "value1")
    cache.set("key2", "value2")
    cache.invalidate("key1")
    assert cache.get("key1") is None
    assert cache.get("key2") == "value2"


def test_settings_cache_invalidate_all():
    cache = _SettingsCache()
    cache.set("key1", "value1")
    cache.set("key2", "value2")
    cache.invalidate()
    assert cache.get("key1") is None
    assert cache.get("key2") is None


# ── _ToolTracker ────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_tool_tracker_on_first_event():
    queue = asyncio.Queue()
    tracker = _ToolTracker(queue=queue)
    await tracker.on_first_event()
    item = queue.get_nowait()
    assert "thinking" in item


@pytest.mark.asyncio
async def test_tool_tracker_on_first_event_idempotent():
    queue = asyncio.Queue()
    tracker = _ToolTracker(queue=queue)
    await tracker.on_first_event()
    await tracker.on_first_event()  # second call should be no-op
    assert queue.qsize() == 1


@pytest.mark.asyncio
async def test_tool_tracker_on_tool_start():
    queue = asyncio.Queue()
    tracker = _ToolTracker(queue=queue)
    await tracker.on_tool_start("search_messages", 0, tool_use_id="tu_1")
    # on_tool_start does NOT call on_first_event automatically
    item = queue.get_nowait()
    data = json.loads(item.removeprefix("data: ").strip())
    assert data["type"] == "tool_start"
    assert data["tool"] == "search_messages"


@pytest.mark.asyncio
async def test_tool_tracker_accumulate_and_block_stop():
    queue = asyncio.Queue()
    tracker = _ToolTracker(queue=queue)
    await tracker.on_first_event()
    await tracker.on_tool_start("test_tool", 0)
    # drain thinking + tool_start
    while not queue.empty():
        queue.get_nowait()

    tracker.accumulate_input('{"query": "test"}')
    await tracker.on_block_stop(0)

    item = queue.get_nowait()
    data = json.loads(item.removeprefix("data: ").strip())
    assert data["type"] == "tool_end"
    assert data["tool"] == "test_tool"
    assert data["is_error"] is False
    assert "query" in data["summary"]


@pytest.mark.asyncio
async def test_tool_tracker_on_block_stop_wrong_index():
    """on_block_stop with different index does not emit tool_end."""
    queue = asyncio.Queue()
    tracker = _ToolTracker(queue=queue)
    await tracker.on_first_event()
    await tracker.on_tool_start("test_tool", 0)
    while not queue.empty():
        queue.get_nowait()

    await tracker.on_block_stop(1)  # wrong index
    assert queue.empty()


@pytest.mark.asyncio
async def test_tool_tracker_on_block_stop_bad_json():
    """on_block_stop handles malformed JSON input gracefully."""
    queue = asyncio.Queue()
    tracker = _ToolTracker(queue=queue)
    await tracker.on_first_event()
    await tracker.on_tool_start("test_tool", 0)
    while not queue.empty():
        queue.get_nowait()

    tracker.accumulate_input("not valid json {{{")
    await tracker.on_block_stop(0)

    item = queue.get_nowait()
    data = json.loads(item.removeprefix("data: ").strip())
    assert data["type"] == "tool_end"
    assert data["summary"] == ""  # empty args due to parse failure


@pytest.mark.asyncio
async def test_tool_tracker_on_tool_result():
    queue = asyncio.Queue()
    tracker = _ToolTracker(queue=queue)
    tracker._tool_id_to_name["tu_1"] = "search_messages"
    await tracker.on_tool_result("tu_1", "Found 5 results", is_error=False)

    item = queue.get_nowait()
    data = json.loads(item.removeprefix("data: ").strip())
    assert data["type"] == "tool_result"
    assert data["tool"] == "search_messages"
    assert data["is_error"] is False


@pytest.mark.asyncio
async def test_tool_tracker_on_tool_result_unknown_id():
    queue = asyncio.Queue()
    tracker = _ToolTracker(queue=queue)
    await tracker.on_tool_result("unknown_id", "some content", is_error=True)

    item = queue.get_nowait()
    data = json.loads(item.removeprefix("data: ").strip())
    assert data["tool"] == "tool"  # fallback name


@pytest.mark.asyncio
async def test_tool_tracker_on_status():
    queue = asyncio.Queue()
    tracker = _ToolTracker(queue=queue)
    await tracker.on_status("Processing...")
    item = queue.get_nowait()
    data = json.loads(item.removeprefix("data: ").strip())
    assert data["type"] == "status"
    assert data["text"] == "Processing..."


# ── _classify_probe_failure ────────────────────────────────────────


def test_classify_probe_timeout():
    backend = DeepagentsBackend.__new__(DeepagentsBackend)
    status, reason = backend._classify_probe_failure(RuntimeError("Request timed out"))
    assert status == "unknown"
    assert "timed out" in reason


def test_classify_probe_rate_limit():
    backend = DeepagentsBackend.__new__(DeepagentsBackend)
    status, reason = backend._classify_probe_failure(RuntimeError("429 rate limit"))
    assert status == "unknown"


def test_classify_probe_unsupported():
    backend = DeepagentsBackend.__new__(DeepagentsBackend)
    status, reason = backend._classify_probe_failure(RuntimeError("model does not support tools"))
    assert status == "unsupported"


# ── DeepagentsBackend._extract_result_text ─────────────────────────


def test_extract_result_text_dict_with_messages():
    backend = DeepagentsBackend.__new__(DeepagentsBackend)
    msg = SimpleNamespace(content="final answer")
    result = backend._extract_result_text({"messages": [msg]})
    assert result == "final answer"


def test_extract_result_text_dict_with_list_content():
    backend = DeepagentsBackend.__new__(DeepagentsBackend)
    msg = SimpleNamespace(content=[{"text": "block1"}, {"text": "block2"}])
    result = backend._extract_result_text({"messages": [msg]})
    assert result == "block1\nblock2"


def test_extract_result_text_dict_empty_messages():
    backend = DeepagentsBackend.__new__(DeepagentsBackend)
    result = backend._extract_result_text({"messages": []})
    assert "messages" in result


def test_extract_result_text_non_dict():
    backend = DeepagentsBackend.__new__(DeepagentsBackend)
    result = backend._extract_result_text("simple string")
    assert result == "simple string"


# ── AgentManager cancel/close ───────────────────────────────────────


@pytest.mark.asyncio
async def test_cancel_stream_no_active_task(db):
    mgr = AgentManager(db)
    result = await mgr.cancel_stream(thread_id=9999)
    assert result is False


@pytest.mark.asyncio
async def test_close_all_no_active_tasks(db):
    mgr = AgentManager(db)
    await mgr.close_all()  # should not raise


@pytest.mark.asyncio
async def test_close_all_cancels_active_tasks(db):
    mgr = AgentManager(db)

    async def long_running():
        await asyncio.sleep(100)

    task = asyncio.create_task(long_running())
    mgr._active_tasks[1] = task
    await mgr.close_all()
    assert task.cancelled() or task.done()


# ── AgentManager permission gate ────────────────────────────────────


def test_enable_permission_gate(db):
    from src.agent.permission_gate import get_gate

    mgr = AgentManager(db)
    mgr.enable_permission_gate()
    assert get_gate() is mgr._permission_gate
    # Clean up
    mgr.disable_permission_gate()
    assert get_gate() is None


def test_disable_permission_gate(db):
    from src.agent.permission_gate import get_gate

    mgr = AgentManager(db)
    mgr.enable_permission_gate()
    mgr.disable_permission_gate()
    assert get_gate() is None


# ── AgentManager estimate_prompt_tokens ─────────────────────────────


@pytest.mark.asyncio
async def test_estimate_prompt_tokens(db):
    thread_id = await db.create_agent_thread("token-test")
    await db.save_agent_message(thread_id, "user", "hello world")
    mgr = AgentManager(db)
    tokens = await mgr.estimate_prompt_tokens(thread_id, "new message")
    assert tokens > 0


# ── DeepagentsBackend._provider_from_model ─────────────────────────


def test_provider_from_model_with_colon():
    backend = DeepagentsBackend.__new__(DeepagentsBackend)
    assert backend._provider_from_model("openai:gpt-4") == "openai"


def test_provider_from_model_without_colon():
    backend = DeepagentsBackend.__new__(DeepagentsBackend)
    assert backend._provider_from_model("gpt-4") is None


def test_provider_from_model_empty():
    backend = DeepagentsBackend.__new__(DeepagentsBackend)
    assert backend._provider_from_model("") is None


# ── DeepagentsBackend._legacy_fallback_config ─────────────────────


def test_legacy_fallback_config_no_model():
    config = AppConfig()
    config.agent.fallback_model = ""
    backend = DeepagentsBackend.__new__(DeepagentsBackend)
    backend._config = config
    assert backend._legacy_fallback_config() is None


def test_legacy_fallback_config_no_provider_prefix():
    config = AppConfig()
    config.agent.fallback_model = "llama3"
    backend = DeepagentsBackend.__new__(DeepagentsBackend)
    backend._config = config
    assert backend._legacy_fallback_config() is None


def test_legacy_fallback_config_valid():
    config = AppConfig()
    config.agent.fallback_model = "openai:gpt-4"
    config.agent.fallback_api_key = "test-key"
    backend = DeepagentsBackend.__new__(DeepagentsBackend)
    backend._config = config
    cfg = backend._legacy_fallback_config()
    assert cfg is not None
    assert cfg.provider == "openai"
    assert cfg.selected_model == "openai:gpt-4"
    assert cfg.secret_fields["api_key"] == "test-key"


# ── DeepagentsBackend._legacy_validation_error ─────────────────────


def test_legacy_validation_error_no_model():
    config = AppConfig()
    backend = DeepagentsBackend.__new__(DeepagentsBackend)
    backend._config = config
    from src.agent.provider_registry import ProviderRuntimeConfig

    cfg = ProviderRuntimeConfig(
        provider="openai", enabled=True, priority=0, selected_model="",
    )
    error = backend._legacy_validation_error(cfg)
    assert "not configured" in error


def test_legacy_validation_error_anthropic_no_key():
    config = AppConfig()
    backend = DeepagentsBackend.__new__(DeepagentsBackend)
    backend._config = config
    from src.agent.provider_registry import ProviderRuntimeConfig

    cfg = ProviderRuntimeConfig(
        provider="anthropic", enabled=True, priority=0, selected_model="anthropic:claude-sonnet-4-6",
    )
    error = backend._legacy_validation_error(cfg)
    assert "AGENT_FALLBACK_API_KEY" in error


def test_legacy_validation_error_passes():
    config = AppConfig()
    backend = DeepagentsBackend.__new__(DeepagentsBackend)
    backend._config = config
    from src.agent.provider_registry import ProviderRuntimeConfig

    cfg = ProviderRuntimeConfig(
        provider="openai", enabled=True, priority=0, selected_model="openai:gpt-4",
        secret_fields={"api_key": "test-key"},
    )
    error = backend._legacy_validation_error(cfg)
    assert error == ""


# ── _await_with_countdown with activity extension ──────────────────


@pytest.mark.asyncio
async def test_await_with_countdown_activity_extension():
    """Timeout is extended when activity_ts indicates fresh SDK activity."""
    queue: asyncio.Queue = asyncio.Queue()
    activity_ts = [0.0]  # mutable list

    async def slow_coro():
        await asyncio.sleep(1.5)
        return "done"

    result = await _await_with_countdown(
        slow_coro(),
        timeout=5.0,
        queue=queue,
        label="test",
        countdown_interval=0.5,
        activity_ts=activity_ts,
        activity_extend=30.0,
    )
    assert result == "done"


@pytest.mark.asyncio
async def test_await_with_countdown_max_timeout():
    """max_timeout is a hard ceiling that cannot be exceeded."""
    queue: asyncio.Queue = asyncio.Queue()

    async def slow_coro():
        await asyncio.sleep(100)

    with pytest.raises(asyncio.TimeoutError):
        await _await_with_countdown(
            slow_coro(),
            timeout=2.0,
            queue=queue,
            label="test",
            countdown_interval=0.5,
            max_timeout=2.0,
        )


@pytest.mark.asyncio
async def test_await_with_countdown_cancelled():
    """CancelledError propagates correctly."""
    queue: asyncio.Queue = asyncio.Queue()

    async def cancel_coro():
        await asyncio.sleep(100)

    task = asyncio.create_task(
        _await_with_countdown(
            cancel_coro(),
            timeout=100.0,
            queue=queue,
            label="test",
            countdown_interval=0.5,
        )
    )
    await asyncio.sleep(0.2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


# ── _await_with_countdown thinking status ──────────────────────────


@pytest.mark.asyncio
async def test_await_with_countdown_thinking_status():
    """Shows thinking status when api_request_ts is set and elapsed > 15s."""
    queue: asyncio.Queue = asyncio.Queue()
    import time

    api_request_ts = [time.monotonic() - 20]  # 20 seconds ago

    async def slow_coro():
        await asyncio.sleep(100)

    with pytest.raises(asyncio.TimeoutError):
        await _await_with_countdown(
            slow_coro(),
            timeout=2.0,
            queue=queue,
            label="test",
            countdown_interval=0.5,
            api_request_ts=api_request_ts,
        )

    items = []
    while not queue.empty():
        items.append(queue.get_nowait())
    thinking_items = [i for i in items if "Думает" in i]
    assert len(thinking_items) >= 1, f"Expected thinking status, got: {items}"
