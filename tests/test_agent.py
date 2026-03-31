from __future__ import annotations

import asyncio
import json
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest
from langchain_core.tools import StructuredTool

from src.agent.context import format_context
from src.agent.manager import AgentManager
from src.agent.prompt_template import (
    AGENT_PROMPT_TEMPLATE_SETTING,
    PromptTemplateError,
    validate_prompt_template,
)
from src.agent.provider_registry import ProviderRuntimeConfig
from src.agent.tools import make_mcp_server
from src.config import AppConfig
from src.models import Message
from src.services.agent_provider_service import AgentProviderService


@pytest.fixture(autouse=True)
def _default_agent_env(monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-claude-key")


@pytest.mark.asyncio
async def test_make_mcp_server_returns_server(db):
    server = make_mcp_server(db)
    assert server is not None


@pytest.mark.asyncio
async def test_agent_manager_initialize(db):
    mgr = AgentManager(db)
    mgr.initialize()
    assert mgr._claude_backend._server is not None


@pytest.mark.asyncio
async def test_agent_chat_stream_mocked(db):
    thread_id = await db.create_agent_thread("test thread")
    await db.save_agent_message(thread_id, "user", "test")

    from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

    text_block = TextBlock(text="hello")
    assistant_msg = MagicMock(spec=AssistantMessage)
    assistant_msg.content = [text_block]

    result_msg = MagicMock(spec=ResultMessage)

    async def mock_query(prompt, options):
        yield assistant_msg
        yield result_msg

    mgr = AgentManager(db)
    mgr.initialize()

    chunks = []
    with patch("src.agent.manager.query", mock_query):
        async for chunk in mgr.chat_stream(thread_id, "test"):
            chunks.append(chunk)

    assert chunks, "должны быть SSE-строки"
    assert all(c.startswith("data: ") for c in chunks)
    last_chunk = chunks[-1]
    assert "done" in last_chunk or "full_text" in last_chunk or "hello" in last_chunk


@pytest.mark.asyncio
async def test_chat_stream_stream_events_yield_incremental_chunks(db):
    """StreamEvent content_block_delta/text_delta chunks are forwarded as SSE data."""
    thread_id = await db.create_agent_thread("stream-event thread")
    await db.save_agent_message(thread_id, "user", "test")

    from claude_agent_sdk import AssistantMessage, ResultMessage, StreamEvent, TextBlock

    # Simulate three incremental text chunks via StreamEvent
    def _stream_event(text: str) -> MagicMock:
        ev = MagicMock(spec=StreamEvent)
        ev.event = {"type": "content_block_delta", "delta": {"type": "text_delta", "text": text}}
        return ev

    assistant_msg = MagicMock(spec=AssistantMessage)
    assistant_msg.content = [TextBlock(text="приветмир")]  # full text — should be skipped
    result_msg = MagicMock(spec=ResultMessage)

    async def mock_query(prompt, options):
        yield _stream_event("привет")
        yield _stream_event(" ")
        yield _stream_event("мир")
        yield assistant_msg
        yield result_msg

    mgr = AgentManager(db)
    mgr.initialize()

    chunks = []
    with patch("src.agent.manager.query", mock_query):
        async for chunk in mgr.chat_stream(thread_id, "test"):
            chunks.append(chunk)

    payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks]

    # Incremental text chunks must be present (filter out status/tool events)
    text_payloads = [p for p in payloads if "text" in p and not p.get("done") and "type" not in p]
    assert len(text_payloads) == 3, f"ожидали 3 текстовых чанка, получили: {text_payloads}"
    assert text_payloads[0]["text"] == "привет"
    assert text_payloads[1]["text"] == " "
    assert text_payloads[2]["text"] == "мир"

    # AssistantMessage text must NOT be duplicated when StreamEvents were received
    all_text = "".join(p["text"] for p in text_payloads)
    assert all_text == "привет мир", f"суммарный текст не совпадает: {all_text!r}"

    # Done signal must be present with correct full_text
    done_payloads = [p for p in payloads if p.get("done")]
    assert done_payloads, "не найден done-payload"
    assert done_payloads[0]["full_text"] == "привет мир"


@pytest.mark.asyncio
async def test_chat_stream_fallback_to_assistant_message_when_no_stream_events(db):
    """When no StreamEvents arrive, AssistantMessage text is used as fallback."""
    thread_id = await db.create_agent_thread("fallback thread")
    await db.save_agent_message(thread_id, "user", "test")

    from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

    assistant_msg = MagicMock(spec=AssistantMessage)
    assistant_msg.content = [TextBlock(text="fallback text")]
    result_msg = MagicMock(spec=ResultMessage)

    async def mock_query(prompt, options):
        yield assistant_msg
        yield result_msg

    mgr = AgentManager(db)
    mgr.initialize()

    chunks = []
    with patch("src.agent.manager.query", mock_query):
        async for chunk in mgr.chat_stream(thread_id, "test"):
            chunks.append(chunk)

    payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks]
    text_payloads = [p for p in payloads if "text" in p and not p.get("done")]
    assert any(p["text"] == "fallback text" for p in text_payloads), (
        f"AssistantMessage текст не найден в чанках: {text_payloads}"
    )


@pytest.mark.asyncio
async def test_chat_stream_emits_tool_start_and_tool_end_events(db):
    """StreamEvent content_block_start (tool_use) and content_block_stop emit tool_start/tool_end SSE."""
    thread_id = await db.create_agent_thread("tool-visibility thread")
    await db.save_agent_message(thread_id, "user", "test")

    from claude_agent_sdk import AssistantMessage, ResultMessage, StreamEvent, TextBlock

    def _make_event(event_dict):
        ev = MagicMock(spec=StreamEvent)
        ev.event = event_dict
        return ev

    assistant_msg = MagicMock(spec=AssistantMessage)
    assistant_msg.content = [TextBlock(text="done")]
    result_msg = MagicMock(spec=ResultMessage)

    async def mock_query(prompt, options):
        # tool_use block start at index 0
        yield _make_event({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "tool_use", "id": "tu_1", "name": "search_messages"},
        })
        # partial input
        yield _make_event({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "input_json_delta", "partial_json": '{"query": "test"}'},
        })
        # tool block stop
        yield _make_event({"type": "content_block_stop", "index": 0})
        # text block
        yield _make_event({
            "type": "content_block_delta",
            "delta": {"type": "text_delta", "text": "result"},
        })
        yield assistant_msg
        yield result_msg

    mgr = AgentManager(db)
    mgr.initialize()

    chunks = []
    with patch("src.agent.manager.query", mock_query):
        async for chunk in mgr.chat_stream(thread_id, "test"):
            chunks.append(chunk)

    payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks]
    types = [p.get("type") for p in payloads if "type" in p]

    assert "thinking" in types, f"thinking event not found in {types}"
    assert "tool_start" in types, f"tool_start event not found in {types}"
    assert "tool_end" in types, f"tool_end event not found in {types}"

    tool_start = next(p for p in payloads if p.get("type") == "tool_start")
    assert tool_start["tool"] == "search_messages"

    tool_end = next(p for p in payloads if p.get("type") == "tool_end")
    assert tool_end["tool"] == "search_messages"
    assert "duration" in tool_end
    assert tool_end["is_error"] is False
    assert "query" in tool_end["summary"]


@pytest.mark.asyncio
async def test_chat_stream_emits_tool_start_end_from_assistant_message(db):
    """ToolUseBlock inside AssistantMessage emits tool_start and tool_end SSE events.

    The SDK delivers tool calls via AssistantMessage (not StreamEvent
    content_block_start), so the manager must emit these events manually.
    """
    thread_id = await db.create_agent_thread("tool-via-assistant-msg thread")
    await db.save_agent_message(thread_id, "user", "test")

    from claude_agent_sdk import AssistantMessage, ResultMessage, ToolUseBlock

    assistant_msg = MagicMock(spec=AssistantMessage)
    assistant_msg.content = [
        ToolUseBlock(id="tu_42", name="list_channels", input={"limit": 10}),
    ]
    result_msg = MagicMock(spec=ResultMessage)
    result_msg.usage = {}
    result_msg.model_usage = {}

    async def mock_query(prompt, options):
        yield assistant_msg
        yield result_msg

    mgr = AgentManager(db)
    mgr.initialize()

    chunks = []
    with patch("src.agent.manager.query", mock_query):
        async for chunk in mgr.chat_stream(thread_id, "test"):
            chunks.append(chunk)

    payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks]
    types = [p.get("type") for p in payloads]

    assert "tool_start" in types, f"tool_start missing in {types}"
    assert "tool_end" in types, f"tool_end missing in {types}"

    tool_start = next(p for p in payloads if p.get("type") == "tool_start")
    assert tool_start["tool"] == "list_channels"

    tool_end = next(p for p in payloads if p.get("type") == "tool_end")
    assert tool_end["tool"] == "list_channels"
    assert tool_end["is_error"] is False
    assert "limit" in tool_end["summary"]


@pytest.mark.asyncio
async def test_chat_stream_passes_prompt_as_async_iterable(db):
    """query() receives an AsyncIterable prompt, not a plain string.

    claude-agent-sdk blocks string prompts until result (wait_for_result_and_end_input).
    Using AsyncIterable makes SDK spawn stream_input in background, allowing
    events to flow immediately.
    """
    from collections.abc import AsyncIterable

    from claude_agent_sdk import ResultMessage

    thread_id = await db.create_agent_thread("prompt-type thread")
    await db.save_agent_message(thread_id, "user", "hello")

    result_msg = MagicMock(spec=ResultMessage)
    result_msg.usage = {}
    result_msg.model_usage = {}

    captured_prompt = None

    async def mock_query(prompt, options):
        nonlocal captured_prompt
        captured_prompt = prompt
        yield result_msg

    mgr = AgentManager(db)
    mgr.initialize()

    with patch("src.agent.manager.query", mock_query):
        async for _ in mgr.chat_stream(thread_id, "hello"):
            pass

    assert captured_prompt is not None, "query() was never called"
    assert isinstance(captured_prompt, AsyncIterable), (
        f"prompt should be AsyncIterable, got {type(captured_prompt).__name__}"
    )
    assert not isinstance(captured_prompt, str), "prompt must not be a plain string"


@pytest.mark.asyncio
async def test_chat_stream_options_have_can_use_tool(db):
    """ClaudeAgentOptions must include can_use_tool to auto-approve CLI permissions.

    Without it, CLI sends can_use_tool control requests for network tools
    (read_messages, etc.) and the SDK raises "canUseTool callback is not provided",
    causing tools to fail with "Tool permission stream closed".
    """
    from claude_agent_sdk import ResultMessage

    thread_id = await db.create_agent_thread("can-use-tool thread")
    await db.save_agent_message(thread_id, "user", "test")

    result_msg = MagicMock(spec=ResultMessage)
    result_msg.usage = {}
    result_msg.model_usage = {}

    captured_options = None

    async def mock_query(prompt, options):
        nonlocal captured_options
        captured_options = options
        yield result_msg

    mgr = AgentManager(db)
    mgr.initialize()

    with patch("src.agent.manager.query", mock_query):
        async for _ in mgr.chat_stream(thread_id, "test"):
            pass

    assert captured_options is not None, "query() was never called"
    assert captured_options.can_use_tool is not None, (
        "can_use_tool must be set to auto-approve CLI permission requests"
    )


@pytest.mark.asyncio
async def test_chat_stream_closes_sdk_generator(db):
    """aiter.aclose() must be called to kill the Claude CLI subprocess."""
    from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

    thread_id = await db.create_agent_thread("aclose thread")
    await db.save_agent_message(thread_id, "user", "test")

    assistant_msg = MagicMock(spec=AssistantMessage)
    assistant_msg.content = [TextBlock(text="ok")]
    result_msg = MagicMock(spec=ResultMessage)
    result_msg.usage = {}
    result_msg.model_usage = {}

    aclose_called = False

    async def mock_query(prompt, options):
        nonlocal aclose_called
        try:
            yield assistant_msg
            yield result_msg
        finally:
            aclose_called = True

    mgr = AgentManager(db)
    mgr.initialize()

    with patch("src.agent.manager.query", mock_query):
        async for _ in mgr.chat_stream(thread_id, "test"):
            pass

    assert aclose_called, "aiter.aclose() was never called — subprocess may leak"


@pytest.mark.asyncio
async def test_chat_stream_closes_sdk_generator_on_timeout(db):
    """aiter.aclose() must be called even when timeout fires."""
    thread_id = await db.create_agent_thread("aclose-timeout thread")
    await db.save_agent_message(thread_id, "user", "test")

    aclose_called = False

    async def mock_query(prompt, options):
        nonlocal aclose_called
        try:
            # Never yield anything — will trigger first_event_timeout
            await asyncio.sleep(9999)
            yield  # make it a generator
        finally:
            aclose_called = True

    mgr = AgentManager(db)
    mgr.initialize()
    mgr._config.agent.first_event_timeout = 1  # 1s for fast test

    with patch("src.agent.manager.query", mock_query):
        async for _ in mgr.chat_stream(thread_id, "test"):
            pass

    assert aclose_called, "aiter.aclose() not called on timeout — subprocess may leak"


@pytest.mark.asyncio
async def test_stderr_errors_surfaced_as_warnings(db):
    """Errors from claude-cli stderr are emitted as warning events to the user."""
    from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

    thread_id = await db.create_agent_thread("stderr-warning thread")
    await db.save_agent_message(thread_id, "user", "test")

    assistant_msg = MagicMock(spec=AssistantMessage)
    assistant_msg.content = [TextBlock(text="ok")]
    result_msg = MagicMock(spec=ResultMessage)
    result_msg.usage = {}
    result_msg.model_usage = {}

    async def mock_query(prompt, options):
        # Simulate stderr errors from claude-cli
        if options.stderr:
            options.stderr("2026-03-30T01:28:36.888Z [ERROR] Invalid URL")
            options.stderr("Error: connection refused")
            options.stderr("2026-03-30T01:28:37.000Z [WARN] rate limit approaching")
            options.stderr("2026-03-30T01:28:37.100Z [DEBUG] normal debug line")
        yield assistant_msg
        yield result_msg

    mgr = AgentManager(db)
    mgr.initialize()

    chunks = []
    with patch("src.agent.manager.query", mock_query):
        async for chunk in mgr.chat_stream(thread_id, "test"):
            chunks.append(chunk)

    payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks]
    warnings = [p for p in payloads if p.get("type") == "warning"]

    assert len(warnings) >= 2, f"Expected at least 2 warnings, got {warnings}"
    warning_texts = [w["text"] for w in warnings]
    # [ERROR] tagged line should surface
    assert any("Invalid URL" in t for t in warning_texts), f"Missing 'Invalid URL' in {warning_texts}"
    # Untagged "Error:" line should surface
    assert any("connection refused" in t for t in warning_texts), f"Missing 'connection refused' in {warning_texts}"


@pytest.mark.asyncio
async def test_stderr_debug_lines_not_surfaced(db):
    """DEBUG/TRACE stderr lines must NOT be shown to the user."""
    from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

    thread_id = await db.create_agent_thread("stderr-debug thread")
    await db.save_agent_message(thread_id, "user", "test")

    assistant_msg = MagicMock(spec=AssistantMessage)
    assistant_msg.content = [TextBlock(text="ok")]
    result_msg = MagicMock(spec=ResultMessage)
    result_msg.usage = {}
    result_msg.model_usage = {}

    async def mock_query(prompt, options):
        if options.stderr:
            options.stderr("2026-03-30T01:28:36.888Z [DEBUG] some debug info")
            options.stderr("2026-03-30T01:28:36.888Z [TRACE] trace data")
        yield assistant_msg
        yield result_msg

    mgr = AgentManager(db)
    mgr.initialize()

    chunks = []
    with patch("src.agent.manager.query", mock_query):
        async for chunk in mgr.chat_stream(thread_id, "test"):
            chunks.append(chunk)

    payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks]
    warnings = [p for p in payloads if p.get("type") == "warning"]
    assert len(warnings) == 0, f"DEBUG/TRACE should not produce warnings: {warnings}"


@pytest.mark.asyncio
async def test_stderr_stage_keywords_not_duplicated_as_warnings(db):
    """stderr lines matching stage keywords should emit status, not warning."""
    from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

    thread_id = await db.create_agent_thread("stderr-stage thread")
    await db.save_agent_message(thread_id, "user", "test")

    assistant_msg = MagicMock(spec=AssistantMessage)
    assistant_msg.content = [TextBlock(text="ok")]
    result_msg = MagicMock(spec=ResultMessage)
    result_msg.usage = {}
    result_msg.model_usage = {}

    async def mock_query(prompt, options):
        if options.stderr:
            # "rate limit event" matches _stage_map → should be status, not warning
            options.stderr("rate limit event [WARN] utilization 82%")
        yield assistant_msg
        yield result_msg

    mgr = AgentManager(db)
    mgr.initialize()

    chunks = []
    with patch("src.agent.manager.query", mock_query):
        async for chunk in mgr.chat_stream(thread_id, "test"):
            chunks.append(chunk)

    payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks]
    warnings = [p for p in payloads if p.get("type") == "warning"]
    statuses = [p for p in payloads if p.get("type") == "status"]

    assert len(warnings) == 0, f"Stage keywords should not produce warnings: {warnings}"
    assert any("Rate limit" in s.get("text", "") for s in statuses), (
        f"Stage keyword should produce status event: {statuses}"
    )


@pytest.mark.asyncio
async def test_stderr_api_request_counter_not_deduped(db):
    """Each [api:request] event gets a unique counter, not deduped."""
    from claude_agent_sdk import ResultMessage, TextBlock, AssistantMessage

    thread_id = await db.create_agent_thread("api-counter thread")
    await db.save_agent_message(thread_id, "user", "test")

    assistant_msg = MagicMock(spec=AssistantMessage)
    assistant_msg.content = [TextBlock(text="ok")]
    result_msg = MagicMock(spec=ResultMessage)
    result_msg.usage = {}
    result_msg.model_usage = {}

    async def mock_query(prompt, options):
        if options.stderr:
            options.stderr("2026-03-30T01:28:36.000Z [INFO] [api:request] POST /messages")
            options.stderr("2026-03-30T01:28:40.000Z [INFO] [api:request] POST /messages")
            options.stderr("2026-03-30T01:28:50.000Z [INFO] [api:request] POST /messages")
        yield assistant_msg
        yield result_msg

    mgr = AgentManager(db)
    mgr.initialize()

    chunks = []
    with patch("src.agent.manager.query", mock_query):
        async for chunk in mgr.chat_stream(thread_id, "test"):
            chunks.append(chunk)

    payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks]
    api_statuses = [
        p for p in payloads
        if p.get("type") == "status" and "API #" in p.get("text", "")
    ]

    assert len(api_statuses) == 3, f"Expected 3 API request statuses, got {api_statuses}"
    assert "API #1" in api_statuses[0]["text"]
    assert "API #2" in api_statuses[1]["text"]
    assert "API #3" in api_statuses[2]["text"]


@pytest.mark.asyncio
async def test_user_message_does_not_break_stream(db):
    """UserMessage (tool results sent to Claude) should not break the stream."""
    from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock, UserMessage

    thread_id = await db.create_agent_thread("user-message thread")
    await db.save_agent_message(thread_id, "user", "test")

    assistant_msg = MagicMock(spec=AssistantMessage)
    assistant_msg.content = [TextBlock(text="hello")]
    user_msg = MagicMock(spec=UserMessage)
    result_msg = MagicMock(spec=ResultMessage)
    result_msg.usage = {}
    result_msg.model_usage = {}

    async def mock_query(prompt, options):
        yield assistant_msg
        yield user_msg
        yield result_msg

    mgr = AgentManager(db)
    mgr.initialize()

    chunks = []
    with patch("src.agent.manager.query", mock_query):
        async for chunk in mgr.chat_stream(thread_id, "test"):
            chunks.append(chunk)

    payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks]
    texts = [p.get("text", "") for p in payloads if "text" in p and "type" not in p]
    assert any("hello" in t for t in texts), f"Text should flow through: {texts}"
    assert any(p.get("done") for p in payloads), "Stream should complete with done"


@pytest.mark.asyncio
async def test_chat_stream_emits_tool_result_from_assistant_message(db):
    """ToolResultBlock in AssistantMessage emits tool_result SSE event."""
    thread_id = await db.create_agent_thread("tool-result thread")
    await db.save_agent_message(thread_id, "user", "test")

    from claude_agent_sdk import AssistantMessage, ResultMessage, StreamEvent, TextBlock, ToolResultBlock, ToolUseBlock

    def _make_event(event_dict):
        ev = MagicMock(spec=StreamEvent)
        ev.event = event_dict
        return ev

    # AssistantMessage with ToolUseBlock + ToolResultBlock
    assistant_msg = MagicMock(spec=AssistantMessage)
    assistant_msg.content = [
        ToolUseBlock(id="tu_1", name="list_channels", input={}),
        ToolResultBlock(tool_use_id="tu_1", content="Found 5 channels", is_error=False),
        TextBlock(text="Here are the channels"),
    ]
    result_msg = MagicMock(spec=ResultMessage)

    async def mock_query(prompt, options):
        yield assistant_msg
        yield result_msg

    mgr = AgentManager(db)
    mgr.initialize()

    chunks = []
    with patch("src.agent.manager.query", mock_query):
        async for chunk in mgr.chat_stream(thread_id, "test"):
            chunks.append(chunk)

    payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks]
    tool_results = [p for p in payloads if p.get("type") == "tool_result"]

    assert len(tool_results) >= 1, f"tool_result event not found, payloads: {payloads}"
    assert tool_results[0]["tool"] == "list_channels"
    assert tool_results[0]["is_error"] is False
    assert "Found 5 channels" in tool_results[0]["summary"]


@pytest.mark.asyncio
async def test_chat_stream_retry_emits_status_event(db):
    """When Claude SDK retries after timeout, a status SSE event is emitted."""
    thread_id = await db.create_agent_thread("retry thread")
    await db.save_agent_message(thread_id, "user", "test")

    from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

    call_count = 0

    async def mock_query(prompt, options):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("Control request timeout")
        assistant_msg = MagicMock(spec=AssistantMessage)
        assistant_msg.content = [TextBlock(text="ok")]
        result_msg = MagicMock(spec=ResultMessage)
        yield assistant_msg
        yield result_msg

    mgr = AgentManager(db)
    mgr.initialize()

    chunks = []
    with patch("src.agent.manager.query", mock_query):
        async for chunk in mgr.chat_stream(thread_id, "test"):
            chunks.append(chunk)

    payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks]
    status_events = [p for p in payloads if p.get("type") == "status"]
    assert len(status_events) >= 2, f"expected init + retry status events, got: {status_events}"
    retry_events = [e for e in status_events if "Повтор" in e["text"]]
    assert retry_events, f"retry status event not found, status_events: {status_events}"


@pytest.mark.asyncio
async def test_chat_stream_text_delta_not_broken_by_tool_tracking(db):
    """Existing text streaming still works correctly with tool tracking in place."""
    thread_id = await db.create_agent_thread("text-delta thread")
    await db.save_agent_message(thread_id, "user", "test")

    from claude_agent_sdk import AssistantMessage, ResultMessage, StreamEvent, TextBlock

    def _stream_event(text):
        ev = MagicMock(spec=StreamEvent)
        ev.event = {"type": "content_block_delta", "delta": {"type": "text_delta", "text": text}}
        return ev

    assistant_msg = MagicMock(spec=AssistantMessage)
    assistant_msg.content = [TextBlock(text="привет мир")]
    result_msg = MagicMock(spec=ResultMessage)

    async def mock_query(prompt, options):
        yield _stream_event("привет")
        yield _stream_event(" мир")
        yield assistant_msg
        yield result_msg

    mgr = AgentManager(db)
    mgr.initialize()

    chunks = []
    with patch("src.agent.manager.query", mock_query):
        async for chunk in mgr.chat_stream(thread_id, "test"):
            chunks.append(chunk)

    payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks]
    text_payloads = [p for p in payloads if "text" in p and not p.get("done") and "type" not in p]
    assert len(text_payloads) == 2
    assert text_payloads[0]["text"] == "привет"
    assert text_payloads[1]["text"] == " мир"

    done_payloads = [p for p in payloads if p.get("done")]
    assert done_payloads[0]["full_text"] == "привет мир"


@pytest.mark.asyncio
async def test_chat_stream_multiple_tools_sequence(db):
    """Multiple tool calls in sequence emit correct tool_start/tool_end pairs."""
    thread_id = await db.create_agent_thread("multi-tool thread")
    await db.save_agent_message(thread_id, "user", "test")

    from claude_agent_sdk import AssistantMessage, ResultMessage, StreamEvent, TextBlock

    def _make_event(event_dict):
        ev = MagicMock(spec=StreamEvent)
        ev.event = event_dict
        return ev

    assistant_msg = MagicMock(spec=AssistantMessage)
    assistant_msg.content = [TextBlock(text="result")]
    result_msg = MagicMock(spec=ResultMessage)

    async def mock_query(prompt, options):
        # Tool 1
        yield _make_event({
            "type": "content_block_start", "index": 0,
            "content_block": {"type": "tool_use", "id": "tu_1", "name": "search_messages"},
        })
        yield _make_event({"type": "content_block_stop", "index": 0})
        # Tool 2
        yield _make_event({
            "type": "content_block_start", "index": 1,
            "content_block": {"type": "tool_use", "id": "tu_2", "name": "list_channels"},
        })
        yield _make_event({"type": "content_block_stop", "index": 1})
        # Text
        yield _make_event({
            "type": "content_block_delta",
            "delta": {"type": "text_delta", "text": "result"},
        })
        yield assistant_msg
        yield result_msg

    mgr = AgentManager(db)
    mgr.initialize()

    chunks = []
    with patch("src.agent.manager.query", mock_query):
        async for chunk in mgr.chat_stream(thread_id, "test"):
            chunks.append(chunk)

    payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks]
    tool_starts = [p for p in payloads if p.get("type") == "tool_start"]
    tool_ends = [p for p in payloads if p.get("type") == "tool_end"]

    assert len(tool_starts) == 2, f"expected 2 tool_start, got {tool_starts}"
    assert len(tool_ends) == 2, f"expected 2 tool_end, got {tool_ends}"
    assert tool_starts[0]["tool"] == "search_messages"
    assert tool_starts[1]["tool"] == "list_channels"
    assert tool_ends[0]["tool"] == "search_messages"
    assert tool_ends[1]["tool"] == "list_channels"


@pytest.mark.asyncio
async def test_chat_stream_emits_initial_connection_status(db):
    """First SSE event is always a status event with backend connection info."""
    thread_id = await db.create_agent_thread("init-status thread")
    await db.save_agent_message(thread_id, "user", "test")

    from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

    assistant_msg = MagicMock(spec=AssistantMessage)
    assistant_msg.content = [TextBlock(text="ok")]
    result_msg = MagicMock(spec=ResultMessage)

    async def mock_query(prompt, options):
        yield assistant_msg
        yield result_msg

    mgr = AgentManager(db)
    mgr.initialize()

    chunks = []
    with patch("src.agent.manager.query", mock_query):
        async for chunk in mgr.chat_stream(thread_id, "test"):
            chunks.append(chunk)

    assert chunks, "должны быть SSE-строки"
    first_payload = json.loads(chunks[0].removeprefix("data: ").strip())
    assert first_payload.get("type") == "status", f"first event should be status, got: {first_payload}"
    assert "Подключение" in first_payload["text"]


@pytest.mark.asyncio
async def test_chat_stream_emits_stderr_stage_status(db):
    """Stderr lines with stage keywords emit status events to the stream."""
    thread_id = await db.create_agent_thread("stderr-status")
    await db.save_agent_message(thread_id, "user", "test")

    from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

    assistant_msg = MagicMock(spec=AssistantMessage)
    assistant_msg.content = [TextBlock(text="ok")]
    result_msg = MagicMock(spec=ResultMessage)

    async def mock_query(prompt, options):
        # Simulate stderr callback as claude-cli would
        if options.stderr:
            options.stderr("2026-03-30T01:28:36.888Z [DEBUG] [API:request] Creating client")
            options.stderr("2026-03-30T01:28:37.000Z [DEBUG] Hooks: Found 0 hooks")
        yield assistant_msg
        yield result_msg

    mgr = AgentManager(db)
    mgr.initialize()

    chunks = []
    with patch("src.agent.manager.query", mock_query):
        async for chunk in mgr.chat_stream(thread_id, "test"):
            chunks.append(chunk)

    payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks if c.startswith("data:")]
    status_events = [p for p in payloads if p.get("type") == "status"]
    status_texts = [s["text"] for s in status_events]
    # Initial "Подключение" status
    assert any("Подключение" in t for t in status_texts), f"missing initial status: {status_texts}"
    # Stderr-derived stage status from [API:request] keyword
    assert any("API" in t for t in status_texts), f"missing stderr-derived status: {status_texts}"


@pytest.mark.asyncio
async def test_chat_stream_renders_saved_prompt_template_variables(db):
    await db.set_setting(
        AGENT_PROMPT_TEMPLATE_SETTING,
        "Дата: {date}\nКанал: {channel_title}\nТема: {topic}\nСообщения:\n{source_messages}",
    )
    thread_id = await db.create_agent_thread("test thread")
    context = format_context(
        [
            Message(
                channel_id=100,
                message_id=1,
                text="hello",
                topic_id=10,
                sender_name="User",
                date=_NOW,
            )
        ],
        "ForumChan",
        topic_id=10,
        topics_map={10: "Вопросы"},
    )
    await db.save_agent_message(thread_id, "user", context)
    await db.save_agent_message(thread_id, "user", "test")

    from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

    assistant_msg = MagicMock(spec=AssistantMessage)
    assistant_msg.content = [TextBlock(text="hello")]
    result_msg = MagicMock(spec=ResultMessage)
    expected_system_prompt = (
        f"Дата: {date.today().isoformat()}\n"
        "Канал: ForumChan\n"
        "Тема: Вопросы\n"
        'Сообщения:\n{"id": 1, "date": "2024-01-15", "author": "User", "text": "hello"}'
    )

    async def mock_query(_prompt, options):
        assert options.system_prompt == expected_system_prompt
        yield assistant_msg
        yield result_msg

    mgr = AgentManager(db)
    mgr.initialize()

    with patch("src.agent.manager.query", mock_query):
        chunks = [chunk async for chunk in mgr.chat_stream(thread_id, "test")]

    assert chunks


@pytest.mark.asyncio
async def test_runtime_status_prefers_claude_when_available(db, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "claude-key")
    monkeypatch.setenv("AGENT_FALLBACK_MODEL", "openai:gpt-4.1-mini")
    monkeypatch.setenv("AGENT_FALLBACK_API_KEY", "fallback-key")

    mgr = AgentManager(db)
    status = await mgr.get_runtime_status()

    assert status.selected_backend == "claude"
    assert status.claude_available is True
    assert status.deepagents_available is True


@pytest.mark.asyncio
async def test_runtime_status_prefers_db_backed_deepagents_over_claude(db, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "claude-key")
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)
    await service.save_provider_configs(
        [
            ProviderRuntimeConfig(
                provider="openai",
                enabled=True,
                priority=0,
                selected_model="gpt-4.1-mini",
                secret_fields={"api_key": "openai-key"},
            )
        ]
    )

    mgr = AgentManager(db, config)
    with patch.object(mgr._deepagents_backend, "_build_agent", return_value=None):
        mgr.initialize()
        status = await mgr.get_runtime_status()

    assert status.selected_backend == "deepagents"
    assert status.claude_available is True
    assert status.deepagents_available is True


@pytest.mark.asyncio
async def test_runtime_status_falls_back_to_deepagents_when_claude_missing(db, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    monkeypatch.setenv("AGENT_FALLBACK_MODEL", "openai:gpt-4.1-mini")
    monkeypatch.setenv("AGENT_FALLBACK_API_KEY", "fallback-key")

    mgr = AgentManager(db)
    with patch.object(mgr._deepagents_backend, "_build_agent", return_value=None):
        mgr.initialize()
    status = await mgr.get_runtime_status()

    assert status.selected_backend == "deepagents"
    assert status.error is None


@pytest.mark.asyncio
async def test_runtime_status_respects_dev_override(db, monkeypatch):
    monkeypatch.setenv("ANTHROPIC_API_KEY", "claude-key")
    monkeypatch.setenv("AGENT_FALLBACK_MODEL", "openai:gpt-4.1-mini")
    monkeypatch.setenv("AGENT_FALLBACK_API_KEY", "fallback-key")
    await db.set_setting("agent_dev_mode_enabled", "1")
    await db.set_setting("agent_backend_override", "deepagents")

    mgr = AgentManager(db)
    status = await mgr.get_runtime_status()

    assert status.selected_backend == "deepagents"
    assert status.using_override is True


def test_deepagents_tools_can_be_converted_to_structured_tools(db):
    mgr = AgentManager(db)

    tools = mgr._deepagents_backend._default_tools()
    search_tool = StructuredTool.from_function(next(t for t in tools if t.__name__ == "search_messages"))
    channels_tool = StructuredTool.from_function(next(t for t in tools if t.__name__ == "list_channels"))

    assert "search" in search_tool.description.lower()
    assert "channels" in channels_tool.description.lower()


@pytest.mark.asyncio
async def test_deepagents_search_tool_returns_friendly_error_inside_running_loop(db):
    mgr = AgentManager(db)

    result = mgr._deepagents_backend._search_messages_tool("test")

    assert "Ошибка" in result or "недоступен" in result or "cannot run" in result


def test_deepagents_get_channels_tool_returns_friendly_error_on_db_failure(db, monkeypatch):
    mgr = AgentManager(db)

    async def _broken_get_channels(*args, **kwargs):
        raise RuntimeError("db is unavailable")

    monkeypatch.setattr(db, "get_channels", _broken_get_channels)

    result = mgr._deepagents_backend._get_channels_tool()

    assert "Ошибка" in result or "недоступен" in result


def test_deepagents_backend_uses_bare_model_for_legacy_fallback(db, monkeypatch):
    config = AppConfig()
    config.agent.fallback_model = "anthropic:claude-sonnet-4-6"
    config.agent.fallback_api_key = "fallback-key"
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sdk-key")
    monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth-token")

    create_agent = MagicMock(return_value=MagicMock(run=MagicMock(return_value="ok")))

    def fake_init_chat_model(*, model, model_provider, **kwargs):
        assert model == "claude-sonnet-4-6"
        assert model_provider == "anthropic"
        assert kwargs["api_key"] == "fallback-key"
        return MagicMock()

    mgr = AgentManager(db, config)
    with (
        patch("deepagents.create_deep_agent", create_agent),
        patch("langchain.chat_models.init_chat_model", fake_init_chat_model),
    ):
        mgr._deepagents_backend.initialize()


def test_deepagents_backend_requires_explicit_key_for_anthropic_fallback(db):
    config = AppConfig()
    config.agent.fallback_model = "anthropic:claude-sonnet-4-6"

    mgr = AgentManager(db, config)

    with pytest.raises(RuntimeError, match="AGENT_FALLBACK_API_KEY"):
        mgr._deepagents_backend.initialize()

    assert mgr._deepagents_backend.available is False
    assert mgr._deepagents_backend.init_error is not None


@pytest.mark.asyncio
async def test_runtime_status_reports_failed_deepagents_initialization(db, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    monkeypatch.setenv("AGENT_FALLBACK_MODEL", "anthropic:claude-sonnet-4-6")

    mgr = AgentManager(db)
    mgr.initialize()

    status = await mgr.get_runtime_status()

    assert status.selected_backend is None
    assert status.deepagents_available is False
    assert status.error is not None


@pytest.mark.asyncio
async def test_runtime_status_treats_valid_legacy_fallback_as_available(db, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)

    config = AppConfig()
    config.agent.fallback_model = "openai:gpt-4.1-mini"
    config.agent.fallback_api_key = "fallback-key"

    mgr = AgentManager(db, config)
    with patch.object(
        mgr._deepagents_backend,
        "_build_agent",
        side_effect=RuntimeError("provider init failed"),
    ):
        status = await mgr.get_runtime_status()

    assert status.selected_backend == "deepagents"
    assert status.deepagents_available is True
    assert status.error is None


@pytest.mark.asyncio
async def test_runtime_status_treats_invalid_legacy_fallback_as_unavailable(db, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    config = AppConfig()
    config.agent.fallback_model = "llama3"

    mgr = AgentManager(db, config)
    status = await mgr.get_runtime_status()

    assert status.deepagents_available is False
    assert status.error is not None
    assert "provider:model" in status.error


@pytest.mark.asyncio
async def test_claude_backend_uses_model_from_config(db):
    thread_id = await db.create_agent_thread("test thread")
    await db.save_agent_message(thread_id, "user", "test")

    config = AppConfig()
    config.agent.model = "claude-opus-4-6"
    mgr = AgentManager(db, config)
    mgr.initialize()

    from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

    text_block = TextBlock(text="hello")
    assistant_msg = MagicMock(spec=AssistantMessage)
    assistant_msg.content = [text_block]
    result_msg = MagicMock(spec=ResultMessage)

    async def mock_query(prompt, options):
        assert options.model == "claude-opus-4-6"
        yield assistant_msg
        yield result_msg

    with patch("src.agent.manager.query", mock_query):
        chunks = [chunk async for chunk in mgr.chat_stream(thread_id, "test")]

    assert chunks


# ── DB round-trip tests ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_agent_thread_crud(db):
    tid = await db.create_agent_thread("My Thread")
    assert isinstance(tid, int)

    thread = await db.get_agent_thread(tid)
    assert thread is not None
    assert thread["title"] == "My Thread"

    await db.rename_agent_thread(tid, "Renamed")
    thread = await db.get_agent_thread(tid)
    assert thread["title"] == "Renamed"

    threads = await db.get_agent_threads()
    assert any(t["id"] == tid for t in threads)

    await db.delete_agent_thread(tid)
    assert await db.get_agent_thread(tid) is None


@pytest.mark.asyncio
async def test_agent_messages_cascade_delete(db):
    tid = await db.create_agent_thread("cascade test")
    await db.save_agent_message(tid, "user", "hello")
    await db.save_agent_message(tid, "assistant", "hi")

    msgs = await db.get_agent_messages(tid)
    assert len(msgs) == 2

    # Deleting the thread should cascade-delete messages
    await db.delete_agent_thread(tid)
    msgs = await db.get_agent_messages(tid)
    assert msgs == []


# ── build_prompt tests ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_build_prompt_empty_history(db):
    mgr = AgentManager(db)
    prompt, stats = mgr._build_prompt([], "hello")
    assert "<user>" in prompt
    assert "hello" in prompt
    assert stats["total_msgs"] == 0
    assert stats["kept_msgs"] == 0
    assert stats["prompt_chars"] == len(prompt)


@pytest.mark.asyncio
async def test_build_prompt_multi_turn(db):
    mgr = AgentManager(db)
    history = [
        {"role": "user", "content": "first question"},
        {"role": "assistant", "content": "first answer"},
    ]
    prompt, stats = mgr._build_prompt(history, "second question")
    assert "<user>" in prompt
    assert "<assistant>" in prompt
    assert "first question" in prompt
    assert "first answer" in prompt
    assert "second question" in prompt
    assert stats["total_msgs"] == 2
    assert stats["kept_msgs"] == 2


def test_build_prompt_truncates_over_budget(db):
    """History exceeding 100K token budget is truncated from the oldest."""
    mgr = AgentManager(db)
    # Each message ~10K chars → 40 messages = 400K chars > 400K budget
    big_content = "A" * 10_000
    history = []
    for i in range(40):
        role = "user" if i % 2 == 0 else "assistant"
        history.append({"role": role, "content": big_content})

    prompt, stats = mgr._build_prompt(history, "final question")
    assert stats["total_msgs"] == 40
    assert stats["kept_msgs"] < 40, "should have truncated some messages"
    assert stats["prompt_chars"] <= 100_000 * 4 + 1000  # budget + small overhead
    assert "final question" in prompt  # current message always included


# ── Edge-case tests for special characters ───────────────────────────────────

EDGE_CASES = [
    ("xml_tags", "Привет <user>тег</user> и <assistant>тег</assistant>"),
    ("quotes", "Он сказал \"привет\" и 'пока'"),
    ("backslashes", "C:\\Users\\test\\file.txt"),
    ("newlines", "строка 1\nстрока 2\n\nстрока 3"),
    ("json_in_text", '{"key": "value", "arr": [1,2,3]}'),
    ("code_block", "```python\nprint('hello')\n```"),
    ("unicode_emoji", "Привет 🎉 мир 🌍 тест ✅"),
    ("special_chars", "a & b < c > d \"e\" 'f'"),
    ("long_message", "x" * 50_000),
    ("empty_and_whitespace", "   \n\t\n   "),
    ("markdown_links", "[ссылка](https://example.com?a=1&b=2)"),
    ("curly_braces", "{{template}} ${variable} %(format)s"),
]


@pytest.mark.parametrize("name,text", EDGE_CASES, ids=[c[0] for c in EDGE_CASES])
def test_build_prompt_edge(db, name, text):
    mgr = AgentManager(db)
    # As current message
    prompt, stats = mgr._build_prompt([], text)
    assert "<user>" in prompt
    assert text in prompt

    # As history entry
    history = [
        {"role": "user", "content": text},
        {"role": "assistant", "content": "ответ"},
    ]
    prompt, stats = mgr._build_prompt(history, "follow-up")
    assert text in prompt
    assert "follow-up" in prompt


@pytest.mark.parametrize("name,text", EDGE_CASES, ids=[c[0] for c in EDGE_CASES])
@pytest.mark.asyncio
async def test_chat_stream_edge(db, name, text):
    """chat_stream with mocked query correctly handles special characters."""
    thread_id = await db.create_agent_thread(f"edge-{name}")
    await db.save_agent_message(thread_id, "user", text)

    from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

    text_block = TextBlock(text="ok")
    assistant_msg = MagicMock(spec=AssistantMessage)
    assistant_msg.content = [text_block]
    result_msg = MagicMock(spec=ResultMessage)

    async def mock_query(prompt, options):
        yield assistant_msg
        yield result_msg

    mgr = AgentManager(db)
    mgr.initialize()

    chunks = []
    with patch("src.agent.manager.query", mock_query):
        async for chunk in mgr.chat_stream(thread_id, text):
            chunks.append(chunk)

    assert chunks, f"edge case '{name}' produced no SSE output"
    # Verify valid JSON in every SSE line
    for chunk in chunks:
        raw = chunk.removeprefix("data: ").strip()
        payload = json.loads(raw)
        assert isinstance(payload, dict)


@pytest.mark.asyncio
async def test_chat_stream_edge_history_mix(db):
    """Multiple edge-case messages in a single conversation history."""
    thread_id = await db.create_agent_thread("multi-edge")
    for _, text in EDGE_CASES[:6]:
        await db.save_agent_message(thread_id, "user", text)
        await db.save_agent_message(thread_id, "assistant", f"re: {text[:50]}")
    # Final user message
    final_msg = 'финал: <user>{"a":1}</user> & "test"'
    await db.save_agent_message(thread_id, "user", final_msg)

    from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

    text_block = TextBlock(text="done")
    assistant_msg = MagicMock(spec=AssistantMessage)
    assistant_msg.content = [text_block]
    result_msg = MagicMock(spec=ResultMessage)

    async def mock_query(prompt, options):
        yield assistant_msg
        yield result_msg

    mgr = AgentManager(db)
    mgr.initialize()

    chunks = []
    with patch("src.agent.manager.query", mock_query):
        async for chunk in mgr.chat_stream(thread_id, final_msg):
            chunks.append(chunk)

    assert chunks
    last_raw = chunks[-1].removeprefix("data: ").strip()
    last_payload = json.loads(last_raw)
    assert last_payload.get("done") is True


# ── format_context tests ─────────────────────────────────────────────────────

_NOW = datetime(2024, 1, 15, 12, 0, 0)


def _msg(message_id: int, text: str, topic_id: int | None = None, sender: str = "User") -> Message:
    return Message(
        channel_id=100,
        message_id=message_id,
        text=text,
        topic_id=topic_id,
        sender_name=sender,
        date=_NOW,
    )


def test_format_context_no_topics():
    """Plain channel without topics → flat JSONL, no grouping headers."""
    msgs = [_msg(1, "hello"), _msg(2, "world")]
    result = format_context(msgs, "TestChan", topic_id=None, topics_map={})
    assert "[КОНТЕКСТ: TestChan, 2 сообщений]" in result
    assert "## Без темы" not in result
    assert "## Тема" not in result
    lines = [ln for ln in result.split("\n") if ln.startswith("{")]
    assert len(lines) == 2
    parsed = json.loads(lines[0])
    assert parsed["author"] == "User"
    assert parsed["date"] == "2024-01-15"


def test_format_context_grouped_by_topics():
    """Messages with different topic_ids → grouped with topic names."""
    msgs = [
        _msg(1, "q1", topic_id=10),
        _msg(2, "q2", topic_id=10),
        _msg(3, "hw1", topic_id=20),
        _msg(4, "general", topic_id=None),
    ]
    topics_map = {10: "Вопросы по Python", 20: "Домашние задания"}
    result = format_context(msgs, "ForumChan", topic_id=None, topics_map=topics_map)
    assert "## Без темы" in result
    assert "## Тема: Вопросы по Python" in result
    assert "## Тема: Домашние задания" in result
    # JSONL lines
    jsonl_lines = [ln for ln in result.split("\n") if ln.startswith("{")]
    assert len(jsonl_lines) == 4


def test_format_context_single_topic_flat():
    """When topic_id is selected → flat JSONL, topic name in header."""
    msgs = [_msg(1, "msg1", topic_id=10), _msg(2, "msg2", topic_id=10)]
    topics_map = {10: "Вопросы"}
    result = format_context(msgs, "Chan", topic_id=10, topics_map=topics_map)
    assert 'тема "Вопросы"' in result
    assert "## Тема" not in result  # no grouping headers
    jsonl_lines = [ln for ln in result.split("\n") if ln.startswith("{")]
    assert len(jsonl_lines) == 2


def test_format_context_general_topic_zero():
    """topic_id=0 (General) is handled correctly in both modes."""
    msgs = [
        _msg(1, "general msg", topic_id=0),
        _msg(2, "python q", topic_id=10),
        _msg(3, "no topic", topic_id=None),
    ]
    topics_map = {0: "General", 10: "Python"}

    # Grouped mode (topic_id=None): 0 should appear as "Тема: General", not "Без темы"
    result = format_context(msgs, "Forum", topic_id=None, topics_map=topics_map)
    assert "## Тема: General" in result
    assert "## Тема: Python" in result
    assert "## Без темы" in result
    jsonl_lines = [ln for ln in result.split("\n") if ln.startswith("{")]
    assert len(jsonl_lines) == 3

    # Single-topic mode (topic_id=0): flat JSONL with topic name in header
    result2 = format_context(msgs, "Forum", topic_id=0, topics_map=topics_map)
    assert 'тема "General"' in result2
    assert "## Тема" not in result2  # no grouping headers


def test_format_context_topic_id_not_in_map():
    """topic_id without a name in map → fallback to тема #id."""
    msgs = [_msg(1, "msg1", topic_id=99)]
    result = format_context(msgs, "Chan", topic_id=99, topics_map={})
    assert "тема #99" in result


def test_format_context_unknown_topic_in_grouping():
    """Unknown topic_id during grouping → shows тема #id."""
    msgs = [_msg(1, "msg1", topic_id=55)]
    result = format_context(msgs, "Chan", topic_id=None, topics_map={})
    assert "## Тема #55" in result


def test_validate_prompt_template_rejects_unknown_variable():
    with pytest.raises(PromptTemplateError, match="Недопустимая переменная"):
        validate_prompt_template("Канал: {unknown}")


# ── DeepagentsBackend property tests ───────────────────────────────────────────────


def test_deepagents_backend_available_with_legacy_fallback(db, monkeypatch):
    """DeepagentsBackend.available returns True with valid legacy fallback."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    monkeypatch.setenv("AGENT_FALLBACK_MODEL", "openai:gpt-4.1-mini")
    monkeypatch.setenv("AGENT_FALLBACK_API_KEY", "test-key")

    config = AppConfig()
    mgr = AgentManager(db, config)

    assert mgr._deepagents_backend.available is True


def test_deepagents_backend_fallback_model_from_cache(db, monkeypatch):
    """DeepagentsBackend.fallback_model returns model from last used."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")

    config = AppConfig()
    mgr = AgentManager(db, config)
    mgr._deepagents_backend._last_used_model = "gpt-4.1-turbo"
    mgr._deepagents_backend._last_used_provider = "openai"

    assert mgr._deepagents_backend.fallback_model == "gpt-4.1-turbo"
    assert mgr._deepagents_backend.fallback_provider == "openai"


def test_deepagents_backend_fallback_provider_priority(db, monkeypatch):
    """DeepagentsBackend.fallback_provider uses provider from model string."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setenv("AGENT_FALLBACK_MODEL", "groq:llama-3.1-8b")

    config = AppConfig()
    mgr = AgentManager(db, config)

    assert mgr._deepagents_backend.fallback_provider == "groq"


def test_deepagents_backend_configured_flag(db, monkeypatch):
    """DeepagentsBackend.configured returns True when fallback is set."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setenv("AGENT_FALLBACK_MODEL", "openai:gpt-4.1-mini")

    config = AppConfig()
    mgr = AgentManager(db, config)

    assert mgr._deepagents_backend.configured is True


# ── Runtime status extended tests ──────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_runtime_status_dev_mode_override_respects_backend_override(db, monkeypatch):
    """Runtime status respects dev mode backend override even when other backend is available."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "claude-key")
    monkeypatch.setenv("AGENT_FALLBACK_MODEL", "openai:gpt-4.1-mini")
    monkeypatch.setenv("AGENT_FALLBACK_API_KEY", "fallback-key")
    await db.set_setting("agent_dev_mode_enabled", "1")
    await db.set_setting("agent_backend_override", "claude")

    mgr = AgentManager(db)
    status = await mgr.get_runtime_status()

    assert status.selected_backend == "claude"
    assert status.using_override is True


@pytest.mark.asyncio
async def test_runtime_status_reports_error_when_override_backend_unavailable(db, monkeypatch):
    """Runtime status reports error when overridden backend is unavailable."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    monkeypatch.setenv("AGENT_FALLBACK_MODEL", "openai:gpt-4.1-mini")
    await db.set_setting("agent_dev_mode_enabled", "1")
    await db.set_setting("agent_backend_override", "claude")

    mgr = AgentManager(db)
    with patch.object(mgr._deepagents_backend, "_build_agent", return_value=None):
        mgr.initialize()
    status = await mgr.get_runtime_status()

    assert status.selected_backend == "claude"
    assert status.error is not None
    assert "claude-agent-sdk" in status.error


@pytest.mark.asyncio
async def test_runtime_status_fallback_info_includes_provider_details(db, monkeypatch):
    """Runtime status includes fallback provider/model info."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "claude-key")
    monkeypatch.setenv("AGENT_FALLBACK_MODEL", "groq:llama-3.1-8b")
    monkeypatch.setenv("AGENT_FALLBACK_API_KEY", "groq-key")

    mgr = AgentManager(db)
    status = await mgr.get_runtime_status()

    assert status.fallback_provider == "groq"
    assert "llama" in status.fallback_model.lower()


# ── DeepagentsBackend streaming tests ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_deepagents_backend_chat_stream_handles_exception(db, monkeypatch):
    """DeepagentsBackend.chat_stream handles and propagates exceptions."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    monkeypatch.setenv("AGENT_FALLBACK_MODEL", "openai:gpt-4.1-mini")
    monkeypatch.setenv("AGENT_FALLBACK_API_KEY", "test-key")

    config = AppConfig()
    mgr = AgentManager(db, config)

    thread_id = await db.create_agent_thread("test")
    await db.save_agent_message(thread_id, "user", "hello")

    def fake_init_chat_model(*, model, model_provider, **kwargs):
        raise RuntimeError("Provider init failed")

    queue: asyncio.Queue[str | None] = asyncio.Queue()

    with (
        patch("langchain.chat_models.init_chat_model", fake_init_chat_model),
        pytest.raises(RuntimeError, match="Provider init failed"),
    ):
        await mgr._deepagents_backend.chat_stream(
            thread_id=thread_id,
            prompt="test",
            system_prompt="system",
            stats={},
            model=None,
            queue=queue,
        )


# ── DeepagentsBackend tool tests ───────────────────────────────────────────────────


def test_deepagents_search_tool_handles_exception(db, monkeypatch):
    """_search_messages_tool returns friendly error on exception."""
    mgr = AgentManager(db)

    async def _broken_search(*args, **kwargs):
        raise RuntimeError("DB unavailable")

    monkeypatch.setattr(db, "search_messages", _broken_search)

    # Force running outside loop
    result = mgr._deepagents_backend._search_messages_tool("test")
    assert "Ошибка" in result or "недоступен" in result or "DB unavailable" in result


def test_deepagents_get_channels_tool_returns_empty_list_message(db, monkeypatch):
    """_get_channels_tool returns message when no active channels."""
    mgr = AgentManager(db)

    async def _empty_channels(*args, **kwargs):
        return []

    monkeypatch.setattr(db, "get_channels", _empty_channels)

    result = mgr._deepagents_backend._get_channels_tool()
    assert "не найдены" in result


# ── _build_prompt_stats_only tests ─────────────────────────────────────────────────


def test_build_prompt_stats_only_empty_history(db):
    """_build_prompt_stats_only returns correct stats for empty history."""
    mgr = AgentManager(db)
    stats = mgr._build_prompt_stats_only([], "hello")

    assert stats["total_msgs"] == 0
    assert stats["kept_msgs"] == 0
    assert stats["prompt_chars"] > 0


def test_build_prompt_stats_only_with_history(db):
    """_build_prompt_stats_only returns correct stats with history."""
    mgr = AgentManager(db)
    history = [
        {"role": "user", "content": "first question"},
        {"role": "assistant", "content": "first answer"},
    ]
    stats = mgr._build_prompt_stats_only(history, "new question")

    assert stats["total_msgs"] == 2
    assert stats["kept_msgs"] == 2
    assert "new question" not in stats  # stats only, no prompt built
    assert stats["prompt_chars"] > 0


# ── ImportError handling tests ─────────────────────────────────────────────────────


def test_build_agent_deepagents_import_error_shows_real_cause(db, monkeypatch):
    """When deepagents import fails (e.g. missing langchain_anthropic), show real error."""
    config = AppConfig()
    config.agent.fallback_model = "ollama:llama3"
    mgr = AgentManager(db, config)

    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "deepagents":
            raise ImportError("No module named 'langchain_anthropic'")
        return real_import(name, *args, **kwargs)

    cfg = ProviderRuntimeConfig(
        provider="ollama", enabled=True, priority=0, selected_model="llama3",
        plain_fields={"base_url": "http://localhost:11434"},
    )
    with patch("builtins.__import__", side_effect=fake_import):
        with pytest.raises(RuntimeError, match="Не удалось импортировать deepagents"):
            mgr._deepagents_backend._build_agent(cfg, record_last_used=False)

    assert "langchain_anthropic" in mgr._deepagents_backend._init_error


def test_build_agent_langchain_import_error_blames_correct_provider(db, monkeypatch):
    """When langchain provider import fails, error correctly names the provider package."""
    config = AppConfig()
    config.agent.fallback_model = "ollama:llama3"
    mgr = AgentManager(db, config)

    cfg = ProviderRuntimeConfig(
        provider="ollama", enabled=True, priority=0, selected_model="llama3",
        plain_fields={"base_url": "http://localhost:11434"},
    )

    def fake_init_chat_model(**kwargs):
        raise ImportError("No module named 'langchain_ollama'")

    with (
        patch("deepagents.create_deep_agent"),
        patch("langchain.chat_models.init_chat_model", side_effect=fake_init_chat_model),
    ):
        with pytest.raises(RuntimeError, match="langchain-ollama"):
            mgr._deepagents_backend._build_agent(cfg, record_last_used=False)

    assert "ollama" in mgr._deepagents_backend._init_error


def test_build_agent_tools_import_error_shows_details(db, monkeypatch):
    """ImportError from create_deep_agent/tools shows detailed message."""
    config = AppConfig()
    config.agent.fallback_model = "ollama:llama3"
    mgr = AgentManager(db, config)

    cfg = ProviderRuntimeConfig(
        provider="ollama", enabled=True, priority=0, selected_model="llama3",
        plain_fields={"base_url": "http://localhost:11434"},
    )

    def fake_create_deep_agent(**kwargs):
        raise ImportError("No module named 'some_optional_dep'")

    with (
        patch("deepagents.create_deep_agent", side_effect=fake_create_deep_agent),
        patch("langchain.chat_models.init_chat_model", return_value=MagicMock()),
    ):
        with pytest.raises(RuntimeError, match="Ошибка импорта при создании агента"):
            mgr._deepagents_backend._build_agent(cfg, record_last_used=False)

    assert "some_optional_dep" in mgr._deepagents_backend._init_error


# ── Refresh re-init tests ─────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_refresh_reinitializes_on_preflight_true(db, monkeypatch):
    """refresh_settings_cache(preflight=True) re-inits even when preflight_available is not None."""
    monkeypatch.setenv("AGENT_FALLBACK_MODEL", "openai:gpt-4.1-mini")
    monkeypatch.setenv("AGENT_FALLBACK_API_KEY", "test-key")

    mgr = AgentManager(db)
    backend = mgr._deepagents_backend

    # Simulate a previously failed preflight
    backend._preflight_available = False
    backend._init_error = "some old error"

    with patch.object(backend, "initialize") as mock_init:
        await mgr.refresh_settings_cache(preflight=True)
        mock_init.assert_called_once()


@pytest.mark.asyncio
async def test_runtime_status_selected_backend_deepagents_override(db, monkeypatch):
    """When override=deepagents, selected_backend is deepagents even if claude_available=True."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "claude-key")
    monkeypatch.setenv("AGENT_FALLBACK_MODEL", "openai:gpt-4.1-mini")
    monkeypatch.setenv("AGENT_FALLBACK_API_KEY", "test-key")
    await db.set_setting("agent_dev_mode_enabled", "1")
    await db.set_setting("agent_backend_override", "deepagents")

    mgr = AgentManager(db)
    with patch.object(mgr._deepagents_backend, "_build_agent", return_value=None):
        mgr.initialize()

    status = await mgr.get_runtime_status()

    assert status.claude_available is True
    assert status.selected_backend == "deepagents"
    assert status.using_override is True


# ---------------------------------------------------------------------------
# Atomic timeout tests
# ---------------------------------------------------------------------------


def test_agent_config_timeout_defaults():
    """AgentConfig has correct default timeout values."""
    config = AppConfig()
    assert config.agent.stream_close_timeout == 60
    assert config.agent.first_event_timeout == 120
    assert config.agent.idle_timeout == 90
    assert config.agent.permission_timeout == 120
    assert config.agent.total_timeout == 300


@pytest.mark.asyncio
async def test_await_with_countdown_normal():
    """_await_with_countdown returns result when coro completes within timeout."""
    from src.agent.manager import _await_with_countdown

    queue: asyncio.Queue = asyncio.Queue()

    async def fast_coro():
        return 42

    result = await _await_with_countdown(fast_coro(), timeout=5, queue=queue, label="test")
    assert result == 42


@pytest.mark.asyncio
async def test_await_with_countdown_timeout_fires():
    """_await_with_countdown raises TimeoutError when coro exceeds timeout."""
    from src.agent.manager import _await_with_countdown

    queue: asyncio.Queue = asyncio.Queue()

    async def slow_coro():
        await asyncio.sleep(100)

    with pytest.raises(asyncio.TimeoutError):
        await _await_with_countdown(slow_coro(), timeout=0.2, queue=queue, label="test", countdown_interval=1)


@pytest.mark.asyncio
async def test_await_with_countdown_emits_status():
    """_await_with_countdown pushes countdown status events to queue."""
    from src.agent.manager import _await_with_countdown

    queue: asyncio.Queue = asyncio.Queue()

    async def slow_coro():
        await asyncio.sleep(100)

    with pytest.raises(asyncio.TimeoutError):
        await _await_with_countdown(
            slow_coro(), timeout=3.0, queue=queue, label="Ожидание", countdown_interval=0.5,
        )

    items = []
    while not queue.empty():
        items.append(queue.get_nowait())

    countdown_items = [i for i in items if "countdown" in i and "до таймаута" in i]
    assert len(countdown_items) >= 1, f"ожидали хотя бы один countdown event, получили: {items}"


@pytest.mark.asyncio
async def test_first_event_timeout_fires(db):
    """When query() yields no events, first_event_timeout triggers with error."""
    thread_id = await db.create_agent_thread("timeout thread")
    await db.save_agent_message(thread_id, "user", "test")

    async def stalling_query(prompt, options):
        await asyncio.sleep(100)
        # Never yields — simulates stalled connection
        if False:
            yield  # noqa: F841 — make it an async generator

    config = AppConfig()
    config.agent.first_event_timeout = 1
    config.agent.idle_timeout = 1
    config.agent.total_timeout = 5

    mgr = AgentManager(db, config=config)
    mgr.initialize()

    chunks = []
    with patch("src.agent.manager.query", stalling_query):
        async for chunk in mgr.chat_stream(thread_id, "test"):
            chunks.append(chunk)

    payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks if c.startswith("data: ")]
    errors = [p for p in payloads if "error" in p]
    assert errors, f"ожидали ошибку таймаута, получили: {payloads}"
    assert "90" not in errors[0]["error"], "должен использовать конфиг first_event_timeout=1, не дефолт 90"


@pytest.mark.asyncio
async def test_idle_timeout_fires(db):
    """When query() stops yielding mid-stream, idle_timeout triggers."""
    thread_id = await db.create_agent_thread("idle-timeout thread")
    await db.save_agent_message(thread_id, "user", "test")

    from claude_agent_sdk import StreamEvent

    def _stream_event(text: str) -> MagicMock:
        ev = MagicMock(spec=StreamEvent)
        ev.event = {"type": "content_block_delta", "delta": {"type": "text_delta", "text": text}}
        return ev

    async def stalling_after_first(prompt, options):
        yield _stream_event("hello")
        await asyncio.sleep(100)  # stalls after first event

    config = AppConfig()
    config.agent.first_event_timeout = 5
    config.agent.idle_timeout = 1
    config.agent.total_timeout = 10

    mgr = AgentManager(db, config=config)
    mgr.initialize()

    chunks = []
    with patch("src.agent.manager.query", stalling_after_first):
        async for chunk in mgr.chat_stream(thread_id, "test"):
            chunks.append(chunk)

    payloads = [json.loads(c.removeprefix("data: ").strip()) for c in chunks if c.startswith("data: ")]
    errors = [p for p in payloads if "error" in p]
    assert errors, f"ожидали ошибку idle таймаута, получили: {payloads}"
    assert "замолчал" in errors[0]["error"]


@pytest.mark.asyncio
async def test_permission_timeout_from_config(db):
    """permission_timeout from AgentConfig is passed to AgentRequestContext."""
    from src.agent.permission_gate import AgentRequestContext

    config = AppConfig()
    config.agent.permission_timeout = 77

    ctx = AgentRequestContext(
        session_id="test",
        thread_id=1,
        queue=asyncio.Queue(),
        db_permissions={},
        permission_timeout=config.agent.permission_timeout,
    )
    assert ctx.permission_timeout == 77


@pytest.mark.asyncio
async def test_stream_close_timeout_from_config(db, monkeypatch):
    """stream_close_timeout from config is used for CLAUDE_CODE_STREAM_CLOSE_TIMEOUT env var."""
    monkeypatch.delenv("CLAUDE_CODE_STREAM_CLOSE_TIMEOUT", raising=False)
    config = AppConfig()
    config.agent.stream_close_timeout = 42
    config.agent.total_timeout = 30  # ensure stream_close_timeout > total_timeout

    from src.agent.manager import ClaudeSdkBackend

    backend = ClaudeSdkBackend(db, config)
    with patch("src.agent.tools.make_mcp_server", return_value=MagicMock()):
        backend.initialize()

    import os
    # effective = max(42, 30) = 42 → 42000ms
    assert os.environ.get("CLAUDE_CODE_STREAM_CLOSE_TIMEOUT") == "42000"
