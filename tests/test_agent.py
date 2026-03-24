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
async def test_agent_chat_stream_renders_saved_prompt_template_variables(db):
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

    assert "Search" in search_tool.description
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
    config.agent.fallback_model = "ollama:kimi-k2.5"
    mgr = AgentManager(db, config)

    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "deepagents":
            raise ImportError("No module named 'langchain_anthropic'")
        return real_import(name, *args, **kwargs)

    cfg = ProviderRuntimeConfig(
        provider="ollama", enabled=True, priority=0, selected_model="kimi-k2.5",
        plain_fields={"base_url": "http://localhost:11434"},
    )
    with patch("builtins.__import__", side_effect=fake_import):
        with pytest.raises(RuntimeError, match="Не удалось импортировать deepagents"):
            mgr._deepagents_backend._build_agent(cfg, record_last_used=False)

    assert "langchain_anthropic" in mgr._deepagents_backend._init_error


def test_build_agent_langchain_import_error_blames_correct_provider(db, monkeypatch):
    """When langchain provider import fails, error correctly names the provider package."""
    config = AppConfig()
    config.agent.fallback_model = "ollama:kimi-k2.5"
    mgr = AgentManager(db, config)

    cfg = ProviderRuntimeConfig(
        provider="ollama", enabled=True, priority=0, selected_model="kimi-k2.5",
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
    config.agent.fallback_model = "ollama:kimi-k2.5"
    mgr = AgentManager(db, config)

    cfg = ProviderRuntimeConfig(
        provider="ollama", enabled=True, priority=0, selected_model="kimi-k2.5",
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
async def test_refresh_reinitializes_after_config_change(db, monkeypatch):
    """refresh_settings_cache(preflight=True) re-inits when configs changed (preflight_available reset to None)."""
    monkeypatch.setenv("AGENT_FALLBACK_MODEL", "openai:gpt-4.1-mini")
    monkeypatch.setenv("AGENT_FALLBACK_API_KEY", "test-key")

    mgr = AgentManager(db)
    backend = mgr._deepagents_backend

    # Simulate config change resetting preflight (as DeepagentsBackend.refresh_settings_cache does)
    backend._preflight_available = None
    backend._init_error = None

    with patch.object(backend, "initialize") as mock_init:
        await mgr.refresh_settings_cache(preflight=True)
        mock_init.assert_called_once()


@pytest.mark.asyncio
async def test_refresh_skips_reinit_when_already_initialized(db, monkeypatch):
    """refresh_settings_cache(preflight=True) skips initialize() when preflight_available is already set."""
    monkeypatch.setenv("AGENT_FALLBACK_MODEL", "openai:gpt-4.1-mini")
    monkeypatch.setenv("AGENT_FALLBACK_API_KEY", "test-key")

    mgr = AgentManager(db)
    backend = mgr._deepagents_backend

    # Simulate a previously completed preflight (success or failure)
    backend._preflight_available = False
    backend._init_error = "some old error"

    with patch.object(backend, "initialize") as mock_init:
        await mgr.refresh_settings_cache(preflight=True)
        mock_init.assert_not_called()


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
