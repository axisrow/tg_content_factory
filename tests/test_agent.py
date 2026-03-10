from __future__ import annotations

import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from src.agent.context import format_context
from src.agent.manager import AgentManager
from src.agent.tools import make_mcp_server
from src.models import Message


@pytest.mark.asyncio
async def test_make_mcp_server_returns_server(db):
    server = make_mcp_server(db)
    assert server is not None


@pytest.mark.asyncio
async def test_agent_manager_initialize(db):
    mgr = AgentManager(db)
    mgr.initialize()
    assert mgr._server is not None


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
    ("quotes", 'Он сказал "привет" и \'пока\''),
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
