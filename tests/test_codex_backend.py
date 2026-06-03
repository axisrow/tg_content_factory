"""Unit tests for CodexSdkBackend — streaming → SSE mapping, MCP wiring.

Uses a fake ``openai_codex`` module (no live Codex), mirroring the fake-SDK
pattern used by the codex image-adapter tests. Verifies the backend turns Codex
stream notifications into the same SSE shape the other backends emit and wires
the project MCP server into the Codex thread config.
"""

from __future__ import annotations

import asyncio
import json
import sys
from types import ModuleType, SimpleNamespace

import pytest

pytestmark = pytest.mark.anyio


def _install_fake_codex(monkeypatch, *, deltas, usage=None, tool=None):
    """Install a fake ``openai_codex`` whose turn streams *deltas* then *usage*.

    Notifications mirror the real SDK shape: each is ``Notification(method, payload)``
    where an agent-message delta carries ``payload.delta``, a token-usage update
    carries ``payload.token_usage.last`` (a breakdown), an MCP tool call surfaces
    as ``item/started`` + ``item/completed`` wrapping a ``McpToolCallThreadItem``
    (``payload.item.root`` with ``tool``/``server``/``duration_ms``), and the
    stream ends with a ``turn/completed`` notification.
    """
    captured: dict = {}

    def _note(method, payload):
        return SimpleNamespace(method=method, payload=payload)

    def _item_note(method, item):
        return _note(method, SimpleNamespace(item=SimpleNamespace(root=item)))

    class FakeHandle:
        async def stream(self):
            if tool is not None:
                tool_item = SimpleNamespace(
                    tool=tool, server="telegram_db", duration_ms=1500, error=None
                )
                yield _item_note("item/started", tool_item)
                yield _item_note("item/completed", tool_item)
            for d in deltas:
                yield _note("item/agentMessage/delta", SimpleNamespace(delta=d))
            if usage is not None:
                yield _note(
                    "thread/tokenUsage/updated",
                    SimpleNamespace(token_usage=SimpleNamespace(last=usage)),
                )
            yield _note("turn/completed", SimpleNamespace())

    class FakeThread:
        async def turn(self, text):
            captured["turn_input"] = text
            return FakeHandle()

    class FakeAsyncCodex:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

        async def thread_start(self, **kwargs):
            captured["start_kwargs"] = kwargs
            return FakeThread()

    fake = ModuleType("openai_codex")
    fake.AsyncCodex = FakeAsyncCodex
    fake.Sandbox = SimpleNamespace(workspace_write="workspace-write")
    monkeypatch.setitem(sys.modules, "openai_codex", fake)
    return captured


def _make_backend():
    from src.agent.codex_backend import CodexSdkBackend
    from src.config import AppConfig
    from src.database import Database

    return CodexSdkBackend(Database(":memory:"), AppConfig())


async def _drain(queue: asyncio.Queue) -> list[dict]:
    events = []
    while not queue.empty():
        item = queue.get_nowait()
        if item is None:
            continue
        assert item.startswith("data: ")
        events.append(json.loads(item[len("data: ") :]))
    return events


async def test_chat_stream_emits_text_then_done(monkeypatch):
    captured = _install_fake_codex(
        monkeypatch,
        deltas=["Hello ", "world"],
        usage=SimpleNamespace(input_tokens=10, output_tokens=5, total_tokens=15),
    )
    backend = _make_backend()
    queue: asyncio.Queue = asyncio.Queue()

    await backend.chat_stream(
        thread_id=1,
        prompt="hi",
        system_prompt="be nice",
        stats={},
        model="gpt-5.4",
        queue=queue,
    )

    events = await _drain(queue)
    texts = [e["text"] for e in events if "text" in e]
    assert texts == ["Hello ", "world"]
    done = next(e for e in events if e.get("done"))
    assert done["full_text"] == "Hello world"
    assert done["backend"] == "codex"
    assert done["model"] == "gpt-5.4"
    assert done["usage"] == {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15}
    # system prompt is threaded into Codex base_instructions
    assert captured["start_kwargs"]["base_instructions"] == "be nice"


async def test_chat_stream_wires_project_mcp_server(monkeypatch):
    captured = _install_fake_codex(monkeypatch, deltas=["ok"])
    backend = _make_backend()
    queue: asyncio.Queue = asyncio.Queue()

    await backend.chat_stream(
        thread_id=1,
        prompt="do a thing",
        system_prompt="",
        stats={},
        model=None,
        queue=queue,
    )

    cfg = captured["start_kwargs"]["config"]
    assert "telegram_db" in cfg["mcp_servers"]
    server = cfg["mcp_servers"]["telegram_db"]
    assert server["args"][:2] == ["-m", "src.main"]
    assert "mcp-server" in server["args"]


async def test_chat_stream_emits_tool_events(monkeypatch):
    """An MCP tool call surfaces as tool_start/tool_end SSE events (for the UI/CLI)."""
    _install_fake_codex(monkeypatch, deltas=["done"], tool="list_channels")
    backend = _make_backend()
    queue: asyncio.Queue = asyncio.Queue()

    await backend.chat_stream(
        thread_id=1,
        prompt="list channels",
        system_prompt="",
        stats={},
        model="gpt-5.4",
        queue=queue,
    )

    events = await _drain(queue)
    starts = [e for e in events if e.get("type") == "tool_start"]
    ends = [e for e in events if e.get("type") == "tool_end"]
    assert [e["tool"] for e in starts] == ["list_channels"]
    assert [e["tool"] for e in ends] == ["list_channels"]
    assert ends[0]["duration"] == 1.5
    assert ends[0]["is_error"] is False


async def test_chat_stream_prepends_history(monkeypatch):
    captured = _install_fake_codex(monkeypatch, deltas=["a"])
    backend = _make_backend()
    queue: asyncio.Queue = asyncio.Queue()

    await backend.chat_stream(
        thread_id=1,
        prompt="now",
        system_prompt="",
        stats={},
        model=None,
        queue=queue,
        history_msgs=[{"role": "user", "content": "before"}, {"role": "assistant", "content": "reply"}],
    )

    turn_input = captured["turn_input"]
    assert "before" in turn_input
    assert "reply" in turn_input
    # uses the shared _embed_history_in_prompt XML format; current msg is last
    assert turn_input.endswith("<user>\nnow\n</user>")
