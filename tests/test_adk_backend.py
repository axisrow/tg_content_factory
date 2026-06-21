"""Unit tests for AdkSdkBackend — streaming → SSE mapping, MCP + auth wiring.

Uses a fake ``google.adk`` / ``google.genai`` / ``mcp`` module set (no live ADK),
mirroring the fake-SDK pattern used by ``test_codex_backend``. Verifies the
backend turns ADK ``run_async`` events into the same SSE shape the other backends
emit and wires the project MCP server (``python -m src.main mcp-server``) into the
ADK toolset.
"""

from __future__ import annotations

import asyncio
import json
import sys
from types import ModuleType, SimpleNamespace

import pytest

pytestmark = pytest.mark.anyio


def _install_fake_adk(monkeypatch, *, events, hang=False):
    """Install a fake ``google.adk`` whose ``run_async`` yields *events*.

    Each event mirrors the real ADK ``Event`` shape we consume:
    ``event.content.parts[i]`` carries ``text`` (a streamed delta), a
    ``function_call`` (``.name``), or a ``function_response`` (``.name``); and
    ``event.usage_metadata`` carries ``prompt_token_count`` /
    ``candidates_token_count`` / ``total_token_count``.

    Captures the constructed agent + run kwargs so wiring assertions can inspect
    the MCP toolset args and the user message text.
    """
    captured: dict = {}

    class FakeRunner:
        def __init__(self, *, agent, app_name):
            captured["agent"] = agent
            captured["app_name"] = app_name
            self.session_service = self

        async def create_session(self, *, app_name, user_id):
            captured["session_app_name"] = app_name
            captured["session_user_id"] = user_id
            return SimpleNamespace(id="sess-1")

        async def run_async(self, *, user_id, session_id, new_message, run_config):
            captured["run_user_id"] = user_id
            captured["run_session_id"] = session_id
            captured["new_message"] = new_message
            captured["run_config"] = run_config
            if hang:
                await asyncio.Event().wait()
            for event in events:
                yield event

        async def close(self):
            captured["closed"] = True

    class FakeAgent:
        def __init__(self, *, name, model, description, instruction, tools):
            captured["agent_name"] = name
            captured["model"] = model
            captured["instruction"] = instruction
            captured["tools"] = tools

    class FakeMcpToolset:
        def __init__(self, *, connection_params):
            captured["connection_params"] = connection_params

    class FakeStdioConnectionParams:
        def __init__(self, *, server_params, timeout):
            captured["server_params"] = server_params
            captured["timeout"] = timeout

    class FakeStdioServerParameters:
        def __init__(self, *, command, args):
            captured["mcp_command"] = command
            captured["mcp_args"] = args

    class FakeContent:
        def __init__(self, *, role, parts):
            captured["message_role"] = role
            captured["message_parts"] = parts

    class FakePart:
        def __init__(self, *, text):
            self.text = text

    class FakeStreamingMode:
        SSE = "SSE"

    class FakeRunConfig:
        def __init__(self, *, streaming_mode):
            captured["streaming_mode"] = streaming_mode

    def _mod(name: str) -> ModuleType:
        mod = ModuleType(name)
        monkeypatch.setitem(sys.modules, name, mod)
        return mod

    # google.adk.runners.InMemoryRunner
    runners = _mod("google.adk.runners")
    runners.InMemoryRunner = FakeRunner
    # google.adk.agents.Agent + google.adk.agents.run_config.{RunConfig,StreamingMode}
    agents = _mod("google.adk.agents")
    agents.Agent = FakeAgent
    run_config_mod = _mod("google.adk.agents.run_config")
    run_config_mod.RunConfig = FakeRunConfig
    run_config_mod.StreamingMode = FakeStreamingMode
    # google.adk.tools.mcp_tool.{McpToolset, mcp_session_manager.StdioConnectionParams}
    mcp_tool = _mod("google.adk.tools.mcp_tool")
    mcp_tool.McpToolset = FakeMcpToolset
    session_manager = _mod("google.adk.tools.mcp_tool.mcp_session_manager")
    session_manager.StdioConnectionParams = FakeStdioConnectionParams
    # mcp.StdioServerParameters
    mcp_mod = _mod("mcp")
    mcp_mod.StdioServerParameters = FakeStdioServerParameters
    # google.genai.types.{Content, Part}
    genai = _mod("google.genai")
    genai_types = _mod("google.genai.types")
    genai_types.Content = FakeContent
    genai_types.Part = FakePart
    genai.types = genai_types

    return captured


def _make_backend(client_pool=None):
    from src.agent.adk_backend import AdkSdkBackend
    from src.config import AppConfig
    from src.database import Database

    return AdkSdkBackend(Database(":memory:"), AppConfig(), client_pool=client_pool)


def _text_event(text):
    part = SimpleNamespace(text=text, function_call=None, function_response=None)
    return SimpleNamespace(content=SimpleNamespace(parts=[part]), usage_metadata=None)


def _tool_call_event(name):
    part = SimpleNamespace(
        text=None, function_call=SimpleNamespace(name=name), function_response=None
    )
    return SimpleNamespace(content=SimpleNamespace(parts=[part]), usage_metadata=None)


def _tool_response_event(name):
    part = SimpleNamespace(
        text=None, function_call=None, function_response=SimpleNamespace(name=name)
    )
    return SimpleNamespace(content=SimpleNamespace(parts=[part]), usage_metadata=None)


def _usage_event(prompt, candidates, total):
    usage = SimpleNamespace(
        prompt_token_count=prompt,
        candidates_token_count=candidates,
        total_token_count=total,
    )
    return SimpleNamespace(content=None, usage_metadata=usage)


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
    _install_fake_adk(
        monkeypatch,
        events=[
            _text_event("Hello "),
            _text_event("world"),
            _usage_event(10, 5, 15),
        ],
    )
    backend = _make_backend()
    queue: asyncio.Queue = asyncio.Queue()

    await backend.chat_stream(
        thread_id=1,
        prompt="hi",
        system_prompt="be nice",
        stats={},
        model="gemini-2.5-flash",
        queue=queue,
    )

    events = await _drain(queue)
    texts = [e["text"] for e in events if "text" in e]
    assert texts == ["Hello ", "world"]
    done = next(e for e in events if e.get("done"))
    assert done["full_text"] == "Hello world"
    assert done["backend"] == "adk"
    assert done["model"] == "gemini-2.5-flash"
    assert done["usage"] == {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15}


async def test_chat_stream_threads_system_prompt_as_instruction(monkeypatch):
    captured = _install_fake_adk(monkeypatch, events=[_text_event("ok")])
    backend = _make_backend()
    queue: asyncio.Queue = asyncio.Queue()

    await backend.chat_stream(
        thread_id=1,
        prompt="x",
        system_prompt="you are helpful",
        stats={},
        model=None,
        queue=queue,
    )

    assert captured["instruction"] == "you are helpful"
    # default model when none requested
    assert captured["model"] == "gemini-2.5-flash"
    assert captured["streaming_mode"] == "SSE"


async def test_chat_stream_wires_project_mcp_server(monkeypatch):
    captured = _install_fake_adk(monkeypatch, events=[_text_event("ok")])
    backend = _make_backend()
    queue: asyncio.Queue = asyncio.Queue()

    await backend.chat_stream(
        thread_id=1, prompt="do a thing", system_prompt="", stats={}, model=None, queue=queue
    )

    args = captured["mcp_args"]
    assert args[:2] == ["-m", "src.main"]
    assert "mcp-server" in args
    # No client pool → the subprocess must run with --no-pool.
    assert "--no-pool" in args
    # The agent is built with exactly one tool — the project MCP toolset.
    assert len(captured["tools"]) == 1
    # The toolset's stdio command is this interpreter (a real subprocess, not a stub).
    assert captured["mcp_command"] == sys.executable


async def test_chat_stream_with_pool_omits_no_pool(monkeypatch):
    captured = _install_fake_adk(monkeypatch, events=[_text_event("ok")])
    backend = _make_backend(client_pool=object())
    queue: asyncio.Queue = asyncio.Queue()

    await backend.chat_stream(
        thread_id=1, prompt="x", system_prompt="", stats={}, model=None, queue=queue
    )

    assert "--no-pool" not in captured["mcp_args"]


async def test_chat_stream_emits_tool_events(monkeypatch):
    """An MCP tool call/response surfaces as tool_start/tool_end SSE events."""
    _install_fake_adk(
        monkeypatch,
        events=[
            _tool_call_event("list_channels"),
            _tool_response_event("list_channels"),
            _text_event("done"),
        ],
    )
    backend = _make_backend()
    queue: asyncio.Queue = asyncio.Queue()

    await backend.chat_stream(
        thread_id=1,
        prompt="list channels",
        system_prompt="",
        stats={},
        model="gemini-2.5-flash",
        queue=queue,
    )

    events = await _drain(queue)
    starts = [e for e in events if e.get("type") == "tool_start"]
    ends = [e for e in events if e.get("type") == "tool_end"]
    assert [e["tool"] for e in starts] == ["list_channels"]
    assert [e["tool"] for e in ends] == ["list_channels"]
    assert ends[0]["is_error"] is False


async def test_chat_stream_times_out(monkeypatch):
    """A stalled ADK turn surfaces an error frame instead of hanging forever."""
    _install_fake_adk(monkeypatch, events=[], hang=True)
    backend = _make_backend()
    backend._config.agent.total_timeout = 0  # fire the deadline immediately
    queue: asyncio.Queue = asyncio.Queue()

    await backend.chat_stream(
        thread_id=1, prompt="x", system_prompt="", stats={}, model="gemini-2.5-flash", queue=queue
    )

    events = await _drain(queue)
    assert any("error" in e for e in events), events
    # No done frame on the timeout path.
    assert not any(e.get("done") for e in events)


async def test_chat_stream_closes_runner(monkeypatch):
    """The runner (and its MCP subprocess) is closed when the turn ends."""
    captured = _install_fake_adk(monkeypatch, events=[_text_event("ok")])
    backend = _make_backend()
    queue: asyncio.Queue = asyncio.Queue()

    await backend.chat_stream(
        thread_id=1, prompt="x", system_prompt="", stats={}, model=None, queue=queue
    )

    assert captured.get("closed") is True


async def test_chat_stream_prepends_history(monkeypatch):
    captured = _install_fake_adk(monkeypatch, events=[_text_event("a")])
    backend = _make_backend()
    queue: asyncio.Queue = asyncio.Queue()

    await backend.chat_stream(
        thread_id=1,
        prompt="now",
        system_prompt="",
        stats={},
        model=None,
        queue=queue,
        history_msgs=[
            {"role": "user", "content": "before"},
            {"role": "assistant", "content": "reply"},
        ],
    )

    message_text = captured["message_parts"][0].text
    assert "before" in message_text
    assert "reply" in message_text
    # uses the shared _embed_history_in_prompt XML format; current msg is last
    assert message_text.endswith("<user>\nnow\n</user>")


def test_available_requires_sdk_and_api_key(monkeypatch):
    import src.agent.adk_backend as ab

    backend = _make_backend()
    # No SDK installed → unavailable regardless of key.
    monkeypatch.setattr(ab, "_adk_sdk_installed", lambda: False)
    monkeypatch.setenv("GOOGLE_API_KEY", "k")
    assert backend.available is False

    # SDK installed but no key → unavailable.
    monkeypatch.setattr(ab, "_adk_sdk_installed", lambda: True)
    for var in ab.ADK_API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    assert backend.available is False

    # SDK installed + a key → available.
    monkeypatch.setenv("GEMINI_API_KEY", "k")
    assert backend.available is True
