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


def _install_fake_codex(
    monkeypatch,
    *,
    deltas,
    usage=None,
    tool=None,
    tool_error=False,
    noise_item=False,
    hang=False,
    turn_status="completed",
    turn_error_message=None,
):
    """Install a fake ``openai_codex`` whose turn streams *deltas* then *usage*.

    Notifications mirror the real SDK shape (verified against ``openai_codex``
    0.1.0b2 ``notification_registry``): each is ``Notification(method, payload)``
    where an agent-message delta carries ``payload.delta``, a token-usage update
    carries ``payload.token_usage.last`` (a breakdown), an MCP tool call surfaces
    as ``item/started`` + ``item/completed`` wrapping a ``McpToolCallThreadItem``
    (``payload.item.root`` with ``tool``/``server``/``duration_ms``/``error``),
    and the stream ends with a ``turn/completed`` notification.

    - ``tool_error=True`` makes the tool-call carry an ``McpToolCallError``-shaped
      object (``.message``), matching the real ``error: McpToolCallError | None``
      field, so the ``is_error`` mapping is exercised on a non-None error.
    - ``noise_item=True`` also emits a non-MCP ``item/started`` (a reasoning item
      with no ``tool``/``server``), which the backend must ignore — the real SDK
      emits ``item/*`` for many item types, not just tool calls.
    - ``turn_status`` / ``turn_error_message`` set the ``turn/completed`` payload's
      ``turn.status`` and ``turn.error.message`` — ``turn_status="failed"`` with a
      message models a crashed turn (``Turn.status=failed`` + ``Turn.error``) the
      backend must surface as an error frame, not a "successful" empty response.
    """
    captured: dict = {}

    def _note(method, payload):
        return SimpleNamespace(method=method, payload=payload)

    def _item_note(method, item):
        return _note(method, SimpleNamespace(item=SimpleNamespace(root=item)))

    class FakeHandle:
        async def stream(self):
            if hang:
                # Never emit turn/completed — simulate a stalled Codex subprocess
                # so the backend's total_timeout wrapper must fire.
                await asyncio.Event().wait()
            if noise_item:
                # A reasoning item: real shape has no tool/server — must be ignored.
                reasoning = SimpleNamespace(text="thinking", type="reasoning")
                yield _item_note("item/started", reasoning)
                yield _item_note("item/completed", reasoning)
            if tool is not None:
                error = SimpleNamespace(message="boom") if tool_error else None
                tool_item = SimpleNamespace(
                    tool=tool, server="telegram_db", duration_ms=1500, error=error
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
            # turn/completed carries the Turn: status ("completed"/"failed"/...)
            # and error (TurnError-shaped, .message) — only populated on failure.
            turn_err = (
                SimpleNamespace(message=turn_error_message)
                if turn_error_message is not None
                else None
            )
            turn = SimpleNamespace(status=turn_status, error=turn_err)
            yield _note("turn/completed", SimpleNamespace(turn=turn))

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
    fake.Sandbox = SimpleNamespace(workspace_write="workspace-write", read_only="read-only")
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


async def test_chat_stream_uses_read_only_sandbox(monkeypatch):
    """Agent chat runs Codex read-only — tool work happens in the mcp-server subprocess."""
    captured = _install_fake_codex(monkeypatch, deltas=["ok"])
    backend = _make_backend()
    queue: asyncio.Queue = asyncio.Queue()

    await backend.chat_stream(
        thread_id=1, prompt="x", system_prompt="", stats={}, model=None, queue=queue
    )

    assert captured["start_kwargs"]["sandbox"] == "read-only"


async def test_chat_stream_times_out(monkeypatch):
    """A stalled Codex turn surfaces an error frame instead of hanging forever."""
    _install_fake_codex(monkeypatch, deltas=[], hang=True)
    backend = _make_backend()
    backend._config.agent.total_timeout = 0  # fire the deadline immediately
    queue: asyncio.Queue = asyncio.Queue()

    await backend.chat_stream(
        thread_id=1, prompt="x", system_prompt="", stats={}, model="gpt-5.4", queue=queue
    )

    events = await _drain(queue)
    assert any("error" in e for e in events), events
    # No done frame on the timeout path.
    assert not any(e.get("done") for e in events)


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


async def test_chat_stream_tool_error_sets_is_error(monkeypatch):
    """A failed MCP tool call (error is an McpToolCallError-shaped object) → is_error True.

    The real ``McpToolCallThreadItem.error`` is ``McpToolCallError | None`` (an
    object with ``.message``), not a bare string — so the backend keys ``is_error``
    off *presence*, not truthiness of a string. This guards the error path the
    happy-path tool test never exercises.
    """
    _install_fake_codex(monkeypatch, deltas=["x"], tool="list_channels", tool_error=True)
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
    ends = [e for e in events if e.get("type") == "tool_end"]
    assert ends and ends[0]["is_error"] is True


async def test_chat_stream_ignores_non_mcp_items(monkeypatch):
    """Non-MCP ``item/*`` notifications (e.g. reasoning) emit no tool events.

    The SDK fires ``item/started``/``item/completed`` for many item types; only
    ``McpToolCallThreadItem`` (has both ``tool`` and ``server``) is a tool call.
    A reasoning item must not leak a spurious tool_start/tool_end.
    """
    _install_fake_codex(monkeypatch, deltas=["hi"], noise_item=True)
    backend = _make_backend()
    queue: asyncio.Queue = asyncio.Queue()

    await backend.chat_stream(
        thread_id=1,
        prompt="think",
        system_prompt="",
        stats={},
        model="gpt-5.4",
        queue=queue,
    )

    events = await _drain(queue)
    assert not [e for e in events if e.get("type") in ("tool_start", "tool_end")]
    # text still flows and the turn completes normally
    assert [e["text"] for e in events if "text" in e] == ["hi"]


async def test_chat_stream_failed_turn_emits_error_not_empty_done(monkeypatch):
    """A failed turn (turn/completed status=failed) surfaces the error, not a fake success.

    Regression for #1252: the SDK delivers turn failures (auth expiry, model
    error, MCP-server crash) *inside* turn/completed as ``Turn.status=failed`` +
    ``Turn.error``. The backend used to ``break`` unconditionally and emit
    ``{'done': True, 'full_text': ''}`` — a "successful" empty answer with no
    error indication. It must instead emit an error frame and no done frame.
    """
    _install_fake_codex(
        monkeypatch,
        deltas=[],
        turn_status="failed",
        turn_error_message="authentication expired",
    )
    backend = _make_backend()
    queue: asyncio.Queue = asyncio.Queue()

    await backend.chat_stream(
        thread_id=1, prompt="x", system_prompt="", stats={}, model="gpt-5.4", queue=queue
    )

    events = await _drain(queue)
    errors = [e for e in events if "error" in e]
    assert errors, events
    assert "authentication expired" in errors[0]["error"]
    # A failed turn must NOT be reported as a successful (empty) done frame.
    assert not any(e.get("done") for e in events)


async def test_chat_stream_failed_turn_without_message_still_errors(monkeypatch):
    """A failed turn with no Turn.error still yields an error frame, not empty success.

    ``Turn.error`` is only *populated* on failure but is still ``| None``; a
    failed status with a missing message must fall back to a generic error rather
    than slip through as an empty done frame.
    """
    _install_fake_codex(
        monkeypatch, deltas=[], turn_status="failed", turn_error_message=None
    )
    backend = _make_backend()
    queue: asyncio.Queue = asyncio.Queue()

    await backend.chat_stream(
        thread_id=1, prompt="x", system_prompt="", stats={}, model="gpt-5.4", queue=queue
    )

    events = await _drain(queue)
    assert [e for e in events if "error" in e], events
    assert not any(e.get("done") for e in events)


async def test_chat_stream_non_failed_status_still_completes(monkeypatch):
    """A non-failed terminal status (e.g. completed) still emits a normal done frame.

    Guards the failure check from over-triggering: only ``status=failed`` is an
    error; ``completed`` (and other non-failure statuses) must still stream the
    done payload with the collected text.
    """
    _install_fake_codex(monkeypatch, deltas=["ok"], turn_status="completed")
    backend = _make_backend()
    queue: asyncio.Queue = asyncio.Queue()

    await backend.chat_stream(
        thread_id=1, prompt="x", system_prompt="", stats={}, model="gpt-5.4", queue=queue
    )

    events = await _drain(queue)
    assert not [e for e in events if "error" in e], events
    done = next(e for e in events if e.get("done"))
    assert done["full_text"] == "ok"


def test_notification_methods_match_sdk_registry():
    """Our consumed method strings are real keys in the SDK's notification registry.

    This is the guard that would have caught the original streaming bug: the
    backend used to parse an invented note shape. Here we assert every method
    string the stream loop branches on actually exists in
    ``openai_codex.generated.notification_registry.NOTIFICATION_MODELS`` — so a
    rename or typo fails loudly instead of silently dropping every event.

    Skipped when the optional ``openai_codex`` SDK is not installed (CI without
    the ``[codex]`` extra).
    """
    pytest.importorskip("openai_codex")
    from openai_codex.generated import notification_registry as nr

    from src.agent import codex_backend as cb

    registry = nr.NOTIFICATION_MODELS
    consumed = {
        cb.NOTE_AGENT_MESSAGE_DELTA,
        cb.NOTE_ITEM_STARTED,
        cb.NOTE_ITEM_COMPLETED,
        cb.NOTE_TOKEN_USAGE_UPDATED,
        cb.NOTE_TURN_COMPLETED,
    }
    missing = consumed - set(registry)
    assert not missing, f"method strings not in SDK registry: {sorted(missing)}"


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
