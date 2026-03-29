"""Tests for the agent TUI (agent_tui.py)."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.config import AppConfig

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_sse_chunk(text: str) -> str:
    return f"data: {json.dumps({'text': text})}\n\n"


def _make_sse_done(full_text: str) -> str:
    return f"data: {json.dumps({'done': True, 'full_text': full_text})}\n\n"


def _make_sse_error(msg: str) -> str:
    return f"data: {json.dumps({'error': msg})}\n\n"


async def _fake_stream(*chunks: str):
    """Async generator yielding SSE chunks."""
    for chunk in chunks:
        yield chunk


def _make_manager(*chunks: str) -> MagicMock:
    """AgentManager mock whose chat_stream yields the given chunks."""
    mgr = MagicMock()
    mgr.chat_stream = MagicMock(return_value=_fake_stream(*chunks))
    return mgr


# ---------------------------------------------------------------------------
# StreamingMessage unit tests (no Textual pilot needed)
# ---------------------------------------------------------------------------


def test_streaming_message_append_accumulates():
    from unittest.mock import MagicMock

    from src.cli.commands.agent_tui import StreamingMessage

    widget = StreamingMessage()
    widget._content = ""
    widget.set_timer = MagicMock()  # avoid needing event loop
    widget.append_text("hello")
    widget.append_text(" world")
    assert widget._content == "hello world"


def test_streaming_message_finalize_changes_class():
    from src.cli.commands.agent_tui import StreamingMessage

    widget = StreamingMessage()
    widget._content = "done"
    widget._md = MagicMock()
    widget.finalize()
    assert "assistant-bubble" in widget.classes
    assert "streaming-bubble" not in widget.classes


def test_streaming_message_set_error_changes_class():
    from src.cli.commands.agent_tui import StreamingMessage

    widget = StreamingMessage()
    widget._md = MagicMock()
    widget.set_error("boom")
    assert "user-bubble" in widget.classes
    assert "streaming-bubble" not in widget.classes
    assert widget.border_title == "Ошибка"


# ---------------------------------------------------------------------------
# ThreadItem unit tests
# ---------------------------------------------------------------------------


def test_thread_item_active_class():
    from src.cli.commands.agent_tui import ThreadItem

    item = ThreadItem(thread_id=1, title="test", active=True)
    assert "active" in item.classes

    item2 = ThreadItem(thread_id=2, title="test2", active=False)
    assert "active" not in item2.classes


def test_thread_item_set_active():
    from src.cli.commands.agent_tui import ThreadItem

    item = ThreadItem(thread_id=1, title="test")
    item.set_active(True)
    assert "active" in item.classes
    item.set_active(False)
    assert "active" not in item.classes


# ---------------------------------------------------------------------------
# AgentTuiApp integration tests via Textual pilot
# ---------------------------------------------------------------------------


@pytest.fixture
def app_factory(db):
    """Returns a factory that builds AgentTuiApp with fresh mock manager."""

    def _factory(chunks=None):
        from src.cli.commands.agent_tui import AgentTuiApp

        if chunks is None:
            chunks = [_make_sse_chunk("hi"), _make_sse_done("hi")]
        config = AppConfig()
        mgr = _make_manager(*chunks)
        return AgentTuiApp(db=db, config=config, agent_manager=mgr)

    return _factory


@pytest.mark.asyncio
async def test_tui_mounts_and_shows_threads(db, app_factory):
    """App mounts, auto-creates a thread, sidebar renders it."""
    app = app_factory()
    async with app.run_test(size=(120, 40)) as pilot:
        await pilot.pause()
        from src.cli.commands.agent_tui import ThreadItem

        items = app.query(ThreadItem)
        assert len(items) >= 1


@pytest.mark.asyncio
async def test_tui_shows_existing_thread_messages(db, app_factory):
    """Existing messages are rendered when thread loads."""
    tid = await db.create_agent_thread("my thread")
    await db.save_agent_message(tid, "user", "hello from user")
    await db.save_agent_message(tid, "assistant", "hello from agent")

    app = app_factory()
    async with app.run_test(size=(120, 40)) as pilot:
        await pilot.pause()
        from src.cli.commands.agent_tui import MessageBubble

        bubbles = app.query(MessageBubble)
        roles = [b._role for b in bubbles]
        assert "user" in roles
        assert "assistant" in roles


@pytest.mark.asyncio
async def test_tui_send_message_calls_chat_stream(db, app_factory):
    """Sending a message triggers chat_stream on the AgentManager."""
    config = AppConfig()
    chunks = [_make_sse_chunk("response"), _make_sse_done("response")]
    mgr = _make_manager(*chunks)

    from src.cli.commands.agent_tui import AgentTuiApp

    app = AgentTuiApp(db=db, config=config, agent_manager=mgr)
    async with app.run_test(size=(120, 40)) as pilot:
        await pilot.pause()
        from textual.widgets import TextArea

        input_area = app.query_one("#input", TextArea)
        input_area.load_text("test message")
        await pilot.click("#send-btn")
        await pilot.pause(0.2)

    mgr.chat_stream.assert_called_once()
    call_args = mgr.chat_stream.call_args
    assert call_args[0][1] == "test message" or call_args[1].get("message") == "test message"
    # thread_id and message are positional args
    assert "test message" in str(call_args)


@pytest.mark.asyncio
async def test_tui_send_message_saves_to_db(db, app_factory):
    """User message and assistant response are saved to DB."""
    config = AppConfig()
    chunks = [_make_sse_chunk("agent reply"), _make_sse_done("agent reply")]
    mgr = _make_manager(*chunks)

    from src.cli.commands.agent_tui import AgentTuiApp

    app = AgentTuiApp(db=db, config=config, agent_manager=mgr)
    async with app.run_test(size=(120, 40)) as pilot:
        await pilot.pause()
        thread_id = app.current_thread_id
        from textual.widgets import TextArea

        input_area = app.query_one("#input", TextArea)
        input_area.load_text("my question")
        await pilot.click("#send-btn")
        await pilot.pause(0.3)

    msgs = await db.get_agent_messages(thread_id)
    roles = [m["role"] for m in msgs]
    assert "user" in roles
    contents = [m["content"] for m in msgs]
    assert "my question" in contents


@pytest.mark.asyncio
async def test_tui_new_thread_action(db, app_factory):
    """Ctrl+N creates a new thread."""
    app = app_factory()
    async with app.run_test(size=(120, 40)) as pilot:
        await pilot.pause()
        threads_before = await db.get_agent_threads()
        await pilot.press("ctrl+n")
        await pilot.pause(0.1)
        threads_after = await db.get_agent_threads()
        assert len(threads_after) == len(threads_before) + 1


@pytest.mark.asyncio
async def test_tui_delete_thread_action(db, app_factory):
    """Ctrl+D deletes the active thread and auto-creates a new one if needed."""
    app = app_factory()
    async with app.run_test(size=(120, 40)) as pilot:
        await pilot.pause()
        threads_before = await db.get_agent_threads()
        assert len(threads_before) >= 1
        active_id = app.current_thread_id
        await pilot.press("ctrl+d")
        await pilot.pause(0.1)
        threads_after = await db.get_agent_threads()
        remaining_ids = [t["id"] for t in threads_after]
        assert active_id not in remaining_ids


@pytest.mark.asyncio
async def test_tui_toggle_sidebar(db, app_factory):
    """Ctrl+T toggles sidebar visibility."""
    app = app_factory()
    async with app.run_test(size=(120, 40)) as pilot:
        await pilot.pause()
        from src.cli.commands.agent_tui import ThreadSidebar

        sidebar = app.query_one(ThreadSidebar)
        assert sidebar.display is True
        await pilot.press("ctrl+t")
        await pilot.pause(0.1)
        assert sidebar.display is False
        await pilot.press("ctrl+t")
        await pilot.pause(0.1)
        assert sidebar.display is True


@pytest.mark.asyncio
async def test_tui_thread_auto_rename(db, app_factory):
    """First message auto-renames 'Новый тред'."""
    config = AppConfig()
    chunks = [_make_sse_chunk("ok"), _make_sse_done("ok")]
    mgr = _make_manager(*chunks)

    from src.cli.commands.agent_tui import AgentTuiApp

    app = AgentTuiApp(db=db, config=config, agent_manager=mgr)
    async with app.run_test(size=(120, 40)) as pilot:
        await pilot.pause()
        thread_id = app.current_thread_id
        from textual.widgets import TextArea

        input_area = app.query_one("#input", TextArea)
        input_area.load_text("Hello world!")
        await pilot.click("#send-btn")
        await pilot.pause(0.2)

    thread = await db.get_agent_thread(thread_id)
    assert thread["title"] == "Hello world!"


@pytest.mark.asyncio
async def test_tui_error_chunk_cleans_up(db, app_factory):
    """SSE error chunk triggers cleanup via delete_last_agent_exchange."""
    config = AppConfig()
    chunks = [_make_sse_error("something went wrong")]
    mgr = _make_manager(*chunks)
    db.delete_last_agent_exchange = AsyncMock()

    from src.cli.commands.agent_tui import AgentTuiApp

    app = AgentTuiApp(db=db, config=config, agent_manager=mgr)
    async with app.run_test(size=(120, 40)) as pilot:
        await pilot.pause()
        from textual.widgets import TextArea

        input_area = app.query_one("#input", TextArea)
        input_area.load_text("trigger error")
        await pilot.click("#send-btn")
        await pilot.pause(0.2)

    db.delete_last_agent_exchange.assert_called_once()


# ---------------------------------------------------------------------------
# ALLOW_SELECT / copy tests
# ---------------------------------------------------------------------------


def test_allow_select_enabled():
    """ALLOW_SELECT must be True so Textual's built-in text selection works."""
    from src.cli.commands.agent_tui import AgentTuiApp

    assert getattr(AgentTuiApp, "ALLOW_SELECT", False) is True


def test_ctrl_c_not_bound():
    """ctrl+c must not be captured by app bindings — it's reserved for copy."""
    from src.cli.commands.agent_tui import AgentTuiApp

    bound_keys = {b.key for b in AgentTuiApp.BINDINGS}
    assert "ctrl+c" not in bound_keys


@pytest.mark.asyncio
async def test_copy_to_clipboard_called(db, app_factory, monkeypatch):
    """action_copy_text() delegates to copy_to_clipboard()."""
    app = app_factory()
    captured = []
    monkeypatch.setattr(app, "copy_to_clipboard", lambda text: captured.append(text))
    async with app.run_test(size=(120, 40)) as pilot:
        await pilot.pause()
        # Manually set selected text on the screen and call the action
        screen = app.screen
        monkeypatch.setattr(screen, "get_selected_text", lambda: "copied text")
        screen.action_copy_text()
    assert captured == ["copied text"]


def test_copy_to_clipboard_uses_native_tool(monkeypatch):
    """copy_to_clipboard calls pbcopy on macOS / xclip on Linux, not OSC 52."""
    from src.cli.commands.agent_tui import AgentTuiApp

    app = AgentTuiApp.__new__(AgentTuiApp)
    app._clipboard = ""

    calls = []

    def fake_run(cmd, **kwargs):
        calls.append((cmd, kwargs.get("input")))
        return MagicMock(returncode=0)

    monkeypatch.setattr("subprocess.run", fake_run)
    monkeypatch.setattr("sys.platform", "darwin")

    app.copy_to_clipboard("hello")
    assert calls == [(["pbcopy"], b"hello")]
    assert app._clipboard == "hello"


def test_clipboard_reads_from_native_tool(monkeypatch):
    """clipboard property reads from pbpaste/xclip instead of internal buffer."""
    from src.cli.commands.agent_tui import AgentTuiApp

    app = AgentTuiApp.__new__(AgentTuiApp)
    app._clipboard = "internal"

    monkeypatch.setattr("sys.platform", "darwin")
    monkeypatch.setattr(
        "subprocess.run",
        lambda cmd, **kw: MagicMock(stdout="from-system", returncode=0),
    )

    assert app.clipboard == "from-system"


# ---------------------------------------------------------------------------
# CLI one-shot mode (prompt=) tests
# ---------------------------------------------------------------------------


class TestAgentChatOneShotMode:
    """Tests for `agent chat -p <message>` one-shot mode."""

    def _run_chat(self, cli_env, prompt: str, thread_id=None, monkeypatch=None, capsys=None):
        """Helper: run agent chat in one-shot mode with mocked AgentManager."""
        from unittest.mock import MagicMock
        from unittest.mock import patch as _patch

        from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

        from src.cli.commands.agent import run
        from tests.helpers import cli_ns as _ns

        if monkeypatch:
            monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")

        text_block = TextBlock(text="one-shot reply")
        assistant_msg = MagicMock(spec=AssistantMessage)
        assistant_msg.content = [text_block]
        result_msg = MagicMock(spec=ResultMessage)

        async def mock_query(prompt, options):
            yield assistant_msg
            yield result_msg

        async def fake_init_db(_path):
            return AppConfig(), cli_env

        with (
            _patch("src.cli.runtime.init_db", side_effect=fake_init_db),
            _patch("src.agent.manager.query", mock_query),
        ):
            run(_ns(agent_action="chat", prompt=prompt, thread_id=thread_id, model=None))

    def test_one_shot_prints_response(self, cli_env, capsys, monkeypatch):
        self._run_chat(cli_env, "hello", monkeypatch=monkeypatch)
        assert "one-shot reply" in capsys.readouterr().out

    def test_one_shot_creates_thread_if_none(self, cli_env, monkeypatch):
        import sqlite3

        self._run_chat(cli_env, "hi", monkeypatch=monkeypatch)
        conn = sqlite3.connect(cli_env._db_path)
        try:
            count = conn.execute("SELECT COUNT(*) FROM agent_threads").fetchone()[0]
        finally:
            conn.close()
        assert count >= 1

    def test_one_shot_uses_existing_thread(self, cli_env, monkeypatch):
        import sqlite3

        tid = asyncio.run(cli_env.create_agent_thread("existing"))
        self._run_chat(cli_env, "hi", thread_id=tid, monkeypatch=monkeypatch)
        # CLI closes the aiosqlite connection, use raw sqlite3 to read state
        conn = sqlite3.connect(cli_env._db_path)
        conn.row_factory = sqlite3.Row
        try:
            rows = [dict(r) for r in conn.execute(
                "SELECT role, content FROM agent_messages WHERE thread_id = ?", (tid,)
            ).fetchall()]
        finally:
            conn.close()
        assert any(r["role"] == "user" and r["content"] == "hi" for r in rows)


# ---------------------------------------------------------------------------
# CLI interactive mode (no prompt) launches TUI
# ---------------------------------------------------------------------------


class TestAgentChatInteractiveMode:
    """Tests that `agent chat` without --prompt launches TUI."""

    def test_no_prompt_launches_tui(self, cli_env, monkeypatch):
        """When prompt is None, AgentTuiApp.run_async() is called."""
        from unittest.mock import AsyncMock
        from unittest.mock import patch as _patch

        from src.cli.commands.agent import run
        from tests.helpers import cli_ns as _ns

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")

        async def fake_init_db(_path):
            return AppConfig(), cli_env

        mock_run_async = AsyncMock()

        with (
            _patch("src.cli.runtime.init_db", side_effect=fake_init_db),
            _patch("src.cli.commands.agent_tui.AgentTuiApp.run_async", mock_run_async),
        ):
            run(_ns(agent_action="chat", prompt=None, thread_id=None, model=None))

        mock_run_async.assert_called_once()


# ---------------------------------------------------------------------------
# Parser tests
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Send/Stop button toggle tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_button_toggles_to_stop_during_streaming(db, app_factory):
    """Button must show '■ Стоп (esc)' with error variant while streaming."""
    gate = asyncio.Event()

    async def _slow_stream(*_a, **_kw):
        yield _make_sse_chunk("hello")
        await gate.wait()  # block until released
        yield _make_sse_done("hello")

    config = AppConfig()
    mgr = MagicMock()
    mgr.chat_stream = MagicMock(side_effect=_slow_stream)

    from src.cli.commands.agent_tui import AgentTuiApp

    app = AgentTuiApp(db=db, config=config, agent_manager=mgr)
    async with app.run_test(size=(120, 40)) as pilot:
        await pilot.pause()
        from textual.widgets import Button, TextArea

        input_area = app.query_one("#input", TextArea)
        input_area.load_text("test")
        await pilot.click("#send-btn")
        await pilot.pause(0.1)

        # Button should be in streaming state
        btn = app.query_one("#send-btn", Button)
        assert "Стоп" in str(btn.label)
        assert btn.variant == "error"

        gate.set()  # release the stream
        await pilot.pause(0.3)

        # Button should revert after streaming completes
        assert str(btn.label) == "Отправить"
        assert btn.variant == "success"


@pytest.mark.asyncio
async def test_button_reverts_after_cancel(db, app_factory):
    """Pressing ESC during streaming cancels and reverts button to Отправить."""
    gate = asyncio.Event()

    async def _slow_stream(*_a, **_kw):
        yield _make_sse_chunk("partial")
        await gate.wait()
        yield _make_sse_done("partial")

    config = AppConfig()
    mgr = MagicMock()
    mgr.chat_stream = MagicMock(side_effect=_slow_stream)

    from src.cli.commands.agent_tui import AgentTuiApp

    app = AgentTuiApp(db=db, config=config, agent_manager=mgr)
    db.delete_last_agent_exchange = AsyncMock()
    async with app.run_test(size=(120, 40)) as pilot:
        await pilot.pause()
        from textual.widgets import Button, TextArea

        input_area = app.query_one("#input", TextArea)
        input_area.load_text("test cancel")
        await pilot.click("#send-btn")
        await pilot.pause(0.1)

        btn = app.query_one("#send-btn", Button)
        assert "Стоп" in str(btn.label)

        await pilot.press("escape")
        await pilot.pause(0.3)

        assert str(btn.label) == "Отправить"
        assert btn.variant == "success"

    gate.set()  # cleanup


@pytest.mark.asyncio
async def test_stop_button_click_cancels_streaming(db, app_factory):
    """Clicking the stop button during streaming cancels the stream."""
    gate = asyncio.Event()

    async def _slow_stream(*_a, **_kw):
        yield _make_sse_chunk("partial")
        await gate.wait()
        yield _make_sse_done("partial")

    config = AppConfig()
    mgr = MagicMock()
    mgr.chat_stream = MagicMock(side_effect=_slow_stream)

    from src.cli.commands.agent_tui import AgentTuiApp

    app = AgentTuiApp(db=db, config=config, agent_manager=mgr)
    db.delete_last_agent_exchange = AsyncMock()
    async with app.run_test(size=(120, 40)) as pilot:
        await pilot.pause()
        from textual.widgets import Button, TextArea

        input_area = app.query_one("#input", TextArea)
        input_area.load_text("click stop")
        await pilot.click("#send-btn")
        await pilot.pause(0.1)

        btn = app.query_one("#send-btn", Button)
        assert "Стоп" in str(btn.label)

        # Click the stop button
        await pilot.click("#send-btn")
        await pilot.pause(0.3)

        # Should revert to send state
        assert str(btn.label) == "Отправить"
        assert btn.variant == "success"

    gate.set()


@pytest.mark.asyncio
async def test_no_race_condition_button_set_before_worker_runs(db, app_factory):
    """Verify _set_button_streaming(True) executes before worker starts.

    Reviewer claim: run_worker() could let the worker's finally block run
    before _set_button_streaming(True). This test verifies the actual order
    of execution with an instant-completing stream (worst case).
    """
    call_order = []

    config = AppConfig()
    # Instant stream — completes immediately with no awaits
    chunks = [_make_sse_done("")]
    mgr = _make_manager(*chunks)

    from src.cli.commands.agent_tui import AgentTuiApp

    app = AgentTuiApp(db=db, config=config, agent_manager=mgr)

    original_set_button = app._set_button_streaming

    def tracking_set_button(streaming: bool):
        call_order.append(("set_button", streaming))
        original_set_button(streaming)

    app._set_button_streaming = tracking_set_button

    async with app.run_test(size=(120, 40)) as pilot:
        await pilot.pause()
        from textual.widgets import TextArea

        input_area = app.query_one("#input", TextArea)
        input_area.load_text("instant")
        await pilot.click("#send-btn")
        await pilot.pause(0.5)

    # True must come before False — no race condition
    true_indices = [i for i, (fn, val) in enumerate(call_order) if val is True]
    false_indices = [i for i, (fn, val) in enumerate(call_order) if val is False]
    assert len(true_indices) >= 1, f"set_button(True) never called: {call_order}"
    assert len(false_indices) >= 1, f"set_button(False) never called: {call_order}"
    assert true_indices[0] < false_indices[0], f"Race detected! Order: {call_order}"


class TestAgentChatParser:
    def test_prompt_flag_long(self):
        from src.cli.parser import build_parser

        parser = build_parser()
        args = parser.parse_args(["agent", "chat", "--prompt", "hello"])
        assert args.agent_action == "chat"
        assert args.prompt == "hello"

    def test_prompt_flag_short(self):
        from src.cli.parser import build_parser

        parser = build_parser()
        args = parser.parse_args(["agent", "chat", "-p", "hello"])
        assert args.prompt == "hello"

    def test_no_prompt_is_none(self):
        from src.cli.parser import build_parser

        parser = build_parser()
        args = parser.parse_args(["agent", "chat"])
        assert args.prompt is None

    def test_prompt_with_model(self):
        from src.cli.parser import build_parser

        parser = build_parser()
        args = parser.parse_args(["agent", "chat", "-p", "hi", "--model", "claude-haiku-4-5-20251001"])
        assert args.prompt == "hi"
        assert args.model == "claude-haiku-4-5-20251001"

    def test_prompt_with_thread_id(self):
        from src.cli.parser import build_parser

        parser = build_parser()
        args = parser.parse_args(["agent", "chat", "-p", "hi", "--thread-id", "5"])
        assert args.prompt == "hi"
        assert args.thread_id == 5
