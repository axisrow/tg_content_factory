"""`agent chat` CLI modes: one-shot (-p), interactive (TUI from the optional tg_tui package), parser.

Moved out of the TUI tests when the TUI became packages/tg_tui (#1720 wave 2):
these exercise the core CLI, so they stay in the core suite and stand in a fake
tg_tui module instead of needing the package installed.
"""

from __future__ import annotations

import asyncio
import sys
from types import ModuleType


def fake_tg_tui_modules(app_cls) -> dict[str, ModuleType]:
    package = ModuleType("tg_tui")
    app_module = ModuleType("tg_tui.app")
    app_module.AgentTuiApp = app_cls
    package.app = app_module
    return {"tg_tui": package, "tg_tui.app": app_module}


# ---------------------------------------------------------------------------
# CLI one-shot mode (prompt=) tests
# ---------------------------------------------------------------------------


class TestAgentChatOneShotMode:
    """Tests for `agent chat -p <message>` one-shot mode."""

    def _run_chat(self, cli_env, prompt: str, thread_id=None, cli_init_patch=None, monkeypatch=None, capsys=None):
        """Helper: run agent chat in one-shot mode with mocked AgentManager."""
        from unittest.mock import MagicMock
        from unittest.mock import patch as _patch

        from claude_agent_sdk import AssistantMessage, ResultMessage, TextBlock

        from tests.helpers import cli_ns as _ns
        from tg_agent.cli.commands.agent import run

        if monkeypatch:
            monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")

        text_block = TextBlock(text="one-shot reply")
        assistant_msg = MagicMock(spec=AssistantMessage)
        assistant_msg.content = [text_block]
        result_msg = MagicMock(spec=ResultMessage)

        async def mock_query(prompt, options):
            yield assistant_msg
            yield result_msg

        with (
            cli_init_patch(cli_env, "tg_agent.cli.runtime.init_db"),
            _patch("tg_agent.agent.backends.claude_sdk.query", mock_query),
        ):
            run(_ns(agent_action="chat", prompt=prompt, thread_id=thread_id, model=None))

    def test_one_shot_prints_response(self, cli_env, cli_init_patch, capsys, monkeypatch):
        self._run_chat(cli_env, "hello", cli_init_patch=cli_init_patch, monkeypatch=monkeypatch)
        assert "one-shot reply" in capsys.readouterr().out

    def test_one_shot_creates_thread_if_none(self, cli_env, cli_init_patch, monkeypatch):
        import sqlite3

        self._run_chat(cli_env, "hi", cli_init_patch=cli_init_patch, monkeypatch=monkeypatch)
        conn = sqlite3.connect(cli_env._db_path)
        try:
            count = conn.execute("SELECT COUNT(*) FROM agent_threads").fetchone()[0]
        finally:
            conn.close()
        assert count >= 1

    def test_one_shot_uses_existing_thread(self, cli_env, cli_init_patch, monkeypatch):
        import sqlite3

        tid = asyncio.run(cli_env.create_agent_thread("existing"))
        self._run_chat(cli_env, "hi", thread_id=tid, cli_init_patch=cli_init_patch, monkeypatch=monkeypatch)
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

    def test_one_shot_prints_status_to_stderr(self, cli_env, cli_init_patch, capsys, monkeypatch):
        """Status events are printed to stderr, not stdout, in one-shot mode."""
        from unittest.mock import patch as _patch

        from tests.helpers import cli_ns as _ns
        from tg_agent.cli.commands.agent import run

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")

        async def fake_chat_stream(*_a, **_kw):
            yield 'data: {"type": "status", "text": "Создание клиента"}\n\n'
            yield 'data: {"type": "status", "text": "Запрос к API"}\n\n'
            yield 'data: {"text": "reply text"}\n\n'
            yield 'data: {"done": true, "full_text": "reply text"}\n\n'

        with (
            cli_init_patch(cli_env, "tg_agent.cli.runtime.init_db"),
            _patch("tg_agent.agent.manager.AgentManager.chat_stream", fake_chat_stream),
        ):
            run(_ns(agent_action="chat", prompt="test", thread_id=None, model=None))

        captured = capsys.readouterr()
        assert "reply text" in captured.out
        # Status events must NOT leak into stdout
        assert "Создание клиента" not in captured.out
        assert "Запрос к API" not in captured.out


# ---------------------------------------------------------------------------
# CLI interactive mode (no prompt) launches TUI
# ---------------------------------------------------------------------------


class TestAgentChatInteractiveMode:
    """Tests that `agent chat` without --prompt launches TUI."""

    def test_no_prompt_launches_tui(self, cli_env, cli_init_patch, monkeypatch):
        """When prompt is None, AgentTuiApp.run_async() is called."""
        from unittest.mock import AsyncMock, MagicMock
        from unittest.mock import patch as _patch

        from tests.helpers import cli_ns as _ns
        from tg_agent.cli.commands.agent import run

        monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")

        mock_run_async = AsyncMock()
        app_cls = MagicMock(return_value=MagicMock(run_async=mock_run_async))

        with (
            cli_init_patch(cli_env, "tg_agent.cli.runtime.init_db"),
            _patch.dict(sys.modules, fake_tg_tui_modules(app_cls)),
        ):
            run(_ns(agent_action="chat", prompt=None, thread_id=None, model=None))

        mock_run_async.assert_called_once()


class TestAgentChatParser:
    @staticmethod
    def _invoke(argv):
        from unittest.mock import MagicMock, patch

        from typer.testing import CliRunner

        from tg_agent.cli.typer_app import app

        mock_impl = MagicMock()
        with (
            patch("tg_agent.cli.typer_commands.agent_cmd.chat_impl", mock_impl),
            patch("tg_agent.cli.typer_commands.run_async", side_effect=lambda coro: coro.close()),
        ):
            result = CliRunner().invoke(app, argv)
        assert result.exit_code == 0, result.output
        return mock_impl.call_args.kwargs

    def test_prompt_flag_long(self):
        kwargs = self._invoke(["agent", "chat", "--prompt", "hello"])
        assert kwargs["prompt"] == "hello"

    def test_prompt_flag_short(self):
        kwargs = self._invoke(["agent", "chat", "-p", "hello"])
        assert kwargs["prompt"] == "hello"

    def test_no_prompt_is_none(self):
        kwargs = self._invoke(["agent", "chat"])
        assert kwargs["prompt"] is None

    def test_prompt_with_model(self):
        kwargs = self._invoke(["agent", "chat", "-p", "hi", "--model", "claude-haiku-4-5-20251001"])
        assert kwargs["prompt"] == "hi"
        assert kwargs["model"] == "claude-haiku-4-5-20251001"

    def test_prompt_with_thread_id(self):
        kwargs = self._invoke(["agent", "chat", "-p", "hi", "--thread-id", "5"])
        assert kwargs["prompt"] == "hi"
        assert kwargs["thread_id"] == 5
