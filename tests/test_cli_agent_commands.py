"""Tests for src/cli/commands/agent.py — CLI agent subcommands."""
from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.agent.provider_registry import ZAI_DEFAULT_BASE_URL, ProviderRuntimeConfig
from src.cli.commands.agent import _test_escaping, _test_tools, run
from src.config import AppConfig
from src.database import Database
from src.services.agent_provider_service import AgentProviderService
from tests.helpers import cli_ns, fake_asyncio_run, make_cli_config, make_cli_db

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _args(**overrides):
    defaults = {
        "config": "config.yaml",
        "agent_action": "threads",
    }
    defaults.update(overrides)
    return cli_ns(**defaults)


def _make_mgr(**overrides) -> MagicMock:
    mgr = MagicMock()
    mgr.available = True
    mgr.initialize = MagicMock()
    mgr.refresh_settings_cache = AsyncMock()
    mgr.close_all = AsyncMock()
    mgr.chat_stream = AsyncMock()
    for k, v in overrides.items():
        setattr(mgr, k, v)
    return mgr


# ---------------------------------------------------------------------------
# _test_escaping
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_escaping_agent_not_available(capsys):
    """When agent is not available, prints skip message and returns."""
    db = make_cli_db()
    config = make_cli_config()

    with patch("src.agent.manager.AgentManager", return_value=_make_mgr(available=False)):
        await _test_escaping(db, config)

    out = capsys.readouterr().out
    assert "пропуск" in out or "не настроены" in out


@pytest.mark.anyio
async def test_escaping_stream_ok(capsys):
    """Successful escaping test streams responses and counts passed."""
    db = make_cli_db()
    config = make_cli_config()
    mgr = _make_mgr(available=True)

    # Build fake SSE stream
    async def _fake_stream(*a, **kw):
        for name, text in [("xml_tags", "<b>bold</b>")]:
            yield f'data: {json.dumps({"text": f"processed-{name}"})}\n'
            yield f'data: {json.dumps({"done": True})}\n'

    mgr.chat_stream = _fake_stream

    with patch("src.agent.manager.AgentManager", return_value=mgr):
        await _test_escaping(db, config)

    out = capsys.readouterr().out
    assert "OK" in out or "passed" in out.lower() or "Итого" in out


# ---------------------------------------------------------------------------
# _test_tools
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_tools_agent_not_available(capsys):
    """When agent is not available, prints skip message."""
    db = make_cli_db()
    config = make_cli_config()

    with patch("src.agent.manager.AgentManager", return_value=_make_mgr(available=False)):
        await _test_tools(db, config)

    out = capsys.readouterr().out
    assert "пропуск" in out or "не настроен" in out


@pytest.mark.anyio
async def test_tools_stream_with_tool_events(capsys):
    """Successful tools test receives tool_start/tool_end events."""
    db = make_cli_db()
    config = make_cli_config()
    mgr = _make_mgr(available=True)

    async def _fake_stream(*a, **kw):
        yield f'data: {json.dumps({"type": "tool_start", "tool": "list_channels"})}\n'
        yield f'data: {json.dumps({"type": "tool_end", "tool": "list_channels", "duration": 0.5})}\n'
        yield f'data: {json.dumps({"text": "Found 3 channels", "done": True})}\n'

    mgr.chat_stream = _fake_stream

    with patch("src.agent.manager.AgentManager", return_value=mgr):
        await _test_tools(db, config)

    out = capsys.readouterr().out
    assert "OK" in out or "passed" in out.lower() or "Итого" in out


# ---------------------------------------------------------------------------
# run() — threads
# ---------------------------------------------------------------------------


def test_run_threads_empty(capsys):
    db = make_cli_db(get_agent_threads=AsyncMock(return_value=[]))
    config = make_cli_config()
    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="threads"))
    assert "Нет тредов" in capsys.readouterr().out


def test_run_threads_list(capsys):
    threads = [
        {"id": 1, "title": "Test Thread", "created_at": "2024-01-01"},
        {"id": 2, "title": "Another", "created_at": "2024-01-02"},
    ]
    db = make_cli_db(get_agent_threads=AsyncMock(return_value=threads))
    config = make_cli_config()
    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="threads"))
    out = capsys.readouterr().out
    assert "Test Thread" in out
    assert "Another" in out


# ---------------------------------------------------------------------------
# run() — thread-create
# ---------------------------------------------------------------------------


def test_run_thread_create_default_title(capsys):
    db = make_cli_db(create_agent_thread=AsyncMock(return_value=42))
    config = make_cli_config()
    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="thread-create"))
    out = capsys.readouterr().out
    assert "#42" in out


def test_run_thread_create_custom_title(capsys):
    db = make_cli_db(create_agent_thread=AsyncMock(return_value=5))
    config = make_cli_config()
    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="thread-create", title="Custom Title"))
    out = capsys.readouterr().out
    assert "Custom Title" in out
    assert "#5" in out


# ---------------------------------------------------------------------------
# run() — thread-delete
# ---------------------------------------------------------------------------


def test_run_thread_delete(capsys):
    db = make_cli_db()
    config = make_cli_config()
    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="thread-delete", thread_id=7))
    out = capsys.readouterr().out
    assert "#7" in out
    assert "удалён" in out


# ---------------------------------------------------------------------------
# run() — thread-rename
# ---------------------------------------------------------------------------


def test_run_thread_rename(capsys):
    db = make_cli_db()
    config = make_cli_config()
    long_title = "A" * 200
    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="thread-rename", thread_id=3, title=long_title))
    db.rename_agent_thread.assert_called_once_with(3, long_title[:100])
    out = capsys.readouterr().out
    assert "#3" in out


# ---------------------------------------------------------------------------
# run() — messages
# ---------------------------------------------------------------------------


def test_run_messages_empty(capsys):
    db = make_cli_db(get_agent_messages=AsyncMock(return_value=[]))
    config = make_cli_config()
    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="messages", thread_id=1, limit=None))
    assert "Нет сообщений" in capsys.readouterr().out


def test_run_messages_with_limit(capsys):
    msgs = [
        {"role": "user", "content": "Hello world", "created_at": "2024-01-01"},
        {"role": "assistant", "content": "Hi there!", "created_at": "2024-01-01"},
        {"role": "user", "content": "Third msg", "created_at": "2024-01-01"},
    ]
    db = make_cli_db(get_agent_messages=AsyncMock(return_value=msgs))
    config = make_cli_config()
    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="messages", thread_id=1, limit=2))
    out = capsys.readouterr().out
    # Should only show last 2 messages
    assert "Hi there" in out
    assert "Third msg" in out


def test_run_messages_all(capsys):
    msgs = [
        {"role": "user", "content": "Hello", "created_at": "2024-01-01"},
    ]
    db = make_cli_db(get_agent_messages=AsyncMock(return_value=msgs))
    config = make_cli_config()
    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="messages", thread_id=1, limit=None))
    out = capsys.readouterr().out
    assert "Hello" in out


# ---------------------------------------------------------------------------
# run() — context
# ---------------------------------------------------------------------------


def test_run_context_thread_not_found(capsys):
    db = make_cli_db(get_agent_thread=AsyncMock(return_value=None))
    config = make_cli_config()
    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="context", thread_id=99, channel_id=100, limit=10, topic_id=None))
    out = capsys.readouterr().out
    assert "не найден" in out


def test_run_context_happy_path(capsys):
    ch = MagicMock()
    ch.title = "Test Channel"
    msgs = [MagicMock(text="hello")]
    thread = {"id": 1, "title": "t"}
    db = make_cli_db(
        get_agent_thread=AsyncMock(return_value=thread),
        search_messages=AsyncMock(return_value=(msgs, 1)),
        get_channel_by_channel_id=AsyncMock(return_value=ch),
        get_forum_topics=AsyncMock(return_value=[{"id": 10, "title": "Topic A"}]),
    )
    config = make_cli_config()
    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.agent.context.format_context", return_value="formatted context"):
        with patch("asyncio.run", fake_asyncio_run):
            run(_args(agent_action="context", thread_id=1, channel_id=100, limit=10, topic_id=None))
    db.save_agent_message.assert_called_once_with(
        thread_id=1, role="user", content="formatted context"
    )


# ---------------------------------------------------------------------------
# run() — chat with prompt
# ---------------------------------------------------------------------------


def test_run_chat_prompt_new_thread(capsys):
    db = make_cli_db(create_agent_thread=AsyncMock(return_value=10))
    config = make_cli_config()
    mgr = _make_mgr(available=True)
    auth = MagicMock(cleanup=AsyncMock())
    pool = MagicMock(disconnect_all=AsyncMock())

    async def _fake_stream(*a, **kw):
        yield f'data: {json.dumps({"text": "Hello"})}\n'
        yield f'data: {json.dumps({"done": True})}\n'

    mgr.chat_stream = _fake_stream

    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.agent.runtime.init_pool", AsyncMock(return_value=(auth, pool))), \
         patch("src.cli.commands.agent.runtime.redirect_logging_to_file", return_value=None), \
         patch("src.cli.commands.agent.runtime.restore_logging"), \
         patch("src.agent.manager.AgentManager", return_value=mgr), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="chat", prompt="test", thread_id=None, model=None))

    db.create_agent_thread.assert_called_once_with("Новый тред")
    db.save_agent_message.assert_called()


def test_run_chat_prompt_existing_thread(capsys):
    db = make_cli_db()
    config = make_cli_config()
    mgr = _make_mgr(available=True)
    auth = MagicMock(cleanup=AsyncMock())
    pool = MagicMock(disconnect_all=AsyncMock())

    async def _fake_stream(*a, **kw):
        yield f'data: {json.dumps({"text": "Reply"})}\n'
        yield f'data: {json.dumps({"done": True})}\n'

    mgr.chat_stream = _fake_stream

    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.agent.runtime.init_pool", AsyncMock(return_value=(auth, pool))), \
         patch("src.cli.commands.agent.runtime.redirect_logging_to_file", return_value=None), \
         patch("src.cli.commands.agent.runtime.restore_logging"), \
         patch("src.agent.manager.AgentManager", return_value=mgr), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="chat", prompt="test", thread_id=5, model=None))

    # Should NOT create a new thread
    db.create_agent_thread.assert_not_called()


def test_run_chat_error_in_stream(capsys):
    db = make_cli_db()
    config = make_cli_config()
    mgr = _make_mgr(available=True)
    auth = MagicMock(cleanup=AsyncMock())
    pool = MagicMock(disconnect_all=AsyncMock())

    async def _fake_stream(*a, **kw):
        yield f'data: {json.dumps({"error": "something failed", "details": "traceback"})}\n'

    mgr.chat_stream = _fake_stream

    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.agent.runtime.init_pool", AsyncMock(return_value=(auth, pool))), \
         patch("src.cli.commands.agent.runtime.redirect_logging_to_file", return_value=None), \
         patch("src.cli.commands.agent.runtime.restore_logging"), \
         patch("src.agent.manager.AgentManager", return_value=mgr), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="chat", prompt="test", thread_id=5, model=None))

    db.delete_last_agent_exchange.assert_called_once_with(5)


# ---------------------------------------------------------------------------
# run() — chat with tool events
# ---------------------------------------------------------------------------


def test_run_chat_tool_events(capsys):
    db = make_cli_db()
    config = make_cli_config()
    mgr = _make_mgr(available=True)
    auth = MagicMock(cleanup=AsyncMock())
    pool = MagicMock(disconnect_all=AsyncMock())

    async def _fake_stream(*a, **kw):
        yield f'data: {json.dumps({"type": "tool_start", "tool": "search"})}\n'
        yield f'data: {json.dumps({"type": "tool_end", "tool": "search", "duration": 1.2, "summary": "found 5"})}\n'
        yield f'data: {json.dumps({"text": "Done"})}\n'
        yield f'data: {json.dumps({"done": True})}\n'

    mgr.chat_stream = _fake_stream

    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.agent.runtime.init_pool", AsyncMock(return_value=(auth, pool))), \
         patch("src.cli.commands.agent.runtime.redirect_logging_to_file", return_value=None), \
         patch("src.cli.commands.agent.runtime.restore_logging"), \
         patch("src.agent.manager.AgentManager", return_value=mgr), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="chat", prompt="search for x", thread_id=1, model=None))


def test_run_chat_tool_end_with_error(capsys):
    db = make_cli_db()
    config = make_cli_config()
    mgr = _make_mgr(available=True)
    auth = MagicMock(cleanup=AsyncMock())
    pool = MagicMock(disconnect_all=AsyncMock())

    async def _fake_stream(*a, **kw):
        yield f'data: {json.dumps({"type": "tool_start", "tool": "list_channels"})}\n'
        yield f'data: {json.dumps({"type": "tool_end", "tool": "list_channels", "is_error": True})}\n'
        yield f'data: {json.dumps({"text": "Oops"})}\n'
        yield f'data: {json.dumps({"done": True})}\n'

    mgr.chat_stream = _fake_stream

    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.agent.runtime.init_pool", AsyncMock(return_value=(auth, pool))), \
         patch("src.cli.commands.agent.runtime.redirect_logging_to_file", return_value=None), \
         patch("src.cli.commands.agent.runtime.restore_logging"), \
         patch("src.agent.manager.AgentManager", return_value=mgr), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="chat", prompt="list channels", thread_id=1, model=None))


# ---------------------------------------------------------------------------
# run() — chat status/countdown events
# ---------------------------------------------------------------------------


def test_run_chat_status_events(capsys):
    db = make_cli_db()
    config = make_cli_config()
    mgr = _make_mgr(available=True)
    auth = MagicMock(cleanup=AsyncMock())
    pool = MagicMock(disconnect_all=AsyncMock())

    async def _fake_stream(*a, **kw):
        yield f'data: {json.dumps({"type": "status", "text": "thinking..."})}\n'
        yield f'data: {json.dumps({"type": "countdown", "text": "3s left"})}\n'
        yield f'data: {json.dumps({"text": "Result"})}\n'
        yield f'data: {json.dumps({"done": True})}\n'

    mgr.chat_stream = _fake_stream

    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.agent.runtime.init_pool", AsyncMock(return_value=(auth, pool))), \
         patch("src.cli.commands.agent.runtime.redirect_logging_to_file", return_value=None), \
         patch("src.cli.commands.agent.runtime.restore_logging"), \
         patch("src.agent.manager.AgentManager", return_value=mgr), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="chat", prompt="test", thread_id=1, model=None))


# ---------------------------------------------------------------------------
# run() — chat interactive TUI mode
# ---------------------------------------------------------------------------


def test_run_chat_interactive_tui():
    db = make_cli_db()
    config = make_cli_config()
    mgr = _make_mgr(available=True)
    auth = MagicMock(cleanup=AsyncMock())
    pool = MagicMock(disconnect_all=AsyncMock())
    tui_app = MagicMock()
    tui_app.run_async = AsyncMock()

    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.agent.runtime.init_pool", AsyncMock(return_value=(auth, pool))), \
         patch("src.cli.commands.agent.runtime.redirect_logging_to_file", return_value=None), \
         patch("src.cli.commands.agent.runtime.restore_logging"), \
         patch("src.agent.manager.AgentManager", return_value=mgr), \
         patch("src.cli.commands.agent_tui.AgentTuiApp", return_value=tui_app), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="chat", prompt=None, thread_id=None, model=None))

    tui_app.run_async.assert_called_once()


# ---------------------------------------------------------------------------
# run() — cleanup errors handled gracefully
# ---------------------------------------------------------------------------


def test_run_cleanup_mgr_close_error():
    db = make_cli_db()
    config = make_cli_config()
    mgr = _make_mgr(available=True, close_all=AsyncMock(side_effect=RuntimeError("boom")))
    auth = MagicMock(cleanup=AsyncMock())
    pool = MagicMock(disconnect_all=AsyncMock())

    async def _fake_stream(*a, **kw):
        yield f'data: {json.dumps({"text": "hi"})}\n'
        yield f'data: {json.dumps({"done": True})}\n'

    mgr.chat_stream = _fake_stream

    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.agent.runtime.init_pool", AsyncMock(return_value=(auth, pool))), \
         patch("src.cli.commands.agent.runtime.redirect_logging_to_file", return_value=None), \
         patch("src.cli.commands.agent.runtime.restore_logging"), \
         patch("src.agent.manager.AgentManager", return_value=mgr), \
         patch("asyncio.run", fake_asyncio_run):
        # Should not raise despite mgr.close_all error
        run(_args(agent_action="chat", prompt="test", thread_id=1, model=None))


def test_run_cleanup_pool_disconnect_error():
    db = make_cli_db()
    config = make_cli_config()
    mgr = _make_mgr(available=True)
    auth = MagicMock(cleanup=AsyncMock())
    pool = MagicMock(disconnect_all=AsyncMock(side_effect=RuntimeError("pool err")))

    async def _fake_stream(*a, **kw):
        yield f'data: {json.dumps({"text": "hi"})}\n'
        yield f'data: {json.dumps({"done": True})}\n'

    mgr.chat_stream = _fake_stream

    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.agent.runtime.init_pool", AsyncMock(return_value=(auth, pool))), \
         patch("src.cli.commands.agent.runtime.redirect_logging_to_file", return_value=None), \
         patch("src.cli.commands.agent.runtime.restore_logging"), \
         patch("src.agent.manager.AgentManager", return_value=mgr), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="chat", prompt="test", thread_id=1, model=None))


# ---------------------------------------------------------------------------
# run() — test-escaping / test-tools actions
# ---------------------------------------------------------------------------


def test_run_test_escaping_action():
    db = make_cli_db()
    config = make_cli_config()

    async def _fake_escaping(d, c):
        pass

    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.agent._test_escaping", _fake_escaping), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="test-escaping"))


def test_run_test_tools_action():
    db = make_cli_db()
    config = make_cli_config()

    async def _fake_tools(d, c):
        pass

    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.agent._test_tools", _fake_tools), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="test-tools"))


# ---------------------------------------------------------------------------
# run() — non-chat actions do not redirect logging
# ---------------------------------------------------------------------------


def test_run_threads_no_log_redirect():
    db = make_cli_db()
    config = make_cli_config()
    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.agent.runtime.redirect_logging_to_file") as mock_redirect, \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="threads"))
    mock_redirect.assert_not_called()


# ---------------------------------------------------------------------------
# run() — thinking/tool_result events ignored
# ---------------------------------------------------------------------------


def test_run_chat_thinking_event_ignored(capsys):
    db = make_cli_db()
    config = make_cli_config()
    mgr = _make_mgr(available=True)
    auth = MagicMock(cleanup=AsyncMock())
    pool = MagicMock(disconnect_all=AsyncMock())

    async def _fake_stream(*a, **kw):
        yield f'data: {json.dumps({"type": "thinking"})}\n'
        yield f'data: {json.dumps({"type": "tool_result"})}\n'
        yield f'data: {json.dumps({"text": "Final answer"})}\n'
        yield f'data: {json.dumps({"done": True})}\n'

    mgr.chat_stream = _fake_stream

    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.agent.runtime.init_pool", AsyncMock(return_value=(auth, pool))), \
         patch("src.cli.commands.agent.runtime.redirect_logging_to_file", return_value=None), \
         patch("src.cli.commands.agent.runtime.restore_logging"), \
         patch("src.agent.manager.AgentManager", return_value=mgr), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="chat", prompt="test", thread_id=1, model=None))

    out = capsys.readouterr().out
    assert "Final answer" in out


# ---------------------------------------------------------------------------
# run() — non-JSON chunks in stream are skipped
# ---------------------------------------------------------------------------


def test_run_chat_non_json_chunk_skipped(capsys):
    db = make_cli_db()
    config = make_cli_config()
    mgr = _make_mgr(available=True)
    auth = MagicMock(cleanup=AsyncMock())
    pool = MagicMock(disconnect_all=AsyncMock())

    async def _fake_stream(*a, **kw):
        yield "data: not-json\n"
        yield f'data: {json.dumps({"text": "ok"})}\n'
        yield f'data: {json.dumps({"done": True})}\n'

    mgr.chat_stream = _fake_stream

    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.agent.runtime.init_pool", AsyncMock(return_value=(auth, pool))), \
         patch("src.cli.commands.agent.runtime.redirect_logging_to_file", return_value=None), \
         patch("src.cli.commands.agent.runtime.restore_logging"), \
         patch("src.agent.manager.AgentManager", return_value=mgr), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="chat", prompt="test", thread_id=1, model=None))

    out = capsys.readouterr().out
    assert "ok" in out


# ---------------------------------------------------------------------------
# run() — chat with model arg
# ---------------------------------------------------------------------------


def test_run_chat_with_model(capsys):
    db = make_cli_db()
    config = make_cli_config()
    mgr = _make_mgr(available=True)
    auth = MagicMock(cleanup=AsyncMock())
    pool = MagicMock(disconnect_all=AsyncMock())

    async def _fake_stream(*a, **kw):
        yield f'data: {json.dumps({"text": "response"})}\n'
        yield f'data: {json.dumps({"done": True})}\n'

    mgr.chat_stream = _fake_stream

    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.agent.runtime.init_pool", AsyncMock(return_value=(auth, pool))), \
         patch("src.cli.commands.agent.runtime.redirect_logging_to_file", return_value=None), \
         patch("src.cli.commands.agent.runtime.restore_logging"), \
         patch("src.agent.manager.AgentManager", return_value=mgr), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="chat", prompt="hi", thread_id=1, model="gpt-4o"))


# ---------------------------------------------------------------------------
# _test_tools — sys.exit on failure
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_tools_exits_on_failure():
    db = make_cli_db()
    config = make_cli_config()
    mgr = _make_mgr(available=True)

    async def _fake_stream(*a, **kw):
        yield f'data: {json.dumps({"error": "tool error"})}\n'

    mgr.chat_stream = _fake_stream

    with patch("src.agent.manager.AgentManager", return_value=mgr):
        with pytest.raises(SystemExit):
            await _test_tools(db, config)


# ---------------------------------------------------------------------------
# run() — context large content warning
# ---------------------------------------------------------------------------


def test_run_context_large_content(capsys):
    ch = MagicMock()
    ch.title = "Big Channel"
    msgs = [MagicMock(text="x" * 1000)]
    thread = {"id": 1, "title": "t"}
    large_content = "C" * 300_000
    db = make_cli_db(
        get_agent_thread=AsyncMock(return_value=thread),
        search_messages=AsyncMock(return_value=(msgs, 1)),
        get_channel_by_channel_id=AsyncMock(return_value=ch),
        get_forum_topics=AsyncMock(return_value=[]),
    )
    config = make_cli_config()
    with patch("src.cli.commands.agent.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.agent.context.format_context", return_value=large_content), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(agent_action="context", thread_id=1, channel_id=100, limit=10, topic_id=None))

    out = capsys.readouterr().out
    assert "символов всего" in out


def test_run_chat_prompt_real_manager_uses_db_deepagents_provider(cli_db, capsys, monkeypatch):
    config = AppConfig()
    config.security.session_encryption_key = "provider-agent-cli-secret"
    asyncio.run(
        AgentProviderService(cli_db, config).save_provider_configs(
            [
                ProviderRuntimeConfig(
                    provider="zai",
                    enabled=True,
                    priority=0,
                    selected_model="glm-5-turbo",
                    plain_fields={"base_url": ""},
                    secret_fields={"api_key": "zai-key"},
                )
            ]
        )
    )
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    captured: dict[str, object] = {}

    async def fake_init_db(_config_path: str):
        cmd_db = Database(cli_db._db_path)
        await cmd_db.initialize()
        return config, cmd_db

    async def fake_init_pool(_config, _db):
        return MagicMock(cleanup=AsyncMock()), MagicMock(disconnect_all=AsyncMock())

    def fake_init_chat_model(*, model, model_provider, **kwargs):
        captured["model"] = model
        captured["provider"] = model_provider
        captured["kwargs"] = kwargs
        return SimpleNamespace(model_provider=model_provider)

    def fake_create_deep_agent(model, tools, system_prompt):
        captured["tool_count"] = len(tools)
        captured["system_prompt"] = system_prompt
        assert model.model_provider == "openai"
        return MagicMock(run=MagicMock(return_value="integration reply"))

    with (
        patch("src.cli.commands.agent.runtime.init_db", side_effect=fake_init_db),
        patch("src.cli.commands.agent.runtime.init_pool", side_effect=fake_init_pool),
        patch("src.cli.commands.agent.runtime.redirect_logging_to_file", return_value=None),
        patch("src.cli.commands.agent.runtime.restore_logging"),
        patch("langchain.chat_models.init_chat_model", side_effect=fake_init_chat_model),
        patch("deepagents.create_deep_agent", side_effect=fake_create_deep_agent),
    ):
        run(_args(agent_action="chat", prompt="hello integration", thread_id=None, model=None))

    out = capsys.readouterr().out
    assert "Агент: integration reply" in out
    assert captured["model"] == "glm-5-turbo"
    assert captured["provider"] == "openai"
    assert captured["kwargs"]["api_key"] == "zai-key"
    assert captured["kwargs"]["base_url"] == ZAI_DEFAULT_BASE_URL
    messages = asyncio.run(cli_db.get_agent_messages(1))
    assert [msg["role"] for msg in messages] == ["user", "assistant"]
    assert messages[0]["content"] == "hello integration"
    assert messages[1]["content"] == "integration reply"
