import json
from unittest.mock import PropertyMock, patch

import pytest
from claude_agent_sdk import CLIConnectionError, CLINotFoundError, ProcessError

from src.agent.manager import AgentManager, ClaudeSdkBackend, DeepagentsBackend


@pytest.mark.anyio
async def test_agent_manager_handles_ollama_500_error(db, monkeypatch):
    """Test that Ollama 500 errors are translated to friendly messages."""

    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)

    mgr = AgentManager(db)

    async def mock_chat_stream(*args, **kwargs):
        raise RuntimeError("ollama: Internal Server Error (status code: 500)")

    with (
        patch.object(DeepagentsBackend, "available", new_callable=PropertyMock) as mock_deep_avail,
        patch.object(ClaudeSdkBackend, "available", new_callable=PropertyMock) as mock_claude_avail,
        patch.object(DeepagentsBackend, "chat_stream", side_effect=mock_chat_stream),
    ):
        mock_deep_avail.return_value = True
        mock_claude_avail.return_value = False

        thread_id = await db.create_agent_thread("test thread")
        await db.save_agent_message(thread_id, "user", "hello")

        chunks = []
        async for chunk in mgr.chat_stream(thread_id, "hello"):
            chunks.append(chunk)

        assert len(chunks) > 0
        last_chunk = chunks[-1]
        assert "data: " in last_chunk
        payload = json.loads(last_chunk.replace("data: ", ""))

        assert "error" in payload
        error_msg = payload["error"]

        assert "Внутренняя ошибка сервиса Ollama (500)" in error_msg
        assert "Возможно, модель не загрузилась" in error_msg


@pytest.mark.anyio
async def test_agent_manager_handles_ollama_connection_error(db, monkeypatch):
    """Test that Ollama connection errors are translated to friendly messages."""

    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)

    mgr = AgentManager(db)

    async def mock_chat_stream(*args, **kwargs):
        raise RuntimeError("ollama: connection refused")

    with (
        patch.object(DeepagentsBackend, "available", new_callable=PropertyMock) as mock_deep_avail,
        patch.object(ClaudeSdkBackend, "available", new_callable=PropertyMock) as mock_claude_avail,
        patch.object(DeepagentsBackend, "chat_stream", side_effect=mock_chat_stream),
    ):
        mock_deep_avail.return_value = True
        mock_claude_avail.return_value = False

        thread_id = await db.create_agent_thread("test thread 2")
        await db.save_agent_message(thread_id, "user", "hello")

        chunks = []
        async for chunk in mgr.chat_stream(thread_id, "hello"):
            chunks.append(chunk)

        assert len(chunks) > 0
        payload = json.loads(chunks[-1].replace("data: ", ""))
        error_msg = payload["error"]

        assert "Не удалось подключиться к Ollama" in error_msg
        assert "Проверьте, что сервис запущен" in error_msg


@pytest.mark.anyio
async def test_agent_manager_handles_generic_error(db, monkeypatch):
    """Test that generic errors are passed through (with prefix)."""

    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)

    mgr = AgentManager(db)

    async def mock_chat_stream(*args, **kwargs):
        raise RuntimeError("Some random error")

    with (
        patch.object(DeepagentsBackend, "available", new_callable=PropertyMock) as mock_deep_avail,
        patch.object(ClaudeSdkBackend, "available", new_callable=PropertyMock) as mock_claude_avail,
        patch.object(DeepagentsBackend, "chat_stream", side_effect=mock_chat_stream),
    ):
        mock_deep_avail.return_value = True
        mock_claude_avail.return_value = False

        thread_id = await db.create_agent_thread("test thread 3")
        await db.save_agent_message(thread_id, "user", "hello")

        chunks = []
        async for chunk in mgr.chat_stream(thread_id, "hello"):
            chunks.append(chunk)

        payload = json.loads(chunks[-1].replace("data: ", ""))
        error_msg = payload["error"]

        assert "Some random error" in error_msg
        assert "Внутренняя ошибка сервиса Ollama" not in error_msg


# ---------------------------------------------------------------------------
# Claude SDK backend errors (ProcessError, CLINotFoundError, CLIConnectionError)
# ---------------------------------------------------------------------------

async def _collect_claude_error(db, monkeypatch, exc):
    """Helper: run AgentManager.chat_stream with Claude backend raising *exc*."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-fake")

    mgr = AgentManager(db)

    with (
        patch.object(ClaudeSdkBackend, "available", new_callable=PropertyMock) as mock_claude,
        patch.object(DeepagentsBackend, "available", new_callable=PropertyMock) as mock_deep,
        patch.object(ClaudeSdkBackend, "chat_stream", side_effect=exc),
    ):
        mock_claude.return_value = True
        mock_deep.return_value = False

        thread_id = await db.create_agent_thread("claude error test")
        await db.save_agent_message(thread_id, "user", "hello")

        chunks = []
        async for chunk in mgr.chat_stream(thread_id, "hello"):
            chunks.append(chunk)

    # Find the last SSE data payload that contains an error
    for chunk in reversed(chunks):
        if "data: " in chunk and '"error"' in chunk:
            return json.loads(chunk.replace("data: ", ""))
    raise AssertionError(f"No error payload in chunks: {chunks}")


@pytest.mark.anyio
async def test_claude_backend_process_error(db, monkeypatch):
    """ProcessError from claude-agent-sdk should surface with details."""
    exc = ProcessError("CLI crashed", exit_code=1, stderr="segfault in libfoo")
    payload = await _collect_claude_error(db, monkeypatch, exc)

    assert "error" in payload
    assert "claude" in payload["error"].lower()
    assert "segfault" in payload["error"] or "CLI crashed" in payload["error"]


@pytest.mark.anyio
async def test_claude_backend_cli_not_found(db, monkeypatch):
    """CLINotFoundError should tell the user to install Claude Code."""
    exc = CLINotFoundError("Claude Code not found", cli_path="/usr/bin/claude")
    payload = await _collect_claude_error(db, monkeypatch, exc)

    assert "error" in payload
    assert "claude" in payload["error"].lower()


@pytest.mark.anyio
async def test_claude_backend_cli_connection_error(db, monkeypatch):
    """CLIConnectionError should report connection failure."""
    exc = CLIConnectionError("Connection refused")
    payload = await _collect_claude_error(db, monkeypatch, exc)

    assert "error" in payload
    assert "claude" in payload["error"].lower()
