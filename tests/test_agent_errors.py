import json
from unittest.mock import PropertyMock, patch

import pytest

from src.agent.manager import AgentManager, ClaudeSdkBackend, DeepagentsBackend


@pytest.mark.asyncio
async def test_agent_manager_handles_ollama_500_error(db, monkeypatch):
    """Test that Ollama 500 errors are translated to friendly messages."""

    # Setup
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)

    mgr = AgentManager(db)

    # Mock DeepagentsBackend to raise the specific error
    async def mock_chat_stream(*args, **kwargs):
        raise RuntimeError("ollama: Internal Server Error (status code: 500)")

    # Patch the CLASS properties, not the instance attributes
    with patch.object(DeepagentsBackend, "available", new_callable=PropertyMock) as mock_deep_avail, \
         patch.object(ClaudeSdkBackend, "available", new_callable=PropertyMock) as mock_claude_avail, \
         patch.object(DeepagentsBackend, "chat_stream", side_effect=mock_chat_stream):

        mock_deep_avail.return_value = True
        mock_claude_avail.return_value = False

        # Create a thread
        thread_id = await db.create_agent_thread("test thread")
        await db.save_agent_message(thread_id, "user", "hello")

        # Run chat stream
        chunks = []
        async for chunk in mgr.chat_stream(thread_id, "hello"):
            chunks.append(chunk)

        # Verify
        assert len(chunks) > 0
        last_chunk = chunks[-1]
        assert "data: " in last_chunk
        payload = json.loads(last_chunk.replace("data: ", ""))

        assert "error" in payload
        error_msg = payload["error"]
        print(f"Got error message: {error_msg}")

        # Check for friendly message
        assert "Внутренняя ошибка сервиса Ollama (500)" in error_msg
        assert "Возможно, модель не загрузилась" in error_msg

@pytest.mark.asyncio
async def test_agent_manager_handles_ollama_connection_error(db, monkeypatch):
    """Test that Ollama connection errors are translated to friendly messages."""

    # Setup
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)

    mgr = AgentManager(db)

    # Mock DeepagentsBackend to raise connection error
    async def mock_chat_stream(*args, **kwargs):
        raise RuntimeError("ollama: connection refused")

    with patch.object(DeepagentsBackend, "available", new_callable=PropertyMock) as mock_deep_avail, \
         patch.object(ClaudeSdkBackend, "available", new_callable=PropertyMock) as mock_claude_avail, \
         patch.object(DeepagentsBackend, "chat_stream", side_effect=mock_chat_stream):

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

@pytest.mark.asyncio
async def test_agent_manager_handles_generic_error(db, monkeypatch):
    """Test that generic errors are passed through (with prefix)."""

    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)

    mgr = AgentManager(db)

    async def mock_chat_stream(*args, **kwargs):
        raise RuntimeError("Some random error")

    with patch.object(DeepagentsBackend, "available", new_callable=PropertyMock) as mock_deep_avail, \
         patch.object(ClaudeSdkBackend, "available", new_callable=PropertyMock) as mock_claude_avail, \
         patch.object(DeepagentsBackend, "chat_stream", side_effect=mock_chat_stream):

        mock_deep_avail.return_value = True
        mock_claude_avail.return_value = False

        thread_id = await db.create_agent_thread("test thread 3")
        await db.save_agent_message(thread_id, "user", "hello")

        chunks = []
        async for chunk in mgr.chat_stream(thread_id, "hello"):
            chunks.append(chunk)

        payload = json.loads(chunks[-1].replace("data: ", ""))
        error_msg = payload["error"]

        # Should contain original error
        assert "Some random error" in error_msg
        # Should NOT contain our friendly messages
        assert "Внутренняя ошибка сервиса Ollama" not in error_msg
