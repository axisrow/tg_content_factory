from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.agent.manager import AgentManager
from src.agent.tools import make_mcp_server


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
