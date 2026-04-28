"""Tests for agent route streaming, generation, and large-context paths."""
from __future__ import annotations

import json
import sqlite3
from unittest.mock import patch

import pytest


@pytest.fixture
async def client(route_client, agent_manager_mock):
    client = route_client
    client._transport_app.state.agent_manager = agent_manager_mock
    yield client


@pytest.fixture
async def db(base_app):
    _, db, _ = base_app
    return db


# === inject_context: large context warning (line 181-185) ===


@pytest.mark.anyio
async def test_inject_context_large_context_warning(client, db):
    """Test inject context logs warning for very large content (>200K chars)."""
    thread_id = await db.create_agent_thread("Context")

    with patch("src.agent.context.format_context", return_value="x" * 250_000):
        resp = await client.post(
            f"/agent/threads/{thread_id}/context",
            content=json.dumps({"channel_id": 100, "limit": 50000}),
            headers={"Content-Type": "application/json"},
        )
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert len(data["content"]) > 200_000


# === chat: generate() — save assistant message on done (line 282-283) ===


@pytest.mark.anyio
async def test_chat_streaming_saves_assistant_message(client, db):
    """Test chat streaming saves assistant message when done."""
    thread_id = await db.create_agent_thread("Chat")

    resp = await client.post(
        f"/agent/threads/{thread_id}/chat",
        content=json.dumps({"message": "hello"}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200

    # Consume the stream to trigger the save
    async for line in resp.aiter_lines():
        pass

    messages = await db.get_agent_messages(thread_id)
    assistant_msgs = [m for m in messages if m["role"] == "assistant"]
    assert len(assistant_msgs) == 1
    assert assistant_msgs[0]["content"] == "hi"


# === chat: generate() — IntegrityError on save (line 284-285) ===


@pytest.mark.anyio
async def test_chat_streaming_integrity_error_on_save(client, db):
    """Test chat streaming handles IntegrityError when saving assistant message."""
    thread_id = await db.create_agent_thread("Chat")

    mock_mgr = client._transport_app.state.agent_manager

    async def _fake_stream(*a, **kw):
        yield 'data: {"done": true, "full_text": "response"}\n\n'

    mock_mgr.chat_stream = _fake_stream

    real_save = db.save_agent_message

    async def _failing_save(*args, **kwargs):
        if args[1] == "assistant":
            raise sqlite3.IntegrityError("deleted")
        return await real_save(*args, **kwargs)

    with patch.object(db, "save_agent_message", side_effect=_failing_save):
        resp = await client.post(
            f"/agent/threads/{thread_id}/chat",
            content=json.dumps({"message": "hello"}),
            headers={"Content-Type": "application/json"},
        )
    assert resp.status_code == 200
    async for line in resp.aiter_lines():
        pass


# === chat: generate() — error in stream (line 286-290) ===


@pytest.mark.anyio
async def test_chat_streaming_error_in_stream(client, db):
    """Test chat streaming handles error chunk from agent."""
    thread_id = await db.create_agent_thread("Chat")

    mock_mgr = client._transport_app.state.agent_manager

    async def _error_stream(*a, **kw):
        yield 'data: {"error": "model overloaded"}\n\n'

    mock_mgr.chat_stream = _error_stream

    resp = await client.post(
        f"/agent/threads/{thread_id}/chat",
        content=json.dumps({"message": "hello"}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    async for line in resp.aiter_lines():
        pass


# === chat: generate() — IntegrityError on delete_last_exchange (line 289) ===


@pytest.mark.anyio
async def test_chat_streaming_error_integrity_delete(client, db):
    """Test chat streaming handles IntegrityError during error cleanup."""
    thread_id = await db.create_agent_thread("Chat")

    mock_mgr = client._transport_app.state.agent_manager

    async def _error_stream(*a, **kw):
        yield 'data: {"error": "failed"}\n\n'

    mock_mgr.chat_stream = _error_stream

    with patch.object(db, "delete_last_agent_exchange", side_effect=sqlite3.IntegrityError("gone")):
        resp = await client.post(
            f"/agent/threads/{thread_id}/chat",
            content=json.dumps({"message": "hello"}),
            headers={"Content-Type": "application/json"},
        )
    assert resp.status_code == 200
    async for line in resp.aiter_lines():
        pass


# === chat: generate() — JSONDecodeError (line 291) ===


@pytest.mark.anyio
async def test_chat_streaming_malformed_chunk(client, db):
    """Test chat streaming skips malformed JSON chunks."""
    thread_id = await db.create_agent_thread("Chat")

    mock_mgr = client._transport_app.state.agent_manager

    async def _bad_stream(*a, **kw):
        yield "data: not-json\n\n"
        yield 'data: {"done": true, "full_text": "ok"}\n\n'

    mock_mgr.chat_stream = _bad_stream

    resp = await client.post(
        f"/agent/threads/{thread_id}/chat",
        content=json.dumps({"message": "hello"}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    async for line in resp.aiter_lines():
        pass


# === chat: generate() — generic exception (line 293) ===


@pytest.mark.anyio
async def test_chat_streaming_generic_exception(client, db):
    """Test chat streaming handles generic exceptions during message processing."""
    thread_id = await db.create_agent_thread("Chat")

    mock_mgr = client._transport_app.state.agent_manager

    async def _fail_stream(*a, **kw):
        yield 'data: {"done": true, "full_text": "response"}\n\n'

    mock_mgr.chat_stream = _fail_stream

    real_save = db.save_agent_message

    async def _failing_save(*args, **kwargs):
        if args[1] == "assistant":
            raise RuntimeError("unexpected")
        return await real_save(*args, **kwargs)

    with patch.object(db, "save_agent_message", side_effect=_failing_save):
        resp = await client.post(
            f"/agent/threads/{thread_id}/chat",
            content=json.dumps({"message": "hello"}),
            headers={"Content-Type": "application/json"},
        )
    assert resp.status_code == 200
    async for line in resp.aiter_lines():
        pass


# === chat: no model parameter (line 232-233) ===


@pytest.mark.anyio
async def test_chat_with_invalid_model_ignored(client, db):
    """Test chat with invalid model string is ignored (model=None)."""
    thread_id = await db.create_agent_thread("Chat")

    resp = await client.post(
        f"/agent/threads/{thread_id}/chat",
        content=json.dumps({"message": "hello", "model": "nonexistent-model"}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200


# === forum-topics cached (line 130-131) ===


@pytest.mark.anyio
async def test_forum_topics_cached(client, db):
    """Test get forum topics returns cached data from DB."""
    # Insert topics via upsert_forum_topics (not RuntimeSnapshot)
    await db.upsert_forum_topics(100, [{"id": 1, "title": "Cached Topic"}])

    resp = await client.get("/agent/forum-topics?channel_id=100")
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert len(data) == 1
    assert data[0]["title"] == "Cached Topic"
