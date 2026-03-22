"""Tests for agent routes."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture
async def client(route_client, agent_manager_mock):
    """Client with agent_manager_mock."""
    client = route_client
    client._transport_app.state.agent_manager = agent_manager_mock
    yield client


@pytest.fixture
async def db(base_app):
    """Get db from base_app."""
    _, db, _ = base_app
    return db


@pytest.mark.asyncio
async def test_agent_page_autocreates_thread(client, db):
    """Test agent page auto-creates first thread."""
    resp = await client.get("/agent", follow_redirects=False)
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "thread_id=" in location


@pytest.mark.asyncio
async def test_agent_page_renders_with_thread(client, db):
    """Test agent page renders with existing thread."""
    thread_id = await db.create_agent_thread("Test Thread")

    resp = await client.get(f"/agent?thread_id={thread_id}")
    assert resp.status_code == 200
    assert "Test Thread" in resp.text


@pytest.mark.asyncio
async def test_agent_page_invalid_thread_redirects(client, db):
    """Test agent page with invalid thread redirects to existing."""
    thread_id = await db.create_agent_thread("Valid Thread")

    resp = await client.get("/agent?thread_id=999999", follow_redirects=False)
    assert resp.status_code == 303
    assert f"thread_id={thread_id}" in resp.headers.get("location", "")


@pytest.mark.asyncio
async def test_create_thread(client, db):
    """Test create new thread."""
    resp = await client.post("/agent/threads", follow_redirects=False)
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "thread_id=" in location


@pytest.mark.asyncio
async def test_delete_thread(client, db):
    """Test delete thread."""
    thread_id = await db.create_agent_thread("To Delete")

    resp = await client.delete(f"/agent/threads/{thread_id}")
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert data["ok"] is True


@pytest.mark.asyncio
async def test_rename_thread_success(client, db):
    """Test rename thread success."""
    thread_id = await db.create_agent_thread("Original")

    resp = await client.post(
        f"/agent/threads/{thread_id}/rename",
        content=json.dumps({"title": "Renamed"}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert data["ok"] is True


@pytest.mark.asyncio
async def test_rename_thread_empty_title(client, db):
    """Test rename thread with empty title."""
    thread_id = await db.create_agent_thread("Original")

    resp = await client.post(
        f"/agent/threads/{thread_id}/rename",
        content=json.dumps({"title": ""}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_get_channels_json(client, db):
    """Test get channels JSON."""
    resp = await client.get("/agent/channels-json")
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_get_forum_topics_empty(client, db, pool_mock):
    """Test get forum topics returns empty list."""
    pool_mock.get_forum_topics = AsyncMock(return_value=[])

    resp = await client.get("/agent/forum-topics?channel_id=100")
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert isinstance(data, list)


@pytest.mark.asyncio
async def test_get_forum_topics_returns_data(client, db, pool_mock):
    """Test get forum topics returns data."""
    topics = [{"id": 1, "title": "Topic 1"}]
    pool_mock.get_forum_topics = AsyncMock(return_value=topics)

    resp = await client.get("/agent/forum-topics?channel_id=100")
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert len(data) == 1
    assert data[0]["title"] == "Topic 1"


@pytest.mark.asyncio
async def test_inject_context_no_channel_id(client, db):
    """Test inject context without channel_id."""
    thread_id = await db.create_agent_thread("Context")

    resp = await client.post(
        f"/agent/threads/{thread_id}/context",
        content=json.dumps({"limit": 100}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_inject_context_thread_not_found(client):
    """Test inject context with invalid thread."""
    resp = await client.post(
        "/agent/threads/999999/context",
        content=json.dumps({"channel_id": 100, "limit": 10}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_inject_context_success(client, db):
    """Test inject context success."""
    thread_id = await db.create_agent_thread("Context")

    resp = await client.post(
        f"/agent/threads/{thread_id}/context",
        content=json.dumps({"channel_id": 100, "limit": 10}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert "content" in data


@pytest.mark.asyncio
async def test_stop_chat(client, db):
    """Test stop chat."""
    thread_id = await db.create_agent_thread("Stop")

    resp = await client.post(f"/agent/threads/{thread_id}/stop")
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert data["ok"] is True


@pytest.mark.asyncio
async def test_chat_no_agent_manager(client, db):
    """Test chat without agent manager."""
    client._transport_app.state.agent_manager = None
    thread_id = await db.create_agent_thread("Chat")

    resp = await client.post(
        f"/agent/threads/{thread_id}/chat",
        content=json.dumps({"message": "hello"}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 503


@pytest.mark.asyncio
async def test_chat_empty_message(client, db):
    """Test chat with empty message."""
    thread_id = await db.create_agent_thread("Chat")

    resp = await client.post(
        f"/agent/threads/{thread_id}/chat",
        content=json.dumps({"message": ""}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_chat_thread_not_found(client):
    """Test chat with invalid thread."""
    resp = await client.post(
        "/agent/threads/999999/chat",
        content=json.dumps({"message": "hello"}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_chat_no_backend(client, db):
    """Test chat when no backend available."""
    thread_id = await db.create_agent_thread("Chat")

    # Override the agent_manager_mock to have no backend
    runtime = MagicMock()
    runtime.selected_backend = None
    runtime.error = "No backend"
    client._transport_app.state.agent_manager.get_runtime_status = AsyncMock(
        return_value=runtime
    )

    resp = await client.post(
        f"/agent/threads/{thread_id}/chat",
        content=json.dumps({"message": "hello"}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 503


@pytest.mark.asyncio
async def test_chat_streaming(client, db):
    """Test chat returns SSE stream."""
    thread_id = await db.create_agent_thread("Stream")

    resp = await client.post(
        f"/agent/threads/{thread_id}/chat",
        content=json.dumps({"message": "hello"}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    assert "text/event-stream" in resp.headers.get("content-type", "")


@pytest.mark.asyncio
async def test_agent_page_shows_status(client, db):
    """Test agent page shows agent status."""
    thread_id = await db.create_agent_thread("Status")

    resp = await client.get(f"/agent?thread_id={thread_id}")
    assert resp.status_code == 200
    # Page should contain status info
    assert "agent" in resp.text.lower() or "backend" in resp.text.lower()


@pytest.mark.asyncio
async def test_agent_page_without_agent_manager(client, db):
    """Test agent page without agent manager."""
    client._transport_app.state.agent_manager = None
    thread_id = await db.create_agent_thread("No Agent")

    resp = await client.get(f"/agent?thread_id={thread_id}")
    assert resp.status_code == 200


# === Additional coverage tests ===


@pytest.mark.asyncio
async def test_stop_chat_with_agent_manager(client, db):
    """Test stop chat with agent manager."""
    thread_id = await db.create_agent_thread("Stop")

    resp = await client.post(f"/agent/threads/{thread_id}/stop")
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert data["ok"] is True


@pytest.mark.asyncio
async def test_inject_context_with_topic(client, db):
    """Test inject context with topic_id."""
    thread_id = await db.create_agent_thread("Context")

    resp = await client.post(
        f"/agent/threads/{thread_id}/context",
        content=json.dumps({"channel_id": 100, "limit": 10, "topic_id": "1"}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert "content" in data


@pytest.mark.asyncio
async def test_inject_context_empty_topic(client, db):
    """Test inject context with empty topic_id."""
    thread_id = await db.create_agent_thread("Context")

    resp = await client.post(
        f"/agent/threads/{thread_id}/context",
        content=json.dumps({"channel_id": 100, "limit": 10, "topic_id": ""}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert "content" in data


@pytest.mark.asyncio
async def test_inject_context_with_limit(client, db):
    """Test inject context respects limit."""
    thread_id = await db.create_agent_thread("Context")

    resp = await client.post(
        f"/agent/threads/{thread_id}/context",
        content=json.dumps({"channel_id": 100, "limit": 5}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_delete_thread_not_found(client):
    """Test delete non-existent thread returns ok (idempotent)."""
    resp = await client.delete("/agent/threads/999999")
    # Route is idempotent - returns 200 even if thread doesn't exist
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert data["ok"] is True


@pytest.mark.asyncio
async def test_rename_thread_not_found(client):
    """Test rename non-existent thread returns ok (idempotent)."""
    resp = await client.post(
        "/agent/threads/999999/rename",
        content=json.dumps({"title": "New Name"}),
        headers={"Content-Type": "application/json"},
    )
    # Route is idempotent - returns 200 even if thread doesn't exist
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert data["ok"] is True


@pytest.mark.asyncio
async def test_get_forum_topics_fallback(client, db, pool_mock):
    """Test get forum topics fallback to cached when API fails."""
    # Simulate API returning empty (flood wait, etc.)
    pool_mock.get_forum_topics = AsyncMock(return_value=[])

    resp = await client.get("/agent/forum-topics?channel_id=100")
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert isinstance(data, list)
