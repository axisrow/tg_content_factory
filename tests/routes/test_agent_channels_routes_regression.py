"""Tests for agent and channel route regression paths."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
async def client(route_client, agent_manager_mock):
    """Client with agent_manager_mock."""
    route_client._transport_app.state.agent_manager = agent_manager_mock
    yield route_client


@pytest.fixture
async def db(base_app):
    _, db, _ = base_app
    return db


# ============================================================================
# src/web/routes/agent.py -- edge-case lines
# ============================================================================


@pytest.mark.anyio
async def test_agent_page_no_threads_creates_thread(client, db):
    """Covers auto-creating thread when none exist."""
    threads = await db.get_agent_threads()
    for t in threads:
        await db.delete_agent_thread(t["id"])

    resp = await client.get("/agent", follow_redirects=False)
    assert resp.status_code == 303
    assert "thread_id=" in resp.headers.get("location", "")


@pytest.mark.anyio
async def test_agent_page_has_threads_no_thread_id(client, db):
    """Covers redirect to first thread when threads exist but no thread_id param."""
    tid = await db.create_agent_thread("First")
    resp = await client.get("/agent", follow_redirects=False)
    assert resp.status_code == 303
    assert f"thread_id={tid}" in resp.headers.get("location", "")


@pytest.mark.anyio
async def test_agent_chat_whitespace_message(client, db):
    """Covers chat with whitespace-only message."""
    tid = await db.create_agent_thread("Chat")
    resp = await client.post(
        f"/agent/threads/{tid}/chat",
        content=json.dumps({"message": "   "}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400


@pytest.mark.anyio
async def test_agent_chat_invalid_model(client, db):
    """Covers chat with unrecognized model parameter."""
    tid = await db.create_agent_thread("Chat")
    resp = await client.post(
        f"/agent/threads/{tid}/chat",
        content=json.dumps({"message": "hello", "model": "nonexistent-model"}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200


# ============================================================================
# src/web/routes/channels.py -- tag + command endpoints
# ============================================================================


@pytest.mark.anyio
async def test_channels_list_tags_empty(client, db):
    """Covers GET /channels/tags with no tags."""
    resp = await client.get("/channels/tags")
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert "tags" in data


@pytest.mark.anyio
async def test_channels_create_tag(client, db):
    """Covers POST /channels/tags creates a tag."""
    resp = await client.post(
        "/channels/tags",
        data={"name": "test-tag"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=tag_created" in resp.headers["location"]


@pytest.mark.anyio
async def test_channels_delete_tag(client, db):
    """Covers DELETE /channels/tags/{name}."""
    await db.repos.channels.create_tag("del-me")
    resp = await client.delete("/channels/tags/del-me")
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert data["ok"] is True


@pytest.mark.anyio
async def test_channels_get_channel_tags(client, db):
    """Covers GET /channels/{pk}/tags."""
    from src.models import Channel

    await db.add_channel(Channel(channel_id=9999, title="TagTest"))
    channels = await db.get_channels_with_counts()
    pk = next(c.id for c in channels if c.channel_id == 9999)

    resp = await client.get(f"/channels/{pk}/tags")
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert "tags" in data


@pytest.mark.anyio
async def test_channels_set_channel_tags(client, db):
    """Covers POST /channels/{pk}/tags."""
    from src.models import Channel

    await db.add_channel(Channel(channel_id=8888, title="SetTags"))
    channels = await db.get_channels_with_counts()
    pk = next(c.id for c in channels if c.channel_id == 8888)

    resp = await client.post(
        f"/channels/{pk}/tags",
        data={"tags": "tag1, tag2,tag3"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=tags_updated" in resp.headers["location"]


@pytest.mark.anyio
async def test_channels_refresh_types(client, db):
    """Covers POST /channels/refresh-types."""
    with patch("src.web.routes.channels.deps.telegram_command_service") as mock_svc:
        mock_telegram_svc = MagicMock()
        mock_telegram_svc.enqueue = AsyncMock(return_value=42)
        mock_svc.return_value = mock_telegram_svc

        resp = await client.post("/channels/refresh-types", follow_redirects=False)
        assert resp.status_code == 303
        assert "command_id=42" in resp.headers["location"]


@pytest.mark.anyio
async def test_channels_refresh_meta(client, db):
    """Covers POST /channels/refresh-meta."""
    with patch("src.web.routes.channels.deps.telegram_command_service") as mock_svc:
        mock_telegram_svc = MagicMock()
        mock_telegram_svc.enqueue = AsyncMock(return_value=43)
        mock_svc.return_value = mock_telegram_svc

        resp = await client.post("/channels/refresh-meta", follow_redirects=False)
        assert resp.status_code == 303
        assert "command_id=43" in resp.headers["location"]


@pytest.mark.anyio
async def test_channels_add_empty_identifier(client, db):
    """Covers POST /channels/add with empty identifier."""
    resp = await client.post(
        "/channels/add",
        data={"identifier": "   "},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=resolve" in resp.headers["location"]


@pytest.mark.anyio
async def test_channels_show_all_view(client, db):
    """Covers channels list with view=all param."""
    resp = await client.get("/channels/?view=all")
    assert resp.status_code == 200
