"""Tests for agent route thread selection, context, and forum topic paths."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
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


# ── agent_page: lines 48-55 (redirects) ────────────────────────────────


@pytest.mark.anyio
async def test_agent_page_no_threads_creates_thread(client, db):
    """Test agent page auto-creates thread when none exist (lines 57-58)."""
    # Ensure no threads
    threads = await db.get_agent_threads()
    for t in threads:
        await db.delete_agent_thread(t["id"])

    resp = await client.get("/agent", follow_redirects=False)
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "thread_id=" in location
    # Verify thread was actually created
    assert "/agent?thread_id=" in location


@pytest.mark.anyio
async def test_agent_page_no_thread_id_redirects_to_first(client, db):
    """Test agent page with no thread_id redirects to first thread (lines 53-55)."""
    thread_id = await db.create_agent_thread("First Thread")

    resp = await client.get("/agent", follow_redirects=False)
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert f"thread_id={thread_id}" in location


@pytest.mark.anyio
async def test_agent_page_invalid_thread_id_redirects(client, db):
    """Test agent page with invalid thread_id redirects to first (lines 48-50)."""
    thread_id = await db.create_agent_thread("Valid")

    resp = await client.get("/agent?thread_id=999999", follow_redirects=False)
    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert f"thread_id={thread_id}" in location


# ── rename_thread: line 95-99 ──────────────────────────────────────────


@pytest.mark.anyio
async def test_rename_thread_whitespace_title(client, db):
    """Test rename thread with whitespace-only title (line 96)."""
    thread_id = await db.create_agent_thread("Original")

    resp = await client.post(
        f"/agent/threads/{thread_id}/rename",
        content=json.dumps({"title": "   "}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400


# ── get_forum_topics: lines 126-128, 132-136 ───────────────────────────


@pytest.mark.anyio
async def test_get_forum_topics_api_fails_falls_back_to_db(client, db, pool_mock):
    """Test get forum topics falls back to DB cache when API returns None (lines 131-136)."""
    pool_mock.get_forum_topics = AsyncMock(return_value=None)

    resp = await client.get("/agent/forum-topics?channel_id=100")
    assert resp.status_code == 202
    data = json.loads(resp.text)
    assert "command_id" in data


@pytest.mark.anyio
async def test_get_forum_topics_api_returns_data_caches_to_db(client, db, pool_mock):
    """Test get forum topics caches fresh data to DB (lines 126-128)."""
    topics = [{"id": 1, "title": "Topic 1"}, {"id": 2, "title": "Topic 2"}]
    pool_mock.get_forum_topics = AsyncMock(return_value=topics)

    resp = await client.get("/agent/forum-topics?channel_id=100")
    assert resp.status_code == 202
    data = json.loads(resp.text)
    assert "command_id" in data


# ── inject_context: line 134, 142-153 ──────────────────────────────────


@pytest.mark.anyio
async def test_inject_context_with_topic_id_none_string(client, db):
    """Test inject context with topic_id as 'None' string (line 150)."""
    thread_id = await db.create_agent_thread("Context")

    resp = await client.post(
        f"/agent/threads/{thread_id}/context",
        content=json.dumps({"channel_id": 100, "limit": 10, "topic_id": None}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert "content" in data


@pytest.mark.anyio
async def test_inject_context_large_limit_capped(client, db):
    """Test inject context with limit > 10000 is capped (line 147)."""
    thread_id = await db.create_agent_thread("Context")

    resp = await client.post(
        f"/agent/threads/{thread_id}/context",
        content=json.dumps({"channel_id": 100, "limit": 50000}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert "content" in data


@pytest.mark.anyio
async def test_inject_context_zero_limit_uses_default(client, db):
    """Test inject context with limit=0 uses default 10000 (line 147)."""
    thread_id = await db.create_agent_thread("Context")

    resp = await client.post(
        f"/agent/threads/{thread_id}/context",
        content=json.dumps({"channel_id": 100, "limit": 0}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200


# ── resolve_permission: lines 196-204 ──────────────────────────────────


@pytest.mark.anyio
async def test_resolve_permission_invalid_choice(client, db):
    """Test resolve permission with invalid choice (line 198)."""
    thread_id = await db.create_agent_thread("Perm")

    resp = await client.post(
        f"/agent/threads/{thread_id}/permission/test-request-id",
        content=json.dumps({"choice": "invalid"}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400


@pytest.mark.anyio
async def test_resolve_permission_no_agent_manager(client, db):
    """Test resolve permission when agent_manager is None (lines 200-202)."""
    thread_id = await db.create_agent_thread("Perm")
    client._transport_app.state.agent_manager = None

    resp = await client.post(
        f"/agent/threads/{thread_id}/permission/test-request-id",
        content=json.dumps({"choice": "once"}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 503


@pytest.mark.anyio
async def test_resolve_permission_valid_choices(client, db):
    """Test resolve permission with all valid choices."""
    thread_id = await db.create_agent_thread("Perm")

    for choice in ("once", "session", "deny"):
        gate = MagicMock()
        gate.resolve = MagicMock(return_value=True)
        client._transport_app.state.agent_manager.permission_gate = gate

        resp = await client.post(
            f"/agent/threads/{thread_id}/permission/test-request-id",
            content=json.dumps({"choice": choice}),
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 200
        data = json.loads(resp.text)
        assert data["ok"] is True


@pytest.mark.anyio
async def test_resolve_permission_unknown_request_returns_404(client, db):
    """Unknown or already-resolved permission requests must not look successful."""
    thread_id = await db.create_agent_thread("Perm")
    gate = MagicMock()
    gate.resolve = MagicMock(return_value=False)
    client._transport_app.state.agent_manager.permission_gate = gate

    resp = await client.post(
        f"/agent/threads/{thread_id}/permission/missing-request-id",
        content=json.dumps({"choice": "session"}),
        headers={"Content-Type": "application/json"},
    )

    assert resp.status_code == 404
    gate.resolve.assert_called_once_with("missing-request-id", "session")


@pytest.mark.anyio
async def test_resolve_permission_session_choice_updates_gate(client, db):
    """Web resolve route stores a real session approval when the browser POST arrives."""
    from src.agent.permission_gate import (
        AgentRequestContext,
        PermissionGate,
        reset_request_context,
        set_request_context,
    )

    thread_id = await db.create_agent_thread("Perm")
    gate = PermissionGate()
    session_id = "web-session"
    phone = "+66982102247"
    client._transport_app.state.agent_manager.permission_gate = gate
    ctx = AgentRequestContext(
        session_id=session_id,
        thread_id=thread_id,
        queue=asyncio.Queue(),
        permission_gate=gate,
        permission_timeout=5,
    )
    token = set_request_context(ctx)
    try:
        pending = asyncio.create_task(gate.check("send_reaction", phone))
        event = await asyncio.wait_for(ctx.queue.get(), timeout=1)
        payload = json.loads(event.removeprefix("data: ").strip())

        resp = await client.post(
            f"/agent/threads/{thread_id}/permission/{payload['request_id']}",
            content=json.dumps({"choice": "session"}),
            headers={"Content-Type": "application/json"},
        )

        assert resp.status_code == 200
        assert resp.json()["ok"] is True
        assert await asyncio.wait_for(pending, timeout=1) is None
    finally:
        reset_request_context(token)

    assert gate.is_session_approved("send_reaction", session_id, phone)


# ── stop_chat: lines 211-213 ───────────────────────────────────────────


@pytest.mark.anyio
async def test_stop_chat_with_agent_manager_cancel(client, db):
    """Test stop chat calls cancel_stream on agent manager (lines 212-213)."""
    thread_id = await db.create_agent_thread("Stop")
    mock_mgr = client._transport_app.state.agent_manager
    mock_mgr.cancel_stream = AsyncMock(return_value=True)

    resp = await client.post(f"/agent/threads/{thread_id}/stop")
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert data["cancelled"] is True
    mock_mgr.cancel_stream.assert_called_once_with(thread_id)


@pytest.mark.anyio
async def test_stop_chat_no_agent_manager(client, db):
    """Test stop chat without agent manager (line 211)."""
    thread_id = await db.create_agent_thread("Stop")
    client._transport_app.state.agent_manager = None

    resp = await client.post(f"/agent/threads/{thread_id}/stop")
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert data["cancelled"] is False


# ── chat: lines 221-241, 248, 264-282 ─────────────────────────────────


@pytest.mark.anyio
async def test_chat_using_override_with_error(client, db):
    """Test chat when using_override is True with error (lines 237-238)."""
    thread_id = await db.create_agent_thread("Chat")

    runtime = MagicMock()
    runtime.selected_backend = "deepagents"
    runtime.using_override = True
    runtime.error = "Provider unavailable"
    client._transport_app.state.agent_manager.get_runtime_status = AsyncMock(
        return_value=runtime
    )

    resp = await client.post(
        f"/agent/threads/{thread_id}/chat",
        content=json.dumps({"message": "hello"}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 503


@pytest.mark.anyio
async def test_chat_prompt_too_large(client, db):
    """Test chat when estimated prompt exceeds 100K tokens (lines 247-251)."""
    thread_id = await db.create_agent_thread("Chat")

    client._transport_app.state.agent_manager.estimate_prompt_tokens = AsyncMock(
        return_value=150_000
    )

    resp = await client.post(
        f"/agent/threads/{thread_id}/chat",
        content=json.dumps({"message": "hello"}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400


@pytest.mark.anyio
async def test_chat_auto_renames_thread(client, db):
    """Test chat auto-renames thread from first message (lines 257-258)."""
    thread_id = await db.create_agent_thread("Новый тред")

    resp = await client.post(
        f"/agent/threads/{thread_id}/chat",
        content=json.dumps({"message": "My first message"}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200

    # Verify thread was renamed
    thread = await db.get_agent_thread(thread_id)
    assert thread["title"] == "My first message"


# ── thread messages JSON (parity: agent messages) ─────────────────────


@pytest.mark.anyio
async def test_get_thread_messages_json(client, db):
    thread_id = await db.create_agent_thread("Conv")
    await db.save_agent_message(thread_id, "user", "hi there")
    await db.save_agent_message(thread_id, "assistant", "hello")

    resp = await client.get(f"/agent/threads/{thread_id}/messages")
    assert resp.status_code == 200
    data = resp.json()
    assert data["thread_id"] == thread_id
    assert len(data["messages"]) == 2
    assert data["messages"][0]["content"] == "hi there"


@pytest.mark.anyio
async def test_get_thread_messages_not_found(client):
    resp = await client.get("/agent/threads/9999/messages")
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_chat_does_not_rename_custom_thread(client, db):
    """Test chat does not rename thread with custom title."""
    thread_id = await db.create_agent_thread("Custom Title")

    resp = await client.post(
        f"/agent/threads/{thread_id}/chat",
        content=json.dumps({"message": "Hello world"}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200

    # Thread title should remain unchanged
    thread = await db.get_agent_thread(thread_id)
    assert thread["title"] == "Custom Title"


@pytest.mark.anyio
async def test_chat_with_model_parameter(client, db):
    """Test chat with explicit model parameter (line 225-226)."""
    thread_id = await db.create_agent_thread("Chat")

    from src.agent.models import CLAUDE_MODEL_IDS

    if CLAUDE_MODEL_IDS:
        model = list(CLAUDE_MODEL_IDS)[0]
    else:
        model = None

    resp = await client.post(
        f"/agent/threads/{thread_id}/chat",
        content=json.dumps({"message": "hello", "model": model}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200


@pytest.mark.anyio
@pytest.mark.parametrize("backend", ["codex", "adk"])
async def test_chat_codex_adk_model_survives_validation(client, db, backend):
    """A submitted model ID must not crash chat for codex/adk backends (#1002).

    These backends have no UI model picker; the page no longer renders a Claude
    dropdown for them, but a stale localStorage value could still POST a model.
    The web handler must accept the request (200) and forward the model to
    chat_stream, which is where the per-backend allow-list (model_for_backend)
    drops a cross-backend ID. We capture the model kwarg and assert the exact
    value forwarded so a wrong value can't slip past a mere membership check.
    """
    thread_id = await db.create_agent_thread("Chat")

    runtime = MagicMock()
    runtime.selected_backend = backend
    runtime.using_override = False
    runtime.error = None
    client._transport_app.state.agent_manager.get_runtime_status = AsyncMock(return_value=runtime)

    sentinel = object()
    captured = {"model": sentinel}

    async def fake_stream(*args, **kwargs):
        captured["model"] = kwargs.get("model")
        yield 'data: {"done": true, "full_text": "ok"}\n\n'

    client._transport_app.state.agent_manager.chat_stream = fake_stream

    # A stale Claude model id from localStorage must not 500 the codex/adk chat.
    # select_model() accepts it (a real Claude ID), so the handler forwards it
    # verbatim; chat_stream is where model_for_backend would then drop it.
    resp = await client.post(
        f"/agent/threads/{thread_id}/chat",
        content=json.dumps({"message": "hi", "model": "claude-sonnet-4-6"}),
        headers={"Content-Type": "application/json"},
    )

    assert resp.status_code == 200
    assert captured["model"] == "claude-sonnet-4-6"  # reached chat_stream, exact value

    # An unknown/garbage model id is coerced to None by the handler's select_model
    # before chat_stream ever sees it — never a 500.
    captured["model"] = sentinel
    resp = await client.post(
        f"/agent/threads/{thread_id}/chat",
        content=json.dumps({"message": "hi again", "model": "not-a-real-model"}),
        headers={"Content-Type": "application/json"},
    )

    assert resp.status_code == 200
    assert captured["model"] is None


@pytest.mark.anyio
async def test_chat_enables_interactive_permissions(client, db):
    """Web chat requests opt into request-scoped PermissionGate prompts."""
    thread_id = await db.create_agent_thread("Chat")
    captured = {}

    async def fake_stream(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        yield 'data: {"done": true, "full_text": "ok"}\n\n'

    client._transport_app.state.agent_manager.chat_stream = fake_stream

    resp = await client.post(
        f"/agent/threads/{thread_id}/chat",
        content=json.dumps({"message": "hello"}),
        headers={"Content-Type": "application/json"},
    )

    assert resp.status_code == 200
    assert captured["kwargs"]["interactive_permissions"] is True


def test_permission_dialog_items_all_have_cursor_span():
    """Every permission menu item must satisfy highlight()'s .perm-cursor lookup."""
    template = Path("src/web/templates/agent.html").read_text(encoding="utf-8")
    for choice in ("once", "session", "deny"):
        marker = f'class="perm-item" data-choice="{choice}"'
        start = template.index(marker)
        end = template.index("</div>", start)
        item_markup = template[start:end]
        assert 'class="perm-cursor"' in item_markup


def test_permission_dialog_keyboard_and_post_contract():
    """Pressing 2 must map to session and POST failures must not auto-deny."""
    template = Path("src/web/templates/agent.html").read_text(encoding="utf-8")

    assert "document.addEventListener('keydown', onKey, true)" in template
    assert "document.removeEventListener('keydown', onKey, true)" in template
    assert "code === 'Digit2' || code === 'Numpad2'" in template
    assert "pick('session')" in template
    assert "resolvePermissionRequest(data.request_id, choice)" in template
    assert "!permissionResp.ok || !permissionResult.ok" in template
    assert "while (true)" in template
    assert "showPermissionDialog(data.tool, data.phone || '', permissionError)" in template
    assert "Не удалось отправить выбор. Проверьте соединение и выберите ещё раз." in template
    assert "body: JSON.stringify({choice: 'deny'})" not in template
    assert "истечёт по таймауту" not in template


# ── delete_thread: lines 86-88 (permission gate clearing) ──────────────


@pytest.mark.anyio
async def test_delete_thread_clears_permission_gate(client, db):
    """Test delete thread clears permission gate for session (lines 86-88)."""
    thread_id = await db.create_agent_thread("Perm")

    mock_mgr = MagicMock()
    mock_mgr.cancel_stream = AsyncMock(return_value=True)
    mock_gate = MagicMock()
    mock_gate.clear_thread = MagicMock()
    mock_gate.clear_session = MagicMock()
    mock_mgr.permission_gate = mock_gate
    client._transport_app.state.agent_manager = mock_mgr

    resp = await client.delete(f"/agent/threads/{thread_id}")
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert data["cancelled"] is True
    mock_mgr.cancel_stream.assert_called_once_with(thread_id, wait_timeout=5.0)
    mock_gate.clear_thread.assert_called_once_with("web", thread_id)
    mock_gate.clear_session.assert_called_once_with("web")


@pytest.mark.anyio
async def test_delete_thread_no_permission_gate(client, db):
    """Test delete thread with no permission_gate (line 87)."""
    thread_id = await db.create_agent_thread("Perm")

    mock_mgr = client._transport_app.state.agent_manager
    mock_mgr.permission_gate = None

    resp = await client.delete(f"/agent/threads/{thread_id}")
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert data["ok"] is True


@pytest.mark.anyio
async def test_delete_thread_no_agent_manager(client, db):
    """Test delete thread when agent_manager is None (line 86)."""
    thread_id = await db.create_agent_thread("Perm")
    client._transport_app.state.agent_manager = None

    resp = await client.delete(f"/agent/threads/{thread_id}")
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert data["ok"] is True


# ── get_channels_json: line 106 ────────────────────────────────────────


@pytest.mark.anyio
async def test_get_channels_json_with_data(client, db):
    """Test get channels JSON with active channels."""
    from src.models import Channel

    await db.add_channel(Channel(channel_id=200, title="Active Channel", channel_type="channel"))

    resp = await client.get("/agent/channels-json")
    assert resp.status_code == 200
    data = json.loads(resp.text)
    assert isinstance(data, list)
    # Should include at least the active channel
    assert any(c["id"] == 200 for c in data)
