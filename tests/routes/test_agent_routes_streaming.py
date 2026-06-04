"""Tests for agent route streaming, generation, and large-context paths."""
from __future__ import annotations

import asyncio
import json
import sqlite3
from unittest.mock import AsyncMock, patch

import pytest

from src.database import DatabaseBusyError


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


@pytest.mark.anyio
async def test_inject_context_rejects_non_object_json(client, db):
    thread_id = await db.create_agent_thread("Context")

    resp = await client.post(
        f"/agent/threads/{thread_id}/context",
        content=json.dumps(["not", "an", "object"]),
        headers={"Content-Type": "application/json"},
    )

    assert resp.status_code == 400
    assert resp.json()["detail"] == "Request body must be a JSON object"


@pytest.mark.anyio
async def test_inject_context_rejects_non_integral_json_numbers(client, db):
    thread_id = await db.create_agent_thread("Context")

    bool_resp = await client.post(
        f"/agent/threads/{thread_id}/context",
        json={"channel_id": True, "limit": 10},
    )
    float_resp = await client.post(
        f"/agent/threads/{thread_id}/context",
        json={"channel_id": 100, "limit": 1.9},
    )

    assert bool_resp.status_code == 400
    assert bool_resp.json()["detail"] == "channel_id must be an integer"
    assert float_resp.status_code == 400
    assert float_resp.json()["detail"] == "limit must be an integer"


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


@pytest.mark.anyio
async def test_chat_database_busy_returns_retryable_503(client, db):
    thread_id = await db.create_agent_thread("Chat")

    with patch.object(
        db,
        "save_agent_message",
        side_effect=DatabaseBusyError("Database is busy. Retry the request in a few seconds."),
    ):
        resp = await client.post(
            f"/agent/threads/{thread_id}/chat",
            content=json.dumps({"message": "hello"}),
            headers={"Content-Type": "application/json"},
        )

    assert resp.status_code == 503
    assert resp.headers["retry-after"] == "2"
    assert "База данных занята" in resp.json()["detail"]


@pytest.mark.anyio
async def test_chat_rejects_malformed_json(client, db):
    thread_id = await db.create_agent_thread("Chat")

    resp = await client.post(
        f"/agent/threads/{thread_id}/chat",
        content="{",
        headers={"Content-Type": "application/json"},
    )

    assert resp.status_code == 400
    assert resp.json()["detail"] == "Request body must be valid JSON"


@pytest.mark.anyio
async def test_chat_streaming_total_timeout_cancels_with_bounded_wait(client, db, monkeypatch):
    from src.web.agent import handlers

    thread_id = await db.create_agent_thread("Chat")
    mock_mgr = client._transport_app.state.agent_manager
    mock_mgr.cancel_stream = AsyncMock(return_value=True)
    monkeypatch.setattr(handlers, "_SSE_KEEPALIVE_INTERVAL", 0.01)
    monkeypatch.setattr(client._transport_app.state.config.agent, "total_timeout", 0.02)
    cleanup_started = asyncio.Event()
    cleanup_release = asyncio.Event()

    async def _hung_stream(*a, **kw):
        try:
            await asyncio.Event().wait()
            yield 'data: {"done": true, "full_text": "late"}\n\n'
        finally:
            cleanup_started.set()
            await cleanup_release.wait()

    mock_mgr.chat_stream = _hung_stream

    try:
        resp = await asyncio.wait_for(
            client.post(
                f"/agent/threads/{thread_id}/chat",
                json={"message": "hello"},
            ),
            timeout=0.2,
        )
        await asyncio.wait_for(cleanup_started.wait(), timeout=0.2)
    finally:
        cleanup_release.set()

    assert resp.status_code == 200
    assert "Agent response timed out" in resp.text
    mock_mgr.cancel_stream.assert_awaited_once_with(thread_id, wait_timeout=5.0)


@pytest.mark.anyio
async def test_chat_streaming_keepalive_does_not_cancel_active_stream(client, db, monkeypatch):
    from src.web.agent import handlers

    thread_id = await db.create_agent_thread("Chat")
    mock_mgr = client._transport_app.state.agent_manager
    mock_mgr.cancel_stream = AsyncMock(return_value=True)
    monkeypatch.setattr(handlers, "_SSE_KEEPALIVE_INTERVAL", 0.01)
    monkeypatch.setattr(client._transport_app.state.config.agent, "total_timeout", 1)

    async def _slow_stream(*a, **kw):
        await asyncio.sleep(0.03)
        yield 'data: {"done": true, "full_text": "finished"}\n\n'

    mock_mgr.chat_stream = _slow_stream

    resp = await client.post(
        f"/agent/threads/{thread_id}/chat",
        json={"message": "hello"},
    )

    assert resp.status_code == 200
    assert '"type": "status"' in resp.text
    assert "finished" in resp.text
    assert "Agent response timed out" not in resp.text
    mock_mgr.cancel_stream.assert_not_awaited()

    messages = await db.get_agent_messages(thread_id)
    assistant_msgs = [m for m in messages if m["role"] == "assistant"]
    assert len(assistant_msgs) == 1
    assert assistant_msgs[0]["content"] == "finished"


@pytest.mark.anyio
async def test_chat_streaming_second_message_survives_keepalive_after_first_turn(client, db, monkeypatch):
    from src.web.agent import handlers

    thread_id = await db.create_agent_thread("Chat")
    mock_mgr = client._transport_app.state.agent_manager
    mock_mgr.cancel_stream = AsyncMock(return_value=True)
    monkeypatch.setattr(handlers, "_SSE_KEEPALIVE_INTERVAL", 0.01)
    monkeypatch.setattr(client._transport_app.state.config.agent, "total_timeout", 1)
    prompts: list[str] = []

    async def _two_turn_stream(_thread_id, prompt, **_kw):
        prompts.append(prompt)
        if len(prompts) == 1:
            yield 'data: {"done": true, "full_text": "first answer"}\n\n'
            return
        await asyncio.sleep(0.03)
        yield 'data: {"done": true, "full_text": "second answer"}\n\n'

    mock_mgr.chat_stream = _two_turn_stream

    first_resp = await client.post(
        f"/agent/threads/{thread_id}/chat",
        json={"message": "first prompt"},
    )
    second_resp = await client.post(
        f"/agent/threads/{thread_id}/chat",
        json={"message": "second prompt"},
    )

    assert first_resp.status_code == 200
    assert second_resp.status_code == 200
    assert "first answer" in first_resp.text
    assert '"type": "status"' in second_resp.text
    assert "second answer" in second_resp.text
    assert "Agent response timed out" not in second_resp.text
    assert prompts == ["first prompt", "second prompt"]
    mock_mgr.cancel_stream.assert_not_awaited()

    messages = await db.get_agent_messages(thread_id)
    assert [(m["role"], m["content"]) for m in messages] == [
        ("user", "first prompt"),
        ("assistant", "first answer"),
        ("user", "second prompt"),
        ("assistant", "second answer"),
    ]


@pytest.mark.anyio
async def test_chat_permission_request_waits_without_idle_timeout(client, db, monkeypatch):
    from src.web.agent import handlers

    thread_id = await db.create_agent_thread("Chat")
    mock_mgr = client._transport_app.state.agent_manager
    mock_mgr.cancel_stream = AsyncMock(return_value=True)
    monkeypatch.setattr(handlers, "_SSE_KEEPALIVE_INTERVAL", 0.01)
    monkeypatch.setattr(client._transport_app.state.config.agent, "total_timeout", 0.02)

    async def _permission_then_done(*a, **kw):
        yield 'data: {"type": "permission_request", "request_id": "r1", "tool": "WebSearch"}\n\n'
        await asyncio.sleep(0.05)
        yield 'data: {"done": true, "full_text": "allowed"}\n\n'

    mock_mgr.chat_stream = _permission_then_done

    resp = await client.post(
        f"/agent/threads/{thread_id}/chat",
        json={"message": "hello"},
    )

    assert resp.status_code == 200
    assert "permission_request" in resp.text
    assert "allowed" in resp.text
    assert "Agent response timed out" not in resp.text
    mock_mgr.cancel_stream.assert_not_awaited()


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


@pytest.mark.anyio
async def test_chat_save_failure_warns_inside_done_payload(client, db):
    """A non-IntegrityError on assistant save still streams the reply, and carries the warning
    INSIDE the done payload so the client can render it before tearing down (#676/#729)."""
    thread_id = await db.create_agent_thread("Chat")

    mock_mgr = client._transport_app.state.agent_manager

    async def _fake_stream(*a, **kw):
        yield 'data: {"done": true, "full_text": "the answer"}\n\n'

    mock_mgr.chat_stream = _fake_stream

    real_save = db.save_agent_message

    async def _failing_save(*args, **kwargs):
        if args[1] == "assistant":
            raise DatabaseBusyError("Database is busy. Retry the request in a few seconds.")
        return await real_save(*args, **kwargs)

    with patch.object(db, "save_agent_message", side_effect=_failing_save):
        resp = await client.post(
            f"/agent/threads/{thread_id}/chat",
            content=json.dumps({"message": "hello"}),
            headers={"Content-Type": "application/json"},
        )
        assert resp.status_code == 200
        chunks = [chunk async for chunk in resp.aiter_text()]
    body = "".join(chunks)

    payloads = [
        json.loads(seg[len("data: "):])
        for seg in body.split("\n\n")
        if seg.startswith("data: ")
    ]
    # Exactly one done payload, carrying both the reply and the save_warning field.
    done_payloads = [p for p in payloads if p.get("done")]
    assert len(done_payloads) == 1
    done = done_payloads[0]
    assert done["full_text"] == "the answer"
    assert "save_warning" in done
    assert "сохранить" in done["save_warning"]
    # No stray separate warning event was emitted (it would be dropped by the client).
    assert not [p for p in payloads if p.get("type") == "warning"]

    # And the assistant message is genuinely absent from the DB.
    messages = await db.get_agent_messages(thread_id)
    assert [m for m in messages if m["role"] == "assistant"] == []


@pytest.mark.anyio
async def test_chat_successful_save_has_no_warning(client, db):
    """The happy path leaves no save_warning on the done payload (#729 regression guard)."""
    thread_id = await db.create_agent_thread("Chat")

    mock_mgr = client._transport_app.state.agent_manager

    async def _fake_stream(*a, **kw):
        yield 'data: {"done": true, "full_text": "ok"}\n\n'

    mock_mgr.chat_stream = _fake_stream

    resp = await client.post(
        f"/agent/threads/{thread_id}/chat",
        content=json.dumps({"message": "hello"}),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    body = "".join([chunk async for chunk in resp.aiter_text()])

    payloads = [
        json.loads(seg[len("data: "):])
        for seg in body.split("\n\n")
        if seg.startswith("data: ")
    ]
    done_payloads = [p for p in payloads if p.get("done")]
    assert len(done_payloads) == 1
    assert "save_warning" not in done_payloads[0]


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
