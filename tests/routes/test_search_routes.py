"""Tests for search routes."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import Channel, Message, SearchResult


@pytest.mark.anyio
async def test_root_redirects_to_search_when_no_agent(route_client):
    """Test root redirects to /search when agent unavailable."""
    resp = await route_client.get("/", follow_redirects=False)
    assert resp.status_code == 303
    assert "/search" in resp.headers.get("location", "")


@pytest.mark.anyio
async def test_root_redirects_to_agent_when_available(route_client):
    """Test root redirects to /agent when agent manager available."""
    from src.agent.manager import AgentManager

    agent_manager_mock = MagicMock(spec=AgentManager)
    agent_manager_mock.available = True

    route_client._transport_app.state.agent_manager = agent_manager_mock

    resp = await route_client.get("/", follow_redirects=False)
    assert resp.status_code == 303
    assert "/agent" in resp.headers.get("location", "")


@pytest.mark.anyio
async def test_search_page_renders(route_client):
    """Test search page renders with account."""
    resp = await route_client.get("/search")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_sidebar_hides_agent_link_when_unavailable(route_client):
    """The sidebar should preserve the existing agent availability gate."""
    resp = await route_client.get("/search")

    assert resp.status_code == 200
    assert 'href="/agent"' not in resp.text


@pytest.mark.anyio
async def test_sidebar_shows_agent_link_when_available(route_client):
    """Available agent backends should still get an active-capable sidebar link."""
    from src.agent.manager import AgentManager

    agent_manager_mock = MagicMock(spec=AgentManager)
    agent_manager_mock.available = True
    route_client._transport_app.state.agent_manager = agent_manager_mock

    resp = await route_client.get("/search")

    assert resp.status_code == 200
    assert 'href="/agent"' in resp.text


@pytest.mark.anyio
async def test_search_page_with_message(route_client):
    """Test search page with message param."""
    resp = await route_client.get("/search?msg=test_message")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_search_with_query(route_client, monkeypatch):
    """Test search with query executes search."""
    mock_result = SearchResult(messages=[], total=0, query="test")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.search.handlers.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?q=test")
    assert resp.status_code == 200
    mock_svc.search.assert_called_once()


@pytest.mark.anyio
async def test_search_result_does_not_render_translate_button_without_db_id(route_client, monkeypatch):
    """Live/transient search results without DB id must not call /translate/None."""
    mock_result = SearchResult(
        messages=[
            Message(
                id=None,
                channel_id=100,
                message_id=1,
                text="Привет, это достаточно длинный текст",
                detected_lang="ru",
                date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            )
        ],
        total=1,
        query="привет",
    )
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.search.handlers.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?q=привет")

    assert resp.status_code == 200
    assert 'data-msg-id="None"' not in resp.text
    assert 'data-target="translation-None"' not in resp.text


@pytest.mark.anyio
async def test_search_result_renders_translate_button_with_db_id(route_client, monkeypatch):
    mock_result = SearchResult(
        messages=[
            Message(
                id=123,
                channel_id=100,
                message_id=1,
                text="Привет, это достаточно длинный текст",
                detected_lang="ru",
                date=datetime(2025, 1, 1, tzinfo=timezone.utc),
            )
        ],
        total=1,
        query="привет",
    )
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.search.handlers.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?q=привет")

    assert resp.status_code == 200
    assert 'data-msg-id="123"' in resp.text
    assert 'data-target="translation-123"' in resp.text


@pytest.mark.anyio
async def test_search_invalid_channel_id(route_client, monkeypatch):
    """Test search with invalid channel_id shows error."""
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(
        return_value=SearchResult(messages=[], total=0, query="")
    )
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.search.handlers.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?q=test&channel_id=bad")
    assert resp.status_code == 200
    assert "Некорректный ID" in resp.text or "invalid" in resp.text.lower()


@pytest.mark.anyio
async def test_search_pagination(route_client, monkeypatch):
    """Test search with pagination parameter."""
    mock_result = SearchResult(messages=[], total=0, query="")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.search.handlers.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?q=test&page=2")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_search_fts_mode(route_client, monkeypatch):
    """Test search with FTS mode."""
    mock_result = SearchResult(messages=[], total=0, query="")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.search.handlers.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?q=test&is_fts=true")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_search_hybrid_mode(route_client, monkeypatch):
    """Test search with hybrid mode."""
    mock_result = SearchResult(messages=[], total=0, query="")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.search.handlers.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?q=test&mode=hybrid")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_search_error_rendered(route_client, monkeypatch):
    """Test search error is rendered in page."""
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(side_effect=Exception("Search failed"))
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.search.handlers.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?q=test")
    assert resp.status_code == 200
    assert "ошибка" in resp.text.lower() or "error" in resp.text.lower()


@pytest.mark.anyio
async def test_search_date_filters(route_client, monkeypatch):
    """Test search with date filters."""
    mock_result = SearchResult(messages=[], total=0, query="")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.search.handlers.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get(
        "/search?q=test&date_from=2024-01-01&date_to=2024-12-31"
    )
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_search_length_filter(route_client, monkeypatch):
    """Test search with length filter syntax."""
    mock_result = SearchResult(messages=[], total=0, query="test")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.search.handlers.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?q=test%20len%3C500&mode=local")
    assert resp.status_code == 200


# --- Browse mode tests ---


@pytest.mark.anyio
async def test_browse_mode_with_channel_id(route_client, monkeypatch, base_app):
    """Browse mode: channel_id without query shows latest messages from that channel."""
    app, db, pool = base_app
    # Add a channel to the DB
    from src.models import Channel

    await db.add_channel(Channel(channel_id=200, title="Browse Test Channel"))

    mock_result = SearchResult(messages=[], total=0, query="")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.search.handlers.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?channel_id=200&mode=local")
    assert resp.status_code == 200
    # Should call search with mode="local" (browse forces local mode)
    mock_svc.search.assert_called_once()
    call_kwargs = mock_svc.search.call_args
    assert call_kwargs.kwargs.get("channel_id") == 200 or call_kwargs[1].get("channel_id") == 200


@pytest.mark.anyio
async def test_browse_mode_no_channel_id(route_client, monkeypatch):
    """Browse mode without channel_id just shows empty search page."""
    mock_result = SearchResult(messages=[], total=0, query="")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.search.handlers.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?mode=local")
    assert resp.status_code == 200
    # No search should be called (no query, no channel_id)
    mock_svc.search.assert_not_called()


@pytest.mark.anyio
async def test_browse_mode_with_query(route_client, monkeypatch, base_app):
    """Browse mode is NOT active when query is present - normal search instead."""
    mock_result = SearchResult(messages=[], total=0, query="test")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.search.handlers.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?q=test&channel_id=200&mode=local")
    assert resp.status_code == 200
    # Should call search normally (not browse mode)
    mock_svc.search.assert_called_once()


@pytest.mark.anyio
async def test_browse_mode_error_handling(route_client, monkeypatch, base_app):
    """Browse mode error is handled gracefully."""
    app, db, pool = base_app
    from src.models import Channel

    await db.add_channel(Channel(channel_id=300, title="Error Channel"))

    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(side_effect=Exception("Browse failed"))
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.search.handlers.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?channel_id=300&mode=local")
    assert resp.status_code == 200
    # Error should be rendered on page
    assert "error" in resp.text.lower() or "ошибка" in resp.text.lower()


@pytest.mark.anyio
async def test_extract_length_filter():
    """Test _extract_length helper function."""
    from src.web.routes.search import _extract_length

    cleaned, min_len, max_len = _extract_length("test len<500")
    assert cleaned == "test"
    assert min_len is None
    assert max_len == 500

    cleaned, min_len, max_len = _extract_length("test len>100")
    assert cleaned == "test"
    assert min_len == 100
    assert max_len is None

    cleaned, min_len, max_len = _extract_length("test")
    assert cleaned == "test"
    assert min_len is None
    assert max_len is None


# ── Onboarding redirect paths ────────────────────────────────────────


@pytest.mark.anyio
async def test_search_redirects_when_auth_not_configured(base_app):
    """Test search page redirects to /settings when auth is not configured."""
    import base64

    from httpx import ASGITransport, AsyncClient

    app, db, pool_mock = base_app
    # Make auth unconfigured (api_id=0)
    app.state.auth.update_credentials(0, "")

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=False,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as c:
        resp = await c.get("/search")
        assert resp.status_code == 303
        assert "/settings" in resp.headers["location"]

    # Restore
    app.state.auth.update_credentials(12345, "test_hash")


@pytest.mark.anyio
async def test_search_redirects_when_no_accounts(base_app):
    """Test search page redirects to /settings when no accounts exist."""
    import base64

    from httpx import ASGITransport, AsyncClient

    app, db, pool_mock = base_app
    # Delete all accounts
    accounts = await db.get_accounts(active_only=False)
    for acc in accounts:
        if acc.id is not None:
            await db.delete_account(acc.id)

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=False,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as c:
        resp = await c.get("/search")
        assert resp.status_code == 303
        assert "/settings" in resp.headers["location"]


# ── check_quota failure path ─────────────────────────────────────────


@pytest.mark.anyio
async def test_search_quota_failure(route_client, monkeypatch):
    """Test search page handles check_quota failure gracefully."""
    mock_result = SearchResult(messages=[], total=0, query="")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(side_effect=Exception("Quota check failed"))
    monkeypatch.setattr(
        "src.web.search.handlers.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?q=test")
    assert resp.status_code == 200


# ── translate endpoint ───────────────────────────────────────────────


async def _insert_message_get_id(db, channel_id, message_id, text, date=None):
    """Helper: insert a message and return its DB row id."""
    from datetime import datetime, timezone

    from src.models import Channel, Message

    await db.add_channel(Channel(channel_id=channel_id, title=f"Ch{channel_id}"))
    msg = Message(
        channel_id=channel_id,
        message_id=message_id,
        text=text,
        date=date or datetime(2025, 1, 1, tzinfo=timezone.utc),
    )
    await db.insert_message(msg)
    # Look up the row id
    rows = await db.execute_fetchall(
        "SELECT id FROM messages WHERE channel_id = ? AND message_id = ?",
        (channel_id, message_id),
    )
    return rows[0]["id"]


@pytest.mark.anyio
async def test_translate_message_not_found(route_client, base_app):
    """Test translate endpoint with non-existent message."""
    resp = await route_client.post(
        "/search/translate/99999",
        json={"target_lang": "en"},
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 404
    data = resp.json()
    assert data["ok"] is False
    assert "not found" in data["error"].lower()


@pytest.mark.anyio
async def test_translate_message_no_text(route_client, base_app):
    """Test translate endpoint with message that has no text."""
    app, db, _ = base_app

    msg_id = await _insert_message_get_id(db, 999, 1, None)

    resp = await route_client.post(
        f"/search/translate/{msg_id}",
        json={"target_lang": "en"},
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400
    data = resp.json()
    assert data["ok"] is False


@pytest.mark.anyio
async def test_translate_message_cached(route_client, base_app):
    """Test translate endpoint returns cached translation."""
    app, db, _ = base_app

    msg_id = await _insert_message_get_id(db, 998, 1, "Привет мир")
    # Set cached translation
    await db.repos.messages.update_translation(msg_id, "en", "Hello world")
    await db.repos.messages.update_detected_lang(msg_id, "ru")

    resp = await route_client.post(
        f"/search/translate/{msg_id}",
        json={"target_lang": "en"},
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["cached"] is True
    assert data["translation"] == "Hello world"


@pytest.mark.anyio
async def test_translate_message_same_language(route_client, base_app):
    """Test translate endpoint when detected lang matches target."""
    app, db, _ = base_app

    msg_id = await _insert_message_get_id(db, 997, 1, "Hello world")
    await db.repos.messages.update_detected_lang(msg_id, "en")

    resp = await route_client.post(
        f"/search/translate/{msg_id}",
        json={"target_lang": "en"},
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data.get("same_lang") is True


@pytest.mark.anyio
async def test_translate_message_service_not_configured(route_client, base_app, monkeypatch):
    """Test translate endpoint when translation service is not available."""
    app, db, _ = base_app

    msg_id = await _insert_message_get_id(db, 996, 1, "Привет мир")
    await db.repos.messages.update_detected_lang(msg_id, "ru")
    # Ensure no container with translation_service
    app.state.container = None

    resp = await route_client.post(
        f"/search/translate/{msg_id}",
        json={"target_lang": "en"},
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 503
    data = resp.json()
    assert data["ok"] is False


@pytest.mark.anyio
async def test_translate_message_with_service(route_client, base_app, monkeypatch):
    """Test translate endpoint with a working translation service."""
    app, db, _ = base_app

    msg_id = await _insert_message_get_id(db, 995, 1, "Привет мир")
    await db.repos.messages.update_detected_lang(msg_id, "ru")

    # Set up a translation service on the container
    mock_translation = AsyncMock()
    mock_translation.translate_message = AsyncMock(return_value="Hello world")

    # Create a mock container with translation_service that also has a real db
    mock_container = MagicMock()
    mock_container.translation_service = mock_translation
    mock_container.db = db
    app.state.container = mock_container

    resp = await route_client.post(
        f"/search/translate/{msg_id}",
        json={"target_lang": "en"},
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["translation"] == "Hello world"
    assert data["cached"] is False

    # Clean up
    app.state.container = None


@pytest.mark.anyio
async def test_translate_message_non_en_target(route_client, base_app):
    """Test translate endpoint with non-en target language and cached translation."""
    app, db, _ = base_app

    msg_id = await _insert_message_get_id(db, 994, 1, "Hello world")
    await db.repos.messages.update_translation(msg_id, "custom", "Bonjour monde")
    await db.repos.messages.update_detected_lang(msg_id, "en")

    resp = await route_client.post(
        f"/search/translate/{msg_id}",
        json={"target_lang": "fr"},
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["ok"] is True
    assert data["cached"] is True
    assert data["translation"] == "Bonjour monde"


@pytest.mark.anyio
async def test_read_messages_route_returns_json(route_client):
    """GET /messages/{identifier} reads collected messages (parity: messages read)."""
    db = route_client._transport_app.state.db
    await db.add_channel(Channel(channel_id=777, title="ReadChan", username="readchan"))
    await db.insert_messages_batch([
        Message(channel_id=777, message_id=1, text="hello world", date=datetime(2024, 6, 1, tzinfo=timezone.utc)),
    ])

    resp = await route_client.get("/messages/readchan")
    assert resp.status_code == 200
    data = resp.json()
    assert data["channel_id"] == 777
    assert data["total"] >= 1
    assert any("hello world" in (m.get("text") or "") for m in data["messages"])


@pytest.mark.anyio
async def test_read_messages_route_clamps_limit(route_client, monkeypatch):
    db = route_client._transport_app.state.db
    await db.add_channel(Channel(channel_id=778, title="ClampChan", username="clampchan"))
    from src.database.repositories.messages import MessageSearchPage

    search_messages = AsyncMock(return_value=MessageSearchPage(messages=[], total=0))
    monkeypatch.setattr(db, "search_messages", search_messages)

    resp = await route_client.get("/messages/clampchan?limit=100000")

    assert resp.status_code == 200
    assert search_messages.await_args.kwargs["limit"] == 500


@pytest.mark.anyio
async def test_read_messages_route_channel_not_found(route_client):
    resp = await route_client.get("/messages/nonexistent_channel")
    assert resp.status_code == 404


@pytest.mark.anyio
async def test_search_page_shows_plus_when_has_more(route_client, monkeypatch):
    """With has_more the counter renders «N+» and the next-page link appears (#766)."""
    msg = Message(
        id=1,
        channel_id=100,
        message_id=1,
        text="hit",
        date=datetime(2024, 6, 1, tzinfo=timezone.utc),
    )
    mock_result = SearchResult(messages=[msg], total=50, has_more=True, query="test")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.search.handlers.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?q=test")

    assert resp.status_code == 200
    assert "50+" in resp.text
    assert "Далее" in resp.text


@pytest.mark.anyio
async def test_search_page_no_next_when_no_more(route_client, monkeypatch):
    """Without has_more there is no «Далее» link and no «+» on the counter (#766)."""
    msg = Message(
        id=1,
        channel_id=100,
        message_id=1,
        text="hit",
        date=datetime(2024, 6, 1, tzinfo=timezone.utc),
    )
    mock_result = SearchResult(messages=[msg], total=1, has_more=False, query="test")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.search.handlers.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?q=test")

    assert resp.status_code == 200
    assert "1+" not in resp.text
    assert "Далее" not in resp.text


@pytest.mark.anyio
async def test_read_messages_json_includes_has_more(route_client):
    """GET /messages/{identifier} exposes has_more next to the lower-bound total (#766)."""
    db = route_client._transport_app.state.db
    await db.add_channel(Channel(channel_id=779, title="MoreChan", username="morechan"))
    await db.insert_messages_batch([
        Message(channel_id=779, message_id=i, text=f"msg {i}", date=datetime(2024, 6, 1, tzinfo=timezone.utc))
        for i in range(1, 4)
    ])

    resp = await route_client.get("/messages/morechan?limit=2")

    assert resp.status_code == 200
    data = resp.json()
    assert data["has_more"] is True
    assert len(data["messages"]) == 2

    resp_all = await route_client.get("/messages/morechan?limit=50")
    data_all = resp_all.json()
    assert data_all["has_more"] is False
    assert data_all["total"] == 3


@pytest.mark.anyio
async def test_search_page_next_link_for_exact_total_modes(route_client, monkeypatch):
    """Semantic/hybrid modes return an exact total without has_more — «Далее»
    must still appear when total exceeds the current page (review on #824)."""
    msg = Message(
        id=1,
        channel_id=100,
        message_id=1,
        text="hit",
        date=datetime(2024, 6, 1, tzinfo=timezone.utc),
    )
    mock_result = SearchResult(messages=[msg], total=120, has_more=False, query="test")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.search.handlers.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?q=test&mode=semantic")

    assert resp.status_code == 200
    assert "Далее" in resp.text
