"""Tests for search routes."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import SearchResult


@pytest.mark.asyncio
async def test_root_redirects_to_search_when_no_agent(route_client):
    """Test root redirects to /search when agent unavailable."""
    resp = await route_client.get("/", follow_redirects=False)
    assert resp.status_code == 303
    assert "/search" in resp.headers.get("location", "")


@pytest.mark.asyncio
async def test_root_redirects_to_agent_when_available(route_client):
    """Test root redirects to /agent when agent manager available."""
    from src.agent.manager import AgentManager

    agent_manager_mock = MagicMock(spec=AgentManager)
    agent_manager_mock.available = True

    route_client._transport_app.state.agent_manager = agent_manager_mock

    resp = await route_client.get("/", follow_redirects=False)
    assert resp.status_code == 303
    assert "/agent" in resp.headers.get("location", "")


@pytest.mark.asyncio
async def test_search_page_renders(route_client):
    """Test search page renders with account."""
    resp = await route_client.get("/search")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_search_page_with_message(route_client):
    """Test search page with message param."""
    resp = await route_client.get("/search?msg=test_message")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_search_with_query(route_client, monkeypatch):
    """Test search with query executes search."""
    mock_result = SearchResult(messages=[], total=0, query="test")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.routes.search.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?q=test")
    assert resp.status_code == 200
    mock_svc.search.assert_called_once()


@pytest.mark.asyncio
async def test_search_invalid_channel_id(route_client, monkeypatch):
    """Test search with invalid channel_id shows error."""
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(
        return_value=SearchResult(messages=[], total=0, query="")
    )
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.routes.search.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?q=test&channel_id=bad")
    assert resp.status_code == 200
    assert "Некорректный ID" in resp.text or "invalid" in resp.text.lower()


@pytest.mark.asyncio
async def test_search_pagination(route_client, monkeypatch):
    """Test search with pagination parameter."""
    mock_result = SearchResult(messages=[], total=0, query="")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.routes.search.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?q=test&page=2")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_search_fts_mode(route_client, monkeypatch):
    """Test search with FTS mode."""
    mock_result = SearchResult(messages=[], total=0, query="")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.routes.search.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?q=test&is_fts=true")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_search_hybrid_mode(route_client, monkeypatch):
    """Test search with hybrid mode."""
    mock_result = SearchResult(messages=[], total=0, query="")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.routes.search.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?q=test&mode=hybrid")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_search_error_rendered(route_client, monkeypatch):
    """Test search error is rendered in page."""
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(side_effect=Exception("Search failed"))
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.routes.search.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?q=test")
    assert resp.status_code == 200
    assert "ошибка" in resp.text.lower() or "error" in resp.text.lower()


@pytest.mark.asyncio
async def test_search_date_filters(route_client, monkeypatch):
    """Test search with date filters."""
    mock_result = SearchResult(messages=[], total=0, query="")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.routes.search.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get(
        "/search?q=test&date_from=2024-01-01&date_to=2024-12-31"
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_search_length_filter(route_client, monkeypatch):
    """Test search with length filter syntax."""
    mock_result = SearchResult(messages=[], total=0, query="test")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.routes.search.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?q=test%20len%3C500&mode=local")
    assert resp.status_code == 200


# --- Browse mode tests ---


@pytest.mark.asyncio
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
        "src.web.routes.search.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?channel_id=200&mode=local")
    assert resp.status_code == 200
    # Should call search with mode="local" (browse forces local mode)
    mock_svc.search.assert_called_once()
    call_kwargs = mock_svc.search.call_args
    assert call_kwargs.kwargs.get("channel_id") == 200 or call_kwargs[1].get("channel_id") == 200


@pytest.mark.asyncio
async def test_browse_mode_no_channel_id(route_client, monkeypatch):
    """Browse mode without channel_id just shows empty search page."""
    mock_result = SearchResult(messages=[], total=0, query="")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.routes.search.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?mode=local")
    assert resp.status_code == 200
    # No search should be called (no query, no channel_id)
    mock_svc.search.assert_not_called()


@pytest.mark.asyncio
async def test_browse_mode_with_query(route_client, monkeypatch, base_app):
    """Browse mode is NOT active when query is present - normal search instead."""
    mock_result = SearchResult(messages=[], total=0, query="test")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.routes.search.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?q=test&channel_id=200&mode=local")
    assert resp.status_code == 200
    # Should call search normally (not browse mode)
    mock_svc.search.assert_called_once()


@pytest.mark.asyncio
async def test_browse_mode_error_handling(route_client, monkeypatch, base_app):
    """Browse mode error is handled gracefully."""
    app, db, pool = base_app
    from src.models import Channel

    await db.add_channel(Channel(channel_id=300, title="Error Channel"))

    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(side_effect=Exception("Browse failed"))
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.routes.search.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await route_client.get("/search?channel_id=300&mode=local")
    assert resp.status_code == 200
    # Error should be rendered on page
    assert "error" in resp.text.lower() or "ошибка" in resp.text.lower()


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_search_quota_failure(route_client, monkeypatch):
    """Test search page handles check_quota failure gracefully."""
    mock_result = SearchResult(messages=[], total=0, query="")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(side_effect=Exception("Quota check failed"))
    monkeypatch.setattr(
        "src.web.routes.search.deps.search_service",
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
