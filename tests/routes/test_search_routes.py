"""Tests for search routes."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import SearchResult


@pytest.fixture
async def client(route_client):
    """Use shared route_client fixture."""
    return route_client


@pytest.mark.asyncio
async def test_root_redirects_to_search_when_no_agent(client):
    """Test root redirects to /search when agent unavailable."""
    resp = await client.get("/", follow_redirects=False)
    assert resp.status_code == 303
    assert "/search" in resp.headers.get("location", "")


@pytest.mark.asyncio
async def test_root_redirects_to_agent_when_available(client):
    """Test root redirects to /agent when agent manager available."""
    from src.agent.manager import AgentManager

    agent_manager_mock = MagicMock(spec=AgentManager)
    agent_manager_mock.available = True

    client._transport_app.state.agent_manager = agent_manager_mock

    resp = await client.get("/", follow_redirects=False)
    assert resp.status_code == 303
    assert "/agent" in resp.headers.get("location", "")


@pytest.mark.asyncio
async def test_search_page_renders(client):
    """Test search page renders with account."""
    resp = await client.get("/search")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_search_page_with_message(client):
    """Test search page with message param."""
    resp = await client.get("/search?msg=test_message")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_search_with_query(client, monkeypatch):
    """Test search with query executes search."""
    mock_result = SearchResult(messages=[], total=0, query="test")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.routes.search.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await client.get("/search?q=test")
    assert resp.status_code == 200
    mock_svc.search.assert_called_once()


@pytest.mark.asyncio
async def test_search_invalid_channel_id(client, monkeypatch):
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

    resp = await client.get("/search?q=test&channel_id=bad")
    assert resp.status_code == 200
    assert "Некорректный ID" in resp.text or "invalid" in resp.text.lower()


@pytest.mark.asyncio
async def test_search_pagination(client, monkeypatch):
    """Test search with pagination parameter."""
    mock_result = SearchResult(messages=[], total=0, query="")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.routes.search.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await client.get("/search?q=test&page=2")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_search_fts_mode(client, monkeypatch):
    """Test search with FTS mode."""
    mock_result = SearchResult(messages=[], total=0, query="")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.routes.search.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await client.get("/search?q=test&is_fts=true")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_search_hybrid_mode(client, monkeypatch):
    """Test search with hybrid mode."""
    mock_result = SearchResult(messages=[], total=0, query="")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.routes.search.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await client.get("/search?q=test&mode=hybrid")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_search_error_rendered(client, monkeypatch):
    """Test search error is rendered in page."""
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(side_effect=Exception("Search failed"))
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.routes.search.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await client.get("/search?q=test")
    assert resp.status_code == 200
    assert "ошибка" in resp.text.lower() or "error" in resp.text.lower()


@pytest.mark.asyncio
async def test_search_date_filters(client, monkeypatch):
    """Test search with date filters."""
    mock_result = SearchResult(messages=[], total=0, query="")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.routes.search.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await client.get(
        "/search?q=test&date_from=2024-01-01&date_to=2024-12-31"
    )
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_search_length_filter(client, monkeypatch):
    """Test search with length filter syntax."""
    mock_result = SearchResult(messages=[], total=0, query="test")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.routes.search.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await client.get("/search?q=test%20len%3C500&mode=local")
    assert resp.status_code == 200


# --- Browse mode tests ---


@pytest.mark.asyncio
async def test_browse_mode_with_channel_id(client, monkeypatch, base_app):
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

    resp = await client.get("/search?channel_id=200&mode=local")
    assert resp.status_code == 200
    # Should call search with mode="local" (browse forces local mode)
    mock_svc.search.assert_called_once()
    call_kwargs = mock_svc.search.call_args
    assert call_kwargs.kwargs.get("channel_id") == 200 or call_kwargs[1].get("channel_id") == 200


@pytest.mark.asyncio
async def test_browse_mode_no_channel_id(client, monkeypatch):
    """Browse mode without channel_id just shows empty search page."""
    mock_result = SearchResult(messages=[], total=0, query="")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.routes.search.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await client.get("/search?mode=local")
    assert resp.status_code == 200
    # No search should be called (no query, no channel_id)
    mock_svc.search.assert_not_called()


@pytest.mark.asyncio
async def test_browse_mode_with_query(client, monkeypatch, base_app):
    """Browse mode is NOT active when query is present - normal search instead."""
    mock_result = SearchResult(messages=[], total=0, query="test")
    mock_svc = MagicMock()
    mock_svc.search = AsyncMock(return_value=mock_result)
    mock_svc.check_quota = AsyncMock(return_value=None)
    monkeypatch.setattr(
        "src.web.routes.search.deps.search_service",
        lambda r: mock_svc,
    )

    resp = await client.get("/search?q=test&channel_id=200&mode=local")
    assert resp.status_code == 200
    # Should call search normally (not browse mode)
    mock_svc.search.assert_called_once()


@pytest.mark.asyncio
async def test_browse_mode_error_handling(client, monkeypatch, base_app):
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

    resp = await client.get("/search?channel_id=300&mode=local")
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
