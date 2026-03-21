"""Tests for search_queries routes."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture
async def client(route_client):
    """Use shared route_client fixture."""
    return route_client


@pytest.fixture
async def db(base_app):
    """Get db from base_app."""
    _, db, _ = base_app
    return db


@pytest.mark.asyncio
async def test_search_queries_page_renders_empty(client):
    """Test search queries page renders empty."""
    resp = await client.get("/search-queries/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_search_queries_page_lists_items(client):
    """Test search queries page lists items after add."""
    await client.post(
        "/search-queries/add",
        data={"query": "test query", "interval_minutes": "60"},
    )
    resp = await client.get("/search-queries/")
    assert resp.status_code == 200
    assert "test query" in resp.text


@pytest.mark.asyncio
async def test_add_search_query_redirects(client):
    """Test add search query redirects."""
    resp = await client.post(
        "/search-queries/add",
        data={"query": "new query", "interval_minutes": "30"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=sq_added" in resp.headers["location"]


@pytest.mark.asyncio
async def test_add_search_query_with_all_fields(client):
    """Test add search query with all fields."""
    resp = await client.post(
        "/search-queries/add",
        data={
            "query": "complex query",
            "interval_minutes": "120",
            "is_regex": "on",  # checkbox sends "on" when checked
            "is_fts": "",  # is_regex and is_fts are mutually exclusive
            "notify_on_collect": "on",
            "track_stats": "on",
            "exclude_patterns": "spam",
            "max_length": "1000",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=sq_added" in resp.headers["location"]


@pytest.mark.asyncio
async def test_toggle_search_query(client):
    """Test toggle search query."""
    await client.post(
        "/search-queries/add",
        data={"query": "toggle test", "interval_minutes": "60"},
    )
    resp = await client.post("/search-queries/1/toggle", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=sq_toggled" in resp.headers["location"]


@pytest.mark.asyncio
async def test_edit_search_query(client):
    """Test edit search query."""
    await client.post(
        "/search-queries/add",
        data={"query": "original", "interval_minutes": "60"},
    )
    resp = await client.post(
        "/search-queries/1/edit",
        data={"query": "edited query", "interval_minutes": "90"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=sq_edited" in resp.headers["location"]


@pytest.mark.asyncio
async def test_delete_search_query(client, db):
    """Test delete search query."""
    await client.post(
        "/search-queries/add",
        data={"query": "to delete", "interval_minutes": "60"},
    )
    resp = await client.post("/search-queries/1/delete", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=sq_deleted" in resp.headers["location"]


@pytest.mark.asyncio
async def test_run_search_query(client):
    """Test run search query."""
    await client.post(
        "/search-queries/add",
        data={"query": "run test", "interval_minutes": "60"},
    )

    with patch("src.web.routes.search_queries.deps.search_query_service") as mock_svc:
        mock_svc.return_value.run_once = AsyncMock()
        resp = await client.post("/search-queries/1/run", follow_redirects=False)
        assert resp.status_code == 303
        assert "msg=sq_run" in resp.headers["location"]


@pytest.mark.asyncio
async def test_scheduler_synced_after_add(client):
    """Test scheduler syncs after add when running."""
    client._transport_app.state.scheduler._running = True

    with patch(
        "src.web.routes.search_queries.deps.get_scheduler"
    ) as mock_get_scheduler:
        mock_scheduler = MagicMock()
        mock_scheduler.is_running = True
        mock_scheduler.sync_search_query_jobs = AsyncMock()
        mock_get_scheduler.return_value = mock_scheduler

        resp = await client.post(
            "/search-queries/add",
            data={"query": "sync test", "interval_minutes": "60"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        mock_scheduler.sync_search_query_jobs.assert_called_once()


@pytest.mark.asyncio
async def test_scheduler_synced_after_toggle(client):
    """Test scheduler syncs after toggle when running."""
    await client.post(
        "/search-queries/add",
        data={"query": "sync toggle", "interval_minutes": "60"},
    )

    with patch(
        "src.web.routes.search_queries.deps.get_scheduler"
    ) as mock_get_scheduler:
        mock_scheduler = MagicMock()
        mock_scheduler.is_running = True
        mock_scheduler.sync_search_query_jobs = AsyncMock()
        mock_get_scheduler.return_value = mock_scheduler

        resp = await client.post("/search-queries/1/toggle", follow_redirects=False)
        assert resp.status_code == 303
        mock_scheduler.sync_search_query_jobs.assert_called_once()


@pytest.mark.asyncio
async def test_delete_nonexistent_no_crash(client):
    """Test delete nonexistent query doesn't crash."""
    resp = await client.post("/search-queries/999999/delete", follow_redirects=False)
    assert resp.status_code == 303
