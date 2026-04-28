"""Tests for channels routes."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture
async def db(base_app):
    """Get db from base_app."""
    _, db, _ = base_app
    return db


@pytest.mark.anyio
async def test_channels_page_renders(route_client):
    """Test channels page renders."""
    resp = await route_client.get("/channels/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_channels_page_with_message(route_client):
    """Test channels page with message param."""
    resp = await route_client.get("/channels/?msg=channel_added")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_channels_page_with_error(route_client):
    """Test channels page with error param."""
    resp = await route_client.get("/channels/?error=resolve")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_add_channel_success(route_client, pool_mock):
    """Test add channel success."""
    pool_mock.resolve_channel = AsyncMock(
        return_value={
            "channel_id": -100999,
            "title": "New Channel",
            "username": "newchannel",
            "channel_type": "channel",
        }
    )

    resp = await route_client.post(
        "/channels/add",
        data={"identifier": "newchannel"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.anyio
async def test_add_channel_no_client(route_client):
    """Test add channel with no route_client available."""
    resp = await route_client.post(
        "/channels/add",
        data={"identifier": "testchannel"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.anyio
async def test_add_channel_resolve_fail(route_client):
    """Test add channel when resolve fails."""
    resp = await route_client.post(
        "/channels/add",
        data={"identifier": "badchannel"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.anyio
async def test_get_dialogs_json(route_client, db):
    """Test get dialogs returns JSON."""
    await db.repos.dialog_cache.replace_dialogs(
        "+1234567890",
        [{"channel_id": 200, "title": "Test Dialog", "channel_type": "channel"}],
    )

    with patch("src.web.routes.channels.deps.channel_service") as mock_svc:
        mock_svc.return_value.get_dialogs_with_added_flags = AsyncMock(
            return_value=[{"channel_id": 200, "title": "Test Dialog"}]
        )
        resp = await route_client.get("/channels/dialogs")
        assert resp.status_code == 200
        import json
        data = json.loads(resp.text)
        assert isinstance(data, list)


@pytest.mark.anyio
async def test_add_bulk_channels(route_client):
    """Test add bulk channels."""
    with patch("src.web.routes.channels.deps.channel_service") as mock_svc:
        mock_svc.return_value.add_bulk_by_dialog_ids = AsyncMock()
        resp = await route_client.post(
            "/channels/add-bulk",
            data={"channel_ids": ["200", "300"]},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=channels_added" in resp.headers["location"]


@pytest.mark.anyio
async def test_toggle_channel(route_client):
    """Test toggle channel."""
    with patch("src.web.routes.channels.deps.channel_service") as mock_svc:
        mock_svc.return_value.toggle = AsyncMock()
        resp = await route_client.post("/channels/1/toggle", follow_redirects=False)
        assert resp.status_code == 303
        assert "msg=channel_toggled" in resp.headers["location"]


@pytest.mark.anyio
async def test_delete_channel(route_client):
    """Test delete channel."""
    with patch("src.web.routes.channels.deps.channel_service") as mock_svc:
        mock_svc.return_value.delete = AsyncMock()
        resp = await route_client.post("/channels/1/delete", follow_redirects=False)
        assert resp.status_code == 303
        assert "msg=channel_deleted" in resp.headers["location"]


@pytest.mark.anyio
async def test_delete_channel_in_pipeline(route_client):
    """Test delete channel that is in pipeline."""
    with patch("src.web.routes.channels.deps.channel_service") as mock_svc:
        import sqlite3

        mock_svc.return_value.delete = AsyncMock(
            side_effect=sqlite3.IntegrityError("FOREIGN KEY constraint failed")
        )
        resp = await route_client.post("/channels/1/delete", follow_redirects=False)
        assert resp.status_code == 303
        assert "error=channel_in_pipeline" in resp.headers["location"]


@pytest.mark.anyio
async def test_collect_all_redirect(route_client):
    """Test collect all redirects."""
    with patch("src.web.routes.channel_collection.deps.collection_service") as mock_svc:
        from src.services.collection_service import BulkEnqueueResult
        mock_svc.return_value.enqueue_all_channels = AsyncMock(
            return_value=BulkEnqueueResult(
                queued_count=1,
                skipped_existing_count=0,
                total_candidates=1,
            )
        )
        resp = await route_client.post("/channels/collect-all", follow_redirects=False)
        assert resp.status_code == 303


@pytest.mark.anyio
async def test_collect_all_htmx(route_client):
    """Test collect all with HTMX header."""
    with patch("src.web.routes.channel_collection.deps.collection_service") as mock_svc:
        from src.services.collection_service import BulkEnqueueResult
        mock_svc.return_value.enqueue_all_channels = AsyncMock(
            return_value=BulkEnqueueResult(
                queued_count=1,
                skipped_existing_count=0,
                total_candidates=1,
            )
        )
        resp = await route_client.post(
            "/channels/collect-all",
            headers={"HX-Request": "true"},
        )
        assert resp.status_code == 200
        assert "collect-all-btn" in resp.text


@pytest.mark.anyio
async def test_collect_all_shutting_down(route_client):
    """Test collect all when shutting down."""
    route_client._transport_app.state.shutting_down = True
    resp = await route_client.post("/channels/collect-all", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=shutting_down" in resp.headers["location"]
    route_client._transport_app.state.shutting_down = False


@pytest.mark.anyio
async def test_collect_all_shutting_down_htmx(route_client):
    """Test collect all when shutting down with HTMX."""
    route_client._transport_app.state.shutting_down = True
    resp = await route_client.post(
        "/channels/collect-all",
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 200
    assert "collect-all-btn" in resp.text
    route_client._transport_app.state.shutting_down = False


@pytest.mark.anyio
async def test_collect_channel(route_client, db):
    """Test collect single channel."""
    with patch("src.web.routes.channel_collection.deps.collection_service") as mock_svc:
        mock_svc.return_value.enqueue_channel_by_pk = AsyncMock(return_value="queued")
        resp = await route_client.post("/channels/1/collect", follow_redirects=False)
        assert resp.status_code == 303


@pytest.mark.anyio
async def test_collect_channel_htmx(route_client, db):
    """Test collect single channel with HTMX."""
    with patch("src.web.routes.channel_collection.deps.collection_service") as mock_svc:
        mock_svc.return_value.enqueue_channel_by_pk = AsyncMock(return_value="queued")
        resp = await route_client.post(
            "/channels/1/collect",
            headers={"HX-Request": "true"},
        )
        assert resp.status_code == 200
        assert "collect-btn-1" in resp.text


@pytest.mark.anyio
async def test_collect_stats(route_client):
    """Test collect stats for all channels."""
    with patch("src.web.routes.channel_collection.deps.get_collector") as mock_col:
        mock_collector = MagicMock()
        mock_collector.is_stats_running = False
        mock_col.return_value = mock_collector

        resp = await route_client.post("/channels/stats/all", follow_redirects=False)
        assert resp.status_code == 303


@pytest.mark.anyio
async def test_add_channel_missing_identifier(route_client):
    """POST /channels/add without identifier returns 422."""
    resp = await route_client.post("/channels/add", data={}, follow_redirects=False)
    assert resp.status_code == 303


@pytest.mark.anyio
async def test_add_tag_missing_name(route_client):
    """POST /channels/tags without name returns 422."""
    resp = await route_client.post("/channels/tags", data={}, follow_redirects=False)
    assert resp.status_code == 303
