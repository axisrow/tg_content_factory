"""Tests for channel collection routes."""
from __future__ import annotations

import pytest

from tests.routes.conftest import _add_channel


@pytest.mark.asyncio
async def test_collect_all_channels_no_htmx(route_client, base_app):
    """POST collect-all without HTMX redirects."""
    app, db, pool = base_app
    await _add_channel(db)

    resp = await route_client.post("/channels/collect-all", follow_redirects=False)
    assert resp.status_code in (303, 302)
    assert "/channels" in resp.headers.get("location", "")


@pytest.mark.asyncio
async def test_collect_all_channels_htmx(route_client, base_app):
    """POST collect-all with HTMX returns HTML fragment."""
    app, db, pool = base_app
    await _add_channel(db)

    resp = await route_client.post(
        "/channels/collect-all",
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 200
    assert "collect-all-btn" in resp.text


@pytest.mark.asyncio
async def test_collect_all_channels_empty(route_client, base_app):
    """POST collect-all with no active channels returns empty message."""
    app, db, pool = base_app

    channel = await db.get_channel_by_channel_id(100)
    if channel and channel.id is not None:
        await db.set_channel_active(channel.id, False)

    resp = await route_client.post(
        "/channels/collect-all",
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 200
    assert "Нет активных каналов" in resp.text


@pytest.mark.asyncio
async def test_collect_all_channels_shutting_down(route_client, base_app):
    """POST collect-all during shutdown returns warning."""
    app, db, pool = base_app
    app.state.shutting_down = True

    try:
        resp = await route_client.post(
            "/channels/collect-all",
            headers={"HX-Request": "true"},
        )
        assert resp.status_code == 200
    finally:
        app.state.shutting_down = False


@pytest.mark.asyncio
async def test_collect_all_stats_success(route_client, base_app):
    """POST stats/all starts stats collection."""
    app, db, pool = base_app
    await _add_channel(db)

    resp = await route_client.post("/channels/stats/all", follow_redirects=False)
    assert resp.status_code in (303, 302)
    assert "/channels" in resp.headers.get("location", "")


@pytest.mark.asyncio
async def test_collect_all_stats_already_running(route_client, base_app):
    """POST stats/all when already running redirects with error."""
    app, db, pool = base_app
    await _add_channel(db)

    from src.models import StatsAllTaskPayload

    await db.create_stats_task(
        StatsAllTaskPayload(channel_ids=[100], batch_size=20)
    )

    resp = await route_client.post("/channels/stats/all", follow_redirects=False)
    assert resp.status_code in (303, 302)
    assert "stats_running" in resp.headers.get("location", "")


@pytest.mark.asyncio
async def test_collect_single_channel_htmx(route_client, base_app):
    """POST collect single channel with HTMX."""
    app, db, pool = base_app
    pk = await _add_channel(db)

    resp = await route_client.post(
        f"/channels/{pk}/collect",
        headers={"HX-Request": "true"},
    )
    assert resp.status_code == 200
    assert f"collect-btn-{pk}" in resp.text


@pytest.mark.asyncio
async def test_collect_single_channel_no_htmx(route_client, base_app):
    """POST collect single channel without HTMX redirects."""
    app, db, pool = base_app
    pk = await _add_channel(db)

    resp = await route_client.post(f"/channels/{pk}/collect", follow_redirects=False)
    assert resp.status_code in (303, 302)
    assert "/channels" in resp.headers.get("location", "")
