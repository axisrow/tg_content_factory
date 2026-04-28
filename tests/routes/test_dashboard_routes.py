"""Tests for dashboard routes."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest


@pytest.mark.anyio
async def test_dashboard_renders_with_data(route_client, base_app):
    """Dashboard renders with stats cards when auth configured and accounts exist."""
    app, db, pool = base_app
    # base_app already adds an account, so dashboard should render
    resp = await route_client.get("/dashboard/")
    assert resp.status_code == 200
    assert "Панель" in resp.text
    # Required dashboard cards
    assert "Аккаунты" in resp.text
    assert "Каналы" in resp.text


@pytest.mark.anyio
async def test_dashboard_redirect_no_accounts(base_app):
    """Dashboard redirects to settings when no accounts exist."""
    from httpx import ASGITransport, AsyncClient

    config = base_app[0].state.config
    config.web.password = "testpass"

    # Create a fresh app without accounts
    app_no_acc = base_app[0]
    db_no_acc = base_app[1]

    # Delete all accounts
    accounts = await db_no_acc.get_accounts(active_only=False)
    for acc in accounts:
        if acc.id is not None:
            await db_no_acc.delete_account(acc.id)

    import base64

    transport = ASGITransport(app=app_no_acc)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=False,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as c:
        resp = await c.get("/dashboard/")
        assert resp.status_code in (303, 302)
        assert "/settings" in resp.headers.get("location", "")


@pytest.mark.anyio
async def test_dashboard_contains_stats(route_client, base_app):
    """Dashboard page renders the stats numbers from db.get_stats()."""
    app, db, pool = base_app
    stats = await db.get_stats()
    assert stats is not None
    # db.get_stats() must expose the counters the template depends on.
    for key in ("accounts", "channels", "channels_filtered", "channels_tracked"):
        assert key in stats
    resp = await route_client.get("/dashboard/")
    assert resp.status_code == 200
    # The template renders the raw numbers from stats — check at least the
    # accounts and channels counters appear in the rendered HTML.
    assert f"<strong>{stats['accounts']}</strong> в базе" in resp.text
    assert f"В базе: <strong>{stats['channels']}</strong>" in resp.text


# ── _time_ago helper tests ──────────────────────────────────────────


@pytest.mark.anyio
async def test_time_ago_none():
    """Test _time_ago returns None for None input."""
    from src.web.routes.dashboard import _time_ago
    assert _time_ago(None) is None


@pytest.mark.anyio
async def test_time_ago_just_now():
    """Test _time_ago returns 'только что' for very recent."""
    from src.web.routes.dashboard import _time_ago
    now = datetime.now(tz=timezone.utc)
    assert _time_ago(now) == "только что"


@pytest.mark.anyio
async def test_time_ago_minutes():
    """Test _time_ago returns minutes ago."""
    from src.web.routes.dashboard import _time_ago
    dt = datetime.now(tz=timezone.utc) - timedelta(minutes=5)
    result = _time_ago(dt)
    assert "5" in result
    assert "мин" in result


@pytest.mark.anyio
async def test_time_ago_hours():
    """Test _time_ago returns hours ago."""
    from src.web.routes.dashboard import _time_ago
    dt = datetime.now(tz=timezone.utc) - timedelta(hours=3)
    result = _time_ago(dt)
    assert "3" in result
    assert "ч" in result


@pytest.mark.anyio
async def test_time_ago_days():
    """Test _time_ago returns days ago."""
    from src.web.routes.dashboard import _time_ago
    dt = datetime.now(tz=timezone.utc) - timedelta(days=5)
    result = _time_ago(dt)
    assert "5" in result
    assert "д" in result


@pytest.mark.anyio
async def test_time_ago_naive_datetime():
    """Test _time_ago handles naive datetime (no tzinfo)."""
    from src.web.routes.dashboard import _time_ago
    dt = datetime.now(tz=timezone.utc).replace(tzinfo=None) - timedelta(minutes=10)
    result = _time_ago(dt)
    assert "10" in result
    assert "мин" in result


# ── Dashboard redirect when auth not configured ──────────────────────


@pytest.mark.anyio
async def test_dashboard_redirect_auth_not_configured(base_app):
    """Dashboard redirects to /settings when auth is not configured."""
    import base64

    from httpx import ASGITransport, AsyncClient

    app, db, pool_mock = base_app
    # Make auth unconfigured
    app.state.auth.update_credentials(0, "")

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=False,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as c:
        resp = await c.get("/dashboard/")
        assert resp.status_code == 303
        assert "/settings" in resp.headers["location"]

    # Restore
    app.state.auth.update_credentials(12345, "test_hash")


# ── Flood wait logic ────────────────────────────────────────────────


@pytest.mark.anyio
async def test_dashboard_with_active_flood_wait(route_client, base_app):
    """Dashboard surfaces flood-wait count on accounts card when accounts are flooded."""
    app, db, pool = base_app
    # Set flood_wait_until in the future for the existing account
    future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
    accounts = await db.get_accounts(active_only=False)
    for acc in accounts:
        await db.update_account_flood(acc.phone, future_time)

    resp = await route_client.get("/dashboard/")
    assert resp.status_code == 200
    # Template renders an explicit flood-wait warning when >0 accounts are flooded.
    assert "flood-wait" in resp.text


@pytest.mark.anyio
async def test_dashboard_with_expired_flood_wait(route_client, base_app):
    """Dashboard does NOT warn about flood-wait after it expires."""
    app, db, pool = base_app
    past_time = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    accounts = await db.get_accounts(active_only=False)
    for acc in accounts:
        await db.update_account_flood(acc.phone, past_time)

    resp = await route_client.get("/dashboard/")
    assert resp.status_code == 200
    # Expired flood wait must not surface as an active warning.
    assert "flood-wait" not in resp.text


@pytest.mark.anyio
async def test_dashboard_all_connected_flooded(route_client, base_app):
    """Dashboard shows collector_attention link when all connected accounts are flooded."""
    app, db, pool = base_app
    future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
    accounts = await db.get_accounts(active_only=False)
    for acc in accounts:
        await db.update_account_flood(acc.phone, future_time)

    resp = await route_client.get("/dashboard/")
    assert resp.status_code == 200
    # Template only renders this link when collector_attention is truthy.
    assert "Коллектор ограничен" in resp.text


@pytest.mark.anyio
async def test_dashboard_flood_wait_naive_datetime(route_client, base_app):
    """Dashboard handles flood_wait_until with naive datetime (no tzinfo)."""
    app, db, pool = base_app
    # Naive future datetime
    future_time = datetime.now() + timedelta(hours=1)
    accounts = await db.get_accounts(active_only=False)
    for acc in accounts:
        await db.update_account_flood(acc.phone, future_time)

    resp = await route_client.get("/dashboard/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_dashboard_no_connected_clients(route_client, base_app):
    """Dashboard renders when no clients are connected (pool empty)."""
    app, db, pool = base_app
    pool.clients = {}

    resp = await route_client.get("/dashboard/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_dashboard_with_pipeline_data(route_client, base_app):
    """Dashboard renders with pipeline statistics."""
    app, db, pool = base_app
    resp = await route_client.get("/dashboard/")
    assert resp.status_code == 200
