"""Tests for dashboard routes."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture
async def client(route_client):
    return route_client


@pytest.mark.asyncio
async def test_dashboard_renders_with_data(client, base_app):
    """Dashboard renders with stats when auth configured and accounts exist."""
    app, db, pool = base_app
    # base_app already adds an account, so dashboard should render
    resp = await client.get("/dashboard/")
    assert resp.status_code == 200


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_dashboard_contains_stats(client, base_app):
    """Dashboard page contains stats information."""
    app, db, pool = base_app
    stats = await db.get_stats()
    assert stats is not None
    resp = await client.get("/dashboard/")
    assert resp.status_code == 200


# ── _time_ago helper tests ──────────────────────────────────────────


@pytest.mark.asyncio
async def test_time_ago_none():
    """Test _time_ago returns None for None input."""
    from src.web.routes.dashboard import _time_ago
    assert _time_ago(None) is None


@pytest.mark.asyncio
async def test_time_ago_just_now():
    """Test _time_ago returns 'только что' for very recent."""
    from src.web.routes.dashboard import _time_ago
    now = datetime.now(tz=timezone.utc)
    assert _time_ago(now) == "только что"


@pytest.mark.asyncio
async def test_time_ago_minutes():
    """Test _time_ago returns minutes ago."""
    from src.web.routes.dashboard import _time_ago
    dt = datetime.now(tz=timezone.utc) - timedelta(minutes=5)
    result = _time_ago(dt)
    assert "5" in result
    assert "мин" in result


@pytest.mark.asyncio
async def test_time_ago_hours():
    """Test _time_ago returns hours ago."""
    from src.web.routes.dashboard import _time_ago
    dt = datetime.now(tz=timezone.utc) - timedelta(hours=3)
    result = _time_ago(dt)
    assert "3" in result
    assert "ч" in result


@pytest.mark.asyncio
async def test_time_ago_days():
    """Test _time_ago returns days ago."""
    from src.web.routes.dashboard import _time_ago
    dt = datetime.now(tz=timezone.utc) - timedelta(days=5)
    result = _time_ago(dt)
    assert "5" in result
    assert "д" in result


@pytest.mark.asyncio
async def test_time_ago_naive_datetime():
    """Test _time_ago handles naive datetime (no tzinfo)."""
    from src.web.routes.dashboard import _time_ago
    dt = datetime.now(tz=timezone.utc).replace(tzinfo=None) - timedelta(minutes=10)
    result = _time_ago(dt)
    assert "10" in result
    assert "мин" in result


# ── Dashboard redirect when auth not configured ──────────────────────


@pytest.mark.asyncio
async def test_dashboard_redirect_auth_not_configured(base_app):
    """Dashboard redirects to /settings when auth is not configured."""
    from httpx import ASGITransport, AsyncClient
    import base64

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


@pytest.mark.asyncio
async def test_dashboard_with_active_flood_wait(client, base_app):
    """Dashboard renders when account has active flood wait."""
    app, db, pool = base_app
    # Set flood_wait_until in the future for the existing account
    future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
    accounts = await db.get_accounts(active_only=False)
    for acc in accounts:
        await db.update_account_flood(acc.phone, future_time)

    resp = await client.get("/dashboard/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_dashboard_with_expired_flood_wait(client, base_app):
    """Dashboard renders when account has expired flood wait."""
    app, db, pool = base_app
    past_time = datetime.now(tz=timezone.utc) - timedelta(hours=1)
    accounts = await db.get_accounts(active_only=False)
    for acc in accounts:
        await db.update_account_flood(acc.phone, past_time)

    resp = await client.get("/dashboard/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_dashboard_all_connected_flooded(client, base_app):
    """Dashboard shows collector_attention when all connected are flooded."""
    app, db, pool = base_app
    future_time = datetime.now(tz=timezone.utc) + timedelta(hours=1)
    accounts = await db.get_accounts(active_only=False)
    for acc in accounts:
        await db.update_account_flood(acc.phone, future_time)

    resp = await client.get("/dashboard/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_dashboard_flood_wait_naive_datetime(client, base_app):
    """Dashboard handles flood_wait_until with naive datetime (no tzinfo)."""
    app, db, pool = base_app
    # Naive future datetime
    future_time = datetime.now() + timedelta(hours=1)
    accounts = await db.get_accounts(active_only=False)
    for acc in accounts:
        await db.update_account_flood(acc.phone, future_time)

    resp = await client.get("/dashboard/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_dashboard_no_connected_clients(client, base_app):
    """Dashboard renders when no clients are connected (pool empty)."""
    app, db, pool = base_app
    pool.clients = {}

    resp = await client.get("/dashboard/")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_dashboard_with_pipeline_data(client, base_app):
    """Dashboard renders with pipeline statistics."""
    app, db, pool = base_app
    resp = await client.get("/dashboard/")
    assert resp.status_code == 200
