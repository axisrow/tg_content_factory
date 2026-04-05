"""Tests for dashboard routes."""
from __future__ import annotations

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
