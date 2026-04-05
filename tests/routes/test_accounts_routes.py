"""Tests for account management routes."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.models import Account


@pytest.fixture
async def client(route_client):
    return route_client


@pytest.mark.asyncio
async def test_toggle_account(client, base_app):
    """Toggle account active/inactive."""
    app, db, pool = base_app
    accounts = await db.get_accounts(active_only=False)
    assert len(accounts) > 0
    acc = accounts[0]

    resp = await client.post(f"/settings/{acc.id}/toggle", follow_redirects=False)
    assert resp.status_code in (303, 302), f"Expected redirect, got {resp.status_code}"
    assert "/settings" in resp.headers.get("location", "")


@pytest.mark.asyncio
async def test_delete_account(client, base_app):
    """Delete account."""
    app, db, pool = base_app
    # Add a second account so we still have one after deletion
    await db.add_account(Account(phone="+9999999999", session_string="session_del"))
    accounts = await db.get_accounts(active_only=False)
    to_delete = next(a for a in accounts if a.phone == "+9999999999")

    resp = await client.post(f"/settings/{to_delete.id}/delete", follow_redirects=False)
    assert resp.status_code in (303, 302)
    assert "/settings" in resp.headers.get("location", "")

    # Verify deleted
    remaining = await db.get_accounts(active_only=False)
    assert not any(a.phone == "+9999999999" for a in remaining)


@pytest.mark.asyncio
async def test_flood_status_empty(client, base_app):
    """Flood status returns JSON with no active floods."""
    app, db, pool = base_app
    resp = await client.get("/settings/flood-status")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    for item in data:
        assert "phone" in item
        assert "flood_wait_until" in item
        assert "remaining_seconds" in item


@pytest.mark.asyncio
async def test_flood_status_active_flood(client, base_app):
    """Flood status shows active flood wait."""
    app, db, pool = base_app
    accounts = await db.get_accounts(active_only=False)
    acc = accounts[0]
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    await db.update_account_flood(acc.phone, future)

    resp = await client.get("/settings/flood-status")
    assert resp.status_code == 200
    data = resp.json()
    flooded = [item for item in data if item["phone"] == acc.phone]
    assert len(flooded) == 1
    assert flooded[0]["flood_wait_until"] != "ok"
    assert flooded[0]["remaining_seconds"] > 0


@pytest.mark.asyncio
async def test_flood_status_expired_flood(client, base_app):
    """Flood status shows ok for expired flood wait."""
    app, db, pool = base_app
    accounts = await db.get_accounts(active_only=False)
    acc = accounts[0]
    past = datetime.now(timezone.utc) - timedelta(hours=1)
    await db.update_account_flood(acc.phone, past)

    resp = await client.get("/settings/flood-status")
    assert resp.status_code == 200
    data = resp.json()
    entry = [item for item in data if item["phone"] == acc.phone]
    assert len(entry) == 1
    assert entry[0]["flood_wait_until"] == "ok"
    assert entry[0]["remaining_seconds"] == 0


@pytest.mark.asyncio
async def test_flood_clear_success(client, base_app):
    """Flood clear resets flood wait."""
    app, db, pool = base_app
    accounts = await db.get_accounts(active_only=False)
    acc = accounts[0]
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    await db.update_account_flood(acc.phone, future)

    resp = await client.post(f"/settings/{acc.id}/flood-clear", follow_redirects=False)
    assert resp.status_code in (303, 302)
    assert "/settings" in resp.headers.get("location", "")


@pytest.mark.asyncio
async def test_flood_clear_not_found(client, base_app):
    """Flood clear for non-existent account redirects."""
    resp = await client.post("/settings/99999/flood-clear", follow_redirects=False)
    assert resp.status_code in (303, 302)
    assert "error=account_not_found" in resp.headers.get("location", "")
