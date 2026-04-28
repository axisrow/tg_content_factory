"""Tests for account management routes."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.models import Account


@pytest.mark.anyio
async def test_toggle_account_enqueues_command(route_client, base_app):
    """Web toggle only enqueues `accounts.toggle`; worker reconciles the pool."""
    app, db, pool = base_app
    accounts = await db.get_accounts(active_only=False)
    assert len(accounts) > 0
    acc = accounts[0]

    resp = await route_client.post(f"/settings/{acc.id}/toggle", follow_redirects=False)
    assert resp.status_code in (303, 302)
    location = resp.headers.get("location", "")
    assert "/settings" in location
    assert "account_toggle_queued" in location
    assert "command_id=" in location

    pool.add_client.assert_not_called() if hasattr(pool, "add_client") else None
    pool.remove_client.assert_not_called()

    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "accounts.toggle"
    assert commands[0].payload == {"account_id": acc.id}


@pytest.mark.anyio
async def test_delete_account_enqueues_command(route_client, base_app):
    """Web delete only enqueues `accounts.delete`; the DB row survives until the worker runs."""
    app, db, pool = base_app
    await db.add_account(Account(phone="+9999999999", session_string="session_del"))
    accounts = await db.get_accounts(active_only=False)
    to_delete = next(a for a in accounts if a.phone == "+9999999999")

    resp = await route_client.post(f"/settings/{to_delete.id}/delete", follow_redirects=False)
    assert resp.status_code in (303, 302)
    location = resp.headers.get("location", "")
    assert "/settings" in location
    assert "account_delete_queued" in location
    assert "command_id=" in location

    # Web must not have touched the DB row directly — only a worker running
    # `accounts.delete` is allowed to delete.
    pool.remove_client.assert_not_called()
    remaining = await db.get_accounts(active_only=False)
    assert any(a.phone == "+9999999999" for a in remaining)

    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "accounts.delete"
    assert commands[0].payload == {"account_id": to_delete.id}


@pytest.mark.anyio
async def test_flood_status_empty(route_client, base_app):
    """Flood status returns JSON with no active floods."""
    app, db, pool = base_app
    resp = await route_client.get("/settings/flood-status")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    for item in data:
        assert "phone" in item
        assert "flood_wait_until" in item
        assert "remaining_seconds" in item


@pytest.mark.anyio
async def test_flood_status_active_flood(route_client, base_app):
    """Flood status shows active flood wait."""
    app, db, pool = base_app
    accounts = await db.get_accounts(active_only=False)
    acc = accounts[0]
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    await db.update_account_flood(acc.phone, future)

    resp = await route_client.get("/settings/flood-status")
    assert resp.status_code == 200
    data = resp.json()
    flooded = [item for item in data if item["phone"] == acc.phone]
    assert len(flooded) == 1
    assert flooded[0]["flood_wait_until"] != "ok"
    assert flooded[0]["remaining_seconds"] > 0


@pytest.mark.anyio
async def test_flood_status_expired_flood(route_client, base_app):
    """Flood status shows ok for expired flood wait."""
    app, db, pool = base_app
    accounts = await db.get_accounts(active_only=False)
    acc = accounts[0]
    past = datetime.now(timezone.utc) - timedelta(hours=1)
    await db.update_account_flood(acc.phone, past)

    resp = await route_client.get("/settings/flood-status")
    assert resp.status_code == 200
    data = resp.json()
    entry = [item for item in data if item["phone"] == acc.phone]
    assert len(entry) == 1
    assert entry[0]["flood_wait_until"] == "ok"
    assert entry[0]["remaining_seconds"] == 0


@pytest.mark.anyio
async def test_flood_clear_success(route_client, base_app):
    """Flood clear resets flood wait."""
    app, db, pool = base_app
    accounts = await db.get_accounts(active_only=False)
    acc = accounts[0]
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    await db.update_account_flood(acc.phone, future)

    resp = await route_client.post(f"/settings/{acc.id}/flood-clear", follow_redirects=False)
    assert resp.status_code in (303, 302)
    assert "/settings" in resp.headers.get("location", "")


@pytest.mark.anyio
async def test_flood_clear_not_found(route_client, base_app):
    """Flood clear for non-existent account redirects."""
    resp = await route_client.post("/settings/99999/flood-clear", follow_redirects=False)
    assert resp.status_code in (303, 302)
    assert "error=account_not_found" in resp.headers.get("location", "")
