"""Tests for dialogs route action endpoints and validation paths."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

# === participants ===


@pytest.mark.anyio
async def test_participants_missing_fields(route_client):
    resp = await route_client.get("/dialogs/participants?phone=")
    assert resp.status_code == 400


@pytest.mark.anyio
async def test_participants_queues(route_client):
    resp = await route_client.get(
        "/dialogs/participants?phone=%2B1234567890&chat_id=-100123",
        follow_redirects=False,
    )
    assert resp.status_code == 202
    assert resp.json()["command_id"] is not None


@pytest.mark.anyio
async def test_participants_with_search_queues(route_client):
    resp = await route_client.get(
        "/dialogs/participants?phone=%2B1234567890&chat_id=-100123&search=test",
        follow_redirects=False,
    )
    assert resp.status_code == 202
    assert resp.json()["command_id"] is not None


@pytest.mark.anyio
async def test_participants_cache_hit(route_client, monkeypatch):
    snapshot = SimpleNamespace(payload={"participants": [{"id": 1}]})
    mock_db = MagicMock()
    mock_db.repos.runtime_snapshots.get_snapshot = AsyncMock(return_value=snapshot)
    monkeypatch.setattr("src.web.deps.get_db", lambda r: mock_db)
    resp = await route_client.get("/dialogs/participants?phone=%2B1234567890&chat_id=-100123")
    assert resp.status_code == 200
    assert resp.json()["participants"]


# === edit-admin ===


@pytest.mark.anyio
async def test_edit_admin_missing_fields(route_client):
    resp = await route_client.post(
        "/dialogs/edit-admin",
        data={"phone": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.anyio
async def test_edit_admin_success(route_client):
    resp = await route_client.post(
        "/dialogs/edit-admin",
        data={
            "phone": "+1234567890",
            "chat_id": "-100123",
            "user_id": "42",
            "is_admin": "1",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers.get("location", "")


# === edit-permissions ===


@pytest.mark.anyio
async def test_edit_permissions_no_flags(route_client):
    resp = await route_client.post(
        "/dialogs/edit-permissions",
        data={
            "phone": "+1234567890",
            "chat_id": "-100123",
            "user_id": "42",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "no_permission_flags" in resp.headers.get("location", "")


@pytest.mark.anyio
async def test_edit_permissions_success(route_client):
    resp = await route_client.post(
        "/dialogs/edit-permissions",
        data={
            "phone": "+1234567890",
            "chat_id": "-100123",
            "user_id": "42",
            "send_messages": "1",
            "send_media": "0",
            "until_date": "2026-12-31",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.anyio
async def test_edit_permissions_missing_fields(route_client):
    resp = await route_client.post(
        "/dialogs/edit-permissions",
        data={"send_messages": "1"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "missing_fields" in resp.headers.get("location", "")


# === kick ===


@pytest.mark.anyio
async def test_kick_missing_fields(route_client):
    resp = await route_client.post(
        "/dialogs/kick",
        data={"phone": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.anyio
async def test_kick_success(route_client):
    resp = await route_client.post(
        "/dialogs/kick",
        data={
            "phone": "+1234567890",
            "chat_id": "-100123",
            "user_id": "42",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303


# === broadcast-stats ===


@pytest.mark.anyio
async def test_broadcast_stats_missing_fields(route_client):
    resp = await route_client.get("/dialogs/broadcast-stats?phone=")
    assert resp.status_code == 400


@pytest.mark.anyio
async def test_broadcast_stats_queues(route_client):
    resp = await route_client.get(
        "/dialogs/broadcast-stats?phone=%2B1234567890&chat_id=-100123",
        follow_redirects=False,
    )
    assert resp.status_code == 202


# === archive ===


@pytest.mark.anyio
async def test_archive_missing_fields(route_client):
    resp = await route_client.post(
        "/dialogs/archive",
        data={"phone": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.anyio
async def test_archive_success(route_client):
    resp = await route_client.post(
        "/dialogs/archive",
        data={
            "phone": "+1234567890",
            "chat_id": "-100123",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303


# === unarchive ===


@pytest.mark.anyio
async def test_unarchive_missing_fields(route_client):
    resp = await route_client.post(
        "/dialogs/unarchive",
        data={"phone": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.anyio
async def test_unarchive_success(route_client):
    resp = await route_client.post(
        "/dialogs/unarchive",
        data={
            "phone": "+1234567890",
            "chat_id": "-100123",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303


# === mark-read ===


@pytest.mark.anyio
async def test_mark_read_missing_fields(route_client):
    resp = await route_client.post(
        "/dialogs/mark-read",
        data={"phone": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.anyio
async def test_mark_read_success(route_client):
    resp = await route_client.post(
        "/dialogs/mark-read",
        data={
            "phone": "+1234567890",
            "chat_id": "-100123",
            "max_id": "999",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.anyio
async def test_mark_read_no_max_id(route_client):
    resp = await route_client.post(
        "/dialogs/mark-read",
        data={
            "phone": "+1234567890",
            "chat_id": "-100123",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
