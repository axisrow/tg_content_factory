"""Tests for uncovered dialogs route endpoints."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest


@pytest.fixture
async def client(route_client):
    return route_client


# === participants ===


@pytest.mark.asyncio
async def test_participants_missing_fields(client):
    resp = await client.get("/dialogs/participants?phone=")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_participants_queues(client):
    resp = await client.get(
        "/dialogs/participants?phone=%2B1234567890&chat_id=-100123",
        follow_redirects=False,
    )
    assert resp.status_code == 202
    assert resp.json()["command_id"] is not None


@pytest.mark.asyncio
async def test_participants_with_search_queues(client):
    resp = await client.get(
        "/dialogs/participants?phone=%2B1234567890&chat_id=-100123&search=test",
        follow_redirects=False,
    )
    assert resp.status_code == 202
    assert resp.json()["command_id"] is not None


@pytest.mark.asyncio
async def test_participants_cache_hit(client, monkeypatch):
    snapshot = SimpleNamespace(payload={"participants": [{"id": 1}]})
    mock_db = MagicMock()
    mock_db.repos.runtime_snapshots.get_snapshot = AsyncMock(return_value=snapshot)
    monkeypatch.setattr("src.web.deps.get_db", lambda r: mock_db)
    resp = await client.get("/dialogs/participants?phone=%2B1234567890&chat_id=-100123")
    assert resp.status_code == 200
    assert resp.json()["participants"]


# === edit-admin ===


@pytest.mark.asyncio
async def test_edit_admin_missing_fields(client):
    resp = await client.post(
        "/dialogs/edit-admin",
        data={"phone": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_edit_admin_success(client):
    resp = await client.post(
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


@pytest.mark.asyncio
async def test_edit_permissions_no_flags(client):
    resp = await client.post(
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


@pytest.mark.asyncio
async def test_edit_permissions_success(client):
    resp = await client.post(
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


@pytest.mark.asyncio
async def test_edit_permissions_missing_fields(client):
    resp = await client.post(
        "/dialogs/edit-permissions",
        data={"send_messages": "1"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "missing_fields" in resp.headers.get("location", "")


# === kick ===


@pytest.mark.asyncio
async def test_kick_missing_fields(client):
    resp = await client.post(
        "/dialogs/kick",
        data={"phone": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_kick_success(client):
    resp = await client.post(
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


@pytest.mark.asyncio
async def test_broadcast_stats_missing_fields(client):
    resp = await client.get("/dialogs/broadcast-stats?phone=")
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_broadcast_stats_queues(client):
    resp = await client.get(
        "/dialogs/broadcast-stats?phone=%2B1234567890&chat_id=-100123",
        follow_redirects=False,
    )
    assert resp.status_code == 202


# === archive ===


@pytest.mark.asyncio
async def test_archive_missing_fields(client):
    resp = await client.post(
        "/dialogs/archive",
        data={"phone": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_archive_success(client):
    resp = await client.post(
        "/dialogs/archive",
        data={
            "phone": "+1234567890",
            "chat_id": "-100123",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303


# === unarchive ===


@pytest.mark.asyncio
async def test_unarchive_missing_fields(client):
    resp = await client.post(
        "/dialogs/unarchive",
        data={"phone": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_unarchive_success(client):
    resp = await client.post(
        "/dialogs/unarchive",
        data={
            "phone": "+1234567890",
            "chat_id": "-100123",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303


# === mark-read ===


@pytest.mark.asyncio
async def test_mark_read_missing_fields(client):
    resp = await client.post(
        "/dialogs/mark-read",
        data={"phone": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_mark_read_success(client):
    resp = await client.post(
        "/dialogs/mark-read",
        data={
            "phone": "+1234567890",
            "chat_id": "-100123",
            "max_id": "999",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_mark_read_no_max_id(client):
    resp = await client.post(
        "/dialogs/mark-read",
        data={
            "phone": "+1234567890",
            "chat_id": "-100123",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
