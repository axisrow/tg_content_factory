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


# === react ===


@pytest.mark.anyio
async def test_react_rejects_unsupported_emoji(route_client):
    resp = await route_client.post(
        "/dialogs/react",
        data={
            "phone": "+1234567890",
            "chat_id": "-100123",
            "message_id": "42",
            "emoji": "✅",
        },
        follow_redirects=False,
    )

    assert resp.status_code == 303
    location = resp.headers.get("location", "")
    assert "invalid_reaction" in location
    assert "command_id=" not in location


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


# === join / resolve (parity: dialogs join / dialogs resolve) ===


@pytest.mark.anyio
async def test_join_dialog_missing_fields(route_client):
    resp = await route_client.post("/dialogs/join", data={"phone": ""}, follow_redirects=False)
    assert resp.status_code == 303


@pytest.mark.anyio
async def test_join_dialog_enqueues(route_client):
    resp = await route_client.post(
        "/dialogs/join",
        data={"phone": "+1234567890", "target": "@somechannel"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers.get("location", "")


@pytest.mark.anyio
async def test_resolve_entity_missing_fields(route_client):
    resp = await route_client.post("/dialogs/resolve", data={"identifier": ""}, follow_redirects=False)
    assert resp.status_code == 303


@pytest.mark.anyio
async def test_resolve_entity_enqueues(route_client):
    resp = await route_client.post(
        "/dialogs/resolve",
        data={"phone": "+1234567890", "identifier": "@someuser"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers.get("location", "")


# === queue cancel / clear-pending (issue #621) ===


async def _seed_pending_command(db, *, phone="+1234567890", emoji="👍"):
    """Insert one PENDING dialogs.react command and return its id."""
    from src.models import TelegramCommand, TelegramCommandStatus

    return await db.repos.telegram_commands.create_command(
        TelegramCommand(
            command_type="dialogs.react",
            payload={"phone": phone, "chat_id": "-100123", "message_id": 5, "emoji": emoji},
            status=TelegramCommandStatus.PENDING,
        )
    )


@pytest.mark.anyio
async def test_cancel_queue_command_cancels_pending(route_client, base_app):
    from src.models import TelegramCommandStatus

    _, db, _ = base_app
    command_id = await _seed_pending_command(db)

    resp = await route_client.post(
        f"/dialogs/queue/{command_id}/cancel",
        data={"phone": "+1234567890"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_cancelled" in resp.headers.get("location", "")

    cmd = await db.repos.telegram_commands.get_command(command_id)
    assert cmd.status == TelegramCommandStatus.CANCELLED


@pytest.mark.anyio
async def test_cancel_queue_command_missing_id_redirects_with_error(route_client, base_app):
    _, db, _ = base_app
    # 999999 does not exist → cancel() returns False → error in redirect, no crash.
    resp = await route_client.post(
        "/dialogs/queue/999999/cancel",
        data={"phone": "+1234567890"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_not_cancellable" in resp.headers.get("location", "")


@pytest.mark.anyio
async def test_clear_pending_queue_commands_bulk_cancels(route_client, base_app):
    from src.models import TelegramCommandStatus

    _, db, _ = base_app
    ids = [await _seed_pending_command(db, emoji=e) for e in ("👍", "❤️", "🔥")]

    resp = await route_client.post(
        "/dialogs/queue/clear-pending",
        data={"command_type": "dialogs.react"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "pending_commands_cancelled" in resp.headers.get("location", "")

    for command_id in ids:
        cmd = await db.repos.telegram_commands.get_command(command_id)
        assert cmd.status == TelegramCommandStatus.CANCELLED


@pytest.mark.anyio
async def test_clear_pending_queue_commands_empty_reports_empty(route_client, base_app):
    # No pending commands seeded → cancelled == 0 → "pending_commands_empty".
    resp = await route_client.post(
        "/dialogs/queue/clear-pending",
        data={"command_type": "dialogs.react"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "pending_commands_empty" in resp.headers.get("location", "")
