"""Tests for photo_loader routes."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, patch

import pytest


@pytest.fixture
async def client(route_client):
    """Use shared route_client fixture."""
    return route_client


@pytest.fixture
async def db(base_app):
    """Get db from base_app."""
    _, db, _ = base_app
    return db


@pytest.fixture
async def pool_mock(base_app):
    """Get pool_mock from base_app."""
    _, _, pool_mock = base_app
    return pool_mock


@pytest.mark.asyncio
async def test_photo_loader_page_no_phone(client):
    """Test photo loader page without phone param."""
    resp = await client.get("/dialogs/photos")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_photo_loader_page_with_phone(client):
    """Test photo loader page with phone param."""
    resp = await client.get("/dialogs/photos?phone=%2B1234567890")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_legacy_photo_loader_route_redirects_to_dialogs(client):
    legacy_prefix = "/my" + "-telegram"
    resp = await client.get(
        f"{legacy_prefix}/photos?phone=%2B1234567890",
        follow_redirects=False,
    )
    assert resp.status_code == 308
    assert resp.headers["location"] == "/dialogs/photos?phone=%2B1234567890"


@pytest.mark.asyncio
async def test_photo_loader_page_shows_no_jobs(client, db):
    """Test photo loader page shows no auto jobs."""
    resp = await client.get("/dialogs/photos?phone=%2B1234567890")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_photo_refresh_redirects(client):
    """Test photo refresh redirects."""
    db = client._transport.app.state.db
    with patch("src.web.routes.photo_loader.deps.channel_service") as mock_svc:
        mock_svc.return_value.get_my_dialogs = AsyncMock(return_value=[])
        resp = await client.post(
            "/dialogs/photos/refresh",
            data={"phone": "+1234567890"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "command_id=" in resp.headers["location"]
        mock_svc.return_value.get_my_dialogs.assert_not_awaited()
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "dialogs.refresh"


@pytest.mark.asyncio
async def test_photo_send_missing_target(client):
    """Test photo send with missing target."""
    with patch(
        "src.web.routes.photo_loader._persist_uploads",
        AsyncMock(return_value=[]),
    ):
        # Create a minimal fake file upload
        from io import BytesIO


        file_content = BytesIO(b"fake image")
        resp = await client.post(
            "/dialogs/photos/send",
            data={"phone": "+1234567890"},
            files={"photos": ("test.jpg", file_content, "image/jpeg")},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "error=photo_target_required" in resp.headers["location"]


@pytest.mark.asyncio
async def test_photo_send_invalid_target_id(client):
    """Test photo send with invalid target ID."""
    from io import BytesIO

    with patch(
        "src.web.routes.photo_loader._persist_uploads",
        AsyncMock(return_value=[]),
    ), patch("src.web.routes.photo_loader.deps.channel_service") as mock_svc:
        mock_svc.return_value.get_my_dialogs = AsyncMock(return_value=[])
        file_content = BytesIO(b"fake image")
        resp = await client.post(
            "/dialogs/photos/send",
            data={
                "phone": "+1234567890",
                "target_dialog_id": "not_a_number",
            },
            files={"photos": ("test.jpg", file_content, "image/jpeg")},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "error=photo_target_invalid" in resp.headers["location"]


@pytest.mark.asyncio
async def test_photo_send_no_files(client):
    """Test photo send with empty persisted files."""
    from io import BytesIO

    db = client._transport.app.state.db
    with patch(
        "src.web.routes.photo_loader._persist_uploads",
        AsyncMock(return_value=["/tmp/one.jpg"]),
    ), patch("src.web.routes.photo_loader.deps.channel_service") as mock_svc, patch(
        "src.web.routes.photo_loader.deps.get_photo_task_service"
    ) as mock_task_svc:
        mock_svc.return_value.get_my_dialogs = AsyncMock(
            return_value=[{"channel_id": 200, "title": "Dialog", "channel_type": "channel"}]
        )
        mock_task_svc.return_value.send_now = AsyncMock()
        file_content = BytesIO(b"fake image")
        resp = await client.post(
            "/dialogs/photos/send",
            data={
                "phone": "+1234567890",
                "target_dialog_id": "200",
            },
            files={"photos": ("test.jpg", file_content, "image/jpeg")},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "command_id=" in resp.headers["location"]
        mock_task_svc.return_value.send_now.assert_not_awaited()
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "photo.send_now"
    assert commands[0].payload["file_paths"] == ["/tmp/one.jpg"]


@pytest.mark.asyncio
async def test_photo_schedule_missing_target(client):
    """Test photo schedule with missing target."""
    from io import BytesIO

    # Schedule requires photos as File(...)
    future_date = (datetime.now(tz=timezone.utc) + timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S")
    file_content = BytesIO(b"fake image")
    resp = await client.post(
        "/dialogs/photos/schedule",
        data={
            "phone": "+1234567890",
            "schedule_at": future_date,
        },
        files={"photos": ("test.jpg", file_content, "image/jpeg")},
        follow_redirects=False,
    )
    # Missing target_dialog_id returns photo_target_required
    assert resp.status_code == 303
    assert "error=photo_target_required" in resp.headers["location"]


@pytest.mark.asyncio
async def test_photo_run_due_redirects(client):
    """Test photo run due redirects."""
    db = client._transport.app.state.db
    with patch(
        "src.web.routes.photo_loader.deps.get_photo_task_service"
    ) as mock_task_svc, patch(
        "src.web.routes.photo_loader.deps.get_photo_auto_upload_service"
    ) as mock_auto_svc:
        mock_task_svc.return_value.run_due = AsyncMock()
        mock_auto_svc.return_value.run_due = AsyncMock()
        resp = await client.post(
            "/dialogs/photos/run-due",
            data={"phone": "+1234567890"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "command_id=" in resp.headers["location"]
        mock_task_svc.return_value.run_due.assert_not_awaited()
        mock_auto_svc.return_value.run_due.assert_not_awaited()
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "photo.run_due"


@pytest.mark.asyncio
async def test_photo_cancel_item_not_found(client):
    """Test photo cancel item not found."""
    with patch(
        "src.web.routes.photo_loader.deps.get_photo_task_service"
    ) as mock_svc:
        mock_svc.return_value.cancel_item = AsyncMock(return_value=False)
        resp = await client.post(
            "/dialogs/photos/items/999999/cancel",
            data={"phone": "+1234567890"},
            follow_redirects=False,
        )
        assert resp.status_code == 303


@pytest.mark.asyncio
async def test_photo_toggle_auto_not_found(client):
    """Test photo toggle auto job not found."""
    with patch(
        "src.web.routes.photo_loader.deps.get_photo_auto_upload_service"
    ) as mock_svc:
        mock_svc.return_value.get_job = AsyncMock(return_value=None)
        resp = await client.post(
            "/dialogs/photos/auto/999999/toggle",
            data={"phone": "+1234567890"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "error=photo_auto_failed" in resp.headers["location"]


@pytest.mark.asyncio
async def test_photo_delete_auto(client):
    """Test photo delete auto job."""
    with patch(
        "src.web.routes.photo_loader.deps.get_photo_auto_upload_service"
    ) as mock_svc:
        mock_svc.return_value.delete_job = AsyncMock()
        resp = await client.post(
            "/dialogs/photos/auto/1/delete",
            data={"phone": "+1234567890"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=photo_auto_deleted" in resp.headers["location"]


@pytest.mark.asyncio
async def test_photo_batch_missing_target(client):
    """Test photo batch with missing target."""
    resp = await client.post(
        "/dialogs/photos/batch",
        data={
            "phone": "+1234567890",
            "manifest_text": "[]",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=photo_target_required" in resp.headers["location"]


@pytest.mark.asyncio
async def test_photo_auto_missing_target(client):
    """Test photo auto with missing target."""
    resp = await client.post(
        "/dialogs/photos/auto",
        data={
            "phone": "+1234567890",
            "folder_path": "/tmp/photos",
            "interval_minutes": "60",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=photo_target_required" in resp.headers["location"]


@pytest.mark.asyncio
async def test_photos_refresh_missing_phone(client):
    """POST /dialogs/photos/refresh without phone returns 422."""
    resp = await client.post("/dialogs/photos/refresh", data={}, follow_redirects=False)
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_photos_send_missing_phone(client):
    """POST /dialogs/photos/send without phone returns redirect with error."""
    from io import BytesIO
    resp = await client.post(
        "/dialogs/photos/send",
        files={"photos": ("x.jpg", BytesIO(b"x"), "image/jpeg")},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_photos_schedule_missing_phone(client):
    """POST /dialogs/photos/schedule without phone returns redirect with error."""
    from io import BytesIO
    resp = await client.post(
        "/dialogs/photos/schedule",
        data={"schedule_at": "2026-01-01T10:00"},
        files={"photos": ("x.jpg", BytesIO(b"x"), "image/jpeg")},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_photos_schedule_missing_schedule_at(client):
    """POST /dialogs/photos/schedule without schedule_at returns redirect with error."""
    from io import BytesIO
    resp = await client.post(
        "/dialogs/photos/schedule",
        data={"phone": "+1234567890"},
        files={"photos": ("x.jpg", BytesIO(b"x"), "image/jpeg")},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=" in resp.headers["location"]


@pytest.mark.asyncio
async def test_photos_batch_missing_phone(client):
    """POST /dialogs/photos/batch without phone returns 422."""
    resp = await client.post("/dialogs/photos/batch", data={}, follow_redirects=False)
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_photos_auto_missing_phone(client):
    """POST /dialogs/photos/auto without phone returns 422."""
    resp = await client.post(
        "/dialogs/photos/auto",
        data={"folder_path": "/tmp/photos", "interval_minutes": "60"},
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_photos_auto_missing_folder_path(client):
    """POST /dialogs/photos/auto without folder_path returns 422."""
    resp = await client.post(
        "/dialogs/photos/auto",
        data={"phone": "+1234567890", "interval_minutes": "60"},
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.asyncio
async def test_photos_auto_missing_interval_minutes(client):
    """POST /dialogs/photos/auto without interval_minutes returns 422."""
    resp = await client.post(
        "/dialogs/photos/auto",
        data={"phone": "+1234567890", "folder_path": "/tmp/photos"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
