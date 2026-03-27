"""Tests for agent tools: photo_loader, messaging, notifications.

These tests call actual tool handler functions via the @tool decorator's
.handler attribute, ensuring argument parsing, formatting, and error handling
are all exercised.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.database import Database
from src.models import Account

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_db():
    """Create a mock Database for testing tools."""
    db = MagicMock(spec=Database)
    db.get_setting = AsyncMock(return_value=None)
    return db


def _get_tool_handlers(mock_db, client_pool=None, config=None, **kwargs):
    """Build MCP tools and return their handlers keyed by name."""
    captured_tools = []

    with patch(
        "src.agent.tools.create_sdk_mcp_server",
        side_effect=lambda **kw: captured_tools.extend(kw.get("tools", [])),
    ):
        from src.agent.tools import make_mcp_server

        make_mcp_server(mock_db, client_pool=client_pool, config=config, **kwargs)

    return {t.name: t.handler for t in captured_tools}


def _text(result: dict) -> str:
    """Extract text from tool result payload."""
    return result["content"][0]["text"]


def _make_account(phone="+79001234567", is_active=True, is_primary=True):
    acc = MagicMock(spec=Account)
    acc.id = 1
    acc.phone = phone
    acc.is_active = is_active
    acc.is_primary = is_primary
    acc.session_string = "fake"
    return acc


def _make_mock_pool():
    """Create a mock client pool that returns a native client."""
    mock_client = AsyncMock()
    mock_client.get_entity = AsyncMock(return_value=MagicMock(id=123456))
    mock_client.send_message = AsyncMock()
    mock_client.edit_message = AsyncMock()
    mock_client.delete_messages = AsyncMock(return_value=[MagicMock(pts_count=1)])
    mock_client.forward_messages = AsyncMock(return_value=[MagicMock()])
    mock_client.pin_message = AsyncMock()
    mock_client.unpin_message = AsyncMock()
    mock_client.get_participants = AsyncMock(return_value=[])
    mock_client.kick_participant = AsyncMock()
    mock_client.edit_folder = AsyncMock()
    mock_client.send_read_acknowledge = AsyncMock()
    mock_client.edit_admin = AsyncMock()
    mock_client.edit_permissions = AsyncMock()

    mock_pool = MagicMock()
    mock_pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, None))
    return mock_pool, mock_client


def _make_photo_services():
    """Create mocked PhotoTaskService and PhotoAutoUploadService."""
    photo_task_svc = MagicMock()
    photo_task_svc.list_batches = AsyncMock(return_value=[])
    photo_task_svc.list_items = AsyncMock(return_value=[])
    send_result = MagicMock()
    send_result.id = 42
    send_result.status = "pending"
    photo_task_svc.send_now = AsyncMock(return_value=send_result)
    schedule_result = MagicMock()
    schedule_result.id = 7
    photo_task_svc.schedule_send = AsyncMock(return_value=schedule_result)
    photo_task_svc.cancel_item = AsyncMock(return_value=True)
    photo_task_svc.create_batch = AsyncMock(return_value=99)
    photo_task_svc.run_due = AsyncMock(return_value=3)

    auto_upload_svc = MagicMock()
    auto_upload_svc.list_jobs = AsyncMock(return_value=[])
    auto_upload_svc.get_job = AsyncMock(return_value=None)
    auto_upload_svc.update_job = AsyncMock()
    auto_upload_svc.delete_job = AsyncMock()
    auto_upload_svc.create_job = AsyncMock(return_value=5)
    auto_upload_svc.run_due = AsyncMock(return_value=2)

    return photo_task_svc, auto_upload_svc


@contextmanager
def _photo_ctx(photo_task_svc, auto_upload_svc):
    """Context manager that patches photo services at their source modules."""
    with (
        patch("src.services.photo_task_service.PhotoTaskService", return_value=photo_task_svc),
        patch("src.services.photo_auto_upload_service.PhotoAutoUploadService", return_value=auto_upload_svc),
        patch("src.database.bundles.PhotoLoaderBundle"),
        patch("src.services.photo_publish_service.PhotoPublishService"),
    ):
        yield


@contextmanager
def _notif_ctx(notif_svc):
    """Context manager that patches notification services at their source modules."""
    with (
        patch("src.services.notification_service.NotificationService", return_value=notif_svc),
        patch("src.services.notification_target_service.NotificationTargetService", return_value=MagicMock()),
    ):
        yield


# ===========================================================================
# photo_loader.py tools
# ===========================================================================


class TestListPhotoBatches:
    @pytest.mark.asyncio
    async def test_empty_returns_not_found(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["list_photo_batches"]({})
        assert "не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_batches_shows_info(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()

        batch = MagicMock()
        batch.id = 1
        batch.phone = "+79001234567"
        batch.target_dialog_id = 100
        batch.status = "pending"
        batch.total_items = 5
        batch.created_at = "2025-01-01"
        photo_task_svc.list_batches = AsyncMock(return_value=[batch])

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["list_photo_batches"]({})
        text = _text(result)
        assert "Батчи фото (1)" in text
        assert "batch_id=1" in text


class TestListPhotoItems:
    @pytest.mark.asyncio
    async def test_empty_returns_not_found(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["list_photo_items"]({})
        assert "не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_items_shows_info(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()

        item = MagicMock()
        item.id = 10
        item.batch_id = 1
        item.status = "scheduled"
        item.scheduled_at = "2025-01-01T12:00:00"
        photo_task_svc.list_items = AsyncMock(return_value=[item])

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["list_photo_items"]({})
        text = _text(result)
        assert "Элементы (1)" in text
        assert "item_id=10" in text


class TestSendPhotosNow:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["send_photos_now"]({"phone": "+79001234567", "target": "123", "file_paths": "a.jpg"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["send_photos_now"]({"phone": "+79001234567", "target": "123", "file_paths": "a.jpg"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_success(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])

        with (
            _photo_ctx(photo_task_svc, auto_upload_svc),
            patch("src.services.photo_task_service.PhotoTarget"),
        ):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["send_photos_now"](
                {"phone": "+79001234567", "target": "12345", "file_paths": "a.jpg,b.jpg", "confirm": True}
            )
        text = _text(result)
        assert "Фото отправлены" in text


class TestSchedulePhotos:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["schedule_photos"](
            {"phone": "+79001234567", "target": "123", "file_paths": "a.jpg", "schedule_at": "2025-01-01T10:00:00"}
        )
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["schedule_photos"](
            {"phone": "+79001234567", "target": "123", "file_paths": "a.jpg", "schedule_at": "2025-01-01T10:00:00"}
        )
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_success(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])

        with (
            _photo_ctx(photo_task_svc, auto_upload_svc),
            patch("src.services.photo_task_service.PhotoTarget"),
        ):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["schedule_photos"](
                {
                    "phone": "+79001234567",
                    "target": "12345",
                    "file_paths": "a.jpg",
                    "schedule_at": "2025-01-01T10:00:00",
                    "confirm": True,
                }
            )
        text = _text(result)
        assert "запланированы" in text


class TestCancelPhotoItem:
    @pytest.mark.asyncio
    async def test_missing_item_id_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["cancel_photo_item"]({})
        assert "item_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["cancel_photo_item"]({"item_id": 5})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_found_item_cancelled(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()
        photo_task_svc.cancel_item = AsyncMock(return_value=True)

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["cancel_photo_item"]({"item_id": 5, "confirm": True})
        assert "отменено" in _text(result)

    @pytest.mark.asyncio
    async def test_not_found_returns_message(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()
        photo_task_svc.cancel_item = AsyncMock(return_value=False)

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["cancel_photo_item"]({"item_id": 99, "confirm": True})
        assert "Не удалось отменить" in _text(result)


class TestListAutoUploads:
    @pytest.mark.asyncio
    async def test_empty_returns_not_configured(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["list_auto_uploads"]({})
        assert "не настроены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_jobs_shows_info(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()

        job = MagicMock()
        job.id = 3
        job.phone = "+79001234567"
        job.target_dialog_id = 555
        job.folder_path = "/photos"
        job.interval_minutes = 30
        job.is_active = True
        auto_upload_svc.list_jobs = AsyncMock(return_value=[job])

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["list_auto_uploads"]({})
        text = _text(result)
        assert "Автозагрузки (1)" in text
        assert "id=3" in text


class TestToggleAutoUpload:
    @pytest.mark.asyncio
    async def test_missing_job_id_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["toggle_auto_upload"]({})
        assert "job_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_not_found_returns_error(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()
        auto_upload_svc.get_job = AsyncMock(return_value=None)

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["toggle_auto_upload"]({"job_id": 999})
        assert "не найдена" in _text(result)

    @pytest.mark.asyncio
    async def test_active_job_gets_paused(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()
        job = MagicMock()
        job.is_active = True
        auto_upload_svc.get_job = AsyncMock(return_value=job)

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["toggle_auto_upload"]({"job_id": 3})
        assert "приостановлена" in _text(result)

    @pytest.mark.asyncio
    async def test_inactive_job_gets_activated(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()
        job = MagicMock()
        job.is_active = False
        auto_upload_svc.get_job = AsyncMock(return_value=job)

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["toggle_auto_upload"]({"job_id": 4})
        assert "активирована" in _text(result)


class TestDeleteAutoUpload:
    @pytest.mark.asyncio
    async def test_missing_job_id_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["delete_auto_upload"]({})
        assert "job_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["delete_auto_upload"]({"job_id": 5})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_deleted(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["delete_auto_upload"]({"job_id": 5, "confirm": True})
        assert "удалена" in _text(result)


class TestCreatePhotoBatch:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["create_photo_batch"]({"phone": "+79001234567", "target": "123", "file_paths": "a.jpg"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_missing_fields_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        # empty file_paths and no target → validation error
        result = await handlers["create_photo_batch"]({"phone": "+79001234567"})
        assert "обязательны" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["create_photo_batch"](
            {"phone": "+79001234567", "target": "123", "file_paths": "a.jpg"}
        )
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_batch_created(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])

        with (
            _photo_ctx(photo_task_svc, auto_upload_svc),
            patch("src.services.photo_task_service.PhotoTarget"),
        ):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["create_photo_batch"](
                {"phone": "+79001234567", "target": "12345", "file_paths": "a.jpg,b.jpg", "confirm": True}
            )
        text = _text(result)
        assert "Батч создан" in text


class TestRunPhotoDue:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["run_photo_due"]({"confirm": True})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["run_photo_due"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_processes_items_and_jobs(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["run_photo_due"]({"confirm": True})
        text = _text(result)
        assert "items=3" in text
        assert "auto_jobs=2" in text


class TestCreateAutoUpload:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["create_auto_upload"](
            {"phone": "+79001234567", "target": "123", "folder_path": "/photos"}
        )
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_missing_fields_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        # neither target nor folder_path provided
        result = await handlers["create_auto_upload"]({"phone": "+79001234567"})
        assert "обязательны" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["create_auto_upload"](
            {"phone": "+79001234567", "target": "123", "folder_path": "/photos"}
        )
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_job_created(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["create_auto_upload"](
                {
                    "phone": "+79001234567",
                    "target": "12345",
                    "folder_path": "/photos",
                    "interval_minutes": 30,
                    "mode": "album",
                    "confirm": True,
                }
            )
        text = _text(result)
        assert "Автозагрузка создана" in text


class TestUpdateAutoUpload:
    @pytest.mark.asyncio
    async def test_missing_job_id_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["update_auto_upload"]({})
        assert "job_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["update_auto_upload"]({"job_id": 1, "folder_path": "/new"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_not_found_returns_error(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()
        auto_upload_svc.get_job = AsyncMock(return_value=None)

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["update_auto_upload"]({"job_id": 999, "confirm": True})
        assert "не найдена" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_updated(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()
        existing = MagicMock()
        auto_upload_svc.get_job = AsyncMock(return_value=existing)

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["update_auto_upload"](
                {"job_id": 3, "folder_path": "/new_folder", "interval_minutes": 60, "confirm": True}
            )
        assert "обновлена" in _text(result)


# ===========================================================================
# messaging.py tools
# ===========================================================================


class TestSendMessage:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["send_message"]({"phone": "+79001234567", "recipient": "@user", "text": "hi"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["send_message"]({"phone": "+79001234567", "recipient": "@user", "text": "hello"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["send_message"](
            {"phone": "+79001234567", "recipient": "@user", "text": "hello", "confirm": True}
        )
        text = _text(result)
        assert "отправлено" in text

    @pytest.mark.asyncio
    async def test_missing_recipient_or_text_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        # no recipient, no text
        result = await handlers["send_message"]({"phone": "+79001234567"})
        text = _text(result)
        assert "обязательны" in text


class TestEditMessage:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["edit_message"](
            {"phone": "+79001234567", "chat_id": "123", "message_id": 1, "text": "new"}
        )
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_message"](
            {"phone": "+79001234567", "chat_id": "123", "message_id": 1, "text": "new"}
        )
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_message"](
            {"phone": "+79001234567", "chat_id": "123", "message_id": 5, "text": "updated text", "confirm": True}
        )
        text = _text(result)
        assert "отредактировано" in text

    @pytest.mark.asyncio
    async def test_missing_message_id_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_message"]({"phone": "+79001234567", "chat_id": "123", "text": "new"})
        text = _text(result)
        assert "обязательны" in text


class TestDeleteMessage:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["delete_message"]({"phone": "+79001234567", "chat_id": "123", "message_ids": "1,2"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_invalid_message_ids_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["delete_message"](
            {"phone": "+79001234567", "chat_id": "123", "message_ids": "abc,xyz"}
        )
        assert "валидные message_ids" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["delete_message"](
            {"phone": "+79001234567", "chat_id": "123", "message_ids": "1,2,3"}
        )
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["delete_message"](
            {"phone": "+79001234567", "chat_id": "123", "message_ids": "1,2", "confirm": True}
        )
        text = _text(result)
        assert "Удалено" in text


class TestForwardMessages:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["forward_messages"](
            {"phone": "+79001234567", "from_chat": "A", "to_chat": "B", "message_ids": "1"}
        )
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_invalid_ids_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["forward_messages"](
            {"phone": "+79001234567", "from_chat": "A", "to_chat": "B", "message_ids": "abc"}
        )
        assert "валидные message_ids" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["forward_messages"](
            {"phone": "+79001234567", "from_chat": "A", "to_chat": "B", "message_ids": "1,2"}
        )
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["forward_messages"](
            {
                "phone": "+79001234567",
                "from_chat": "chatA",
                "to_chat": "chatB",
                "message_ids": "1,2",
                "confirm": True,
            }
        )
        assert "Переслано" in _text(result)


class TestPinMessage:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["pin_message"]({"phone": "+79001234567", "chat_id": "chat", "message_id": 1})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["pin_message"]({"phone": "+79001234567", "chat_id": "chat", "message_id": 10})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["pin_message"](
            {"phone": "+79001234567", "chat_id": "chat", "message_id": 10, "confirm": True}
        )
        assert "закреплено" in _text(result)


class TestUnpinMessage:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["unpin_message"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["unpin_message"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["unpin_message"]({"phone": "+79001234567", "chat_id": "chat", "confirm": True})
        assert "откреплено" in _text(result)


class TestGetParticipants:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["get_participants"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_empty_participants(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_client.get_participants = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["get_participants"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_participants(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        participant = MagicMock()
        participant.id = 111
        participant.first_name = "John"
        participant.last_name = "Doe"
        participant.username = "johndoe"
        mock_client.get_participants = AsyncMock(return_value=[participant])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["get_participants"]({"phone": "+79001234567", "chat_id": "chat"})
        text = _text(result)
        assert "111" in text
        assert "John" in text

    @pytest.mark.asyncio
    async def test_missing_chat_id_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["get_participants"]({"phone": "+79001234567"})
        assert "chat_id обязателен" in _text(result)


class TestKickParticipant:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["kick_participant"]({"phone": "+79001234567", "chat_id": "chat", "user_id": "111"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["kick_participant"]({"phone": "+79001234567", "chat_id": "chat", "user_id": "111"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["kick_participant"](
            {"phone": "+79001234567", "chat_id": "chat", "user_id": "111", "confirm": True}
        )
        assert "исключён" in _text(result)


class TestArchiveChat:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["archive_chat"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["archive_chat"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["archive_chat"]({"phone": "+79001234567", "chat_id": "chat", "confirm": True})
        assert "архивирован" in _text(result)


class TestMarkRead:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["mark_read"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_missing_chat_id_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["mark_read"]({"phone": "+79001234567"})
        assert "chat_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_with_pool_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["mark_read"]({"phone": "+79001234567", "chat_id": "chat"})
        assert "прочитанные" in _text(result)


class TestEditAdmin:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["edit_admin"]({"phone": "+79001234567", "chat_id": "chat", "user_id": "111"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_admin"]({"phone": "+79001234567", "chat_id": "chat", "user_id": "111"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_promote_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_admin"](
            {"phone": "+79001234567", "chat_id": "chat", "user_id": "111", "is_admin": True, "confirm": True}
        )
        assert "обновлены" in _text(result)


class TestEditPermissions:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["edit_permissions"](
            {"phone": "+79001234567", "chat_id": "chat", "user_id": "111", "send_messages": False}
        )
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_no_flags_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_permissions"]({"phone": "+79001234567", "chat_id": "chat", "user_id": "111"})
        text = _text(result)
        assert "флаг" in text

    @pytest.mark.asyncio
    async def test_with_confirm_success(self, mock_db):
        mock_pool, mock_client = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["edit_permissions"](
            {
                "phone": "+79001234567",
                "chat_id": "chat",
                "user_id": "111",
                "send_messages": False,
                "confirm": True,
            }
        )
        assert "обновлены" in _text(result)


# ===========================================================================
# notifications.py tools
# ===========================================================================


class TestGetNotificationStatus:
    @pytest.mark.asyncio
    async def test_not_configured_returns_message(self, mock_db):
        notif_svc = MagicMock()
        notif_svc.get_status = AsyncMock(return_value=None)
        mock_pool, _ = _make_mock_pool()

        with _notif_ctx(notif_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["get_notification_status"]({})
        assert "не настроен" in _text(result)

    @pytest.mark.asyncio
    async def test_configured_shows_bot_details(self, mock_db):
        notif_svc = MagicMock()
        bot = MagicMock()
        bot.bot_username = "my_bot"
        bot.chat_id = 123456
        bot.created_at = "2025-01-01"
        notif_svc.get_status = AsyncMock(return_value=bot)
        mock_pool, _ = _make_mock_pool()

        with _notif_ctx(notif_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["get_notification_status"]({})
        text = _text(result)
        assert "my_bot" in text
        assert "123456" in text


class TestSetupNotificationBot:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["setup_notification_bot"]({"confirm": True})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["setup_notification_bot"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_pool_and_confirm_success(self, mock_db):
        notif_svc = MagicMock()
        bot = MagicMock()
        bot.bot_username = "test_notify_bot"
        bot.chat_id = 789
        notif_svc.setup_bot = AsyncMock(return_value=bot)
        mock_pool, _ = _make_mock_pool()

        with _notif_ctx(notif_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["setup_notification_bot"]({"confirm": True})
        text = _text(result)
        assert "создан" in text


class TestDeleteNotificationBot:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["delete_notification_bot"]({"confirm": True})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["delete_notification_bot"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_deleted(self, mock_db):
        notif_svc = MagicMock()
        notif_svc.teardown_bot = AsyncMock()
        mock_pool, _ = _make_mock_pool()

        with _notif_ctx(notif_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["delete_notification_bot"]({"confirm": True})
        assert "удалён" in _text(result)


class TestTestNotification:
    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["test_notification"]({})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_not_configured_returns_message(self, mock_db):
        notif_svc = MagicMock()
        notif_svc.get_status = AsyncMock(return_value=None)
        mock_pool, _ = _make_mock_pool()

        with _notif_ctx(notif_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["test_notification"]({})
        assert "не настроен" in _text(result)

    @pytest.mark.asyncio
    async def test_configured_sends_test(self, mock_db):
        notif_svc = MagicMock()
        bot = MagicMock()
        notif_svc.get_status = AsyncMock(return_value=bot)
        notif_svc.send_notification = AsyncMock()
        mock_pool, _ = _make_mock_pool()

        with _notif_ctx(notif_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["test_notification"]({})
        text = _text(result)
        assert "отправлено" in text
