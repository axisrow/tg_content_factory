"""Tests for agent tools: photo_loader.py."""
from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.models import Account
from tests.agent_tools_helpers import _get_tool_handlers, _text


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

    mock_session = MagicMock()
    mock_pool = MagicMock()
    mock_pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, None))
    mock_pool.get_client_by_phone = AsyncMock(return_value=(mock_session, None))
    mock_pool.resolve_dialog_entity = AsyncMock(return_value=MagicMock(id=123456))
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


class TestListPhotoBatches:
    @pytest.mark.anyio
    async def test_empty_returns_not_found(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["list_photo_batches"]({})
        assert "не найдены" in _text(result)

    @pytest.mark.anyio
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
    @pytest.mark.anyio
    async def test_empty_returns_not_found(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["list_photo_items"]({})
        assert "не найдены" in _text(result)

    @pytest.mark.anyio
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
    @pytest.mark.anyio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["send_photos_now"](
            {"phone": "+79001234567", "target": "123", "file_paths": "a.jpg"}
        )
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["send_photos_now"](
            {"phone": "+79001234567", "target": "123", "file_paths": "a.jpg"}
        )
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
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
    @pytest.mark.anyio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["schedule_photos"](
            {"phone": "+79001234567", "target": "123", "file_paths": "a.jpg", "schedule_at": "2025-01-01T10:00:00"}
        )
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["schedule_photos"](
            {"phone": "+79001234567", "target": "123", "file_paths": "a.jpg", "schedule_at": "2025-01-01T10:00:00"}
        )
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
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
    @pytest.mark.anyio
    async def test_missing_item_id_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["cancel_photo_item"]({})
        assert "item_id обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["cancel_photo_item"]({"item_id": 5})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_found_item_cancelled(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()
        photo_task_svc.cancel_item = AsyncMock(return_value=True)

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["cancel_photo_item"]({"item_id": 5, "confirm": True})
        assert "отменено" in _text(result)

    @pytest.mark.anyio
    async def test_not_found_returns_message(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()
        photo_task_svc.cancel_item = AsyncMock(return_value=False)

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["cancel_photo_item"]({"item_id": 99, "confirm": True})
        assert "Не удалось отменить" in _text(result)


class TestListAutoUploads:
    @pytest.mark.anyio
    async def test_empty_returns_not_configured(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["list_auto_uploads"]({})
        assert "не настроены" in _text(result)

    @pytest.mark.anyio
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
    @pytest.mark.anyio
    async def test_missing_job_id_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["toggle_auto_upload"]({})
        assert "job_id обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_not_found_returns_error(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()
        auto_upload_svc.get_job = AsyncMock(return_value=None)

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["toggle_auto_upload"]({"job_id": 999})
        assert "не найдена" in _text(result)

    @pytest.mark.anyio
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

    @pytest.mark.anyio
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
    @pytest.mark.anyio
    async def test_missing_job_id_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["delete_auto_upload"]({})
        assert "job_id обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["delete_auto_upload"]({"job_id": 5})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_with_confirm_deleted(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["delete_auto_upload"]({"job_id": 5, "confirm": True})
        assert "удалена" in _text(result)


class TestCreatePhotoBatch:
    @pytest.mark.anyio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["create_photo_batch"](
            {"phone": "+79001234567", "target": "123", "file_paths": "a.jpg"}
        )
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_missing_fields_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["create_photo_batch"]({"phone": "+79001234567"})
        assert "обязательны" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["create_photo_batch"](
            {"phone": "+79001234567", "target": "123", "file_paths": "a.jpg"}
        )
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
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
    @pytest.mark.anyio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["run_photo_due"]({"confirm": True})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["run_photo_due"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
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
    @pytest.mark.anyio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["create_auto_upload"](
            {"phone": "+79001234567", "target": "123", "folder_path": "/photos"}
        )
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_missing_fields_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["create_auto_upload"]({"phone": "+79001234567"})
        assert "обязательны" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["create_auto_upload"](
            {"phone": "+79001234567", "target": "123", "folder_path": "/photos"}
        )
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
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
    @pytest.mark.anyio
    async def test_missing_job_id_returns_error(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["update_auto_upload"]({})
        assert "job_id обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool, _ = _make_mock_pool()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["update_auto_upload"]({"job_id": 1, "folder_path": "/new"})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_not_found_returns_error(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()
        auto_upload_svc.get_job = AsyncMock(return_value=None)

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["update_auto_upload"]({"job_id": 999, "confirm": True})
        assert "не найдена" in _text(result)

    @pytest.mark.anyio
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
