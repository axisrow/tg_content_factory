"""Tests for agent tools: photo_loader.py."""
from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.models import Account, PhotoBatchStatus
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
    mock_pool.release_client = AsyncMock()
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
    photo_task_svc.publish_batch = AsyncMock(return_value=1)
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


class TestResolvePhotoTargetLease:
    @pytest.mark.anyio
    async def test_self_target_releases_client_lease(self):
        """resolve_photo_target must release the acquired client lease so the account
        stays in exclusive collector rotation (#1179). Mirror of
        PhotoPublishService._acquire_client_and_resolve's try/finally release."""
        from src.agent.tools._photo_loader_runtime import resolve_photo_target

        session = MagicMock()
        session.fetch_me = AsyncMock(return_value=MagicMock(id=555))

        class _LeaseTrackingPool:
            def __init__(self):
                self.acquired = 0
                self.released: list[str] = []

            async def get_client_by_phone(self, phone: str):
                self.acquired += 1
                return session, phone

            async def release_client(self, phone: str) -> None:
                self.released.append(phone)

        pool = _LeaseTrackingPool()
        target = await resolve_photo_target(pool, "+79001234567", "me")
        assert target.dialog_id == 555
        assert target.target_type == "saved"
        assert pool.acquired == 1
        assert pool.released == ["+79001234567"]

    @pytest.mark.anyio
    async def test_self_target_releases_even_on_fetch_error(self):
        """If fetch_me raises, the lease must still be released (try/finally) (#1179)."""
        from src.agent.tools._photo_loader_runtime import resolve_photo_target

        class _BoomSession:
            async def fetch_me(self):
                raise RuntimeError("network down")

        class _LeaseTrackingPool:
            def __init__(self):
                self.released: list[str] = []

            async def get_client_by_phone(self, phone: str):
                return _BoomSession(), phone

            async def release_client(self, phone: str) -> None:
                self.released.append(phone)

        pool = _LeaseTrackingPool()
        with pytest.raises(RuntimeError, match="network down"):
            await resolve_photo_target(pool, "+79001234567", "me")
        assert pool.released == ["+79001234567"]


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

    @pytest.mark.anyio
    async def test_me_target_is_resolved_not_int_cast(self, mock_db):
        """target='me' must be resolved like send_photos_now, not int('me') which
        crashes with 'invalid literal for int' (audit #838/10)."""
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()
        session = MagicMock()
        session.fetch_me = AsyncMock(return_value=MagicMock(id=555))
        mock_pool.get_client_by_phone = AsyncMock(return_value=(session, None))
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])

        with (
            _photo_ctx(photo_task_svc, auto_upload_svc),
            patch("src.services.photo_task_service.PhotoTarget"),
        ):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["schedule_photos"](
                {
                    "phone": "+79001234567",
                    "target": "me",
                    "file_paths": "a.jpg",
                    "schedule_at": "2025-01-01T10:00:00",
                    "confirm": True,
                }
            )
        assert "запланированы" in _text(result)
        photo_task_svc.schedule_send.assert_awaited_once()

    @pytest.mark.anyio
    @pytest.mark.parametrize("alias", ["me", "self"])
    async def test_self_target_carries_saved_type(self, mock_db, alias):
        """target='me'/'self' must build a PhotoTarget with target_type='saved' so a cleared
        cache resolves Saved Messages to PeerUser, not PeerChannel(abs(own user-id)) (#842 review).
        Also asserts 'self' is accepted (previously only 'me' was, so int('self') crashed)."""
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()
        session = MagicMock()
        session.fetch_me = AsyncMock(return_value=MagicMock(id=555))
        mock_pool.get_client_by_phone = AsyncMock(return_value=(session, None))
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])

        # Do NOT patch PhotoTarget — we want to inspect the real target fields.
        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["schedule_photos"](
                {
                    "phone": "+79001234567",
                    "target": alias,
                    "file_paths": "a.jpg",
                    "schedule_at": "2025-01-01T10:00:00",
                    "confirm": True,
                }
            )
        assert "запланированы" in _text(result)
        target = photo_task_svc.schedule_send.await_args.kwargs["target"]
        assert target.dialog_id == 555
        assert target.target_type == "saved"
        assert target.title == "Saved Messages"

    @pytest.mark.anyio
    @pytest.mark.parametrize("alias", ["me", "self"])
    async def test_send_now_self_target_carries_saved_type(self, mock_db, alias):
        """send_photos_now mirrors schedule_photos: 'me'/'self' -> Saved Messages (#842 review)."""
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()
        session = MagicMock()
        session.fetch_me = AsyncMock(return_value=MagicMock(id=555))
        mock_pool.get_client_by_phone = AsyncMock(return_value=(session, None))
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            await handlers["send_photos_now"](
                {
                    "phone": "+79001234567",
                    "target": alias,
                    "file_paths": "a.jpg",
                    "confirm": True,
                }
            )
        target = photo_task_svc.send_now.await_args.kwargs["target"]
        assert target.dialog_id == 555
        assert target.target_type == "saved"


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

    @pytest.mark.anyio
    @pytest.mark.parametrize("alias", ["me", "self"])
    async def test_create_batch_self_target_carries_saved_type(self, mock_db, alias):
        """target='me'/'self' must not crash int('me'); it resolves to Saved Messages
        with target_type='saved' so it doesn't mis-resolve to a channel (#1126)."""
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()
        session = MagicMock()
        session.fetch_me = AsyncMock(return_value=MagicMock(id=555))
        mock_pool.get_client_by_phone = AsyncMock(return_value=(session, None))
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])

        # Do NOT patch PhotoTarget — we want to inspect the real target fields.
        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["create_photo_batch"](
                {
                    "phone": "+79001234567",
                    "target": alias,
                    "file_paths": "a.jpg",
                    "confirm": True,
                }
            )
        assert "Батч создан" in _text(result)
        photo_task_svc.create_batch.assert_awaited_once()
        target = photo_task_svc.create_batch.await_args.kwargs["target"]
        assert target.dialog_id == 555
        assert target.target_type == "saved"

    @pytest.mark.anyio
    @pytest.mark.parametrize("target", ["me", "12345"])
    async def test_create_batch_real_service_persists_files(self, db, tmp_path, target):
        """End-to-end against the REAL PhotoTaskService.create_batch (no service mock):
        the handler-built entries must satisfy the service's entry['files'] contract so a
        real .jpg is persisted with non-empty item.file_paths. Regression — an entry-key
        mismatch made the service raise 'No files provided', which the mocked-service
        tests silently missed (#1126)."""
        jpg = tmp_path / "photo.jpg"
        jpg.write_bytes(b"\xff\xd8\xff\xe0")  # JPEG SOI + APP0 magic bytes

        mock_pool, _ = _make_mock_pool()
        session = MagicMock()
        session.fetch_me = AsyncMock(return_value=MagicMock(id=555))
        mock_pool.get_client_by_phone = AsyncMock(return_value=(session, None))

        # No _photo_ctx: the REAL PhotoTaskService + PhotoLoaderBundle back the tool.
        handlers = _get_tool_handlers(db, client_pool=mock_pool)
        result = await handlers["create_photo_batch"](
            {
                "phone": "+79001234567",
                "target": target,
                "file_paths": str(jpg),
                "confirm": True,
            }
        )
        text = _text(result)
        assert "Батч создан" in text
        assert "No files provided" not in text

        # Read back through the real bundle and confirm the file was persisted.
        from src.database.bundles import PhotoLoaderBundle
        from src.services.photo_publish_service import PhotoPublishService
        from src.services.photo_task_service import PhotoTaskService

        readback = PhotoTaskService(
            PhotoLoaderBundle.from_database(db), PhotoPublishService(mock_pool)
        )
        items = await readback.list_items(limit=10)
        assert len(items) == 1
        assert items[0].file_paths == [str(jpg)]
        assert items[0].status == PhotoBatchStatus.HELD

    @pytest.mark.anyio
    async def test_create_batch_album_keeps_single_item(self, db, tmp_path):
        """create_photo_batch must put all files in ONE entry so normalize_mode sees
        len(files)>1 and keeps ALBUM. Splitting one-entry-per-file makes each entry
        have len==1 → normalize downgrades ALBUM→SEPARATE, silently degrading an
        album into N separate single-photo messages (#1180)."""
        from src.models import PhotoSendMode

        files = []
        for name in ("a.jpg", "b.jpg", "c.jpg"):
            fp = tmp_path / name
            fp.write_bytes(b"\xff\xd8\xff\xe0")
            files.append(str(fp))

        mock_pool, _ = _make_mock_pool()
        handlers = _get_tool_handlers(db, client_pool=mock_pool)
        result = await handlers["create_photo_batch"](
            {
                "phone": "+79001234567",
                "target": "12345",
                "file_paths": ",".join(files),
                "confirm": True,
            }
        )
        assert "Батч создан" in _text(result)

        from src.database.bundles import PhotoLoaderBundle
        from src.services.photo_publish_service import PhotoPublishService
        from src.services.photo_task_service import PhotoTaskService

        readback = PhotoTaskService(
            PhotoLoaderBundle.from_database(db), PhotoPublishService(mock_pool)
        )
        items = await readback.list_items(limit=10)
        assert len(items) == 1
        assert items[0].send_mode == PhotoSendMode.ALBUM
        assert set(items[0].file_paths) == set(files)
        assert items[0].status == PhotoBatchStatus.HELD


class TestPublishPhotoBatch:
    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["publish_photo_batch"]({"batch_id": 99})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_with_confirm_publishes_batch(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["publish_photo_batch"]({"batch_id": 99, "confirm": True})

        text = _text(result)
        assert "Батч опубликован" in text
        photo_task_svc.publish_batch.assert_awaited_once_with(99)


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

    @pytest.mark.anyio
    async def test_dry_run_previews_without_send_or_confirm(self, mock_db):
        """dry_run=true returns a preview without confirmation, never sends, and never
        touches the photo-item path."""
        from src.models import PhotoSendMode
        from src.services.photo_auto_upload_service import PhotoAutoPreview

        photo_task_svc, auto_upload_svc = _make_photo_services()
        preview = PhotoAutoPreview(
            job_id=7,
            target_dialog_id=-100123,
            target_title="Канал",
            target_type="channel",
            send_mode=PhotoSendMode.SEPARATE,
            files=["/srv/a.jpg", "/srv/b.png"],
        )
        auto_upload_svc.run_due = AsyncMock(return_value=[preview])
        mock_pool, _ = _make_mock_pool()

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            # No confirm passed — dry-run must not be gated on confirmation.
            result = await handlers["run_photo_due"]({"dry_run": True})

        text = _text(result)
        assert "dry-run" in text
        assert "job #7" in text
        assert "Канал" in text
        assert "/srv/a.jpg" in text
        assert "confirm=true" not in text
        auto_upload_svc.run_due.assert_awaited_once_with(dry_run=True)
        photo_task_svc.run_due.assert_not_awaited()

    @pytest.mark.anyio
    async def test_dry_run_empty_preview(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        auto_upload_svc.run_due = AsyncMock(return_value=[])
        mock_pool, _ = _make_mock_pool()

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["run_photo_due"]({"dry_run": True})

        text = _text(result)
        assert "dry-run" in text
        assert "отправлять нечего" in text
        photo_task_svc.run_due.assert_not_awaited()


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

    @pytest.mark.anyio
    @pytest.mark.parametrize("alias", ["me", "self"])
    async def test_create_auto_upload_self_target_carries_saved_type(self, mock_db, alias):
        """target='me'/'self' must not crash int('me'); the PhotoAutoUploadJob carries
        target_type='saved' and target_dialog_id==own user-id (#1126)."""
        photo_task_svc, auto_upload_svc = _make_photo_services()
        mock_pool, _ = _make_mock_pool()
        session = MagicMock()
        session.fetch_me = AsyncMock(return_value=MagicMock(id=555))
        mock_pool.get_client_by_phone = AsyncMock(return_value=(session, None))
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])

        with _photo_ctx(photo_task_svc, auto_upload_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["create_auto_upload"](
                {
                    "phone": "+79001234567",
                    "target": alias,
                    "folder_path": "/photos",
                    "interval_minutes": 30,
                    "mode": "album",
                    "confirm": True,
                }
            )
        assert "Автозагрузка создана" in _text(result)
        auto_upload_svc.create_job.assert_awaited_once()
        job = auto_upload_svc.create_job.await_args.args[0]
        assert job.target_type == "saved"
        assert job.target_dialog_id == 555


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
