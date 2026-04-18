from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import PhotoAutoUploadJob, PhotoSendMode
from src.services.photo_auto_upload_service import PhotoAutoUploadService


def _make_job(**overrides) -> PhotoAutoUploadJob:
    defaults = dict(
        id=1,
        phone="+1234",
        target_dialog_id=-100,
        folder_path="/tmp/test_photos",
        send_mode=PhotoSendMode.SEPARATE,
        interval_minutes=60,
        is_active=True,
    )
    defaults.update(overrides)
    return PhotoAutoUploadJob(**defaults)


def _make_bundle():
    bundle = MagicMock()
    bundle.create_auto_job = AsyncMock(return_value=1)
    bundle.list_auto_jobs = AsyncMock(return_value=[])
    bundle.get_auto_job = AsyncMock(return_value=None)
    bundle.update_auto_job = AsyncMock()
    bundle.delete_auto_job = AsyncMock()
    bundle.mark_auto_file_sent = AsyncMock()
    bundle.has_sent_auto_file = AsyncMock(return_value=False)
    return bundle


def _make_publish():
    publish = MagicMock()
    publish.send_now = AsyncMock()
    return publish


@pytest.fixture
def service():
    return PhotoAutoUploadService(_make_bundle(), _make_publish())


@pytest.fixture
def bundle(service):
    return service._bundle


# --- create_job ---


async def test_create_job_validates_folder(service, bundle, tmp_path):
    job = _make_job(folder_path=str(tmp_path))
    result = await service.create_job(job)
    assert result == 1
    bundle.create_auto_job.assert_awaited_once_with(job)


async def test_create_job_invalid_folder(service):
    job = _make_job(folder_path="/nonexistent/path")
    with pytest.raises(ValueError, match="Folder not found"):
        await service.create_job(job)


# --- list_jobs ---


async def test_list_jobs(service, bundle):
    jobs = [_make_job(id=1), _make_job(id=2)]
    bundle.list_auto_jobs.return_value = jobs
    result = await service.list_jobs(active_only=True)
    assert result == jobs
    bundle.list_auto_jobs.assert_awaited_once_with(True)


# --- get_job ---


async def test_get_job_found(service, bundle):
    job = _make_job()
    bundle.get_auto_job.return_value = job
    result = await service.get_job(1)
    assert result == job


async def test_get_job_not_found(service, bundle):
    result = await service.get_job(999)
    assert result is None


# --- update_job ---


async def test_update_job(service, bundle, tmp_path):
    await service.update_job(1, folder_path=str(tmp_path), interval_minutes=30)
    bundle.update_auto_job.assert_awaited_once()
    call_kwargs = bundle.update_auto_job.call_args
    assert call_kwargs[1]["interval_minutes"] == 30


async def test_update_job_invalid_folder(service):
    with pytest.raises(ValueError, match="Folder not found"):
        await service.update_job(1, folder_path="/nonexistent/path")


# --- delete_job ---


async def test_delete_job(service, bundle):
    await service.delete_job(1)
    bundle.delete_auto_job.assert_awaited_once_with(1)


# --- _is_due ---


def test_is_due_active_no_last_run():
    job = _make_job(is_active=True, last_run_at=None)
    now = datetime.now(timezone.utc)
    assert PhotoAutoUploadService._is_due(job, now) is True


def test_is_due_inactive():
    job = _make_job(is_active=False, last_run_at=None)
    now = datetime.now(timezone.utc)
    assert PhotoAutoUploadService._is_due(job, now) is False


def test_is_due_within_interval():
    now = datetime.now(timezone.utc)
    job = _make_job(is_active=True, last_run_at=now - timedelta(minutes=30), interval_minutes=60)
    assert PhotoAutoUploadService._is_due(job, now) is False


def test_is_due_past_interval():
    now = datetime.now(timezone.utc)
    job = _make_job(is_active=True, last_run_at=now - timedelta(minutes=61), interval_minutes=60)
    assert PhotoAutoUploadService._is_due(job, now) is True


# --- run_due ---


async def test_run_due_no_jobs(service, bundle):
    bundle.list_auto_jobs.return_value = []
    result = await service.run_due()
    assert result == 0


async def test_run_due_skips_not_due(service, bundle):
    job = _make_job(is_active=False)
    bundle.list_auto_jobs.return_value = [job]
    result = await service.run_due()
    assert result == 0


async def test_run_due_processes_due(service, bundle, tmp_path):
    job = _make_job(folder_path=str(tmp_path))
    bundle.list_auto_jobs.return_value = [job]
    bundle.get_auto_job.return_value = job
    bundle.has_sent_auto_file.return_value = False

    # Create a test image file
    (tmp_path / "test.jpg").write_bytes(b"\xff\xd8\xff")

    result = await service.run_due()
    assert result == 1


# --- run_job ---


async def test_run_job_not_found(service, bundle):
    bundle.get_auto_job.return_value = None
    with pytest.raises(ValueError, match="Auto job not found"):
        await service.run_job(999)


async def test_run_job_no_files(service, bundle, tmp_path):
    job = _make_job(folder_path=str(tmp_path))
    bundle.get_auto_job.return_value = job
    result = await service.run_job(1)
    assert result == 0
    bundle.update_auto_job.assert_awaited()


async def test_run_job_sends_files(service, bundle, tmp_path):
    job = _make_job(folder_path=str(tmp_path))
    bundle.get_auto_job.return_value = job
    bundle.has_sent_auto_file.return_value = False

    # Create test images
    (tmp_path / "img1.jpg").write_bytes(b"\xff\xd8\xff")
    (tmp_path / "img2.png").write_bytes(b"\x89PNG")

    result = await service.run_job(1)
    assert result == 2
    service._publish.send_now.assert_awaited_once()
    assert bundle.mark_auto_file_sent.await_count == 2


async def test_run_job_album_fallback_to_separate(service, bundle, tmp_path):
    job = _make_job(folder_path=str(tmp_path), send_mode=PhotoSendMode.ALBUM)
    bundle.get_auto_job.return_value = job
    bundle.has_sent_auto_file.return_value = False

    # Only one file — album should fall back to separate
    (tmp_path / "single.jpg").write_bytes(b"\xff\xd8\xff")

    result = await service.run_job(1)
    assert result == 1
    call_kwargs = service._publish.send_now.call_args[1]
    assert call_kwargs["send_mode"] == PhotoSendMode.SEPARATE


async def test_run_job_error_records(service, bundle, tmp_path):
    job = _make_job(folder_path=str(tmp_path))
    bundle.get_auto_job.return_value = job
    bundle.has_sent_auto_file.return_value = False
    service._publish.send_now.side_effect = RuntimeError("send failed")

    (tmp_path / "fail.jpg").write_bytes(b"\xff\xd8\xff")

    with pytest.raises(RuntimeError, match="send failed"):
        await service.run_job(1)

    # Error should be recorded
    error_calls = [c for c in bundle.update_auto_job.call_args_list if c[1].get("error")]
    assert len(error_calls) > 0


# --- _validate_folder ---


def test_validate_folder_invalid():
    with pytest.raises(ValueError, match="Folder not found"):
        PhotoAutoUploadService._validate_folder("/nonexistent/path")


def test_validate_folder_file_not_dir(tmp_path):
    f = tmp_path / "file.txt"
    f.write_text("not a dir")
    with pytest.raises(ValueError, match="Folder not found"):
        PhotoAutoUploadService._validate_folder(str(f))


def test_validate_folder_valid(tmp_path):
    PhotoAutoUploadService._validate_folder(str(tmp_path))
