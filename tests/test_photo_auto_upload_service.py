from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import PhotoAutoUploadJob, PhotoSendMode
from src.services.photo_auto_upload_service import PhotoAutoPreview, PhotoAutoUploadService


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

    # send_now now records progress per file via the on_file_sent callback
    # (audit #835/4) — simulate that so the dedup marks are exercised.
    async def _fake_send_now(**kwargs):
        cb = kwargs.get("on_file_sent")
        if cb is not None:
            for path in kwargs["file_paths"]:
                await cb(path, [1])
        return [1] * len(kwargs["file_paths"])

    service._publish.send_now = AsyncMock(side_effect=_fake_send_now)

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


# --- run_job dry-run ---


async def test_run_job_dry_run_does_not_send(service, bundle, tmp_path):
    """dry-run must build a preview without ever calling send_now (no Telegram I/O)."""
    job = _make_job(folder_path=str(tmp_path))
    bundle.get_auto_job.return_value = job
    bundle.has_sent_auto_file.return_value = False

    (tmp_path / "img1.jpg").write_bytes(b"\xff\xd8\xff")
    (tmp_path / "img2.png").write_bytes(b"\x89PNG")

    preview = await service.run_job(1, dry_run=True)

    # No real send — the publish service must not be touched at all.
    service._publish.send_now.assert_not_awaited()
    # Preview carries the files that *would* be sent plus the routing context.
    assert isinstance(preview, PhotoAutoPreview)
    assert preview.job_id == 1
    assert preview.target_dialog_id == job.target_dialog_id
    assert len(preview.files) == 2
    assert all(f.endswith((".jpg", ".png")) for f in preview.files)


async def test_run_job_dry_run_does_not_mark_or_advance_state(service, bundle, tmp_path):
    """dry-run must not touch the dedup table nor shift last_run_at/last_seen_marker."""
    job = _make_job(folder_path=str(tmp_path))
    bundle.get_auto_job.return_value = job
    bundle.has_sent_auto_file.return_value = False

    (tmp_path / "img1.jpg").write_bytes(b"\xff\xd8\xff")

    await service.run_job(1, dry_run=True)

    # Dedup table untouched — file is NOT recorded as sent.
    bundle.mark_auto_file_sent.assert_not_awaited()
    # Job state untouched — no last_run_at / last_seen_marker / error write.
    bundle.update_auto_job.assert_not_awaited()


async def test_run_job_dry_run_no_files_empty_preview(service, bundle, tmp_path):
    """dry-run over an empty/fully-sent folder yields an empty preview, no state writes."""
    job = _make_job(folder_path=str(tmp_path))
    bundle.get_auto_job.return_value = job

    preview = await service.run_job(1, dry_run=True)

    assert isinstance(preview, PhotoAutoPreview)
    assert preview.files == []
    service._publish.send_now.assert_not_awaited()
    bundle.update_auto_job.assert_not_awaited()


async def test_run_job_dry_run_album_fallback_reflected(service, bundle, tmp_path):
    """A single-file ALBUM job previews as SEPARATE, mirroring the real send path."""
    job = _make_job(folder_path=str(tmp_path), send_mode=PhotoSendMode.ALBUM)
    bundle.get_auto_job.return_value = job
    bundle.has_sent_auto_file.return_value = False

    (tmp_path / "single.jpg").write_bytes(b"\xff\xd8\xff")

    preview = await service.run_job(1, dry_run=True)

    assert preview.send_mode == PhotoSendMode.SEPARATE
    service._publish.send_now.assert_not_awaited()


# --- run_due dry-run ---


async def test_run_due_dry_run_returns_previews_without_sending(service, bundle, tmp_path):
    """run_due(dry_run=True) collects per-job previews and never sends or marks."""
    job = _make_job(folder_path=str(tmp_path))
    bundle.list_auto_jobs.return_value = [job]
    bundle.get_auto_job.return_value = job
    bundle.has_sent_auto_file.return_value = False

    (tmp_path / "img1.jpg").write_bytes(b"\xff\xd8\xff")

    previews = await service.run_due(dry_run=True)

    assert isinstance(previews, list)
    assert len(previews) == 1
    assert isinstance(previews[0], PhotoAutoPreview)
    assert previews[0].files
    service._publish.send_now.assert_not_awaited()
    bundle.mark_auto_file_sent.assert_not_awaited()
    bundle.update_auto_job.assert_not_awaited()


async def test_run_due_dry_run_skips_not_due(service, bundle):
    job = _make_job(is_active=False)
    bundle.list_auto_jobs.return_value = [job]
    previews = await service.run_due(dry_run=True)
    assert previews == []


# --- regression: normal mode unchanged ---


async def test_run_job_normal_mode_still_sends(service, bundle, tmp_path):
    """dry_run=False (default) keeps the original send + mark + state-advance behavior."""
    job = _make_job(folder_path=str(tmp_path))
    bundle.get_auto_job.return_value = job
    bundle.has_sent_auto_file.return_value = False

    async def _fake_send_now(**kwargs):
        cb = kwargs.get("on_file_sent")
        if cb is not None:
            for path in kwargs["file_paths"]:
                await cb(path, [1])
        return [1] * len(kwargs["file_paths"])

    service._publish.send_now = AsyncMock(side_effect=_fake_send_now)

    (tmp_path / "img1.jpg").write_bytes(b"\xff\xd8\xff")

    result = await service.run_job(1)
    assert result == 1
    service._publish.send_now.assert_awaited_once()
    bundle.mark_auto_file_sent.assert_awaited()
    bundle.update_auto_job.assert_awaited()


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


# ---------------------------------------------------------------------------
# Recurring-cycle integration test (fake-clock, stateful in-memory bundle)
# ---------------------------------------------------------------------------
# Доказывает инвариант: сработало → через интервал снова → без дублей.
# Fake bundle держит state: sent_files (дедуп) + job (обновляемый last_run_at).
# Fake publish: накапливает sent_paths для проверки.
# Fake clock: now передаётся явно в run_due через монки-патч _is_due-часов.
# ---------------------------------------------------------------------------


class _StatefulBundle:
    """In-memory stateful fake bundle: реальный дедуп + mutable job state."""

    def __init__(self, job: PhotoAutoUploadJob) -> None:
        self._job = job
        self._sent: set[str] = set()

    async def list_auto_jobs(self, active_only: bool = True) -> list[PhotoAutoUploadJob]:
        if active_only and not self._job.is_active:
            return []
        return [self._job]

    async def get_auto_job(self, job_id: int) -> PhotoAutoUploadJob | None:
        if self._job.id == job_id:
            return self._job
        return None

    async def has_sent_auto_file(self, job_id: int, file_path: str) -> bool:
        return file_path in self._sent

    async def mark_auto_file_sent(self, job_id: int, file_path: str) -> None:
        self._sent.add(file_path)

    async def update_auto_job(self, job_id: int, **kwargs: object) -> None:
        # PhotoAutoUploadJob — Pydantic model; model_copy обновляет только переданные поля.
        self._job = self._job.model_copy(update={k: v for k, v in kwargs.items()})

    # Unused by service but expected by interface
    async def create_auto_job(self, job: PhotoAutoUploadJob) -> int:
        return self._job.id or 1

    async def delete_auto_job(self, job_id: int) -> None:
        pass


class _TrackingPublish:
    """Fake publish that records which paths were sent and simulates on_file_sent callback."""

    def __init__(self) -> None:
        self.sent_paths: list[str] = []

    async def send_now(self, *, file_paths: list[str], on_file_sent=None, **_kwargs: object) -> list[int]:
        for path in file_paths:
            self.sent_paths.append(path)
            if on_file_sent is not None:
                await on_file_sent(path, [len(self.sent_paths)])
        return list(range(len(file_paths)))


async def test_recurring_cycle_dedup_and_timing(tmp_path):
    """
    Интеграционный сценарий recurring-цикла (fake-clock, in-memory state):

    T0        : run_due → job due (last_run_at=None) → file1 отправлен, last_run_at=T0
    t0_plus_half : run_due → job НЕ due (не прошёл интервал) → 0 отправок
    t0_plus_n    : run_due → job due → file1 НЕ переотправлен (дедуп), file2 отправлен
    t0_plus_2n   : run_due → job due → новых файлов нет → 0 отправок, last_run_at сдвигается
    """
    interval = 60  # interval_minutes
    t0 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

    # Создаём начальный job (last_run_at=None → due немедленно)
    job = PhotoAutoUploadJob(
        id=1,
        phone="+1234",
        target_dialog_id=-100,
        target_title=None,
        target_type=None,
        folder_path=str(tmp_path),
        send_mode=PhotoSendMode.SEPARATE,
        interval_minutes=interval,
        is_active=True,
        last_run_at=None,
        last_seen_marker=None,
    )

    bundle = _StatefulBundle(job)
    publish = _TrackingPublish()
    service = PhotoAutoUploadService(bundle, publish)  # type: ignore[arg-type]

    # Файл 1 существует с самого начала
    file1 = tmp_path / "img1.jpg"
    file1.write_bytes(b"\xff\xd8\xff")

    # _run_due_at: fake-clock замена run_due() — передаёт явный момент времени в _is_due.
    # run_due() строит now = datetime.now(utc) сам; мы воспроизводим его логику с нужным clock.
    async def _run_due_at(clock_now: datetime) -> int:
        """Запускает due-цикл с явным fake-clock вместо datetime.now(utc)."""
        jobs = await service._bundle.list_auto_jobs(active_only=True)
        due = [j for j in jobs if PhotoAutoUploadService._is_due(j, clock_now)]
        if not due:
            return 0
        processed = 0
        for j in due:
            await service.run_job(j.id or 0)
            processed += 1
        return processed

    # run_job пишет last_run_at = datetime.now(utc) через bundle.update_auto_job.
    # Подменяем update_auto_job чтобы он фиксировал clock_now, а не реальное время —
    # иначе _is_due в следующем шаге оценивал бы не fake-clock, а wall-clock.
    original_update = bundle.update_auto_job

    def _make_patched_update(clock_now: datetime):
        async def _patched_update(job_id: int, **kwargs: object) -> None:
            if "last_run_at" in kwargs and kwargs["last_run_at"] is not None:
                kwargs["last_run_at"] = clock_now
            await original_update(job_id, **kwargs)

        return _patched_update

    # Шаг 1: t0
    bundle.update_auto_job = _make_patched_update(t0)
    count1 = await _run_due_at(t0)
    assert count1 == 1, f"t0: ожидали 1 задание, получили {count1}"
    assert len(publish.sent_paths) == 1, f"t0: ожидали 1 файл отправленным, получили {publish.sent_paths}"
    assert str(file1) in publish.sent_paths
    assert bundle._job.last_run_at == t0, f"t0: last_run_at должен быть t0, не {bundle._job.last_run_at}"

    # Шаг 2: t0+interval/2 — НЕ due
    bundle.update_auto_job = _make_patched_update(t0 + timedelta(minutes=interval // 2))
    count2 = await _run_due_at(t0 + timedelta(minutes=interval // 2))
    assert count2 == 0, f"t0+interval/2: ожидали 0 (не due), получили {count2}"
    sent_after_step2 = len(publish.sent_paths)
    assert sent_after_step2 == 1, "t0+interval/2: новых отправок быть не должно"

    # Шаг 3: t0+interval — due снова, file2 — новый файл, file1 — НЕ переотправляется
    file2 = tmp_path / "img2.jpg"
    file2.write_bytes(b"\xff\xd8\xff\xe0")
    t1 = t0 + timedelta(minutes=interval)
    bundle.update_auto_job = _make_patched_update(t1)
    count3 = await _run_due_at(t1)
    assert count3 == 1, f"t0+interval: ожидали 1 задание, получили {count3}"
    sent_at_step3 = publish.sent_paths[sent_after_step2:]
    assert len(sent_at_step3) == 1, f"t0+interval: ожидали только 1 новый файл, получили {sent_at_step3}"
    assert str(file2) in sent_at_step3, f"t0+interval: file2 должен быть отправлен, не {sent_at_step3}"
    assert str(file1) not in sent_at_step3, "t0+interval: file1 не должен переотправляться (дедуп)"
    assert bundle._job.last_run_at == t1

    # Шаг 4: t0+2*interval — due, но новых файлов нет → 0 отправок, last_run_at обновляется
    t2 = t0 + timedelta(minutes=2 * interval)
    bundle.update_auto_job = _make_patched_update(t2)
    count4 = await _run_due_at(t2)
    assert count4 == 1, f"t0+2*interval: ожидали 1 задание (due), получили {count4}"
    sent_at_step4 = publish.sent_paths[sent_after_step2 + 1:]
    assert len(sent_at_step4) == 0, f"t0+2*interval: файлов для отправки нет, получили {sent_at_step4}"
    assert bundle._job.last_run_at == t2, "t0+2*interval: last_run_at должен сдвинуться до t2"
    assert len(publish.sent_paths) == 2, "Итого отправлено ровно 2 файла за все 4 шага"
