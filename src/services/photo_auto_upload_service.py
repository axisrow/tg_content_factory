from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.database.bundles import PhotoLoaderBundle
from src.models import PhotoAutoUploadJob, PhotoSendMode
from src.services.photo_publish_service import PhotoPublishService
from src.services.photo_task_service import IMAGE_EXTENSIONS

logger = logging.getLogger(__name__)


def _image_candidates(folder: Path) -> list[str]:
    return [
        str(path)
        for path in sorted(folder.iterdir())
        if path.is_file() and path.suffix.lower() in IMAGE_EXTENSIONS
    ]


@dataclass(frozen=True)
class PhotoAutoPreview:
    """A dry-run plan: the files a job *would* publish, and where, without sending.

    Returned by ``run_job(dry_run=True)`` / ``run_due(dry_run=True)`` so callers can
    show "what would be posted" without any Telegram I/O, dedup marking, or job-state
    mutation. ``send_mode`` already reflects the album→separate fallback so the preview
    matches what the real send would do.
    """

    job_id: int
    target_dialog_id: int
    target_title: str | None
    target_type: str | None
    send_mode: PhotoSendMode
    files: list[str] = field(default_factory=list)


class PhotoAutoUploadService:
    def __init__(self, bundle: PhotoLoaderBundle, publish: PhotoPublishService):
        self._bundle = bundle
        self._publish = publish

    async def create_job(self, job: PhotoAutoUploadJob) -> int:
        self._validate_folder(job.folder_path)
        return await self._bundle.create_auto_job(job)

    async def list_jobs(self, active_only: bool = False) -> list[PhotoAutoUploadJob]:
        return await self._bundle.list_auto_jobs(active_only)

    async def get_job(self, job_id: int) -> PhotoAutoUploadJob | None:
        return await self._bundle.get_auto_job(job_id)

    async def update_job(
        self,
        job_id: int,
        *,
        folder_path: str | None = None,
        send_mode: PhotoSendMode | None = None,
        caption: str | None = None,
        interval_minutes: int | None = None,
        is_active: bool | None = None,
    ) -> None:
        if folder_path is not None:
            self._validate_folder(folder_path)
        await self._bundle.update_auto_job(
            job_id,
            folder_path=folder_path,
            send_mode=send_mode,
            caption=caption,
            interval_minutes=interval_minutes,
            is_active=is_active,
        )

    async def delete_job(self, job_id: int) -> None:
        await self._bundle.delete_auto_job(job_id)

    async def run_due(
        self,
        dry_run: bool = False,
        on_progress: Callable[[int, int], None] | None = None,
    ) -> int | list[PhotoAutoPreview]:
        """Run every due job. In dry-run mode, return a per-job preview list instead of
        sending — no Telegram I/O, no dedup marking, no job-state mutation."""
        jobs = await self._bundle.list_auto_jobs(active_only=True)
        now = datetime.now(timezone.utc)
        due_jobs = [job for job in jobs if self._is_due(job, now)]
        if dry_run:
            previews: list[PhotoAutoPreview] = []
            for job in due_jobs:
                preview = await self.run_job(job.id or 0, dry_run=True)
                assert isinstance(preview, PhotoAutoPreview)  # narrow for type-checkers
                previews.append(preview)
            return previews
        processed = 0
        for job in due_jobs:
            await self.run_job(job.id or 0)
            processed += 1
            if on_progress is not None:
                on_progress(processed, len(due_jobs))
        return processed

    async def run_job(self, job_id: int, dry_run: bool = False) -> int | PhotoAutoPreview:
        """Publish new files for a job. In dry-run mode, return a :class:`PhotoAutoPreview`
        of what *would* be sent and exit before any send, dedup mark, or state update."""
        job = await self._bundle.get_auto_job(job_id)
        if job is None:
            raise ValueError(f"Auto job not found: {job_id}")
        self._validate_folder(job.folder_path)
        files = await self._collect_new_files(job)
        send_mode = job.send_mode
        if send_mode == PhotoSendMode.ALBUM and len(files) < 2:
            send_mode = PhotoSendMode.SEPARATE
        if dry_run:
            # Build the plan and bail out before any side effect: no send_now(), no
            # mark_auto_file_sent(), no update_auto_job() — job state stays frozen.
            return PhotoAutoPreview(
                job_id=job.id or 0,
                target_dialog_id=job.target_dialog_id,
                target_title=job.target_title,
                target_type=job.target_type,
                send_mode=send_mode,
                files=files,
            )
        now = datetime.now(timezone.utc)
        if not files:
            await self._bundle.update_auto_job(job.id or 0, error="", last_run_at=now)
            return 0
        try:

            async def _mark_sent(file_path: str, _ids: list[int], _jid: int = job.id or 0) -> None:
                # Mark each file the moment it is published so a mid-batch failure
                # doesn't re-send already-sent files next cycle (audit #835/4).
                await self._bundle.mark_auto_file_sent(_jid, file_path)

            await self._publish.send_now(
                phone=job.phone,
                target_dialog_id=job.target_dialog_id,
                target_type=job.target_type,
                file_paths=files,
                send_mode=send_mode,
                caption=job.caption,
                on_file_sent=_mark_sent,
            )
            await self._bundle.update_auto_job(
                job.id or 0,
                error="",
                last_run_at=now,
                last_seen_marker=files[-1],
            )
            return len(files)
        except Exception as exc:
            logger.exception(
                "Photo auto upload run failed: job_id=%s phone=%s target_dialog_id=%s "
                "folder_path=%r files=%d",
                job.id,
                job.phone,
                job.target_dialog_id,
                job.folder_path,
                len(files),
            )
            await self._bundle.update_auto_job(job.id or 0, error=str(exc), last_run_at=now)
            raise

    async def _collect_new_files(self, job: PhotoAutoUploadJob) -> list[str]:
        folder = Path(job.folder_path)
        candidates = await asyncio.to_thread(_image_candidates, folder)
        fresh: list[str] = []
        for file_path in candidates:
            if await self._bundle.has_sent_auto_file(job.id or 0, file_path):
                continue
            fresh.append(file_path)
        return fresh

    @staticmethod
    def _is_due(job: PhotoAutoUploadJob, now: datetime) -> bool:
        if not job.is_active:
            return False
        if job.last_run_at is None:
            return True
        return job.last_run_at + timedelta(minutes=job.interval_minutes) <= now

    @staticmethod
    def _validate_folder(folder_path: str) -> None:
        path = Path(folder_path)
        if not path.exists() or not path.is_dir():
            raise ValueError(f"Folder not found: {folder_path}")
