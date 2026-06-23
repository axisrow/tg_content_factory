"""Photo send/schedule command handlers (#1047).

Domain: ``photo.*`` — immediate send, scheduled send, and the due-runner that
drains both scheduled items and auto-upload jobs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.services.photo_auto_upload_service import PhotoAutoUploadService
from src.services.photo_task_service import PhotoTarget, PhotoTaskService
from src.utils.datetime import parse_required_schedule_datetime

if TYPE_CHECKING:
    from src.services.dispatcher._base import _DispatcherProtocol

    _Base = _DispatcherProtocol
else:
    _Base = object


class PhotoCommandsMixin(_Base):
    """``photo.*`` command handlers and their service factories."""

    def _photo_task_service(self) -> PhotoTaskService:
        from src.database.bundles import PhotoLoaderBundle
        from src.services.photo_publish_service import PhotoPublishService

        return PhotoTaskService(PhotoLoaderBundle.from_database(self._db), PhotoPublishService(self._pool))

    def _photo_auto_upload_service(self) -> PhotoAutoUploadService:
        from src.database.bundles import PhotoLoaderBundle
        from src.services.photo_publish_service import PhotoPublishService

        return PhotoAutoUploadService(PhotoLoaderBundle.from_database(self._db), PhotoPublishService(self._pool))

    async def _handle_photo_send_now(self, payload: dict[str, Any]) -> dict[str, Any]:
        item = await self._photo_task_service().send_now(
            phone=str(payload["phone"]),
            target=PhotoTarget(
                dialog_id=int(payload["target_dialog_id"]),
                title=payload.get("target_title"),
                target_type=payload.get("target_type"),
            ),
            file_paths=[str(path) for path in payload.get("file_paths", [])],
            mode=str(payload.get("mode", "separate")),
            caption=payload.get("caption"),
        )
        return {"item_id": item.id, "batch_id": item.batch_id}

    async def _handle_photo_schedule_send(self, payload: dict[str, Any]) -> dict[str, Any]:
        item = await self._photo_task_service().schedule_send(
            phone=str(payload["phone"]),
            target=PhotoTarget(
                dialog_id=int(payload["target_dialog_id"]),
                title=payload.get("target_title"),
                target_type=payload.get("target_type"),
            ),
            file_paths=[str(path) for path in payload.get("file_paths", [])],
            mode=str(payload.get("mode", "separate")),
            schedule_at=parse_required_schedule_datetime(str(payload["schedule_at"])),
            caption=payload.get("caption"),
        )
        return {"item_id": item.id, "batch_id": item.batch_id}

    async def _handle_photo_run_due(self, payload: dict[str, Any]) -> dict[str, Any]:
        if payload.get("dry_run"):
            # Preview only: skip the photo-item path (no dry-run there) and ask the
            # auto-upload service for a plan without sending, marking, or advancing state.
            previews = await self._photo_auto_upload_service().run_due(dry_run=True)
            assert isinstance(previews, list)  # narrow run_due's int|list return
            return {
                "dry_run": True,
                "jobs": [
                    {
                        "job_id": preview.job_id,
                        "target_dialog_id": preview.target_dialog_id,
                        "target_title": preview.target_title,
                        "target_type": preview.target_type,
                        "send_mode": preview.send_mode.value,
                        "files": list(preview.files),
                    }
                    for preview in previews
                ],
            }
        items = await self._photo_task_service().run_due()
        jobs = await self._photo_auto_upload_service().run_due()
        return {"processed_items": items, "processed_jobs": jobs}
