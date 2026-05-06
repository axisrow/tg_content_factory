from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import yaml

from src.database.bundles import PhotoLoaderBundle
from src.models import PhotoBatch, PhotoBatchItem, PhotoBatchStatus, PhotoSendMode
from src.services.photo_publish_service import PhotoPublishService
from src.utils.datetime import parse_required_schedule_datetime

IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp"}


@dataclass(frozen=True)
class PhotoTarget:
    dialog_id: int
    title: str | None = None
    target_type: str | None = None


class PhotoTaskService:
    def __init__(self, bundle: PhotoLoaderBundle, publish: PhotoPublishService):
        self._bundle = bundle
        self._publish = publish

    def validate_files(self, file_paths: list[str]) -> list[str]:
        cleaned = [str(Path(path)) for path in file_paths if str(path).strip()]
        if not cleaned:
            raise ValueError("No files provided")
        for path_str in cleaned:
            path = Path(path_str)
            if not path.exists() or not path.is_file():
                raise ValueError(f"File not found: {path}")
            if path.suffix.lower() not in IMAGE_EXTENSIONS:
                raise ValueError(f"Unsupported file type: {path}")
        return cleaned

    @staticmethod
    def normalize_mode(mode: str | PhotoSendMode, files_count: int) -> PhotoSendMode:
        send_mode = mode if isinstance(mode, PhotoSendMode) else PhotoSendMode(mode)
        if send_mode == PhotoSendMode.ALBUM and files_count < 2:
            return PhotoSendMode.SEPARATE
        return send_mode

    async def send_now(
        self,
        *,
        phone: str,
        target: PhotoTarget,
        file_paths: list[str],
        mode: str | PhotoSendMode,
        caption: str | None = None,
    ) -> PhotoBatchItem:
        files = self.validate_files(file_paths)
        send_mode = self.normalize_mode(mode, len(files))
        batch_id = await self._bundle.create_batch(
            PhotoBatch(
                phone=phone,
                target_dialog_id=target.dialog_id,
                target_title=target.title,
                target_type=target.target_type,
                send_mode=send_mode,
                caption=caption,
                status=PhotoBatchStatus.RUNNING,
            )
        )
        item_id = await self._bundle.create_item(
            PhotoBatchItem(
                batch_id=batch_id,
                phone=phone,
                target_dialog_id=target.dialog_id,
                target_title=target.title,
                target_type=target.target_type,
                file_paths=files,
                send_mode=send_mode,
                caption=caption,
                status=PhotoBatchStatus.RUNNING,
                started_at=datetime.now(timezone.utc),
            )
        )
        try:
            message_ids = await self._publish.send_now(
                phone=phone,
                target_dialog_id=target.dialog_id,
                target_type=target.target_type,
                file_paths=files,
                send_mode=send_mode,
                caption=caption,
            )
            now = datetime.now(timezone.utc)
            await self._bundle.update_item(
                item_id,
                status=PhotoBatchStatus.COMPLETED,
                telegram_message_ids=message_ids,
                completed_at=now,
            )
            await self._bundle.update_batch(
                batch_id,
                status=PhotoBatchStatus.COMPLETED,
                last_run_at=now,
            )
        except Exception as exc:
            now = datetime.now(timezone.utc)
            await self._bundle.update_item(
                item_id,
                status=PhotoBatchStatus.FAILED,
                error=str(exc),
                completed_at=now,
            )
            await self._bundle.update_batch(
                batch_id,
                status=PhotoBatchStatus.FAILED,
                error=str(exc),
                last_run_at=now,
            )
            raise
        item = await self._bundle.get_item(item_id)
        assert item is not None
        return item

    async def schedule_send(
        self,
        *,
        phone: str,
        target: PhotoTarget,
        file_paths: list[str],
        mode: str | PhotoSendMode,
        schedule_at: datetime,
        caption: str | None = None,
    ) -> PhotoBatchItem:
        files = self.validate_files(file_paths)
        send_mode = self.normalize_mode(mode, len(files))
        batch_id = await self._bundle.create_batch(
            PhotoBatch(
                phone=phone,
                target_dialog_id=target.dialog_id,
                target_title=target.title,
                target_type=target.target_type,
                send_mode=send_mode,
                caption=caption,
                status=PhotoBatchStatus.SCHEDULED,
            )
        )
        item_id = await self._bundle.create_item(
            PhotoBatchItem(
                batch_id=batch_id,
                phone=phone,
                target_dialog_id=target.dialog_id,
                target_title=target.title,
                target_type=target.target_type,
                file_paths=files,
                send_mode=send_mode,
                caption=caption,
                schedule_at=schedule_at,
                status=PhotoBatchStatus.SCHEDULED,
            )
        )
        try:
            message_ids = await self._publish.send_now(
                phone=phone,
                target_dialog_id=target.dialog_id,
                target_type=target.target_type,
                file_paths=files,
                send_mode=send_mode,
                caption=caption,
                schedule_at=schedule_at,
            )
            await self._bundle.update_item(
                item_id,
                status=PhotoBatchStatus.SCHEDULED,
                telegram_message_ids=message_ids,
            )
        except Exception as exc:
            await self._bundle.update_item(
                item_id,
                status=PhotoBatchStatus.FAILED,
                error=str(exc),
            )
            await self._bundle.update_batch(
                batch_id,
                status=PhotoBatchStatus.FAILED,
                error=str(exc),
            )
            raise
        item = await self._bundle.get_item(item_id)
        assert item is not None
        return item

    async def create_batch(
        self,
        *,
        phone: str,
        target: PhotoTarget,
        entries: list[dict],
        caption: str | None = None,
    ) -> int:
        if not entries:
            raise ValueError("Batch manifest is empty")
        batch_id = await self._bundle.create_batch(
            PhotoBatch(
                phone=phone,
                target_dialog_id=target.dialog_id,
                target_title=target.title,
                target_type=target.target_type,
                send_mode=PhotoSendMode.ALBUM,
                caption=caption,
                status=PhotoBatchStatus.PENDING,
            )
        )
        for entry in entries:
            files = self.validate_files([str(path) for path in entry.get("files", [])])
            send_mode = self.normalize_mode(
                entry.get("mode", PhotoSendMode.ALBUM.value),
                len(files),
            )
            schedule_at = self._parse_schedule_at(entry.get("at"))
            await self._bundle.create_item(
                PhotoBatchItem(
                    batch_id=batch_id,
                    phone=phone,
                    target_dialog_id=target.dialog_id,
                    target_title=target.title,
                    target_type=target.target_type,
                    file_paths=files,
                    send_mode=send_mode,
                    caption=entry.get("caption", caption),
                    schedule_at=schedule_at,
                    status=PhotoBatchStatus.PENDING,
                )
            )
        return batch_id

    async def list_batches(self, limit: int = 50) -> list[PhotoBatch]:
        return await self._bundle.list_batches(limit)

    async def list_items(self, limit: int = 100) -> list[PhotoBatchItem]:
        return await self._bundle.list_items(limit)

    async def run_due(self, limit: int = 20) -> int:
        processed = 0
        while processed < limit:
            item = await self._bundle.claim_next_due_item(datetime.now(timezone.utc))
            if item is None:
                break
            processed += 1
            await self._run_claimed_item(item)
        return processed

    async def _run_claimed_item(self, item: PhotoBatchItem) -> None:
        now = datetime.now(timezone.utc)
        try:
            message_ids = await self._publish.send_now(
                phone=item.phone,
                target_dialog_id=item.target_dialog_id,
                target_type=item.target_type,
                file_paths=item.file_paths,
                send_mode=item.send_mode,
                caption=item.caption,
            )
            await self._bundle.update_item(
                item.id or 0,
                status=PhotoBatchStatus.COMPLETED,
                telegram_message_ids=message_ids,
                completed_at=now,
            )
            if item.batch_id:
                await self._sync_batch_status(item.batch_id, last_run_at=now)
        except Exception as exc:
            await self._bundle.update_item(
                item.id or 0,
                status=PhotoBatchStatus.FAILED,
                error=str(exc),
                completed_at=now,
            )
            if item.batch_id:
                await self._sync_batch_status(
                    item.batch_id,
                    last_run_at=now,
                    fallback_error=str(exc),
                )

    async def cancel_item(self, item_id: int) -> bool:
        return await self._bundle.cancel_item(item_id)

    async def recover_running(self) -> int:
        return await self._bundle.requeue_running_items_on_startup(datetime.now(timezone.utc))

    async def _sync_batch_status(
        self,
        batch_id: int,
        *,
        last_run_at: datetime,
        fallback_error: str | None = None,
    ) -> None:
        items = await self._bundle.list_items_for_batch(batch_id)
        statuses = {item.status for item in items}
        if statuses <= {PhotoBatchStatus.COMPLETED}:
            await self._bundle.update_batch(
                batch_id,
                status=PhotoBatchStatus.COMPLETED,
                last_run_at=last_run_at,
            )
            return
        terminal_pending = {
            PhotoBatchStatus.PENDING,
            PhotoBatchStatus.RUNNING,
        }
        if PhotoBatchStatus.FAILED in statuses and statuses.isdisjoint(terminal_pending):
            await self._bundle.update_batch(
                batch_id,
                status=PhotoBatchStatus.FAILED,
                error=fallback_error,
                last_run_at=last_run_at,
            )
            return
        await self._bundle.update_batch(
            batch_id,
            status=PhotoBatchStatus.RUNNING,
            last_run_at=last_run_at,
        )

    def load_manifest(self, manifest_path: str) -> list[dict]:
        path = Path(manifest_path)
        if not path.exists():
            raise ValueError(f"Manifest not found: {path}")
        if path.suffix.lower() == ".json":
            data = json.loads(path.read_text())
        else:
            data = yaml.safe_load(path.read_text())
        if not isinstance(data, list):
            raise ValueError("Manifest must be a list")
        return data

    @staticmethod
    def _parse_schedule_at(value: object) -> datetime:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("Batch entry must include 'at'")
        return parse_required_schedule_datetime(value)
