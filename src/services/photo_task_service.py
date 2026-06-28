from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import yaml

from src.database.bundles import PhotoLoaderBundle
from src.models import PhotoBatch, PhotoBatchItem, PhotoBatchStatus, PhotoSendMode
from src.services.photo_publish_service import PhotoPublishService
from src.utils.datetime import parse_required_schedule_datetime

logger = logging.getLogger(__name__)

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
        # Accumulate ids of files already published live on Telegram. In SEPARATE
        # mode send_now publishes file-by-file immediately, so if a later file
        # fails the earlier ones are irreversibly live. Without this, the item is
        # marked FAILED with no ids → the CLI/agent/UI report failure → the user
        # reruns the command → file 1 is published a SECOND time (#864 review).
        accumulated: list[int] = []

        async def _record_sent(_path: str, ids: list[int]) -> None:
            # ALBUM fires the callback once per path sharing the same id set, so
            # de-dupe to store the album's ids exactly once.
            for mid in ids:
                if mid not in accumulated:
                    accumulated.append(mid)
            await self._bundle.update_item(
                item_id,
                status=PhotoBatchStatus.RUNNING,
                telegram_message_ids=list(accumulated),
            )

        try:
            message_ids = await self._publish.send_now(
                phone=phone,
                target_dialog_id=target.dialog_id,
                target_type=target.target_type,
                file_paths=files,
                send_mode=send_mode,
                caption=caption,
                on_file_sent=_record_sent,
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
            if accumulated:
                # Some files are already published live (irreversible — there is no
                # unschedule handle for an immediate send). Persist their ids so the
                # live posts are tied to the record for audit/cleanup, and mark the
                # item COMPLETED-with-error rather than FAILED. Do NOT raise: a
                # FAILED report makes callers tell the user it failed, the user
                # reruns, and the published files are duplicated (#864 review).
                await self._bundle.update_item(
                    item_id,
                    status=PhotoBatchStatus.COMPLETED,
                    telegram_message_ids=list(accumulated),
                    error=f"partial send failure: {exc}",
                    completed_at=now,
                )
                await self._bundle.update_batch(
                    batch_id,
                    status=PhotoBatchStatus.COMPLETED,
                    error=f"partial send failure: {exc}",
                    last_run_at=now,
                )
            else:
                # Nothing was published — failing with no ids is correct; raise so
                # the caller/dispatcher reports a genuine failure.
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
        # Accumulate server-scheduled message ids progressively. In SEPARATE mode
        # send_now schedules files one-by-one; if a later file fails, the earlier
        # ones are already queued on Telegram. Persisting their ids onto the item
        # as a still-SCHEDULED (cancellable) state means a partial failure never
        # strands an already-scheduled post without a cancel handle — the same
        # ghost-publish class this PR fixes (audit #835/3,4).
        accumulated: list[int] = []

        async def _record_scheduled(_path: str, ids: list[int]) -> None:
            # ALBUM fires the callback once per path sharing the same id set, so
            # de-dupe to store the album's ids exactly once.
            for mid in ids:
                if mid not in accumulated:
                    accumulated.append(mid)
            await self._bundle.update_item(
                item_id,
                status=PhotoBatchStatus.SCHEDULED,
                telegram_message_ids=list(accumulated),
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
                on_file_sent=_record_scheduled,
            )
            await self._bundle.update_item(
                item_id,
                status=PhotoBatchStatus.SCHEDULED,
                telegram_message_ids=message_ids,
            )
        except Exception as exc:
            if accumulated:
                # Some files were already scheduled server-side: keep the item
                # SCHEDULED with their ids so cancel_item can still unschedule
                # them. Never lose the cancel handle — record the error but leave
                # the item (and batch) in-flight so the post stays cancellable.
                #
                # This is a *partial success*, not a failed operation: do NOT
                # raise. Raising would make the command dispatcher / CLI / agent
                # report the schedule as FAILED, so the user retries and schedules
                # a SECOND copy while the first stays queued (duplicate publish).
                # Fall through to return the durably-created, cancellable item;
                # the recorded error surfaces the partial failure (#864 review).
                await self._bundle.update_item(
                    item_id,
                    status=PhotoBatchStatus.SCHEDULED,
                    telegram_message_ids=list(accumulated),
                    error=f"partial schedule failure: {exc}",
                )
            else:
                # Nothing was scheduled — failing with no ids is correct; raise so
                # the caller/dispatcher reports a genuine failure.
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
                status=PhotoBatchStatus.HELD,
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
                    status=PhotoBatchStatus.HELD,
                )
            )
        return batch_id

    async def publish_batch(self, batch_id: int) -> int:
        batch = await self._bundle.get_batch(batch_id)
        if batch is None:
            return 0
        return await self._bundle.publish_batch(batch_id)

    async def list_batches(self, limit: int = 50) -> list[PhotoBatch]:
        return await self._bundle.list_batches(limit)

    async def list_items(self, limit: int = 100) -> list[PhotoBatchItem]:
        return await self._bundle.list_items(limit)

    async def run_due(
        self,
        limit: int = 20,
        item_id: int | None = None,
        on_progress: Callable[[int, int], None] | None = None,
    ) -> int:
        now = datetime.now(timezone.utc)
        due_total = await self._bundle.count_due_items(now, item_id=item_id)
        if item_id is not None:
            item = await self._bundle.claim_next_due_item(now, item_id=item_id)
            if item is None:
                return 0
            await self._run_claimed_item(item)
            if on_progress is not None:
                on_progress(1, due_total)
            return 1

        total = min(due_total, max(limit, 0))
        processed = 0
        while processed < limit:
            item = await self._bundle.claim_next_due_item(datetime.now(timezone.utc))
            if item is None:
                break
            processed += 1
            await self._run_claimed_item(item)
            if on_progress is not None:
                on_progress(processed, total)
        return processed

    async def _run_claimed_item(self, item: PhotoBatchItem) -> None:
        now = datetime.now(timezone.utc)
        # NB: this due-execution path intentionally omits on_file_sent. A FAILED due
        # item is terminal — claim_next_due_item only claims status='pending' and no
        # verb resets FAILED→PENDING — so it is never re-run and cannot double-publish.
        # If a retry/requeue-failed verb is ever added, wire on_file_sent here (mirror
        # send_now) AND add skip-already-sent logic, or partial sends will duplicate.
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
        item = await self._bundle.get_item(item_id)
        # A SCHEDULED item is already queued server-side on Telegram; it MUST be
        # cancelled there too, otherwise the post still goes out at its scheduled
        # time (audit #835/3). Server-side unschedule is a PRECONDITION for marking
        # such an item CANCELLED: if the RPC fails (flood/network/missing client),
        # do NOT mark it cancelled — that would report success while Telegram still
        # publishes the post, and lose the telegram_message_ids retry target. Leave
        # it SCHEDULED with the error recorded so cancellation can be retried.
        if (
            item is not None
            and item.status == PhotoBatchStatus.SCHEDULED
            and item.telegram_message_ids
        ):
            try:
                await self._publish.unschedule(
                    phone=item.phone,
                    target_dialog_id=item.target_dialog_id,
                    target_type=item.target_type,
                    message_ids=item.telegram_message_ids,
                )
            except Exception as exc:
                logger.warning(
                    "cancel_item: failed to unschedule item %s on Telegram; "
                    "leaving it SCHEDULED so the post can still be cancelled",
                    item_id,
                    exc_info=True,
                )
                await self._bundle.update_item(item_id, error=f"unschedule failed: {exc}")
                return False
        cancelled = await self._bundle.cancel_item(item_id)
        if cancelled and item is not None and item.batch_id:
            await self._sync_batch_status(item.batch_id, last_run_at=datetime.now(timezone.utc))
        return cancelled

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
        # SCHEDULED is a server-side in-flight state; PENDING/RUNNING are local
        # in-flight. While any item is in-flight the batch is not terminal.
        in_flight = {
            PhotoBatchStatus.HELD,
            PhotoBatchStatus.PENDING,
            PhotoBatchStatus.RUNNING,
            PhotoBatchStatus.SCHEDULED,
        }
        if statuses & in_flight:
            # All-SCHEDULED batches surface as SCHEDULED, otherwise RUNNING.
            if statuses <= {PhotoBatchStatus.HELD}:
                status = PhotoBatchStatus.HELD
            elif statuses <= {PhotoBatchStatus.SCHEDULED}:
                status = PhotoBatchStatus.SCHEDULED
            else:
                status = PhotoBatchStatus.RUNNING
            await self._bundle.update_batch(batch_id, status=status, last_run_at=last_run_at)
            return
        # Every item is terminal (COMPLETED / FAILED / CANCELLED). Recognise
        # CANCELLED so a COMPLETED+CANCELLED mix doesn't get stuck RUNNING
        # (audit #837/11).
        if PhotoBatchStatus.FAILED in statuses:
            await self._bundle.update_batch(
                batch_id,
                status=PhotoBatchStatus.FAILED,
                error=fallback_error,
                last_run_at=last_run_at,
            )
        elif statuses <= {PhotoBatchStatus.CANCELLED}:
            await self._bundle.update_batch(
                batch_id, status=PhotoBatchStatus.CANCELLED, last_run_at=last_run_at
            )
        else:
            await self._bundle.update_batch(
                batch_id, status=PhotoBatchStatus.COMPLETED, last_run_at=last_run_at
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
