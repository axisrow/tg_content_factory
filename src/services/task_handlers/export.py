"""Worker task handler for Telegram-Desktop export with media (#834, PR-3).

Runs in the worker container (which owns the live ClientPool). For with_media
exports it downloads each message's media via TelegramActionService (size-skip
from PR-2), then feeds the resulting artifacts to the TelegramExportBuilder
(PR-1). Without media it produces the same offline "not included" tree the CLI/
web inline path does.
"""

from __future__ import annotations

import logging

from src.models import CollectionTask, CollectionTaskStatus, CollectionTaskType, ExportTaskPayload
from src.services.export_service import (
    default_export_dir,
    gather_channel_messages,
    resolve_html_page_size,
    resolve_max_file_size_mb,
)
from src.services.task_handlers.base import TaskHandlerContext
from src.services.telegram_export_builder import (
    MediaArtifact,
    TelegramExportBuilder,
    offline_media_resolver,
)

logger = logging.getLogger(__name__)


class ExportTaskHandler:
    task_types = (CollectionTaskType.EXPORT,)

    def __init__(self, context: TaskHandlerContext):
        self._context = context

    async def handle(self, task: CollectionTask) -> None:
        ctx = self._context
        if task.id is None:
            return
        payload = task.payload
        if not isinstance(payload, ExportTaskPayload):
            await ctx.tasks.update_collection_task(
                task.id, CollectionTaskStatus.FAILED, error="Invalid EXPORT payload"
            )
            return
        if ctx.db is None:
            await ctx.tasks.update_collection_task(
                task.id, CollectionTaskStatus.FAILED, error="Export environment not configured"
            )
            return

        try:
            channel = await ctx.db.get_channel_by_channel_id(payload.channel_id)
            if channel is None:
                await ctx.tasks.update_collection_task(
                    task.id, CollectionTaskStatus.COMPLETED, note=f"Channel {payload.channel_id} not found"
                )
                return
            messages, truncated = await gather_channel_messages(
                ctx.db,
                payload.channel_id,
                date_from=payload.date_from,
                date_to=payload.date_to,
                limit=payload.limit,
            )
            if not messages:
                await ctx.tasks.update_collection_task(
                    task.id, CollectionTaskStatus.COMPLETED, note="No messages to export"
                )
                return

            target = payload.out_dir or str(default_export_dir(payload.channel_id))
            page_size = await resolve_html_page_size(ctx.db)

            resolver = offline_media_resolver
            media_note = "media not requested"
            if payload.with_media:
                resolver, media_note = await self._build_media_resolver(channel, messages, payload, target)

            summary = await TelegramExportBuilder().write_export(
                target,
                channel,
                messages,
                fmt=payload.fmt,
                media_resolver=resolver,
                page_size=page_size,
                truncated=truncated,
            )
            await ctx.tasks.update_collection_task_progress(task.id, summary.message_count)
            note = (
                f"Exported {summary.message_count} msgs to {summary.out_dir} "
                f"(media: {summary.media_included} ok / {summary.media_skipped} skipped; {media_note}"
                f"{'; truncated' if truncated else ''})"
            )
            await ctx.tasks.update_collection_task(task.id, CollectionTaskStatus.COMPLETED, note=note)
        except Exception as exc:  # noqa: BLE001 — surface any failure on the task row
            logger.exception("EXPORT task %s failed", task.id)
            await ctx.tasks.update_collection_task(task.id, CollectionTaskStatus.FAILED, error=str(exc))

    async def _build_media_resolver(self, channel, messages, payload: ExportTaskPayload, target: str):
        """Pre-download media into the export tree, return a sync resolver + note.

        The builder's resolver is synchronous, so all downloads happen here up
        front and the resolver just looks artifacts up by message_id.
        """
        ctx = self._context
        phone = self._resolve_phone(channel)
        if ctx.client_pool is None or phone is None:
            # No live account available — fall back to the offline representation.
            return offline_media_resolver, "no account for media download"

        from src.services.telegram_actions import TelegramActionService

        svc = TelegramActionService(ctx.client_pool)
        max_size_bytes = await resolve_max_file_size_mb(ctx.db, payload.max_file_size_mb) * 1024 * 1024
        # Prefer the @username for public channels: the numeric PeerChannel path
        # cannot resolve without a cached access hash on the chosen account, while
        # the username always resolves (Codex review on #939).
        chat_identity = channel.username or channel.channel_id
        logger.info("EXPORT media download for channel %s via %s", channel.channel_id, phone)
        artifacts: dict[int, MediaArtifact] = {}
        for message in messages:
            if ctx.stop_event.is_set():
                # Honour graceful shutdown — build the export from what we have.
                logger.info("EXPORT media download interrupted by stop_event")
                break
            if not message.media_type:
                continue
            try:
                outcome = await svc.download_media_sized(
                    phone=phone,
                    chat_id=chat_identity,
                    message_id=message.message_id,
                    output_dir=target,
                    max_size_bytes=max_size_bytes,
                )
            except Exception:
                logger.warning("media download failed for msg %s; marking not included", message.message_id)
                artifacts[message.message_id] = MediaArtifact(
                    kind="file", skipped=True, reason="download_failed"
                )
                continue
            if outcome.skipped:
                artifacts[message.message_id] = MediaArtifact(
                    kind=outcome.kind, skipped=True, reason=outcome.reason, size_bytes=outcome.size_bytes
                )
            else:
                artifacts[message.message_id] = MediaArtifact(
                    kind=outcome.kind, rel_path=outcome.rel_path, size_bytes=outcome.size_bytes
                )

        def resolver(message):
            return artifacts.get(message.message_id)

        return resolver, f"downloaded via {phone}"

    def _resolve_phone(self, channel) -> str | None:
        if getattr(channel, "preferred_phone", None):
            return channel.preferred_phone
        clients = getattr(self._context.client_pool, "clients", None) or {}
        return next(iter(clients), None)
