"""Telegram-Desktop export route (issue #834).

Per the operator's choice the export tree is written under ``data/exports/`` on
the server and the response returns the path + summary (no file download). A
text-only export runs inline; a ``with_media`` export is enqueued as a worker
EXPORT task (the worker owns the live Telegram clients needed to fetch media).
"""

from __future__ import annotations

import logging
from typing import Literal, cast

from fastapi import APIRouter, Form, Request
from fastapi.responses import JSONResponse

from src.models import CollectionTaskType, ExportTaskPayload
from src.services.export_service import run_offline_export
from src.web import deps

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/{channel_id}/export")
async def export_channel(
    request: Request,
    channel_id: int,
    format: str = Form("json"),
    with_media: bool = Form(False),
    max_file_size: int | None = Form(None),
    date_from: str | None = Form(None),
    date_to: str | None = Form(None),
    limit: int = Form(5000),
) -> JSONResponse:
    fmt: Literal["json", "html", "both"] = (
        cast(Literal["json", "html", "both"], format) if format in ("json", "html", "both") else "json"
    )
    # Cap the inline export so a single web request can't pull a 100k-message
    # channel into the event loop (Claude review on #937).
    limit = max(1, min(limit, 10_000))
    db = deps.get_db(request)

    if with_media:
        # Media download needs the worker's live ClientPool — enqueue a task.
        payload = ExportTaskPayload(
            channel_id=channel_id,
            fmt=fmt,
            with_media=True,
            max_file_size_mb=max_file_size,
            date_from=date_from or None,
            date_to=date_to or None,
            limit=limit,
            requested_by="web",
        )
        task_id = await db.repos.tasks.create_generic_task(
            CollectionTaskType.EXPORT, title=f"export channel {channel_id} (media)", payload=payload
        )
        return JSONResponse({"task_id": task_id, "status": "enqueued", "with_media": True})

    try:
        summary = await run_offline_export(
            db,
            channel_id,
            fmt=fmt,
            date_from=date_from or None,
            date_to=date_to or None,
            limit=limit,
        )
    except Exception:
        logger.exception("Telegram export failed for channel %s", channel_id)
        return JSONResponse({"error": "export_failed"}, status_code=500)

    if summary is None:
        return JSONResponse({"error": "no_messages"}, status_code=404)

    return JSONResponse(
        {
            "out_dir": summary.out_dir,
            "files": summary.files,
            "message_count": summary.message_count,
            "truncated": summary.truncated,
            "media_included": summary.media_included,
            "media_skipped": summary.media_skipped,
            "skipped_files": summary.skipped,
        }
    )
