"""Telegram-Desktop export route (issue #834).

Per the operator's choice the export tree is written under ``data/exports/`` on
the server and the response returns the path + summary (no file download). The
heavy media-download path runs in the worker (PR-3); this route does the offline
text/metadata export inline.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Form, Request
from fastapi.responses import JSONResponse

from src.services.export_service import resolve_max_file_size_mb, run_offline_export
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
    fmt = format if format in ("json", "html", "both") else "json"
    # Cap the inline export so a single web request can't pull a 100k-message
    # channel into the event loop (Claude review on #937). Larger exports should
    # go through the worker EXPORT task (PR-3).
    limit = max(1, min(limit, 10_000))
    db = deps.get_db(request)

    note = None
    if with_media:
        max_mb = await resolve_max_file_size_mb(db, max_file_size)
        note = (
            "Скачивание медиа выполняется worker'ом; офлайн-экспорт помечает медиа как "
            f"«не включено» (порог пропуска {max_mb} МБ)."
        )

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
            "note": note,
        }
    )
