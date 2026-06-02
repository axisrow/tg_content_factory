from __future__ import annotations

import logging

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse

from src.models import TelegramCommand
from src.parsers import deduplicate_identifiers, parse_file, parse_identifiers

logger = logging.getLogger(__name__)

router = APIRouter()

# Channel-import files are plain identifier lists (txt/csv/xlsx); a few MiB is
# already far more than any realistic list, so cap the upload to avoid reading an
# unbounded blob into memory (#633 bug #20).
MAX_IMPORT_FILE_BYTES = 5 * 1024 * 1024


@router.get("/import", response_class=HTMLResponse)
async def import_page(request: Request):
    return request.app.state.templates.TemplateResponse(
        request,
        "import_channels.html",
        {"results": None},
    )


@router.post("/import", response_class=HTMLResponse)
async def import_channels(
    request: Request,
    file: UploadFile | None = File(None),
    text_input: str = Form(""),
):
    # 1. Collect identifiers from textarea and file
    identifiers: list[str] = []
    if text_input.strip():
        identifiers.extend(parse_identifiers(text_input))

    if file and file.filename:
        # Read at most MAX_IMPORT_FILE_BYTES + 1 so an oversized upload is
        # rejected without pulling the whole blob into memory.
        content = await file.read(MAX_IMPORT_FILE_BYTES + 1)
        if len(content) > MAX_IMPORT_FILE_BYTES:
            limit_mb = MAX_IMPORT_FILE_BYTES // (1024 * 1024)
            return request.app.state.templates.TemplateResponse(
                request,
                "import_channels.html",
                {
                    "results": None,
                    "error": f"Файл слишком большой. Максимум {limit_mb} МБ.",
                },
                status_code=413,
            )
        if content:
            identifiers.extend(parse_file(content, file.filename or ""))

    # 2. Deduplicate
    identifiers = deduplicate_identifiers(identifiers)
    command_id = None
    results = {
        "queued": len(identifiers),
        "added": 0,
        "skipped": 0,
        "failed": 0,
        "total": len(identifiers),
        "details": [],
    }
    if identifiers:
        command_id = await request.app.state.db.repos.telegram_commands.create_command(
            TelegramCommand(
                command_type="channels.import_batch",
                payload={"identifiers": identifiers},
                requested_by="web:import",
            )
        )
        results["details"] = [
            {"identifier": ident, "status": "queued", "detail": "Добавлено в очередь"}
            for ident in identifiers
        ]

    return request.app.state.templates.TemplateResponse(
        request,
        "import_channels.html",
        {"results": results, "command_id": command_id},
    )
