from __future__ import annotations

import logging

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse

from src.models import Channel
from src.parsers import deduplicate_identifiers, parse_file, parse_identifiers

logger = logging.getLogger(__name__)

router = APIRouter()


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
    pool = request.app.state.pool
    db = request.app.state.db

    # 1. Collect identifiers from textarea and file
    identifiers: list[str] = []
    if text_input.strip():
        identifiers.extend(parse_identifiers(text_input))

    if file and file.filename:
        content = await file.read()
        if content:
            identifiers.extend(parse_file(content, file.filename or ""))

    # 2. Deduplicate
    identifiers = deduplicate_identifiers(identifiers)

    # 3. Load existing channels
    existing = await db.get_channels()
    existing_ids = {ch.channel_id for ch in existing}
    # 4. Resolve and add
    details: list[dict] = []
    added = 0
    skipped = 0
    failed = 0

    no_client = False
    for ident in identifiers:
        try:
            info = await pool.resolve_channel(ident.strip())
        except RuntimeError as exc:
            if str(exc) == "no_client":
                no_client = True
                for remaining in identifiers[identifiers.index(ident):]:
                    details.append({
                        "identifier": remaining,
                        "status": "failed",
                        "detail": "Нет доступных аккаунтов Telegram",
                    })
                    failed += 1
                break
            logger.warning("Failed to resolve '%s': %s", ident, exc)
            info = None
        except Exception as exc:
            logger.warning("Failed to resolve '%s': %s", ident, exc)
            info = None

        if no_client:
            break

        if not info:
            details.append({"identifier": ident, "status": "failed", "detail": "Не найден"})
            failed += 1
            continue

        if info["channel_id"] in existing_ids:
            details.append({
                "identifier": ident,
                "status": "skipped",
                "detail": f"Уже добавлен ({info.get('title', '')})",
            })
            skipped += 1
            continue

        deactivate = info.get("deactivate", False)
        channel = Channel(
            channel_id=info["channel_id"],
            title=info["title"],
            username=info["username"],
            channel_type=info.get("channel_type"),
            is_active=not deactivate,
        )
        await db.add_channel(channel)
        existing_ids.add(info["channel_id"])
        detail_text = f"{info.get('title', '')} ({info['channel_id']})"
        if deactivate:
            detail_text += " [неактивен: scam/fake/restricted]"
        details.append({
            "identifier": ident,
            "status": "added",
            "detail": detail_text,
        })
        added += 1

    results = {
        "added": added,
        "skipped": skipped,
        "failed": failed,
        "total": len(identifiers),
        "details": details,
    }

    return request.app.state.templates.TemplateResponse(
        request,
        "import_channels.html",
        {"results": results},
    )
