from __future__ import annotations

import logging
import time
from urllib.parse import quote

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.web import deps

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/", response_class=HTMLResponse)
async def my_telegram_page(
    request: Request,
    phone: str | None = None,
    left: int = 0,
    failed: int = 0,
):
    started_at = time.perf_counter()
    pool = deps.get_pool(request)
    accounts = sorted(pool.clients.keys())
    selected_phone = phone if phone in pool.clients else None
    dialogs = []
    if selected_phone:
        dialogs = await deps.channel_service(request).get_my_dialogs(selected_phone)
    elapsed_ms = int((time.perf_counter() - started_at) * 1000)
    logger.info(
        "my_telegram_page: phone=%s accounts=%d dialogs=%d duration_ms=%d",
        selected_phone,
        len(accounts),
        len(dialogs),
        elapsed_ms,
    )
    return deps.get_templates(request).TemplateResponse(
        request, "my_telegram.html", {
            "accounts": accounts,
            "selected_phone": selected_phone,
            "dialogs": dialogs,
            "left": left,
            "failed": failed,
        }
    )


@router.post("/leave")
async def leave_dialogs(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    dialogs: list[tuple[int, str]] = []
    for item in form.getlist("channel_ids"):
        parts = item.split(":", 1)
        if len(parts) == 2 and parts[0].lstrip("-").isdigit():
            dialogs.append((int(parts[0]), parts[1]))
    results = await deps.channel_service(request).leave_dialogs(phone, dialogs)
    left = sum(1 for v in results.values() if v)
    failed = len(results) - left
    return RedirectResponse(
        url=f"/my-telegram/?phone={quote(phone, safe='')}&left={left}&failed={failed}",
        status_code=303,
    )
