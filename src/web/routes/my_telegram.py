from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from src.web import deps

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def my_telegram_page(request: Request, phone: str | None = None):
    pool = deps.get_pool(request)
    accounts = sorted(pool.clients.keys())
    selected_phone = phone or (accounts[0] if accounts else None)
    dialogs = []
    if selected_phone and selected_phone in pool.clients:
        dialogs = await deps.channel_service(request).get_my_dialogs(selected_phone)
    return deps.get_templates(request).TemplateResponse(
        request, "my_telegram.html", {
            "accounts": accounts,
            "selected_phone": selected_phone,
            "dialogs": dialogs,
        }
    )
