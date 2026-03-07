from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.web import deps

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def my_telegram_page(
    request: Request,
    phone: str | None = None,
    left: int = 0,
    failed: int = 0,
):
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
            "left": left,
            "failed": failed,
        }
    )


@router.post("/leave")
async def leave_dialogs(request: Request):
    form = await request.form()
    phone = form.get("phone", "")
    ids = [int(x) for x in form.getlist("channel_ids") if x.lstrip("-").isdigit()]
    results = await deps.channel_service(request).leave_dialogs(phone, ids)
    left = sum(1 for v in results.values() if v)
    failed = len(results) - left
    return RedirectResponse(
        url=f"/my-telegram/?phone={phone}&left={left}&failed={failed}",
        status_code=303,
    )
