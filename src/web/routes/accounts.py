"""Account management routes — split from settings.py for clarity."""

from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse

from src.web import deps

router = APIRouter()


@router.post("/{account_id}/toggle")
async def toggle_account(request: Request, account_id: int):
    await deps.account_service(request).toggle(account_id)
    return RedirectResponse(url="/settings?msg=account_toggled", status_code=303)


@router.post("/{account_id}/delete")
async def delete_account(request: Request, account_id: int):
    await deps.account_service(request).delete(account_id)
    return RedirectResponse(url="/settings?msg=account_deleted", status_code=303)


@router.get("/flood-status")
async def flood_status(request: Request):
    db = deps.get_db(request)
    accounts = await db.get_accounts()
    now = datetime.now(timezone.utc)
    result = []
    for acc in accounts:
        if acc.flood_wait_until is None:
            status = "ok"
            remaining = 0
        else:
            flood_until = acc.flood_wait_until
            if flood_until.tzinfo is None:
                flood_until = flood_until.replace(tzinfo=timezone.utc)
            if flood_until > now:
                status = flood_until.strftime("%Y-%m-%d %H:%M:%S UTC")
                remaining = int((flood_until - now).total_seconds())
            else:
                status = "ok"
                remaining = 0
        result.append({
            "phone": acc.phone,
            "flood_wait_until": status,
            "remaining_seconds": remaining,
        })
    return JSONResponse(result)


@router.post("/{account_id}/flood-clear")
async def flood_clear(request: Request, account_id: int):
    db = deps.get_db(request)
    accounts = await db.get_accounts()
    acc = next((a for a in accounts if a.id == account_id), None)
    if not acc:
        return RedirectResponse(url="/settings?error=account_not_found", status_code=303)
    await db.update_account_flood(acc.phone, None)
    return RedirectResponse(url="/settings?msg=flood_cleared", status_code=303)
