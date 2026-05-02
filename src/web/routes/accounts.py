"""Account management routes — split from settings.py for clarity."""

from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse

from src.web import deps

router = APIRouter()


@router.post("/{account_id}/toggle")
async def toggle_account(request: Request, account_id: int):
    command_id = await deps.telegram_command_service(request).enqueue(
        "accounts.toggle",
        payload={"account_id": account_id},
        requested_by="web:accounts.toggle",
    )
    return RedirectResponse(
        url=f"/settings?msg=account_toggle_queued&command_id={command_id}",
        status_code=303,
    )


@router.post("/{account_id}/delete")
async def delete_account(request: Request, account_id: int):
    db = deps.get_db(request)
    accounts = await db.get_account_summaries(active_only=False)
    account = next((a for a in accounts if a.id == account_id), None)
    if account is None:
        return RedirectResponse(url="/settings?error=invalid_account", status_code=303)

    command_id = await deps.telegram_command_service(request).enqueue(
        "accounts.delete",
        payload={"account_id": account_id, "phone": account.phone},
        requested_by="web:accounts.delete",
    )
    await db.delete_account(account_id)
    return RedirectResponse(
        url=f"/settings?msg=account_deleted&command_id={command_id}",
        status_code=303,
    )


@router.get("/flood-status")
async def flood_status(request: Request):
    db = deps.get_db(request)
    accounts = await db.get_account_summaries()
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
    accounts = await db.get_account_summaries()
    acc = next((a for a in accounts if a.id == account_id), None)
    if not acc:
        return RedirectResponse(url="/settings?error=account_not_found", status_code=303)
    await db.update_account_flood(acc.phone, None)
    return RedirectResponse(url="/settings?msg=flood_cleared", status_code=303)
