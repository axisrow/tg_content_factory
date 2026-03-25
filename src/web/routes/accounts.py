"""Account management routes — split from settings.py for clarity."""

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

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
