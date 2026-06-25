"""Account management routes — split from settings.py for clarity."""

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse, RedirectResponse

from src.agent.runtime_context import AgentRuntimeContext
from src.agent.tools.accounts import get_live_account_info_text
from src.database.repositories.accounts import AccountSessionDecryptError
from src.web import deps
from src.web.schemas.accounts import AccountInfoResponse, FloodStatusItem
from src.web.schemas.common import ErrorResponse

logger = logging.getLogger(__name__)

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

    form = await request.form()
    notify_to = (str(form.get("notify_to") or "")).strip() or None
    try:
        reassignment = await deps.get_notification_target_service(request).reassign_for_deleted_account(
            account.phone, notify_to, accounts=accounts
        )
    except ValueError:
        return RedirectResponse(url="/settings?error=invalid_notify_account", status_code=303)
    if reassignment.action != "kept":
        notifier = deps.get_notifier(request)
        if notifier:
            notifier.invalidate_me_cache()
        # Web notifier is None — invalidate the worker's me-cache over the queue
        # so it stops sending from the deleted account's me.id (#832).
        await deps.telegram_command_service(request).enqueue(
            "notifications.invalidate_cache",
            payload={},
            requested_by="web:accounts.delete-notify-reassign",
        )

    command_id = await deps.telegram_command_service(request).enqueue(
        "accounts.delete",
        payload={"account_id": account_id, "phone": account.phone},
        requested_by="web:accounts.delete",
    )
    await db.delete_account(account_id)
    msg = {
        "reassigned": "account_deleted_notify_reassigned",
        "cleared": "account_deleted_notify_cleared",
    }.get(reassignment.action, "account_deleted")
    return RedirectResponse(
        url=f"/settings?msg={msg}&command_id={command_id}",
        status_code=303,
    )


@router.post("/{account_id}/set-primary")
async def set_primary_account(request: Request, account_id: int):
    db = deps.get_db(request)
    changed = await db.repos.accounts.set_account_primary(account_id)
    if not changed:
        return RedirectResponse(url="/settings?error=invalid_account", status_code=303)
    return RedirectResponse(url="/settings?msg=account_set_primary", status_code=303)


@router.get(
    "/flood-status",
    response_model=list[FloodStatusItem],
    status_code=200,
    tags=["accounts"],
    summary="List per-account flood-wait status",
)
async def flood_status(request: Request):
    """Return flood-wait status for every account (parity with CLI `account flood-status`).

    Each item is 'ok' when the account is not rate-limited, otherwise carries the
    flood-wait expiry and remaining seconds.
    """
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


@router.get(
    "/{account_id}/info",
    response_model=AccountInfoResponse,
    status_code=200,
    tags=["accounts"],
    summary="Account summary plus live diagnostics",
    responses={404: {"model": ErrorResponse, "description": "Account not found"}},
)
async def account_info(request: Request, account_id: int):
    """Account summary plus live diagnostics as JSON (parity with CLI `account info`).

    Returns 404 with ``{"error": "account_not_found"}`` for an unknown id.
    """
    db = deps.get_db(request)
    accounts = await db.get_account_summaries(active_only=False)
    acc = next((a for a in accounts if a.id == account_id), None)
    if acc is None:
        return JSONResponse({"error": "account_not_found"}, status_code=404)
    try:
        client_pool = deps.get_pool(request)
    except RuntimeError:
        client_pool = None
    runtime = AgentRuntimeContext.build(
        db=db,
        config=getattr(request.app.state, "config", None),
        client_pool=client_pool,
    )
    try:
        live_info = await get_live_account_info_text(runtime, acc.phone)
    except Exception as exc:
        live_info = f"Ошибка получения live Telegram account info: {exc}"
    data = acc.model_dump(mode="json")
    data["live_info"] = live_info
    return JSONResponse(data)


@router.post("/{account_id}/flood-clear")
async def flood_clear(request: Request, account_id: int):
    db = deps.get_db(request)
    accounts = await db.get_account_summaries()
    acc = next((a for a in accounts if a.id == account_id), None)
    if not acc:
        return RedirectResponse(url="/settings?error=account_not_found", status_code=303)
    await db.update_account_flood(acc.phone, None)
    return RedirectResponse(url="/settings?msg=flood_cleared", status_code=303)


@router.post(
    "/{account_id}/export-session",
    tags=["accounts"],
    summary="Export the decrypted StringSession for SSO",
    responses={404: {"model": ErrorResponse, "description": "Account not found"}},
)
async def export_session(request: Request, account_id: int):
    """Return the decrypted plaintext StringSession for SSO (parity with CLI/agent, #828).

    POST (not GET) so the secret never lands in a URL/access log. Guarded by the
    panel's existing WEB_PASS auth (BasicAuthMiddleware → 401 for API clients
    without credentials). The session string grants FULL account access — it is
    returned in the JSON body ONLY on this explicit request and is NEVER logged.
    Returns 404 ``{"error": "account_not_found"}`` for an unknown id.
    """
    db = deps.get_db(request)
    # Resolve identity via summaries (no decrypt) so a broken sibling session
    # can't abort the lookup (#1143); decrypt only the chosen account.
    summaries = await db.get_account_summaries(active_only=False)
    acc = next((a for a in summaries if a.id == account_id), None)
    if acc is None:
        return JSONResponse({"error": "account_not_found"}, status_code=404)

    try:
        session_string = await db.repos.accounts.get_decrypted_session(account_id=account_id)
    except AccountSessionDecryptError as exc:
        # status carries only the failure kind (phone+status), never the secret.
        return JSONResponse(
            {"error": "session_decrypt_failed", "status": exc.status}, status_code=409
        )
    if not session_string:
        return JSONResponse({"error": "no_session"}, status_code=409)

    # Audit the export WITHOUT the value (account_id + actor only) — session_string
    # is a secret and must never be logged.
    logger.info("account.export_session account_id=%s by=web", account_id)
    return JSONResponse({"phone": acc.phone, "session_string": session_string})
