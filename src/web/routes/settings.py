from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from src.services.notification_service import NotificationService
from src.web import deps

router = APIRouter()

CREDENTIALS_MASK = "••••••••"


@router.get("/", response_class=HTMLResponse)
async def settings_page(request: Request):
    auth = deps.get_auth(request)
    db = deps.get_db(request)
    pool = deps.get_pool(request)
    api_id_raw = await db.get_setting("tg_api_id") or ""
    api_hash_raw = await db.get_setting("tg_api_hash") or ""
    min_subscribers_filter = int(await db.get_setting("min_subscribers_filter") or 0)
    accounts = await db.get_accounts()
    connected_phones = set(pool.clients.keys())
    return deps.get_templates(request).TemplateResponse(
        request,
        "settings.html",
        {
            "is_configured": auth.is_configured,
            "api_id": CREDENTIALS_MASK if api_id_raw else "",
            "api_hash": CREDENTIALS_MASK if api_hash_raw else "",
            "min_subscribers_filter": min_subscribers_filter,
            "accounts": accounts,
            "connected_phones": connected_phones,
        },
    )


@router.post("/save-filters")
async def save_filters(request: Request):
    form = await request.form()
    db = deps.get_db(request)
    min_subs = str(form.get("min_subscribers_filter", "0")).strip()
    if not min_subs.isdigit():
        return RedirectResponse(url="/settings?error=invalid_value", status_code=303)
    await db.set_setting("min_subscribers_filter", min_subs)
    if int(min_subs) > 0:
        all_stats = await db.get_latest_stats_for_all()
        to_filter = [
            (channel_id, "low_subscriber_manual")
            for channel_id, stats in all_stats.items()
            if stats.subscriber_count is not None and stats.subscriber_count < int(min_subs)
        ]
        if to_filter:
            await db.set_channels_filtered_bulk(to_filter)
    return RedirectResponse(url="/settings?msg=filters_saved", status_code=303)


@router.post("/save-credentials")
async def save_credentials(request: Request):
    form = await request.form()
    db = deps.get_db(request)
    auth = deps.get_auth(request)

    api_id = str(form.get("api_id", "")).strip()
    api_hash = str(form.get("api_hash", "")).strip()

    id_changed = api_id and api_id != CREDENTIALS_MASK
    hash_changed = api_hash and api_hash != CREDENTIALS_MASK

    if id_changed and not api_id.isdigit():
        return RedirectResponse(url="/settings?error=invalid_api_id", status_code=303)

    if id_changed:
        await db.set_setting("tg_api_id", api_id)
    if hash_changed:
        await db.set_setting("tg_api_hash", api_hash)

    if id_changed or hash_changed:
        actual_id = api_id if id_changed else (await db.get_setting("tg_api_id") or "")
        actual_hash = api_hash if hash_changed else (await db.get_setting("tg_api_hash") or "")
        if actual_id and actual_hash:
            if not actual_id.isdigit():
                return RedirectResponse(url="/settings?error=invalid_api_id", status_code=303)
            auth.update_credentials(int(actual_id), actual_hash)

    return RedirectResponse(url="/settings?msg=credentials_saved", status_code=303)


@router.post("/{account_id}/toggle")
async def toggle_account(request: Request, account_id: int):
    await deps.account_service(request).toggle(account_id)
    return RedirectResponse(url="/settings?msg=account_toggled", status_code=303)


@router.post("/{account_id}/delete")
async def delete_account(request: Request, account_id: int):
    await deps.account_service(request).delete(account_id)
    return RedirectResponse(url="/settings?msg=account_deleted", status_code=303)


@router.post("/notifications/setup")
async def setup_notification_bot(request: Request):
    pool = deps.get_pool(request)
    db = deps.get_db(request)
    notif_cfg = request.app.state.config.notifications
    svc = NotificationService(db, pool, notif_cfg.bot_name_prefix, notif_cfg.bot_username_prefix)
    try:
        bot = await svc.setup_bot()
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)
    return JSONResponse({
        "bot_username": bot.bot_username,
        "bot_id": bot.bot_id,
    })


@router.get("/notifications/status")
async def notification_bot_status(request: Request):
    pool = deps.get_pool(request)
    db = deps.get_db(request)
    notif_cfg = request.app.state.config.notifications
    svc = NotificationService(db, pool, notif_cfg.bot_name_prefix, notif_cfg.bot_username_prefix)
    bot = await svc.get_status()
    if bot is None:
        return JSONResponse({"configured": False})
    return JSONResponse({
        "configured": True,
        "bot_username": bot.bot_username,
        "bot_id": bot.bot_id,
        "created_at": bot.created_at.isoformat() if bot.created_at else None,
    })


@router.post("/notifications/delete")
async def delete_notification_bot(request: Request):
    pool = deps.get_pool(request)
    db = deps.get_db(request)
    notif_cfg = request.app.state.config.notifications
    svc = NotificationService(db, pool, notif_cfg.bot_name_prefix, notif_cfg.bot_username_prefix)
    try:
        await svc.teardown_bot()
    except Exception as exc:
        return JSONResponse({"error": str(exc)}, status_code=500)
    return JSONResponse({"deleted": True})
