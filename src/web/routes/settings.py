import asyncio
import logging
import os

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from src.services.notification_service import NotificationService
from src.settings_utils import parse_int_setting
from src.telegram.notifier import Notifier
from src.web import deps

router = APIRouter()
logger = logging.getLogger(__name__)

CREDENTIALS_MASK = "••••••••"


def _notification_service(request: Request) -> NotificationService:
    notif_cfg = request.app.state.config.notifications
    return NotificationService(
        deps.get_db(request),
        deps.get_notification_target_service(request),
        notif_cfg.bot_name_prefix,
        notif_cfg.bot_username_prefix,
    )


def _wants_json(request: Request) -> bool:
    return "application/json" in request.headers.get("accept", "")


@router.get("/", response_class=HTMLResponse)
async def settings_page(request: Request):
    auth = deps.get_auth(request)
    db = deps.get_db(request)
    pool = deps.get_pool(request)
    api_id_raw = await db.get_setting("tg_api_id") or ""
    api_hash_raw = await db.get_setting("tg_api_hash") or ""
    min_subscribers_filter = parse_int_setting(
        await db.get_setting("min_subscribers_filter"),
        setting_name="min_subscribers_filter",
        default=0,
        logger=logger,
    )
    saved_interval = await db.get_setting("collect_interval_minutes")
    agent_dev_mode_enabled = (await db.get_setting("agent_dev_mode_enabled") or "0") == "1"
    agent_backend_override = await db.get_setting("agent_backend_override") or "auto"
    if agent_backend_override not in {"auto", "claude", "deepagents"}:
        agent_backend_override = "auto"
    config = request.app.state.config
    telegram_credentials_from_env = bool(
        os.environ.get("TG_API_ID", "").strip().isdigit()
        and os.environ.get("TG_API_HASH", "").strip()
    )
    collect_interval_minutes = parse_int_setting(
        saved_interval,
        setting_name="collect_interval_minutes",
        default=config.scheduler.collect_interval_minutes,
        logger=logger,
    )
    accounts = await db.get_accounts()
    connected_phones = set(pool.clients.keys())
    notification_target = await deps.get_notification_target_service(request).describe_target()
    notification_bot = None
    notification_bot_error = None
    if notification_target.state == "available" and callable(
        getattr(pool, "get_client_by_phone", None)
    ):
        try:
            notification_bot = await _notification_service(request).get_status()
        except RuntimeError as exc:
            notification_bot_error = str(exc)
            logger.warning("Failed to load notification bot status: %s", exc)
    return deps.get_templates(request).TemplateResponse(
        request,
        "settings.html",
        {
            "is_configured": auth.is_configured,
            "telegram_credentials_from_env": telegram_credentials_from_env,
            "api_id": CREDENTIALS_MASK if api_id_raw else "",
            "api_hash": CREDENTIALS_MASK if api_hash_raw else "",
            "min_subscribers_filter": min_subscribers_filter,
            "accounts": accounts,
            "account_phones": [acc.phone for acc in accounts],
            "connected_phones": connected_phones,
            "notification_target": notification_target,
            "notification_selected_phone": notification_target.configured_phone or "",
            "notification_bot": notification_bot,
            "notification_bot_error": notification_bot_error,
            "collect_interval_minutes": collect_interval_minutes,
            "agent_dev_mode_enabled": agent_dev_mode_enabled,
            "agent_backend_override": agent_backend_override,
            "agent_fallback_model": config.agent.fallback_model or os.environ.get(
                "AGENT_FALLBACK_MODEL", ""
            ).strip(),
        },
    )


@router.post("/save-scheduler")
async def save_scheduler_settings(request: Request):
    form = await request.form()
    try:
        interval = int(form.get("collect_interval_minutes", 60))
    except (TypeError, ValueError):
        return RedirectResponse(url="/settings?error=invalid_value", status_code=303)
    interval = max(1, min(1440, interval))
    db = deps.get_db(request)
    await db.set_setting("collect_interval_minutes", str(interval))
    scheduler = getattr(request.app.state, "scheduler", None)
    if scheduler:
        scheduler.update_interval(interval)
    return RedirectResponse(url="/settings?msg=scheduler_saved", status_code=303)


@router.post("/save-agent")
async def save_agent_settings(request: Request):
    form = await request.form()
    db = deps.get_db(request)

    dev_mode_enabled = str(form.get("agent_dev_mode_enabled", "")).strip() == "1"
    backend_override = str(form.get("agent_backend_override", "auto")).strip()
    if backend_override not in {"auto", "claude", "deepagents"}:
        backend_override = "auto"

    await db.set_setting("agent_dev_mode_enabled", "1" if dev_mode_enabled else "0")
    await db.set_setting("agent_backend_override", backend_override)
    return RedirectResponse(url="/settings?msg=agent_saved", status_code=303)


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


@router.post("/save-notification-account")
async def save_notification_account(request: Request):
    form = await request.form()
    selected_phone = str(form.get("notification_account_phone", "")).strip()
    db = deps.get_db(request)
    valid_phones = {acc.phone for acc in await db.get_accounts()}
    if selected_phone and selected_phone not in valid_phones:
        return RedirectResponse(url="/settings?error=notification_account_invalid", status_code=303)

    await deps.get_notification_target_service(request).set_configured_phone(selected_phone or None)
    # Invalidate cached me.id so the next notify() re-resolves it for the new account.
    notifier = deps.get_notifier(request)
    if notifier:
        notifier.invalidate_me_cache()
    return RedirectResponse(url="/settings?msg=notification_account_saved", status_code=303)


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


@router.post("/notifications/setup")
async def setup_notification_bot(request: Request):
    try:
        bot = await _notification_service(request).setup_bot()
    except RuntimeError as exc:
        if _wants_json(request):
            return JSONResponse({"error": str(exc)}, status_code=409)
        return RedirectResponse(
            url="/settings?error=notification_account_unavailable",
            status_code=303,
        )
    except Exception as exc:
        logger.exception("Notification setup failed")
        if _wants_json(request):
            return JSONResponse({"error": str(exc)}, status_code=500)
        return RedirectResponse(url="/settings?error=notification_action_failed", status_code=303)

    if _wants_json(request):
        return JSONResponse({
            "bot_username": bot.bot_username,
            "bot_id": bot.bot_id,
        })
    return RedirectResponse(url="/settings?msg=notification_bot_created", status_code=303)


@router.get("/notifications/status")
async def notification_bot_status(request: Request):
    try:
        bot = await _notification_service(request).get_status()
    except RuntimeError as exc:
        return JSONResponse({"configured": False, "error": str(exc)}, status_code=409)
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
    try:
        await _notification_service(request).teardown_bot()
    except RuntimeError as exc:
        if _wants_json(request):
            return JSONResponse({"error": str(exc)}, status_code=409)
        error_code = "notification_bot_missing"
        if "аккаунт" in str(exc).lower():
            error_code = "notification_account_unavailable"
        return RedirectResponse(url=f"/settings?error={error_code}", status_code=303)
    except Exception as exc:
        logger.exception("Notification bot deletion failed")
        if _wants_json(request):
            return JSONResponse({"error": str(exc)}, status_code=500)
        return RedirectResponse(url="/settings?error=notification_action_failed", status_code=303)

    if _wants_json(request):
        return JSONResponse({"deleted": True})
    return RedirectResponse(url="/settings?msg=notification_bot_deleted", status_code=303)


@router.post("/notifications/test")
async def test_notification(request: Request):
    notifier = deps.get_notifier(request)
    admin_chat_id = notifier.admin_chat_id if notifier else None

    bot = await _notification_service(request).get_status()
    if not admin_chat_id and bot:
        admin_chat_id = bot.tg_user_id

    if not admin_chat_id and not bot:
        return RedirectResponse(url="/settings?error=notification_test_failed", status_code=303)

    target_svc = deps.get_notification_target_service(request)

    if bot:
        try:
            # Send /start from the notification account to the bot so the bot
            # can initiate a conversation back. This assumes the notification
            # account IS the admin user receiving push notifications.
            async with target_svc.use_client() as (client, _):
                await asyncio.wait_for(
                    client.send_message(bot.bot_username, "/start"), timeout=30.0
                )
        except Exception as exc:
            logger.warning("Could not send /start to @%s: %s", bot.bot_username, exc)

    msg = "✅ Тест уведомлений: соединение установлено"
    ok = await Notifier(
        target_svc, admin_chat_id, deps.get_notification_bundle(request)
    ).notify(msg)
    if ok:
        return RedirectResponse(url="/settings?msg=notification_test_sent", status_code=303)
    return RedirectResponse(url="/settings?error=notification_test_failed", status_code=303)


@router.post("/{account_id}/toggle")
async def toggle_account(request: Request, account_id: int):
    await deps.account_service(request).toggle(account_id)
    return RedirectResponse(url="/settings?msg=account_toggled", status_code=303)


@router.post("/{account_id}/delete")
async def delete_account(request: Request, account_id: int):
    await deps.account_service(request).delete(account_id)
    return RedirectResponse(url="/settings?msg=account_deleted", status_code=303)
