from datetime import datetime, timezone

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.models import AccountSessionStatus
from src.telegram.flood_wait import is_blocking_flood_wait_until
from src.web import deps

router = APIRouter()


def _time_ago(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    now = datetime.now(tz=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = now - dt
    minutes = int(delta.total_seconds() / 60)
    if minutes < 1:
        return "только что"
    if minutes < 60:
        return f"{minutes} мин назад"
    hours = minutes // 60
    if hours < 24:
        return f"{hours} ч назад"
    return f"{hours // 24} д назад"


@router.get("/")
async def dashboard(request: Request):
    # Onboarding redirects MUST stay synchronous — they decide whether to render the
    # page at all (a skeleton 200 would swallow the redirect). The heavy stats
    # (get_stats does COUNT(*) on millions of messages) load lazily in the fragment (#756).
    auth = deps.get_auth(request)
    if not auth.is_configured:
        return RedirectResponse(url="/settings", status_code=303)

    db = deps.get_db(request)
    accounts = await db.get_account_summaries(active_only=False)
    if not accounts:
        return RedirectResponse(url="/settings?msg=no_accounts", status_code=303)

    return deps.get_templates(request).TemplateResponse(request, "dashboard.html", {})


@router.get("/fragments/overview", response_class=HTMLResponse)
async def fragment_overview(request: Request):
    db = deps.get_db(request)
    accounts = await db.get_account_summaries(active_only=False)
    stats = await db.get_stats()
    scheduler = deps.get_scheduler(request)

    now = datetime.now(tz=timezone.utc)
    flood_wait_count = 0
    all_connected_flooded = True
    connected_count = len(deps.get_pool(request).clients)
    for a in accounts:
        if a.flood_wait_until:
            until = a.flood_wait_until
            if until.tzinfo is None:
                until = until.replace(tzinfo=timezone.utc)
            if is_blocking_flood_wait_until(until, now=now):
                flood_wait_count += 1
        if (
            a.is_active
            and a.session_status == AccountSessionStatus.OK
            and a.phone in deps.get_pool(request).clients
        ):
            until = a.flood_wait_until
            if until is None:
                all_connected_flooded = False
            else:
                if until.tzinfo is None:
                    until = until.replace(tzinfo=timezone.utc)
                if not is_blocking_flood_wait_until(until, now=now):
                    all_connected_flooded = False
    if connected_count == 0:
        all_connected_flooded = False

    last_task = await db.repos.tasks.get_last_completed_collect_task()
    active_tasks = await db.repos.tasks.count_collection_tasks("active")
    calendar_stats = await db.repos.generation_runs.get_calendar_stats()
    all_pipelines = await db.repos.content_pipelines.get_all()
    active_pipelines = sum(1 for p in all_pipelines if p.is_active)

    return deps.get_templates(request).TemplateResponse(
        request,
        "dashboard/_overview.html",
        {
            "stats": stats,
            "scheduler_running": scheduler.is_running,
            "scheduler_interval": scheduler.interval_minutes,
            "accounts_connected": connected_count,
            "accounts_flood_wait": flood_wait_count,
            "collector_attention": all_connected_flooded,
            "last_collect_ago": _time_ago(last_task.completed_at if last_task else None),
            "active_tasks": active_tasks,
            "content_pending": calendar_stats["pending"],
            "content_approved": calendar_stats["approved"],
            "content_published": calendar_stats["published"],
            "pipelines_active": active_pipelines,
            "pipelines_total": len(all_pipelines),
        },
    )
