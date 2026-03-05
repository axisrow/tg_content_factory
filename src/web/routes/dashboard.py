from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

from src.web import deps

router = APIRouter()


@router.get("/")
async def dashboard(request: Request):
    auth = deps.get_auth(request)
    if not auth.is_configured:
        return RedirectResponse(url="/settings", status_code=303)

    db = deps.get_db(request)
    stats = await db.get_stats()
    scheduler = deps.get_scheduler(request)
    return deps.get_templates(request).TemplateResponse(
        request,
        "dashboard.html",
        {
            "stats": stats,
            "scheduler_running": scheduler.is_running,
            "last_run": scheduler.last_run,
            "last_stats": scheduler.last_stats,
            "accounts_connected": len(deps.get_pool(request).clients),
        },
    )
