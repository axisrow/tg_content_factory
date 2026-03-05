from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse

router = APIRouter()


@router.get("/")
async def dashboard(request: Request):
    auth = request.app.state.auth
    if not auth.is_configured:
        return RedirectResponse(url="/settings", status_code=303)

    db = request.app.state.db
    stats = await db.get_stats()
    scheduler = request.app.state.scheduler
    return request.app.state.templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "stats": stats,
            "scheduler_running": scheduler.is_running,
            "last_run": scheduler.last_run,
            "last_stats": scheduler.last_stats,
            "accounts_connected": len(request.app.state.pool.clients),
        },
    )
