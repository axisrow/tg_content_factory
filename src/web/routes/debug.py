from __future__ import annotations

import gc
import platform
import resource

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.web import deps

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def debug_page(request: Request):
    log_buffer = deps.get_log_buffer(request)
    records = log_buffer.get_records() if log_buffer is not None else []
    return deps.get_templates(request).TemplateResponse(request, "debug.html", {"records": records})


@router.get("/logs", response_class=HTMLResponse)
async def debug_logs_partial(request: Request):
    log_buffer = deps.get_log_buffer(request)
    records = log_buffer.get_records() if log_buffer is not None else []
    return deps.get_templates(request).TemplateResponse(
        request, "_debug_logs.html", {"records": records}
    )


@router.get("/timing", response_class=HTMLResponse)
async def debug_timing(request: Request):
    buf = deps.get_timing_buffer(request)
    records = sorted(buf.get_records(), key=lambda r: r["ms"], reverse=True) if buf else []
    return deps.get_templates(request).TemplateResponse(
        request, "debug_timing.html", {"records": records}
    )


@router.get("/timing/rows", response_class=HTMLResponse)
async def debug_timing_rows(request: Request):
    buf = deps.get_timing_buffer(request)
    records = sorted(buf.get_records(), key=lambda r: r["ms"], reverse=True) if buf else []
    return deps.get_templates(request).TemplateResponse(
        request, "_timing_rows.html", {"records": records}
    )


@router.get("/memory", response_class=JSONResponse)
async def debug_memory(request: Request):
    gc.collect()
    rusage = resource.getrusage(resource.RUSAGE_SELF)
    # macOS reports ru_maxrss in bytes, Linux in KB
    rss_bytes = rusage.ru_maxrss if platform.system() == "Darwin" else rusage.ru_maxrss * 1024

    pool = deps.get_pool(request)
    agent_manager = deps.get_agent_manager(request)
    collection_queue = getattr(request.app.state, "collection_queue", None)

    pool_info = {
        "connected_clients": len(pool.clients),
        "dialogs_cache_entries": len(pool._dialogs_cache),
        "active_leases": {k: len(v) for k, v in pool._active_leases.items()},
        "premium_flood_waits": len(pool._premium_flood_wait_until),
        "session_overrides": len(pool._session_overrides),
    }

    return {
        "rss_mb": round(rss_bytes / (1024 * 1024), 1),
        "gc_counts": gc.get_count(),
        "gc_stats": gc.get_stats(),
        "pool": pool_info,
        "agent_active_tasks": len(agent_manager._active_tasks) if agent_manager else 0,
        "collection_retried_tasks": len(collection_queue._retried_tasks) if collection_queue else 0,
    }
