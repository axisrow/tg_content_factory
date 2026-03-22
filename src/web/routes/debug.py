from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

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
