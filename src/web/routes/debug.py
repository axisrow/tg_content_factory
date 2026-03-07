from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from src.web import deps

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def debug_page(request: Request):
    records = deps.get_log_buffer(request).get_records()
    return deps.get_templates(request).TemplateResponse(
        request, "debug.html", {"records": records}
    )


@router.get("/logs", response_class=HTMLResponse)
async def debug_logs_partial(request: Request):
    records = deps.get_log_buffer(request).get_records()
    return deps.get_templates(request).TemplateResponse(
        request, "_debug_logs.html", {"records": records}
    )
