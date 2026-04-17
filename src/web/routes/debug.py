from __future__ import annotations

import asyncio
import gc
import platform
import re
import resource
from collections import deque
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.cli.runtime import APP_LOG_PATH
from src.web import deps

router = APIRouter()

_LOG_RE = re.compile(
    r"^(?P<time>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+"
    r"\[(?P<level>\w+)]\s+"
    r"(?P<logger>\S+?):\s+"
    r"(?P<message>.*)$",
)


def _read_log_tail(path: Path | None = None, max_lines: int = 500) -> list[dict]:
    if path is None:
        path = APP_LOG_PATH
    if not path.exists():
        return []
    try:
        lines: deque[str] = deque(maxlen=max_lines)
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                lines.append(line)
    except OSError:
        return []
    records: list[dict] = []
    for line in lines:
        m = _LOG_RE.match(line.rstrip())
        if m:
            records.append(m.groupdict())
        elif records:
            records[-1]["message"] += "\n" + line.rstrip()
    return records


@router.get("/", response_class=HTMLResponse)
async def debug_page(request: Request):
    loop = asyncio.get_running_loop()
    records = await loop.run_in_executor(None, _read_log_tail)
    return deps.get_templates(request).TemplateResponse(request, "debug.html", {"records": records})


@router.get("/logs", response_class=HTMLResponse)
async def debug_logs_partial(request: Request):
    loop = asyncio.get_running_loop()
    records = await loop.run_in_executor(None, _read_log_tail)
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

    runtime_mode = getattr(request.app.state, "runtime_mode", "web")
    pool_counters_source = "live"
    dialogs_cache = getattr(pool, "_dialogs_cache", {})
    active_leases = getattr(pool, "_active_leases", {})
    premium_flood_waits = getattr(pool, "_premium_flood_wait_until", {})
    session_overrides = getattr(pool, "_session_overrides", {})
    dialogs_cache_entries = len(dialogs_cache) if hasattr(dialogs_cache, "__len__") else 0
    active_leases_info = (
        {k: len(v) for k, v in active_leases.items()} if isinstance(active_leases, dict) else {}
    )
    premium_flood_count = (
        len(premium_flood_waits) if hasattr(premium_flood_waits, "__len__") else 0
    )
    session_overrides_count = (
        len(session_overrides) if hasattr(session_overrides, "__len__") else 0
    )
    if runtime_mode != "worker":
        snap = await deps.get_db(request).repos.runtime_snapshots.get_snapshot("pool_counters")
        pool_counters_source = "snapshot" if snap is not None else "empty"
        if snap is not None:
            payload = snap.payload or {}
            dialogs_cache_entries = int(payload.get("dialogs_cache_entries") or 0)
            active_leases_info = payload.get("active_leases") or {}
            premium_flood_count = int(payload.get("premium_flood_waits") or 0)
            session_overrides_count = int(payload.get("session_overrides") or 0)

    pool_info = {
        "connected_clients": len(pool.clients),
        "dialogs_cache_entries": dialogs_cache_entries,
        "active_leases": active_leases_info,
        "premium_flood_waits": premium_flood_count,
        "session_overrides": session_overrides_count,
        "source": pool_counters_source,
    }

    return {
        "rss_mb": round(rss_bytes / (1024 * 1024), 1),
        "gc_counts": gc.get_count(),
        "gc_stats": gc.get_stats(),
        "pool": pool_info,
        "runtime_mode": runtime_mode,
        "agent_active_tasks": len(agent_manager._active_tasks) if agent_manager else 0,
        "collection_retried_tasks": len(collection_queue._retried_tasks) if collection_queue else 0,
    }
