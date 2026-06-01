"""Response mapping for the scheduler web domain.

Handlers return the lightweight DTOs below; ``scheduler_response`` maps them to
concrete FastAPI responses so route functions no longer own response/URL
building (#654). The query-preserving redirect logic (status/page/limit) and the
two distinct template-render styles are intentionally preserved verbatim.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

from fastapi import Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.web import deps

# Only these inbound query params travel across a redirect — deliberately drops
# any inbound msg=/error=/command_id= so they don't stack (#457 round 3).
_PRESERVED_SCHEDULER_QUERY_KEYS = ("status", "page", "limit")


@dataclass(frozen=True)
class SchedulerRedirect:
    """Redirect back to /scheduler preserving the current filter/page."""

    msg: str | None = None
    error: str | None = None
    extra: dict[str, object] | None = None


@dataclass(frozen=True)
class SchedulerPage:
    """The main scheduler page — rendered via the raw Jinja env (see mapper)."""

    context: dict[str, Any]


@dataclass(frozen=True)
class SchedulerTemplate:
    """A scheduler fragment rendered through Starlette's TemplateResponse."""

    name: str
    context: dict[str, Any]


SchedulerResult = SchedulerRedirect | SchedulerPage | SchedulerTemplate | Any


def _redirect(request: Request, result: SchedulerRedirect) -> RedirectResponse:
    """Build the query-preserving redirect (#457 round 3).

    POST routes used to redirect to ``/scheduler?msg=...`` and drop
    ``?status=active&page=N&limit=M`` — the user ended up back on the default
    ``status=all`` view. Now every redirect keeps the tab the user was looking
    at when they clicked.
    """
    qp: dict[str, str] = {}
    for key in _PRESERVED_SCHEDULER_QUERY_KEYS:
        value = request.query_params.get(key)
        if value is not None and value != "":
            qp[key] = value
    if result.msg is not None:
        qp["msg"] = result.msg
    if result.error is not None:
        qp["error"] = result.error
    if result.extra:
        for k, v in result.extra.items():
            if v is not None:
                qp[k] = str(v)
    suffix = f"?{urlencode(qp)}" if qp else ""
    return RedirectResponse(url=f"/scheduler/{suffix}", status_code=303)


def scheduler_response(request: Request, result: SchedulerResult):
    if isinstance(result, SchedulerRedirect):
        return _redirect(request, result)
    if isinstance(result, SchedulerPage):
        templates = deps.get_templates(request)
        tpl = templates.env.get_template("scheduler.html")
        body = tpl.render({**result.context, "request": request})
        return HTMLResponse(body)
    if isinstance(result, SchedulerTemplate):
        return deps.get_templates(request).TemplateResponse(request, result.name, result.context)
    return result
