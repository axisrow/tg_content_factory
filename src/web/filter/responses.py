"""Response mapping for the filter web domain."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi import Request

from src.web import deps
from src.web.responses import flash_redirect

# Redirect targets used by the filter routes.
MANAGE_PATH = "/channels/filter/manage"
CHANNELS_PATH = "/channels"


@dataclass(frozen=True)
class FilterRedirect:
    path: str
    msg: str | None = None
    error: str | None = None
    extra: dict[str, object] | None = None


@dataclass(frozen=True)
class FilterTemplate:
    name: str
    context: dict[str, Any]


def manage_redirect(*, msg: str | None = None, error: str | None = None, **extra: object) -> FilterRedirect:
    return FilterRedirect(MANAGE_PATH, msg=msg, error=error, extra=extra or None)


def channels_redirect(*, msg: str | None = None, error: str | None = None, **extra: object) -> FilterRedirect:
    return FilterRedirect(CHANNELS_PATH, msg=msg, error=error, extra=extra or None)


FilterResult = FilterRedirect | FilterTemplate | Any


def filter_response(request: Request, result: FilterResult):
    if isinstance(result, FilterRedirect):
        return flash_redirect(result.path, msg=result.msg, error=result.error, extra=result.extra)
    if isinstance(result, FilterTemplate):
        return deps.get_templates(request).TemplateResponse(request, result.name, result.context)
    return result
