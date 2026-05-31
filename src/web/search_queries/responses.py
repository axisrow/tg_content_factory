"""Response mapping for the search-queries web domain."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi import Request

from src.web import deps
from src.web.responses import flash_redirect

SEARCH_QUERIES_PATH = "/search-queries"


@dataclass(frozen=True)
class SearchQueryRedirect:
    msg: str | None = None
    error: str | None = None
    extra: dict[str, object] | None = None


@dataclass(frozen=True)
class SearchQueryTemplate:
    name: str
    context: dict[str, Any]


SearchQueryResult = SearchQueryRedirect | SearchQueryTemplate | Any


def search_query_response(request: Request, result: SearchQueryResult):
    if isinstance(result, SearchQueryRedirect):
        return flash_redirect(SEARCH_QUERIES_PATH, msg=result.msg, error=result.error, extra=result.extra)
    if isinstance(result, SearchQueryTemplate):
        return deps.get_templates(request).TemplateResponse(request, result.name, result.context)
    return result
