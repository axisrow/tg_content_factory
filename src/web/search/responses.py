"""Response mapping for the search web domain."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse

from src.web import deps


@dataclass(frozen=True)
class SearchTemplate:
    name: str
    context: dict[str, Any]


@dataclass(frozen=True)
class SearchRedirect:
    url: str
    status_code: int = 303


@dataclass(frozen=True)
class SearchJson:
    content: Any
    status_code: int = 200


SearchResult = SearchTemplate | SearchRedirect | SearchJson | Any


def search_response(request: Request, result: SearchResult):
    if isinstance(result, SearchTemplate):
        return deps.get_templates(request).TemplateResponse(request, result.name, result.context)
    if isinstance(result, SearchRedirect):
        return RedirectResponse(url=result.url, status_code=result.status_code)
    if isinstance(result, SearchJson):
        return JSONResponse(result.content, status_code=result.status_code)
    return result
