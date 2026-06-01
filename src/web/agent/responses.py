"""Response mapping for the agent web domain."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse

from src.web import deps


@dataclass(frozen=True)
class AgentTemplate:
    name: str
    context: dict[str, Any]


@dataclass(frozen=True)
class AgentJson:
    content: Any
    status_code: int = 200
    headers: dict[str, str] | None = None


@dataclass(frozen=True)
class AgentRedirect:
    url: str
    status_code: int = 303


@dataclass(frozen=True)
class AgentStream:
    iterator: Any
    media_type: str = "text/event-stream"


AgentResult = AgentTemplate | AgentJson | AgentRedirect | AgentStream | Any


def agent_response(request: Request, result: AgentResult):
    if isinstance(result, AgentTemplate):
        return deps.get_templates(request).TemplateResponse(request, result.name, result.context)
    if isinstance(result, AgentJson):
        return JSONResponse(result.content, status_code=result.status_code, headers=result.headers)
    if isinstance(result, AgentRedirect):
        return RedirectResponse(url=result.url, status_code=result.status_code)
    if isinstance(result, AgentStream):
        return StreamingResponse(result.iterator, media_type=result.media_type)
    return result
