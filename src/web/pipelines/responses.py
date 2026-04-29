from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse, Response, StreamingResponse

from src.utils.json import safe_json_dumps
from src.web import deps


@dataclass(frozen=True)
class PipelineRedirect:
    code: str
    error: bool = False
    phone: str | None = None


@dataclass(frozen=True)
class PipelineTemplate:
    name: str
    context: dict[str, Any]


@dataclass(frozen=True)
class PipelineJson:
    content: Any
    status_code: int = 200


@dataclass(frozen=True)
class PipelineStream:
    iterator: Any
    media_type: str = "text/event-stream"


@dataclass(frozen=True)
class PipelineFile:
    content: Any
    filename: str
    media_type: str = "application/json"


PipelineResult = PipelineRedirect | PipelineTemplate | PipelineJson | PipelineStream | PipelineFile | Response | Any


def pipeline_redirect_response(result: PipelineRedirect) -> RedirectResponse:
    key = "error" if result.error else "msg"
    suffix = f"&phone={quote(result.phone, safe='')}" if result.phone else ""
    return RedirectResponse(url=f"/pipelines?{key}={quote(result.code, safe='')}{suffix}", status_code=303)


def pipeline_response(request: Request, result: PipelineResult):
    if isinstance(result, PipelineRedirect):
        return pipeline_redirect_response(result)
    if isinstance(result, PipelineTemplate):
        return deps.get_templates(request).TemplateResponse(request, result.name, result.context)
    if isinstance(result, PipelineJson):
        return JSONResponse(content=result.content, status_code=result.status_code)
    if isinstance(result, PipelineStream):
        return StreamingResponse(result.iterator, media_type=result.media_type)
    if isinstance(result, PipelineFile):
        return Response(
            content=safe_json_dumps(result.content, ensure_ascii=False, indent=2),
            media_type=result.media_type,
            headers={"Content-Disposition": f'attachment; filename="{result.filename}"'},
        )
    return result
