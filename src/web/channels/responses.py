"""Response mapping for the channels web domain."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse

from src.web import deps
from src.web.responses import flash_redirect

CHANNELS_PATH = "/channels"


@dataclass(frozen=True)
class ChannelsRedirect:
    msg: str | None = None
    error: str | None = None
    extra: dict[str, object] | None = None


@dataclass(frozen=True)
class ChannelsTemplate:
    name: str
    context: dict[str, Any]


@dataclass(frozen=True)
class ChannelsJson:
    content: Any


ChannelsResult = ChannelsRedirect | ChannelsTemplate | ChannelsJson | Any


def channels_response(request: Request, result: ChannelsResult):
    if isinstance(result, ChannelsRedirect):
        return flash_redirect(CHANNELS_PATH, msg=result.msg, error=result.error, extra=result.extra)
    if isinstance(result, ChannelsTemplate):
        return deps.get_templates(request).TemplateResponse(request, result.name, result.context)
    if isinstance(result, ChannelsJson):
        return JSONResponse(content=result.content)
    return result
