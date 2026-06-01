"""Response mapping for the image-generation web domain."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse

from src.web import deps


@dataclass(frozen=True)
class ImagesTemplate:
    name: str
    context: dict[str, Any]


@dataclass(frozen=True)
class ImagesJson:
    content: Any
    status_code: int = 200


ImagesResult = ImagesTemplate | ImagesJson | Any


def images_response(request: Request, result: ImagesResult):
    if isinstance(result, ImagesTemplate):
        return deps.get_templates(request).TemplateResponse(request, result.name, result.context)
    if isinstance(result, ImagesJson):
        return JSONResponse(content=result.content, status_code=result.status_code)
    return result
