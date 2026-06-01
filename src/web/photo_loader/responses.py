from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

from fastapi import Request
from fastapi.responses import RedirectResponse, Response

from src.web import deps


@dataclass(frozen=True)
class PhotoLoaderRedirect:
    phone: str
    code: str
    error: bool = False
    command_id: int | None = None


@dataclass(frozen=True)
class PhotoLoaderTemplate:
    name: str
    context: dict[str, Any]


def photo_loader_redirect_response(result: PhotoLoaderRedirect) -> RedirectResponse:
    key = "error" if result.error else "msg"
    suffix = f"&command_id={result.command_id}" if result.command_id is not None else ""
    return RedirectResponse(
        url=f"/dialogs/photos?phone={quote(result.phone, safe='')}&{key}={result.code}{suffix}",
        status_code=303,
    )


PhotoLoaderResult = PhotoLoaderRedirect | PhotoLoaderTemplate


def photo_loader_response(request: Request, result: PhotoLoaderResult) -> Response:
    if isinstance(result, PhotoLoaderTemplate):
        return deps.get_templates(request).TemplateResponse(request, result.name, result.context)
    return photo_loader_redirect_response(result)
