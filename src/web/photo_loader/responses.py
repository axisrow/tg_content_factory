from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import quote

from fastapi.responses import RedirectResponse


@dataclass(frozen=True)
class PhotoLoaderRedirect:
    phone: str
    code: str
    error: bool = False
    command_id: int | None = None


def photo_loader_redirect_response(result: PhotoLoaderRedirect) -> RedirectResponse:
    key = "error" if result.error else "msg"
    suffix = f"&command_id={result.command_id}" if result.command_id is not None else ""
    return RedirectResponse(
        url=f"/dialogs/photos?phone={quote(result.phone, safe='')}&{key}={result.code}{suffix}",
        status_code=303,
    )

