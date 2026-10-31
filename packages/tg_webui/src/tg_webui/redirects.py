from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import urlencode

from fastapi.responses import RedirectResponse


def redirect_see_other(path: str, params: Mapping[str, object | None] | None = None) -> RedirectResponse:
    query = urlencode({key: value for key, value in (params or {}).items() if value not in (None, "")})
    if query:
        separator = "&" if "?" in path else "?"
        path = f"{path}{separator}{query}"
    return RedirectResponse(url=path, status_code=303)


def dialogs_redirect(phone: str | None = None, **params: object | None) -> RedirectResponse:
    return redirect_see_other("/dialogs/", {"phone": phone, **params})
