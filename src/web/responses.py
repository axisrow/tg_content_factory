"""Shared response helpers for web route modules."""

from __future__ import annotations

from typing import Any, Mapping
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from fastapi.responses import JSONResponse, RedirectResponse


def see_other(url: str) -> RedirectResponse:
    """Return a POST/redirect/GET response."""
    return RedirectResponse(url=url, status_code=303)


def flash_redirect(
    path: str,
    *,
    msg: str | None = None,
    error: str | None = None,
    extra: Mapping[str, object | None] | None = None,
    fragment: str | None = None,
) -> RedirectResponse:
    """Redirect with the app's query-string flash convention."""
    if msg is not None and error is not None:
        raise ValueError("flash_redirect accepts either msg or error, not both")

    parts = urlsplit(path)
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    if msg is not None:
        query["msg"] = msg
    if error is not None:
        query["error"] = error
    if extra:
        for key, value in extra.items():
            if value is not None:
                query[key] = str(value)

    target = urlunsplit(
        (
            parts.scheme,
            parts.netloc,
            parts.path,
            urlencode(query),
            fragment if fragment is not None else parts.fragment,
        )
    )
    return see_other(target)


def json_response(content: Any, *, status_code: int = 200) -> JSONResponse:
    return JSONResponse(content=content, status_code=status_code)


def json_ok(**payload: Any) -> JSONResponse:
    return json_response({"ok": True, **payload})


def json_error(error: str, *, status_code: int = 400, **payload: Any) -> JSONResponse:
    return json_response({"ok": False, "error": error, **payload}, status_code=status_code)
