from __future__ import annotations

from urllib.parse import urlparse

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

_SAFE_METHODS = {"GET", "HEAD", "OPTIONS", "TRACE"}


def _normalize_port(scheme: str, port: int | None) -> int:
    if port is not None:
        return port
    if scheme == "https":
        return 443
    return 80


def _is_same_origin(origin_or_referer: str, request: Request) -> bool:
    parsed = urlparse(origin_or_referer)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return False

    source_port = _normalize_port(parsed.scheme, parsed.port)
    target_port = _normalize_port(request.url.scheme, request.url.port)
    return (
        parsed.scheme == request.url.scheme
        and parsed.hostname == request.url.hostname
        and source_port == target_port
    )


class OriginCSRFMiddleware(BaseHTTPMiddleware):
    """
    Lightweight CSRF protection: for unsafe methods, enforce same-origin
    when Origin or Referer is present.
    """

    async def dispatch(self, request: Request, call_next):
        if request.method.upper() in _SAFE_METHODS:
            return await call_next(request)

        origin = request.headers.get("origin")
        if origin:
            if origin == "null" or not _is_same_origin(origin, request):
                return Response("CSRF validation failed", status_code=403)
            return await call_next(request)

        referer = request.headers.get("referer")
        if referer and not _is_same_origin(referer, request):
            return Response("CSRF validation failed", status_code=403)

        # If neither Origin nor Referer is present, allow the request.
        # This matches Django's Origin-based CSRF approach — some HTTP clients
        # and older browsers omit these headers entirely.
        return await call_next(request)
