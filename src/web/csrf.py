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


def _split_header_value(value: str | None) -> str | None:
    if not value:
        return None
    return value.split(",", 1)[0].strip() or None


def _forwarded_values(request: Request) -> tuple[str, str, int]:
    forwarded = _split_header_value(request.headers.get("forwarded"))
    proto: str | None = None
    host: str | None = None
    port: int | None = None

    if forwarded:
        for part in forwarded.split(";"):
            key, _, raw_value = part.strip().partition("=")
            if not raw_value:
                continue
            value = raw_value.strip().strip('"')
            if key.lower() == "proto" and value:
                proto = value
            elif key.lower() == "host" and value:
                host = value

    if not proto:
        proto = _split_header_value(request.headers.get("x-forwarded-proto"))
    if not host:
        host = _split_header_value(request.headers.get("x-forwarded-host"))
    if not host:
        host = request.headers.get("host") or request.url.netloc

    if host.startswith("["):
        end = host.find("]")
        hostname = host[1:end] if end != -1 else host
        remainder = host[end + 1 :] if end != -1 else ""
        if remainder.startswith(":") and remainder[1:].isdigit():
            port = int(remainder[1:])
    elif ":" in host and host.rsplit(":", 1)[1].isdigit():
        hostname, raw_port = host.rsplit(":", 1)
        port = int(raw_port)
    else:
        hostname = host

    scheme = proto or request.url.scheme
    return scheme, hostname, _normalize_port(scheme, port)


def is_secure_request(request: Request) -> bool:
    scheme, _, _ = _forwarded_values(request)
    return scheme == "https"


def _is_same_origin(origin_or_referer: str, request: Request) -> bool:
    parsed = urlparse(origin_or_referer)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return False

    source_port = _normalize_port(parsed.scheme, parsed.port)
    target_scheme, target_host, target_port = _forwarded_values(request)
    return (
        parsed.scheme == target_scheme
        and parsed.hostname == target_host
        and source_port == target_port
    )


def is_same_origin_url(origin_or_referer: str, request: Request) -> bool:
    return _is_same_origin(origin_or_referer, request)


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
