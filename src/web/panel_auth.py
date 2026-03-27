from __future__ import annotations

import os
from urllib.parse import unquote, urlencode, urlparse

from fastapi import Request
from starlette.responses import Response

from src.web.csrf import is_same_origin_url, is_secure_request
from src.web.session import COOKIE_MAX_AGE, COOKIE_NAME, create_session_token, verify_session_token

LOGIN_PATH = "/login"
PANEL_USERNAME = os.environ.get("WEB_USER", "admin")


def get_session_secret(request: Request) -> str | None:
    return getattr(request.app.state, "session_secret", None)


def get_cookie_user(request: Request) -> str | None:
    secret = get_session_secret(request)
    if not secret:
        return None

    cookie = request.cookies.get(COOKIE_NAME)
    if not cookie:
        return None

    user = verify_session_token(cookie, secret)
    if user == PANEL_USERNAME:
        return user
    return None


def set_session_cookie(response: Response, request: Request) -> None:
    secret = get_session_secret(request)
    if not secret:
        return

    token = create_session_token(PANEL_USERNAME, secret)
    response.set_cookie(
        COOKIE_NAME,
        token,
        max_age=COOKIE_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=is_secure_request(request),
    )


def is_public_path(path: str) -> bool:
    return path in ("/health", "/logout", LOGIN_PATH) or path.startswith("/static/")


def sanitize_next(next_value: str | None) -> str:
    if not next_value:
        return "/"
    if not next_value.startswith("/") or next_value.startswith("//"):
        return "/"
    # Block backslash-based open redirects (e.g. /\evil.com, /%5Cevil.com)
    if "\\" in unquote(next_value):
        return "/"
    if next_value == LOGIN_PATH:
        return "/"
    return next_value


def redirect_target_from_request(request: Request) -> str:
    if request.method.upper() in {"GET", "HEAD"}:
        target = request.url.path
        if request.url.query:
            target = f"{target}?{request.url.query}"
        return sanitize_next(target)

    referer = request.headers.get("referer")
    if referer and is_same_origin_url(referer, request):
        parsed = urlparse(referer)
        target = parsed.path or "/"
        if parsed.query:
            target = f"{target}?{parsed.query}"
        return sanitize_next(target)

    return "/"


def login_redirect_url(next_value: str) -> str:
    return f"{LOGIN_PATH}?{urlencode({'next': next_value})}"
