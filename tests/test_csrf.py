"""Tests for CSRF middleware."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from starlette.responses import Response

from src.web.csrf import (
    OriginCSRFMiddleware,
    _forwarded_values,
    _is_same_origin,
    _normalize_port,
    _split_header_value,
    is_same_origin_url,
    is_secure_request,
)

# === _normalize_port tests ===


def test_normalize_port_explicit():
    """Explicit port is returned as-is."""
    assert _normalize_port("http", 8080) == 8080
    assert _normalize_port("https", 8443) == 8443


def test_normalize_port_implicit_http():
    """None port defaults to 80 for http."""
    assert _normalize_port("http", None) == 80


def test_normalize_port_implicit_https():
    """None port defaults to 443 for https."""
    assert _normalize_port("https", None) == 443


# === _split_header_value tests ===


def test_split_header_value_simple():
    """Simple value is returned as-is."""
    assert _split_header_value("example.com") == "example.com"


def test_split_header_value_with_comma():
    """Comma-separated value returns first part."""
    assert _split_header_value("example.com, other.com") == "example.com"


def test_split_header_value_with_spaces():
    """Whitespace is stripped."""
    assert _split_header_value("  example.com  ") == "example.com"


def test_split_header_value_empty():
    """Empty string returns None."""
    assert _split_header_value("") is None
    assert _split_header_value("   ") is None


def test_split_header_value_none():
    """None returns None."""
    assert _split_header_value(None) is None


# === _forwarded_values tests ===


def make_mock_request(
    headers: dict | None = None,
    scheme: str = "http",
    netloc: str = "localhost:8000",
):
    """Create a mock request with specified headers."""
    request = MagicMock()
    request.headers = headers or {}
    request.url = MagicMock()
    request.url.scheme = scheme
    request.url.netloc = netloc
    return request


def test_forwarded_values_no_headers():
    """Returns request defaults when no forwarded headers."""
    request = make_mock_request(scheme="http", netloc="localhost:8000")
    scheme, host, port = _forwarded_values(request)
    assert scheme == "http"
    assert host == "localhost"
    assert port == 8000


def test_forwarded_values_x_forwarded_proto():
    """Uses X-Forwarded-Proto header."""
    request = make_mock_request(headers={"x-forwarded-proto": "https"})
    scheme, host, port = _forwarded_values(request)
    assert scheme == "https"


def test_forwarded_values_x_forwarded_host():
    """Uses X-Forwarded-Host header."""
    request = make_mock_request(headers={"x-forwarded-host": "example.com"})
    scheme, host, port = _forwarded_values(request)
    assert host == "example.com"


def test_forwarded_values_host_with_port():
    """Parses port from Host header."""
    request = make_mock_request(headers={"host": "example.com:9000"})
    scheme, host, port = _forwarded_values(request)
    assert host == "example.com"
    assert port == 9000


def test_forwarded_values_forwarded_header():
    """Parses Forwarded header."""
    request = make_mock_request(headers={"forwarded": 'proto=https;host="api.example.com"'})
    scheme, host, port = _forwarded_values(request)
    assert scheme == "https"
    assert host == "api.example.com"


def test_forwarded_values_ipv6_host():
    """Handles IPv6 host format."""
    request = make_mock_request(headers={"host": "[::1]:8000"})
    scheme, host, port = _forwarded_values(request)
    assert host == "::1"
    assert port == 8000


def test_forwarded_values_ipv6_no_port():
    """Handles IPv6 host without port."""
    request = make_mock_request(headers={"host": "[::1]"})
    scheme, host, port = _forwarded_values(request)
    assert host == "::1"
    # Port normalized to 80 for http
    assert port == 80


# === is_secure_request tests ===


def test_is_secure_request_https():
    """Returns True for HTTPS."""
    request = make_mock_request(headers={"x-forwarded-proto": "https"})
    assert is_secure_request(request) is True


def test_is_secure_request_http():
    """Returns False for HTTP."""
    request = make_mock_request(scheme="http")
    assert is_secure_request(request) is False


# === _is_same_origin tests ===


def test_is_same_origin_matching():
    """Matching origins return True."""
    request = make_mock_request(scheme="https", netloc="example.com:443")
    assert _is_same_origin("https://example.com", request) is True


def test_is_same_origin_different_scheme():
    """Different schemes return False."""
    request = make_mock_request(scheme="https", netloc="example.com")
    assert _is_same_origin("http://example.com", request) is False


def test_is_same_origin_different_host():
    """Different hosts return False."""
    request = make_mock_request(scheme="https", netloc="example.com")
    assert _is_same_origin("https://other.com", request) is False


def test_is_same_origin_different_port():
    """Different ports return False."""
    request = make_mock_request(scheme="http", netloc="example.com:8000")
    assert _is_same_origin("http://example.com:9000", request) is False


def test_is_same_origin_invalid_url():
    """Invalid URLs return False."""
    request = make_mock_request()
    assert _is_same_origin("not-a-url", request) is False


def test_is_same_origin_file_scheme():
    """file:// scheme returns False."""
    request = make_mock_request()
    assert _is_same_origin("file:///path", request) is False


# === is_same_origin_url tests ===


def test_is_same_origin_url_alias():
    """is_same_origin_url is an alias for _is_same_origin."""
    request = make_mock_request(scheme="https", netloc="example.com")
    assert is_same_origin_url("https://example.com", request) is True


# === OriginCSRFMiddleware tests ===


@pytest.mark.asyncio
async def test_csrf_safe_method_get():
    """GET requests pass through."""
    request = MagicMock()
    request.method = "GET"
    request.url.path = "/api/test"
    request.headers = {}
    request.cookies = {}

    call_next = AsyncMock(return_value=Response("OK", status_code=200))

    middleware = OriginCSRFMiddleware(None)
    response = await middleware.dispatch(request, call_next)

    assert response.status_code == 200
    call_next.assert_called_once()


@pytest.mark.asyncio
async def test_csrf_safe_method_head():
    """HEAD requests pass through."""
    request = MagicMock()
    request.method = "HEAD"
    request.url.path = "/api/test"
    request.headers = {}
    request.cookies = {}

    call_next = AsyncMock(return_value=Response("OK", status_code=200))

    middleware = OriginCSRFMiddleware(None)
    response = await middleware.dispatch(request, call_next)

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_csrf_exempt_path_login():
    """Login path is exempt from CSRF."""
    request = MagicMock()
    request.method = "POST"
    request.url.path = "/login"
    request.headers = {}
    request.cookies = {}

    call_next = AsyncMock(return_value=Response("OK", status_code=200))

    middleware = OriginCSRFMiddleware(None)
    response = await middleware.dispatch(request, call_next)

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_csrf_exempt_path_static():
    """Static paths are exempt from CSRF."""
    request = MagicMock()
    request.method = "POST"
    request.url.path = "/static/app.js"
    request.headers = {}
    request.cookies = {}

    call_next = AsyncMock(return_value=Response("OK", status_code=200))

    middleware = OriginCSRFMiddleware(None)
    response = await middleware.dispatch(request, call_next)

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_csrf_post_with_valid_origin():
    """POST with valid Origin passes."""
    request = make_mock_request(scheme="http", netloc="localhost:8000")
    request.method = "POST"
    request.url.path = "/api/test"
    request.headers = {"origin": "http://localhost:8000"}
    request.cookies = {}

    call_next = AsyncMock(return_value=Response("OK", status_code=200))

    middleware = OriginCSRFMiddleware(None)
    response = await middleware.dispatch(request, call_next)

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_csrf_post_with_invalid_origin():
    """POST with invalid Origin is rejected."""
    request = make_mock_request(scheme="http", netloc="localhost:8000")
    request.method = "POST"
    request.url.path = "/api/test"
    request.headers = {"origin": "https://evil.com"}
    request.cookies = {}

    call_next = AsyncMock(return_value=Response("OK", status_code=200))

    middleware = OriginCSRFMiddleware(None)
    response = await middleware.dispatch(request, call_next)

    assert response.status_code == 403
    assert b"CSRF" in response.body


@pytest.mark.asyncio
async def test_csrf_post_with_null_origin():
    """POST with 'null' Origin is rejected."""
    request = MagicMock()
    request.method = "POST"
    request.url.path = "/api/test"
    request.headers = {"origin": "null"}
    request.cookies = {}

    call_next = AsyncMock(return_value=Response("OK", status_code=200))

    middleware = OriginCSRFMiddleware(None)
    response = await middleware.dispatch(request, call_next)

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_csrf_post_with_valid_referer():
    """POST with valid Referer passes."""
    request = make_mock_request(scheme="http", netloc="localhost:8000")
    request.method = "POST"
    request.url.path = "/api/test"
    request.headers = {"referer": "http://localhost:8000/page"}
    request.cookies = {}

    call_next = AsyncMock(return_value=Response("OK", status_code=200))

    middleware = OriginCSRFMiddleware(None)
    response = await middleware.dispatch(request, call_next)

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_csrf_post_with_invalid_referer():
    """POST with invalid Referer is rejected."""
    request = make_mock_request(scheme="http", netloc="localhost:8000")
    request.method = "POST"
    request.url.path = "/api/test"
    request.headers = {"referer": "https://evil.com/page"}
    request.cookies = {}

    call_next = AsyncMock(return_value=Response("OK", status_code=200))

    middleware = OriginCSRFMiddleware(None)
    response = await middleware.dispatch(request, call_next)

    assert response.status_code == 403


@pytest.mark.asyncio
async def test_csrf_post_no_origin_no_referer_no_cookie():
    """POST without Origin/Referer but no session cookie passes."""
    request = MagicMock()
    request.method = "POST"
    request.url.path = "/api/test"
    request.headers = {}
    request.cookies = {}

    call_next = AsyncMock(return_value=Response("OK", status_code=200))

    middleware = OriginCSRFMiddleware(None)
    response = await middleware.dispatch(request, call_next)

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_csrf_post_no_origin_with_session_cookie():
    """POST without Origin/Referer but with session cookie is rejected."""
    from src.web.session import COOKIE_NAME

    request = MagicMock()
    request.method = "POST"
    request.url.path = "/api/test"
    request.headers = {}
    request.cookies = {COOKIE_NAME: "some-session-token"}

    call_next = AsyncMock(return_value=Response("OK", status_code=200))

    middleware = OriginCSRFMiddleware(None)
    response = await middleware.dispatch(request, call_next)

    assert response.status_code == 403
    assert b"missing Origin" in response.body
