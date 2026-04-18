from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from src.web.csrf import (
    OriginCSRFMiddleware,
    _forwarded_values,
    _is_same_origin,
    _normalize_port,
    _split_header_value,
    is_same_origin_url,
    is_secure_request,
)

# --- _normalize_port ---


def test_normalize_port_explicit():
    assert _normalize_port("http", 8080) == 8080


def test_normalize_port_none_http():
    assert _normalize_port("http", None) == 80


def test_normalize_port_none_https():
    assert _normalize_port("https", None) == 443


# --- _split_header_value ---


def test_split_header_none():
    assert _split_header_value(None) is None


def test_split_header_empty():
    assert _split_header_value("") is None


def test_split_header_single():
    assert _split_header_value("example.com") == "example.com"


def test_split_header_multiple():
    assert _split_header_value("first.com, second.com") == "first.com"


# --- _forwarded_values ---


def _make_request(*, scheme="http", host="localhost:80", headers=None):
    request = MagicMock()
    url = MagicMock()
    url.scheme = scheme
    url.netloc = host
    request.url = url
    request.headers = headers or {}
    return request


def test_forwarded_values_no_headers():
    request = _make_request(scheme="http", host="localhost")
    request.headers = {}
    scheme, hostname, port = _forwarded_values(request)
    assert scheme == "http"
    assert hostname == "localhost"


def test_forwarded_values_x_forwarded_proto():
    request = _make_request(
        scheme="http",
        host="localhost",
        headers={"x-forwarded-proto": "https"},
    )
    scheme, hostname, port = _forwarded_values(request)
    assert scheme == "https"
    assert port == 443


def test_forwarded_values_x_forwarded_host():
    request = _make_request(
        scheme="http",
        host="localhost",
        headers={"x-forwarded-host": "api.example.com:8443"},
    )
    scheme, hostname, port = _forwarded_values(request)
    assert hostname == "api.example.com"
    assert port == 8443


def test_forwarded_values_forwarded_header():
    request = _make_request(
        scheme="http",
        host="localhost",
        headers={"forwarded": 'proto=https;host="app.example.com"'},
    )
    scheme, hostname, port = _forwarded_values(request)
    assert scheme == "https"
    assert hostname == "app.example.com"
    assert port == 443


def test_forwarded_values_ipv6_host():
    request = _make_request(
        scheme="http",
        host="[::1]:8080",
        headers={"host": "[::1]:8080"},
    )
    scheme, hostname, port = _forwarded_values(request)
    assert hostname == "::1"
    assert port == 8080


def test_forwarded_values_host_no_port():
    request = _make_request(
        scheme="http",
        host="example.com",
        headers={},
    )
    scheme, hostname, port = _forwarded_values(request)
    assert hostname == "example.com"
    assert port == 80


# --- is_secure_request ---


def test_is_secure_request_https():
    request = _make_request(scheme="https", host="localhost", headers={})
    assert is_secure_request(request) is True


def test_is_secure_request_http():
    request = _make_request(scheme="http", host="localhost", headers={})
    assert is_secure_request(request) is False


# --- _is_same_origin / is_same_origin_url ---


def test_same_origin_match():
    request = _make_request(scheme="http", host="localhost:80", headers={})
    assert _is_same_origin("http://localhost/path", request) is True


def test_same_origin_mismatch_port():
    request = _make_request(scheme="http", host="localhost:80", headers={})
    assert _is_same_origin("http://localhost:8080/path", request) is False


def test_same_origin_mismatch_scheme():
    request = _make_request(scheme="http", host="localhost:80", headers={})
    assert _is_same_origin("https://localhost/path", request) is False


def test_same_origin_mismatch_host():
    request = _make_request(scheme="http", host="localhost:80", headers={})
    assert _is_same_origin("http://evil.com/path", request) is False


def test_same_origin_invalid_scheme():
    request = _make_request(scheme="http", host="localhost:80", headers={})
    assert _is_same_origin("ftp://localhost/path", request) is False


def test_is_same_origin_url_wrapper():
    request = _make_request(scheme="http", host="localhost:80", headers={})
    assert is_same_origin_url("http://localhost/", request) is True
    assert is_same_origin_url("http://evil.com/", request) is False


# --- OriginCSRFMiddleware ---


async def test_csrf_safe_method_passes():
    app = MagicMock()
    middleware = OriginCSRFMiddleware(app)

    request = MagicMock()
    request.method = "GET"
    request.url.path = "/channels"
    request.headers = {}

    call_next = AsyncMock(return_value=MagicMock(status_code=200))
    await middleware.dispatch(request, call_next)
    call_next.assert_awaited_once()


async def test_csrf_exempt_path():
    app = MagicMock()
    middleware = OriginCSRFMiddleware(app)

    request = MagicMock()
    request.method = "POST"
    request.url.path = "/login"
    request.headers = {}

    call_next = AsyncMock(return_value=MagicMock(status_code=200))
    await middleware.dispatch(request, call_next)
    call_next.assert_awaited_once()


async def test_csrf_static_exempt():
    app = MagicMock()
    middleware = OriginCSRFMiddleware(app)

    request = MagicMock()
    request.method = "POST"
    request.url.path = "/static/css/style.css"
    request.headers = {}

    call_next = AsyncMock(return_value=MagicMock(status_code=200))
    await middleware.dispatch(request, call_next)
    call_next.assert_awaited_once()


async def test_csrf_valid_origin_passes():
    app = MagicMock()
    middleware = OriginCSRFMiddleware(app)

    request = MagicMock()
    request.method = "POST"
    request.url.path = "/channels"
    request.url.scheme = "http"
    request.url.netloc = "localhost"
    request.headers = {"origin": "http://localhost"}
    request.cookies = {}

    call_next = AsyncMock(return_value=MagicMock(status_code=200))
    await middleware.dispatch(request, call_next)
    call_next.assert_awaited_once()


async def test_csrf_invalid_origin_blocked():
    app = MagicMock()
    middleware = OriginCSRFMiddleware(app)

    request = MagicMock()
    request.method = "POST"
    request.url.path = "/channels"
    request.url.scheme = "http"
    request.url.netloc = "localhost"
    request.headers = {"origin": "http://evil.com"}
    request.cookies = {}

    call_next = AsyncMock(return_value=MagicMock(status_code=200))
    response = await middleware.dispatch(request, call_next)
    assert response.status_code == 403
    call_next.assert_not_awaited()


async def test_csrf_null_origin_blocked():
    app = MagicMock()
    middleware = OriginCSRFMiddleware(app)

    request = MagicMock()
    request.method = "POST"
    request.url.path = "/channels"
    request.headers = {"origin": "null"}
    request.cookies = {}

    call_next = AsyncMock(return_value=MagicMock(status_code=200))
    response = await middleware.dispatch(request, call_next)
    assert response.status_code == 403


async def test_csrf_valid_referer_passes():
    app = MagicMock()
    middleware = OriginCSRFMiddleware(app)

    request = MagicMock()
    request.method = "POST"
    request.url.path = "/channels"
    request.url.scheme = "http"
    request.url.netloc = "localhost"
    request.headers = {"referer": "http://localhost/channels"}
    request.cookies = {}

    call_next = AsyncMock(return_value=MagicMock(status_code=200))
    await middleware.dispatch(request, call_next)
    call_next.assert_awaited_once()


async def test_csrf_invalid_referer_blocked():
    app = MagicMock()
    middleware = OriginCSRFMiddleware(app)

    request = MagicMock()
    request.method = "POST"
    request.url.path = "/channels"
    request.url.scheme = "http"
    request.url.netloc = "localhost"
    request.headers = {"referer": "http://evil.com/page"}
    request.cookies = {}

    call_next = AsyncMock(return_value=MagicMock(status_code=200))
    response = await middleware.dispatch(request, call_next)
    assert response.status_code == 403


async def test_csrf_no_origin_no_referer_no_cookie_passes():
    app = MagicMock()
    middleware = OriginCSRFMiddleware(app)

    request = MagicMock()
    request.method = "POST"
    request.url.path = "/channels"
    request.headers = {}
    request.cookies = {}

    call_next = AsyncMock(return_value=MagicMock(status_code=200))
    await middleware.dispatch(request, call_next)
    call_next.assert_awaited_once()


async def test_csrf_no_origin_referer_with_cookie_blocked():
    from src.web.session import COOKIE_NAME

    app = MagicMock()
    middleware = OriginCSRFMiddleware(app)

    request = MagicMock()
    request.method = "POST"
    request.url.path = "/channels"
    request.headers = {}
    request.cookies = {COOKIE_NAME: "some_token"}

    call_next = AsyncMock(return_value=MagicMock(status_code=200))
    response = await middleware.dispatch(request, call_next)
    assert response.status_code == 403
