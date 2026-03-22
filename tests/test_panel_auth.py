"""Tests for panel_auth module."""
from __future__ import annotations

from unittest.mock import MagicMock

from src.web.panel_auth import (
    get_cookie_user,
    get_session_secret,
    is_public_path,
    login_redirect_url,
    sanitize_next,
    set_session_cookie,
)


def test_get_session_secret():
    """Test get_session_secret returns secret from app state."""
    request = MagicMock()
    request.app.state.session_secret = "test_secret"

    result = get_session_secret(request)
    assert result == "test_secret"


def test_get_session_secret_none():
    """Test get_session_secret returns None when not set."""
    request = MagicMock()
    delattr(request.app.state, "session_secret")

    result = get_session_secret(request)
    assert result is None


def test_get_cookie_user_no_secret():
    """Test get_cookie_user returns None without secret."""
    request = MagicMock()
    request.cookies = {}
    delattr(request.app.state, "session_secret")

    result = get_cookie_user(request)
    assert result is None


def test_get_cookie_user_no_cookie():
    """Test get_cookie_user returns None without cookie."""
    request = MagicMock()
    request.app.state.session_secret = "test_secret"
    request.cookies = {}

    result = get_cookie_user(request)
    assert result is None


def test_set_session_cookie_no_secret():
    """Test set_session_cookie does nothing without secret."""
    request = MagicMock()
    delattr(request.app.state, "session_secret")
    response = MagicMock()

    set_session_cookie(response, request)
    response.set_cookie.assert_not_called()


def test_is_public_path():
    """Test is_public_path identifies public paths."""
    assert is_public_path("/health") is True
    assert is_public_path("/logout") is True
    assert is_public_path("/login") is True
    assert is_public_path("/static/css/style.css") is True
    assert is_public_path("/dashboard") is False
    assert is_public_path("/settings") is False


def test_sanitize_next_empty():
    """Test sanitize_next with empty value."""
    assert sanitize_next(None) == "/"
    assert sanitize_next("") == "/"


def test_sanitize_next_double_slash():
    """Test sanitize_next blocks double slash."""
    assert sanitize_next("//evil.com") == "/"
    assert sanitize_next("/\\evil.com") == "/"
    assert sanitize_next("/%5Cevil.com") == "/"


def test_sanitize_next_login_path():
    """Test sanitize_next blocks login path."""
    assert sanitize_next("/login") == "/"


def test_sanitize_next_valid():
    """Test sanitize_next allows valid paths."""
    assert sanitize_next("/dashboard") == "/dashboard"
    assert sanitize_next("/settings?page=1") == "/settings?page=1"


def test_login_redirect_url():
    """Test login_redirect_url generates correct URL."""
    result = login_redirect_url("/dashboard")
    assert result == "/login?next=%2Fdashboard"
