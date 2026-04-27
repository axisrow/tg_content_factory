"""Tests for web route helpers and small module behaviors."""
from __future__ import annotations

from unittest.mock import MagicMock

from src.web.routes import dialogs as dialogs_mod

# --- panel_auth behavior tests ---


def test_panel_auth_is_public_path():
    from src.web.panel_auth import is_public_path

    assert is_public_path("/health")
    assert is_public_path("/static/css/style.css")
    assert is_public_path("/logout")
    assert not is_public_path("/dashboard")
    assert not is_public_path("/admin")


def test_panel_auth_sanitize_next():
    from src.web.panel_auth import sanitize_next

    assert sanitize_next(None) == "/"
    assert sanitize_next("") == "/"
    assert sanitize_next("//evil.com") == "/"
    assert sanitize_next("/dashboard") == "/dashboard"
    assert sanitize_next("/settings?q=1") == "/settings?q=1"


def test_panel_auth_login_redirect_url():
    from src.web.panel_auth import login_redirect_url

    url = login_redirect_url("/settings")
    assert "next=" in url
    assert "settings" in url


# --- dialogs route module ---


def test_dialogs_router_has_routes():
    routes = [r.path for r in dialogs_mod.router.routes]
    assert any("/leave" in r for r in routes)


# --- web app behavior ---


def test_web_app_create_app_returns_asgi():
    from src.web.app import create_app

    app = create_app()
    assert hasattr(app, "router")


# --- collection queue behavior ---


async def test_collection_queue_status_enum():
    from src.collection_queue import CollectionTaskStatus

    assert CollectionTaskStatus.PENDING.value == "pending"
    assert CollectionTaskStatus.COMPLETED.value == "completed"


# --- redirect_target_from_request ---


def test_redirect_target_get_request():
    from src.web.panel_auth import redirect_target_from_request

    request = MagicMock()
    request.method = "GET"
    request.url = MagicMock()
    request.url.path = "/settings"
    request.url.query = "tab=general"
    result = redirect_target_from_request(request)
    assert result == "/settings?tab=general"


def test_redirect_target_post_no_referer():
    from src.web.panel_auth import redirect_target_from_request

    request = MagicMock()
    request.method = "POST"
    request.url = MagicMock()
    request.headers = {}
    result = redirect_target_from_request(request)
    assert result == "/"
