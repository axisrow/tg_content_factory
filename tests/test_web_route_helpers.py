"""Tests for web route helpers and small module behaviors."""
from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

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


# --- shared web responses ---


def test_see_other_returns_303_redirect():
    from src.web.responses import see_other

    response = see_other("/settings")

    assert response.status_code == 303
    assert response.headers["location"] == "/settings"


def test_flash_redirect_adds_message_code():
    from src.web.responses import flash_redirect

    response = flash_redirect("/search-queries", msg="sq_added")

    assert response.status_code == 303
    assert response.headers["location"] == "/search-queries?msg=sq_added"


def test_flash_redirect_encodes_values_and_skips_none_extras():
    from src.web.responses import flash_redirect

    response = flash_redirect(
        "/pipelines?phone=%2B123",
        error="bad value",
        extra={"command_id": 42, "skip": None},
        fragment="details",
    )

    assert response.headers["location"] == "/pipelines?phone=%2B123&error=bad+value&command_id=42#details"


def test_flash_redirect_rejects_message_and_error_together():
    from src.web.responses import flash_redirect

    with pytest.raises(ValueError, match="either msg or error"):
        flash_redirect("/settings", msg="saved", error="invalid")


@pytest.mark.parametrize("target", ["https://evil.example/path", "//evil.example/path"])
def test_flash_redirect_rejects_absolute_targets(target):
    from src.web.responses import flash_redirect

    with pytest.raises(ValueError, match="relative path"):
        flash_redirect(target, msg="saved")


def test_json_response_helpers():
    from src.web.responses import json_error, json_ok, json_response

    plain = json_response({"items": []}, status_code=202)
    ok = json_ok(started=True)
    error = json_error("invalid", status_code=422, field="name")

    assert plain.status_code == 202
    assert json.loads(plain.body) == {"items": []}
    assert ok.status_code == 200
    assert json.loads(ok.body) == {"ok": True, "started": True}
    assert error.status_code == 422
    assert json.loads(error.body) == {"ok": False, "error": "invalid", "field": "name"}


def test_json_helpers_reject_reserved_payload_keys():
    from src.web.responses import json_error, json_ok

    with pytest.raises(ValueError, match="reserved key"):
        json_ok(**{"ok": False})

    with pytest.raises(ValueError, match="reserved key"):
        json_error("invalid", **{"ok": True})
