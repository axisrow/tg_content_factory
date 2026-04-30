from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from src.agent.zai_errors import format_provider_error, format_zai_api_error


def _exc_with_body(body: dict | str) -> Exception:
    """Mimic langchain-openai BadRequestError-like exceptions: ``body`` attr."""
    exc = Exception(json.dumps(body) if isinstance(body, dict) else body)
    exc.body = body  # type: ignore[attr-defined]
    return exc


def _exc_with_response(body: dict) -> Exception:
    """Mimic exceptions exposing only ``response.json()``."""
    response = SimpleNamespace(json=lambda: body, text=lambda: json.dumps(body))
    exc = Exception(f"Error code: 429 - {body}")
    exc.response = response  # type: ignore[attr-defined]
    return exc


def test_format_zai_1113_returns_balance_message():
    exc = _exc_with_body(
        {"error": {"code": "1113", "message": "Insufficient balance or no resource package."}}
    )
    msg = format_zai_api_error(exc)
    assert msg is not None
    assert "1113" in msg
    assert "coding/paas/v4" in msg


def test_format_zai_1309_plan_expired():
    exc = _exc_with_body(
        {"error": {"code": "1309", "message": "Your GLM Coding Plan package has expired"}}
    )
    msg = format_zai_api_error(exc)
    assert msg is not None
    assert "1309" in msg
    assert "Coding Plan" in msg


def test_format_zai_1310_with_next_flush_time():
    exc = _exc_with_body(
        {
            "error": {
                "code": "1310",
                "message": "Weekly limit exhausted",
                "next_flush_time": "2026-05-04T00:00:00Z",
            }
        }
    )
    msg = format_zai_api_error(exc)
    assert msg is not None
    assert "1310" in msg
    assert "2026-05-04" in msg


def test_format_zai_1310_without_next_flush_time():
    exc = _exc_with_body({"error": {"code": "1310", "message": "limit exhausted"}})
    msg = format_zai_api_error(exc)
    assert msg is not None
    assert "1310" in msg
    assert "Сброс" not in msg


def test_format_zai_1311_with_model_name():
    exc = _exc_with_body(
        {"error": {"code": "1311", "message": "Plan does not include glm-4.7", "model_name": "glm-4.7"}}
    )
    msg = format_zai_api_error(exc)
    assert msg is not None
    assert "1311" in msg
    assert "glm-4.7" in msg
    assert "/paas/v4" in msg


def test_format_zai_returns_none_for_unknown_codes():
    assert format_zai_api_error(_exc_with_body({"error": {"code": "9999", "message": "x"}})) is None
    assert format_zai_api_error(_exc_with_body({})) is None


def test_format_zai_uses_response_json_when_body_missing():
    exc = _exc_with_response({"error": {"code": "1113", "message": "x"}})
    msg = format_zai_api_error(exc)
    assert msg is not None
    assert "1113" in msg


def test_format_zai_extracts_from_string_body():
    exc = Exception(
        "Error code: 429 - {'error': {'code': '1113', 'message': 'Insufficient balance'}}"
    )
    # No body / response attribute, only str(exc) carries the payload.
    msg = format_zai_api_error(exc)
    assert msg is not None
    assert "1113" in msg


def test_format_zai_returns_none_for_non_json_payload():
    exc = Exception("plain network error")
    assert format_zai_api_error(exc) is None


def test_format_provider_error_wraps_zai_with_friendly_text():
    exc = _exc_with_body({"error": {"code": "1113", "message": "x"}})
    rendered = format_provider_error("zai", exc)
    assert rendered.startswith("zai: ")
    assert "1113" in rendered


def test_format_provider_error_falls_through_for_other_providers():
    exc = Exception("boom")
    rendered = format_provider_error("openai", exc)
    assert rendered == "openai: boom"


def test_format_provider_error_zai_unknown_code_falls_through():
    exc = Exception("plain network error")
    rendered = format_provider_error("zai", exc)
    assert rendered == "zai: plain network error"


@pytest.mark.parametrize("code", ["1113", "1309", "1310", "1311"])
def test_format_zai_recognizes_documented_codes(code):
    exc = _exc_with_body({"error": {"code": code, "message": "test"}})
    msg = format_zai_api_error(exc)
    assert msg is not None
    assert code in msg
