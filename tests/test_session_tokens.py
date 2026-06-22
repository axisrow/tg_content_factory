"""Regression tests for src/web/session.py token verification (#971)."""

from __future__ import annotations

import base64
import json

from src.web.session import _sign, create_session_token, verify_session_token


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def test_round_trip_valid_token():
    tok = create_session_token("admin", "secret")
    assert verify_session_token(tok, "secret") == "admin"


def test_non_ascii_signature_returns_none_not_raises():
    # hmac.compare_digest raises TypeError on a non-ASCII str; a tampered cookie
    # with non-ASCII in the signature segment must be rejected, not crash.
    payload_b64 = _b64url(json.dumps({"user": "x", "exp": 9999999999}).encode())
    assert verify_session_token(f"{payload_b64}.bad✓sig", "secret") is None


def test_signed_non_object_payload_returns_none_not_raises():
    # A correctly-signed but non-object JSON payload (null/int/list/str) has no
    # .get(); verify must return None instead of raising AttributeError.
    for value in (None, 5, "hello", [1, 2, 3]):
        payload_b64 = _b64url(json.dumps(value).encode())
        token = f"{payload_b64}.{_sign(payload_b64, 'secret')}"
        assert verify_session_token(token, "secret") is None
