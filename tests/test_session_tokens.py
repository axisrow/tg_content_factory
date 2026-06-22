"""Regression tests for src/web/session.py token verification (#971, #953)."""

from __future__ import annotations

import base64
import json

from src.web.session import _b64url_encode, _signer, create_session_token, verify_session_token


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _signed(value, secret: str = "secret") -> str:
    """Produce a validly-signed token wrapping an arbitrary JSON value."""
    return _signer(secret).sign(_b64url_encode(json.dumps(value).encode())).decode()


def test_round_trip_valid_token():
    tok = create_session_token("admin", "secret")
    assert verify_session_token(tok, "secret") == "admin"


def test_wrong_secret_rejected():
    tok = create_session_token("admin", "secret")
    assert verify_session_token(tok, "other-secret") is None


def test_tampered_signature_rejected():
    tok = create_session_token("admin", "secret")
    payload_b64, _sig = tok.rsplit(".", 1)
    assert verify_session_token(f"{payload_b64}.deadbeef", "secret") is None


def test_expired_token_rejected():
    tok = create_session_token("admin", "secret", ttl=-1)
    assert verify_session_token(tok, "secret") is None


def test_non_ascii_signature_returns_none_not_raises():
    # A tampered cookie with non-ASCII bytes in the signature segment must be
    # rejected, not crash.
    payload_b64 = _b64url(json.dumps({"user": "x", "exp": 9999999999}).encode())
    assert verify_session_token(f"{payload_b64}.bad✓sig", "secret") is None


def test_signed_non_object_payload_returns_none_not_raises():
    # A correctly-signed but non-object JSON payload (null/int/list/str) has no
    # .get(); verify must return None instead of raising AttributeError.
    for value in (None, 5, "hello", [1, 2, 3]):
        assert verify_session_token(_signed(value), "secret") is None
