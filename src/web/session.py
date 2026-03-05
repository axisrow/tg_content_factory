from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time

COOKIE_NAME = "session"
COOKIE_MAX_AGE = 30 * 24 * 3600  # 30 days


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64url_decode(s: str) -> bytes:
    padding = 4 - len(s) % 4
    if padding != 4:
        s += "=" * padding
    return base64.urlsafe_b64decode(s)


def _sign(payload_b64: str, secret: str) -> str:
    sig = hmac.new(secret.encode(), payload_b64.encode(), hashlib.sha256).digest()
    return _b64url_encode(sig)


def create_session_token(username: str, secret: str, ttl: int = COOKIE_MAX_AGE) -> str:
    payload = json.dumps({"user": username, "exp": int(time.time()) + ttl})
    payload_b64 = _b64url_encode(payload.encode())
    sig_b64 = _sign(payload_b64, secret)
    return f"{payload_b64}.{sig_b64}"


def verify_session_token(token: str, secret: str) -> str | None:
    parts = token.split(".")
    if len(parts) != 2:
        return None
    payload_b64, sig_b64 = parts
    expected_sig = _sign(payload_b64, secret)
    if not hmac.compare_digest(sig_b64, expected_sig):
        return None
    try:
        payload = json.loads(_b64url_decode(payload_b64))
    except (json.JSONDecodeError, Exception):
        return None
    if payload.get("exp", 0) < time.time():
        return None
    return payload.get("user")
