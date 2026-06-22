from __future__ import annotations

import base64
import hashlib
import json
import time
from functools import lru_cache

from itsdangerous import BadSignature, Signer

COOKIE_NAME = "session"
COOKIE_MAX_AGE = 30 * 24 * 3600  # 30 days


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64url_decode(s: str) -> bytes:
    padding = 4 - len(s) % 4
    if padding != 4:
        s += "=" * padding
    return base64.urlsafe_b64decode(s)


@lru_cache(maxsize=4)
def _signer(secret: str) -> Signer:
    # ``key_derivation="none"`` makes the HMAC key the raw secret (no salt/derived
    # key), so the signature is byte-for-byte the old hand-rolled
    # ``base64url(HMAC-SHA256(secret, payload))``. This keeps existing 30-day
    # ``session`` cookies valid across the deploy instead of logging everyone out
    # (review on #953). The "." separator preserves the ``payload.sig`` token shape.
    # Cached per secret: ``verify_session_token`` is on the authenticated-request
    # hot path.
    #
    # ``exp`` is kept in the payload (rather than itsdangerous' TimestampSigner)
    # so the per-token ``ttl`` contract and the ``payload.sig`` shape survive.
    return Signer(secret, sep=".", key_derivation="none", digest_method=hashlib.sha256)


def create_session_token(username: str, secret: str, ttl: int = COOKIE_MAX_AGE) -> str:
    payload = json.dumps({"user": username, "exp": int(time.time()) + ttl})
    payload_b64 = _b64url_encode(payload.encode())
    return _signer(secret).sign(payload_b64).decode()


def verify_session_token(token: str, secret: str) -> str | None:
    try:
        payload_b64 = _signer(secret).unsign(token).decode()
    except (BadSignature, UnicodeDecodeError):
        return None
    try:
        payload = json.loads(_b64url_decode(payload_b64))
    except Exception:
        return None
    # A validly-signed but non-object JSON payload (null/int/list/str) has no
    # .get(); guard before treating it as a dict.
    if not isinstance(payload, dict):
        return None
    if payload.get("exp", 0) < time.time():
        return None
    return payload.get("user")
