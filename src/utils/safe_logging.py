from __future__ import annotations

import hashlib
import logging
import re
import time


def elapsed_ms(started_at: float) -> int:
    """Milliseconds elapsed since a ``time.monotonic()`` checkpoint."""
    return int((time.monotonic() - started_at) * 1000)


def mask_phone(phone: str | None) -> str:
    if not phone:
        return ""
    value = str(phone)
    if len(value) <= 7:
        return f"{value[:2]}..."
    return f"{value[:3]}...{value[-4:]}"


def text_hash(value: str) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:12]


def query_log_fields(query: str) -> dict[str, object]:
    return {
        "query_hash": text_hash(query),
        "query_len": len(query),
    }


# E.164 phone token: a leading "+" then 7+ digits. The "+" anchor keeps it from
# matching hex hashes, channel ids, or numeric metadata (command_id=123).
_PHONE_TOKEN_RE = re.compile(r"\+\d{7,}")
# `phone=<token>` / `for <token>` — the explicit phone-bearing log shapes, where
# the value may be bare digits (no "+"). Bounded by a word boundary on the right.
_PHONE_FIELD_RE = re.compile(r"(?P<key>phone=|for )(?P<val>\+?\d{4,})\b")
# Raw search-query shapes: `query='...'`, `query="..."`, `Search query '...'`.
_QUERY_FIELD_RE = re.compile(
    r"""(?P<key>query[ =]')(?P<val>[^']*)(?P<end>')"""
    r"""|(?P<key2>query=")(?P<val2>[^"]*)(?P<end2>")"""
)


def _mask_phone_token(match: re.Match[str]) -> str:
    return mask_phone(match.group(0))


def redact_log_text(message: str) -> str:
    """Mask phone numbers and raw search queries in a rendered log line.

    Centralised redaction (#785): attached to logging handlers so every record —
    including call sites that still pass raw phone/query — is sanitised before it
    reaches a file, the in-memory buffer, or the console. Mutates the rendered
    string only; structured masking at security-critical call sites stays in place
    as defence-in-depth.
    """

    def _query_repl(m: re.Match[str]) -> str:
        if m.group("key") is not None:
            return f"query=hash:{text_hash(m.group('val'))}"
        return f"query=hash:{text_hash(m.group('val2'))}"

    message = _QUERY_FIELD_RE.sub(_query_repl, message)
    # `phone=`/`for ` carry phones as short as the test fixtures (4+ digits).
    message = _PHONE_FIELD_RE.sub(lambda m: m.group("key") + mask_phone(m.group("val")), message)
    # Any remaining bare E.164 token (e.g. logged inside a sentence).
    message = _PHONE_TOKEN_RE.sub(_mask_phone_token, message)
    return message


class RedactingFormatter(logging.Formatter):
    """Logging formatter that redacts phone/query PII from emitted output.

    Redaction is an output concern, scoped per handler: it sanitises the string
    this handler writes (file, console, in-memory buffer) without mutating the
    shared ``LogRecord``. That keeps record-level consumers — e.g. pytest's
    ``caplog`` — seeing the original message, so the redaction layer never makes
    unit tests order-dependent.
    """

    def format(self, record: logging.LogRecord) -> str:
        return redact_log_text(super().format(record))
