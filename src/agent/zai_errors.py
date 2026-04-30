"""Friendly translations for Z.AI API error responses.

Z.AI returns errors with internal numeric codes (1113, 1309, 1310, 1311…) that
are documented at https://docs.z.ai/api-reference/api-code but the wrapping
HTTP status / SDK error class can be misleading on its own. This module
extracts the inner code from whatever the langchain-openai SDK raises and
returns a localized, actionable message — or ``None`` if the exception is
not a recognized Z.AI error.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any

# https://docs.z.ai/api-reference/api-code
_ACCOUNT_BALANCE_CODE = "1113"
_PLAN_EXPIRED_CODE = "1309"
_PLAN_LIMIT_CODE = "1310"
_PLAN_MODEL_NOT_INCLUDED_CODE = "1311"

_RECOGNIZED_CODES = {
    _ACCOUNT_BALANCE_CODE,
    _PLAN_EXPIRED_CODE,
    _PLAN_LIMIT_CODE,
    _PLAN_MODEL_NOT_INCLUDED_CODE,
}


def _coerce_dict(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode("utf-8", errors="replace")
        except Exception:
            return None
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            # Python-style dict reprs (e.g. ``str(exc)`` from langchain-openai
            # often contains "{'error': {'code': '1113', ...}}" with single
            # quotes) are not valid JSON. Fall back to literal_eval for that
            # narrow case; it is bounded and safe (no Name resolution).
            try:
                import ast

                parsed = ast.literal_eval(value)
            except (TypeError, ValueError, SyntaxError):
                return None
        return parsed if isinstance(parsed, Mapping) else None
    return None


def _extract_payload(exc: BaseException) -> Mapping[str, Any] | None:
    """Try every place the OpenAI / langchain-openai SDKs stash the JSON body."""
    body = getattr(exc, "body", None)
    payload = _coerce_dict(body)
    if payload is not None:
        return payload

    response = getattr(exc, "response", None)
    if response is not None:
        json_method = getattr(response, "json", None)
        if callable(json_method):
            try:
                payload = _coerce_dict(json_method())
                if payload is not None:
                    return payload
            except Exception:
                pass
        text = getattr(response, "text", None)
        if callable(text):
            try:
                text = text()
            except Exception:
                text = None
        payload = _coerce_dict(text)
        if payload is not None:
            return payload

    payload = _coerce_dict(getattr(exc, "args", (None,))[0] if getattr(exc, "args", None) else None)
    if payload is not None:
        return payload

    text = str(exc)
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if match:
        return _coerce_dict(match.group(0))
    return None


def _extract_code(payload: Mapping[str, Any]) -> str | None:
    error = payload.get("error")
    if isinstance(error, Mapping):
        code = error.get("code")
        if code is not None:
            return str(code).strip()
        message = error.get("message")
        if isinstance(message, str):
            match = re.search(r"\b(\d{3,4})\b", message)
            if match:
                return match.group(1)
    code = payload.get("code")
    if code is not None:
        return str(code).strip()
    return None


def _extract_field(payload: Mapping[str, Any], field: str) -> str | None:
    error = payload.get("error")
    if isinstance(error, Mapping):
        value = error.get(field)
        if value:
            return str(value).strip()
        message = error.get("message")
        if isinstance(message, str):
            match = re.search(rf"\${{{field}}}", message)
            if match:
                return None
    value = payload.get(field)
    if value:
        return str(value).strip()
    return None


def format_zai_api_error(exc: BaseException) -> str | None:
    """Return a human-readable Russian message for a known Z.AI error, else None.

    The message includes guidance on how to resolve each documented case:
    1113 (account balance), 1309 (plan expired), 1310 (plan quota), 1311
    (model not included in plan).
    """
    payload = _extract_payload(exc)
    if payload is None:
        return None

    code = _extract_code(payload)
    if code not in _RECOGNIZED_CODES:
        return None

    if code == _ACCOUNT_BALANCE_CODE:
        return (
            "Z.AI вернул «Insufficient balance» (1113). Если у вас GLM Coding "
            "Plan, проверьте, что Base URL — https://api.z.ai/api/coding/paas/v4 "
            "и подписка активна. На pay-per-token PaaS пополните баланс на "
            "z.ai/manage-account/financial."
        )
    if code == _PLAN_EXPIRED_CODE:
        return (
            "Срок GLM Coding Plan истёк (1309). Продлите подписку на z.ai и "
            "повторите запрос."
        )
    if code == _PLAN_LIMIT_CODE:
        next_flush_time = _extract_field(payload, "next_flush_time")
        suffix = f" Сброс лимита: {next_flush_time}." if next_flush_time else ""
        return (
            "Лимит GLM Coding Plan исчерпан (1310)." + suffix
        )
    if code == _PLAN_MODEL_NOT_INCLUDED_CODE:
        model_name = _extract_field(payload, "model_name") or _extract_field(payload, "model")
        suffix = f" Модель: {model_name}." if model_name else ""
        return (
            "Текущая подписка Z.AI не включает выбранную модель (1311)."
            + suffix
            + " Выберите модель из подписки либо переключитесь на pay-per-token "
            "PaaS (https://api.z.ai/api/paas/v4) с балансом на счёте."
        )
    return None


def format_provider_error(provider: str, exc: BaseException) -> str:
    """Convenience wrapper used in the deepagents fail-over loop."""
    if provider == "zai":
        friendly = format_zai_api_error(exc)
        if friendly:
            return f"{provider}: {friendly}"
    return f"{provider}: {exc}"
