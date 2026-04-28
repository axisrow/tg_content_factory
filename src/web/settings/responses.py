from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from fastapi.responses import JSONResponse, RedirectResponse

from src.web.responses import flash_redirect, json_response


@dataclass(frozen=True)
class SettingsFlash:
    msg: str | None = None
    error: str | None = None
    extra: dict[str, object | None] = field(default_factory=dict)
    fragment: str | None = None


@dataclass(frozen=True)
class SettingsJson:
    payload: dict[str, Any]
    status_code: int = 200


def settings_flash_response(result: SettingsFlash) -> RedirectResponse:
    return flash_redirect(
        "/settings",
        msg=result.msg,
        error=result.error,
        extra=result.extra,
        fragment=result.fragment,
    )


def settings_json_response(result: SettingsJson) -> JSONResponse:
    return json_response(result.payload, status_code=result.status_code)


def settings_result_response(result: SettingsFlash | SettingsJson) -> RedirectResponse | JSONResponse:
    if isinstance(result, SettingsJson):
        return settings_json_response(result)
    return settings_flash_response(result)

