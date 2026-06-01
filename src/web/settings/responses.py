from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse, Response

from src.web import deps
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


@dataclass(frozen=True)
class SettingsTemplate:
    name: str
    context: dict[str, Any]


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


SettingsResult = SettingsFlash | SettingsJson | SettingsTemplate


def settings_result_response(
    request: Request,
    result: SettingsResult,
) -> Response:
    if isinstance(result, SettingsTemplate):
        return deps.get_templates(request).TemplateResponse(request, result.name, result.context)
    if isinstance(result, SettingsJson):
        return settings_json_response(result)
    return settings_flash_response(result)
