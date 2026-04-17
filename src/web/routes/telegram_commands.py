from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from src.web import deps

router = APIRouter()

_SENSITIVE_KEYS = {
    "session_string",
    "password",
    "passcode",
    "secret",
    "access_token",
    "refresh_token",
    "token",
    "api_key",
    "api_hash",
    "phone_code_hash",
    "2fa_password",
    "password_2fa",
}


def _redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: ("[REDACTED]" if str(key).lower() in _SENSITIVE_KEYS else _redact(val))
            for key, val in value.items()
        }
    if isinstance(value, list):
        return [_redact(item) for item in value]
    return value


@router.get("/{command_id}")
async def get_command_status(request: Request, command_id: int):
    command = await deps.telegram_command_service(request).get(command_id)
    if command is None:
        raise HTTPException(status_code=404, detail="Command not found")
    data = command.model_dump(mode="json")
    data["payload"] = _redact(data.get("payload"))
    data["result_payload"] = _redact(data.get("result_payload"))
    return JSONResponse(data)
