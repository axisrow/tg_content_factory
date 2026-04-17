from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from src.web import deps

router = APIRouter()


@router.get("/{command_id}")
async def get_command_status(request: Request, command_id: int):
    command = await deps.telegram_command_service(request).get(command_id)
    if command is None:
        raise HTTPException(status_code=404, detail="Command not found")
    return JSONResponse(command.model_dump(mode="json"))
