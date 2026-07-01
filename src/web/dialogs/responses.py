"""Response mapping for the dialogs web domain."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastapi import Request
from fastapi.responses import JSONResponse

from src.web import deps
from src.web.redirects import dialogs_redirect, redirect_see_other


@dataclass(frozen=True)
class DialogTemplate:
    name: str
    context: dict[str, Any]


@dataclass(frozen=True)
class DialogRedirect:
    """Flash redirect back to the dialogs screen (msg/error query params)."""

    phone: str = ""
    msg: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class CommandRedirect:
    """Redirect after enqueueing a Telegram command, carrying its command_id."""

    command_id: int
    phone: str = ""
    target_path: str = "/dialogs/"


@dataclass(frozen=True)
class PathRedirect:
    """See-other redirect to an explicit path with arbitrary query params."""

    path: str
    params: dict[str, object | None] | None = None


@dataclass(frozen=True)
class DialogJson:
    content: Any
    status_code: int = 200


DialogResult = DialogTemplate | DialogRedirect | CommandRedirect | PathRedirect | DialogJson | Any


def dialog_response(request: Request, result: DialogResult):
    if isinstance(result, DialogTemplate):
        return deps.get_templates(request).TemplateResponse(request, result.name, result.context)
    if isinstance(result, DialogRedirect):
        return dialogs_redirect(result.phone, msg=result.msg, error=result.error)
    if isinstance(result, CommandRedirect):
        params: dict[str, object | None] = {"command_id": result.command_id}
        if result.phone and "phone=" not in result.target_path:
            params["phone"] = result.phone
        return redirect_see_other(result.target_path, params)
    if isinstance(result, PathRedirect):
        return redirect_see_other(result.path, result.params)
    if isinstance(result, DialogJson):
        return JSONResponse(result.content, status_code=result.status_code)
    return result
