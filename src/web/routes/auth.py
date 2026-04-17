import logging

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.models import TelegramCommandStatus
from src.web import deps

logger = logging.getLogger(__name__)

router = APIRouter()


def _render(request: Request, name: str, context: dict):
    return request.app.state.templates.TemplateResponse(request, name, context)


def _is_api_configured(request: Request) -> bool:
    return request.app.state.auth.is_configured


async def _auth_command(request: Request):
    raw = request.query_params.get("command_id", "").strip()
    if not raw.isdigit():
        return None
    return await deps.telegram_command_service(request).get(int(raw))


def _auth_redirect(request: Request, command_id: int, *, phone: str = "") -> RedirectResponse:
    target = f"/auth/login?command_id={command_id}"
    if phone:
        target += f"&phone={phone}"
    return RedirectResponse(url=target, status_code=303)


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    api_configured = _is_api_configured(request)
    step = "phone" if api_configured else "credentials"
    command = await _auth_command(request)
    context = {"step": step, "error": None, "phone": "", "api_configured": api_configured}
    if command is not None and command.command_type.startswith("auth."):
        payload = command.payload or {}
        result = command.result_payload or {}
        context["phone"] = payload.get("phone", request.query_params.get("phone", ""))
        if command.status in {TelegramCommandStatus.PENDING, TelegramCommandStatus.RUNNING}:
            context.update(
                {
                    "step": "pending",
                    "command_id": command.id,
                    "pending_action": command.command_type,
                }
            )
        elif command.command_type in {"auth.send_code", "auth.resend_code"}:
            if command.status == TelegramCommandStatus.SUCCEEDED:
                context.update(
                    {
                        "step": "code",
                        "phone": result.get("phone", context["phone"]),
                        "phone_code_hash": result.get("phone_code_hash", ""),
                        "code_type": result.get("code_type"),
                        "next_type": result.get("next_type"),
                        "timeout": result.get("timeout"),
                    }
                )
            else:
                context.update(
                    {
                        "step": "code" if command.command_type == "auth.resend_code" else "phone",
                        "error": command.error,
                        "phone_code_hash": payload.get("phone_code_hash", ""),
                        "code_type": payload.get("code_type", ""),
                        "next_type": payload.get("next_type", ""),
                        "timeout": payload.get("timeout", ""),
                    }
                )
        elif command.command_type == "auth.verify_code":
            if command.status == TelegramCommandStatus.SUCCEEDED:
                return RedirectResponse(url="/settings?msg=account_connected", status_code=303)
            error = command.error or ""
            if "2FA" in error or "password" in error.lower():
                context.update(
                    {
                        "step": "2fa",
                        "error": error,
                        "code": payload.get("code", ""),
                        "phone_code_hash": payload.get("phone_code_hash", ""),
                    }
                )
            else:
                context.update(
                    {
                        "step": "code",
                        "error": error,
                        "phone_code_hash": payload.get("phone_code_hash", ""),
                        "code_type": payload.get("code_type", ""),
                        "next_type": payload.get("next_type", ""),
                        "timeout": payload.get("timeout", ""),
                    }
                )
    return _render(
        request,
        "login.html",
        context,
    )


@router.post("/save-credentials")
async def save_credentials(
    request: Request,
    api_id: int = Form(...),
    api_hash: str = Form(...),
):
    db = request.app.state.db
    auth = request.app.state.auth

    await db.set_setting("tg_api_id", str(api_id))
    await db.set_setting("tg_api_hash", api_hash)
    auth.update_credentials(api_id, api_hash)

    return RedirectResponse(url="/auth/login", status_code=303)


@router.post("/send-code")
async def send_code(request: Request, phone: str = Form(...)):
    if not _is_api_configured(request):
        return _render(
            request,
            "login.html",
            {
                "step": "credentials",
                "error": "API credentials не настроены. Введите api_id и api_hash.",
                "phone": phone,
                "api_configured": False,
            },
        )
    command_id = await deps.telegram_command_service(request).enqueue(
        "auth.send_code",
        payload={"phone": phone},
        requested_by="web:auth.send_code",
    )
    return _auth_redirect(request, command_id, phone=phone)


@router.post("/resend-code")
async def resend_code(
    request: Request,
    phone: str = Form(...),
    phone_code_hash: str = Form(...),
):
    command_id = await deps.telegram_command_service(request).enqueue(
        "auth.resend_code",
        payload={"phone": phone, "phone_code_hash": phone_code_hash},
        requested_by="web:auth.resend_code",
    )
    return _auth_redirect(request, command_id, phone=phone)


@router.post("/verify-code")
async def verify_code(
    request: Request,
    phone: str = Form(...),
    code: str = Form(...),
    phone_code_hash: str = Form(...),
    password_2fa: str = Form(""),
    code_type: str = Form(""),
    next_type: str = Form(""),
    timeout: str = Form(""),
):
    db = request.app.state.db

    existing = await db.get_accounts()
    is_primary = len(existing) == 0
    # Preserve is_primary decision in payload for deterministic worker behavior if needed later.
    command_id = await deps.telegram_command_service(request).enqueue(
        "auth.verify_code",
        payload={
            "phone": phone,
            "code": code,
            "phone_code_hash": phone_code_hash,
            "password_2fa": password_2fa or "",
            "code_type": code_type or "",
            "next_type": next_type or "",
            "timeout": timeout or "",
            "is_primary": is_primary,
        },
        requested_by="web:auth.verify_code",
    )
    return _auth_redirect(request, command_id, phone=phone)
