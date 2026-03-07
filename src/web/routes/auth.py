import logging

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.models import Account

logger = logging.getLogger(__name__)

router = APIRouter()


def _render(request: Request, name: str, context: dict):
    return request.app.state.templates.TemplateResponse(request, name, context)


def _is_api_configured(request: Request) -> bool:
    return request.app.state.auth.is_configured


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    api_configured = _is_api_configured(request)
    step = "phone" if api_configured else "credentials"
    return _render(
        request,
        "login.html",
        {"step": step, "error": None, "phone": "", "api_configured": api_configured},
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
    auth = request.app.state.auth

    if not auth.is_configured:
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

    try:
        info = await auth.send_code(phone)
        return _render(
            request,
            "login.html",
            {
                "step": "code",
                "phone": phone,
                "phone_code_hash": info["phone_code_hash"],
                "code_type": info["code_type"],
                "next_type": info["next_type"],
                "timeout": info["timeout"],
                "error": None,
                "api_configured": True,
            },
        )
    except Exception as e:
        return _render(
            request,
            "login.html",
            {"step": "phone", "error": str(e), "phone": phone, "api_configured": True},
        )


@router.post("/resend-code")
async def resend_code(
    request: Request,
    phone: str = Form(...),
    phone_code_hash: str = Form(...),
):
    auth = request.app.state.auth
    try:
        info = await auth.resend_code(phone)
        return _render(
            request,
            "login.html",
            {
                "step": "code",
                "phone": phone,
                "phone_code_hash": info["phone_code_hash"],
                "code_type": info["code_type"],
                "next_type": info["next_type"],
                "timeout": info["timeout"],
                "error": None,
                "api_configured": True,
            },
        )
    except Exception as e:
        return _render(
            request,
            "login.html",
            {
                "step": "code",
                "phone": phone,
                "phone_code_hash": phone_code_hash,
                "error": str(e),
                "api_configured": True,
            },
        )


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
    auth = request.app.state.auth
    db = request.app.state.db
    pool = request.app.state.pool

    try:
        session_string = await auth.verify_code(
            phone, code, phone_code_hash, password_2fa or None
        )

        existing = await db.get_accounts()
        is_primary = len(existing) == 0

        await pool.add_client(phone, session_string)

        is_premium = False
        client = pool.clients.get(phone)
        if client:
            try:
                me = await client.get_me()
                is_premium = bool(getattr(me, "premium", False))
            except Exception as e:
                logger.warning("Failed to get premium status during auth for %s: %s", phone, e)

        account = Account(
            phone=phone,
            session_string=session_string,
            is_primary=is_primary,
            is_premium=is_premium,
        )
        await db.add_account(account)

        return RedirectResponse(url="/settings?msg=account_connected", status_code=303)
    except ValueError as e:
        error = str(e)
        timeout_val = int(timeout) if timeout.isdigit() else None
        if "2FA" in error or "password" in error.lower():
            return _render(
                request,
                "login.html",
                {
                    "step": "2fa",
                    "phone": phone,
                    "code": code,
                    "phone_code_hash": phone_code_hash,
                    "error": error,
                    "api_configured": True,
                },
            )
        return _render(
            request,
            "login.html",
            {
                "step": "code",
                "phone": phone,
                "phone_code_hash": phone_code_hash,
                "code_type": code_type or None,
                "next_type": next_type or None,
                "timeout": timeout_val,
                "error": error,
                "api_configured": True,
            },
        )
    except Exception as e:
        return _render(
            request,
            "login.html",
            {"step": "phone", "phone": phone, "error": str(e), "api_configured": True},
        )
