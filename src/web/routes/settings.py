from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

router = APIRouter()

CREDENTIALS_MASK = "••••••••"


@router.get("/", response_class=HTMLResponse)
async def settings_page(request: Request):
    auth = request.app.state.auth
    db = request.app.state.db
    pool = request.app.state.pool
    api_id_raw = await db.get_setting("tg_api_id") or ""
    api_hash_raw = await db.get_setting("tg_api_hash") or ""
    accounts = await db.get_accounts()
    connected_phones = set(pool.clients.keys())
    return request.app.state.templates.TemplateResponse(
        request,
        "settings.html",
        {
            "is_configured": auth.is_configured,
            "api_id": CREDENTIALS_MASK if api_id_raw else "",
            "api_hash": CREDENTIALS_MASK if api_hash_raw else "",
            "accounts": accounts,
            "connected_phones": connected_phones,
        },
    )


@router.post("/save-credentials")
async def save_credentials(request: Request):
    form = await request.form()
    db = request.app.state.db
    auth = request.app.state.auth

    api_id = str(form.get("api_id", "")).strip()
    api_hash = str(form.get("api_hash", "")).strip()

    id_changed = api_id and api_id != CREDENTIALS_MASK
    hash_changed = api_hash and api_hash != CREDENTIALS_MASK

    if id_changed:
        await db.set_setting("tg_api_id", api_id)
    if hash_changed:
        await db.set_setting("tg_api_hash", api_hash)

    if id_changed or hash_changed:
        actual_id = api_id if id_changed else (await db.get_setting("tg_api_id") or "")
        actual_hash = api_hash if hash_changed else (await db.get_setting("tg_api_hash") or "")
        if actual_id and actual_hash:
            auth.update_credentials(int(actual_id), actual_hash)

    return RedirectResponse(url="/settings?msg=credentials_saved", status_code=303)


@router.post("/{account_id}/toggle")
async def toggle_account(request: Request, account_id: int):
    db = request.app.state.db
    accounts = await db.get_accounts()
    for acc in accounts:
        if acc.id == account_id:
            await db.set_account_active(account_id, not acc.is_active)
            if not acc.is_active:
                try:
                    await request.app.state.pool.add_client(acc.phone, acc.session_string)
                except Exception:
                    pass
            else:
                await request.app.state.pool.remove_client(acc.phone)
            break
    return RedirectResponse(url="/settings?msg=account_toggled", status_code=303)


@router.post("/{account_id}/delete")
async def delete_account(request: Request, account_id: int):
    db = request.app.state.db
    accounts = await db.get_accounts()
    for acc in accounts:
        if acc.id == account_id:
            await request.app.state.pool.remove_client(acc.phone)
            break
    await db.delete_account(account_id)
    return RedirectResponse(url="/settings?msg=account_deleted", status_code=303)
