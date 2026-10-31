"""Входящие DM: журнал + апрув черновиков на web/REST (#1579, блокер цикла DM).

Куплет контракта automation-first: REST — первичная поверхность (coding
агенты читают журнал и проводят черновики через апрув), HTML-страница —
минимальная проекция той же логики по образцу ``/moderation``. Создание
черновиков и отправка остаются в CLI/воркере (этапы 2.4 #1429): здесь только
чтение журнала (окно TTL 24ч) и решения по черновикам.

Идентификаторы чувствительны (текст личной переписки + телефоны), но обе
поверхности сидят за штатным WEB_PASS/basic-auth панели — как и остальные
маршруты.
"""

from __future__ import annotations

from typing import Literal

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from tg_webui import deps

router = APIRouter()


def _redirect(code: str, *, error: bool = False) -> RedirectResponse:
    key = "error" if error else "msg"
    return RedirectResponse(url=f"/dms?{key}={code}", status_code=303)


# === Web page ==============================================================


@router.get("/", response_class=HTMLResponse)
async def dms_page(request: Request):
    """Журнал входящих + черновики с апрувом (по образцу /moderation).

    Без ленилоада-фрагмента: журнал ограничен TTL 24ч, таблица заведомо мала.
    """
    db = deps.get_db(request)
    dms = await db.repos.incoming_dms.list()
    drafts = await db.repos.dm_reply_drafts.list_drafts()
    return deps.get_templates(request).TemplateResponse(
        request,
        "dms.html",
        {"dms": dms, "drafts": drafts},
    )


@router.post("/drafts/{draft_id}/approve")
async def draft_approve(request: Request, draft_id: int):
    db = deps.get_db(request)
    if await db.repos.dm_reply_drafts.get(draft_id) is None:
        return _redirect("draft_not_found", error=True)
    # Идемпотентно: sending/sent решением владельца назад не переводятся.
    await db.repos.dm_reply_drafts.decide(draft_id, "approved")
    return _redirect("draft_approved")


@router.post("/drafts/{draft_id}/reject")
async def draft_reject(request: Request, draft_id: int):
    db = deps.get_db(request)
    if await db.repos.dm_reply_drafts.get(draft_id) is None:
        return _redirect("draft_not_found", error=True)
    await db.repos.dm_reply_drafts.decide(draft_id, "rejected")
    return _redirect("draft_rejected")


# === REST ==================================================================


@router.get(
    "/api/incoming",
    response_class=JSONResponse,
    tags=["dm"],
    summary="Incoming-DM journal, fresh first (24h TTL window)",
)
async def api_incoming_list(
    request: Request,
    limit: int = Query(default=100, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
):
    """JSON-список журнала входящих DM.

    Каждая запись несёт ``id``/``phone``/``chat_id``/``message_id`` — ровно то,
    что нужно ``dm drafts create`` (#1579: «read paths expose the ids the
    drafts flow needs»).
    """
    db = deps.get_db(request)
    return await db.repos.incoming_dms.list(limit=limit, offset=offset)


@router.get(
    "/api/incoming/{dm_id}",
    response_class=JSONResponse,
    tags=["dm"],
    summary="One incoming-DM journal entry",
)
async def api_incoming_get(request: Request, dm_id: int):
    db = deps.get_db(request)
    dm = await db.repos.incoming_dms.get(dm_id)
    if dm is None:
        return JSONResponse({"error": "dm_not_found"}, status_code=404)
    return dm


async def _decide_draft(
    db, draft_id: int, decision: Literal["approved", "rejected"]
) -> tuple[dict, int]:
    """Решение по черновику → JSON {id, status, changed}; 404 для неизвестного.

    ``changed=False`` — честный no-op (повтор решения или sending/sent), статус
    после вызова возвращается фактический.
    """
    if await db.repos.dm_reply_drafts.get(draft_id) is None:
        return {"error": "draft_not_found"}, 404
    changed = await db.repos.dm_reply_drafts.decide(draft_id, decision)
    after = await db.repos.dm_reply_drafts.get(draft_id)
    return {"id": draft_id, "status": after.status, "changed": changed}, 200


@router.post(
    "/api/drafts/{draft_id}/approve",
    tags=["dm"],
    summary="Approve a reply draft",
)
async def api_draft_approve(request: Request, draft_id: int):
    body, code = await _decide_draft(deps.get_db(request), draft_id, "approved")
    return JSONResponse(body, status_code=code)


@router.post(
    "/api/drafts/{draft_id}/reject",
    tags=["dm"],
    summary="Reject a reply draft",
)
async def api_draft_reject(request: Request, draft_id: int):
    body, code = await _decide_draft(deps.get_db(request), draft_id, "rejected")
    return JSONResponse(body, status_code=code)
