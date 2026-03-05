from fastapi import APIRouter, Form, Request
from fastapi.responses import RedirectResponse

from src.web import deps

router = APIRouter()


@router.post("/keywords/add")
async def add_keyword(
    request: Request,
    pattern: str = Form(...),
    is_regex: bool = Form(False),
):
    await deps.keyword_service(request).add(pattern, is_regex)
    return RedirectResponse(url="/channels?msg=keyword_added", status_code=303)


@router.post("/keywords/{keyword_id}/toggle")
async def toggle_keyword(request: Request, keyword_id: int):
    await deps.keyword_service(request).toggle(keyword_id)
    return RedirectResponse(url="/channels?msg=keyword_toggled", status_code=303)


@router.post("/keywords/{keyword_id}/delete")
async def delete_keyword(request: Request, keyword_id: int):
    await deps.keyword_service(request).delete(keyword_id)
    return RedirectResponse(url="/channels?msg=keyword_deleted", status_code=303)
