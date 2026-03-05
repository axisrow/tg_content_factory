import logging

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from src.web import deps

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def channels_list(request: Request):
    service = deps.channel_service(request)
    show_all = request.query_params.get("view") == "all"
    channels, keywords, latest_stats = await service.list_for_page(
        include_filtered=show_all
    )
    error = request.query_params.get("error")
    msg = request.query_params.get("msg")
    return deps.get_templates(request).TemplateResponse(
        request,
        "channels.html",
        {
            "channels": channels,
            "keywords": keywords,
            "latest_stats": latest_stats,
            "error": error,
            "msg": msg,
            "show_all": show_all,
        },
    )


@router.post("/add")
async def add_channel(request: Request, identifier: str = Form(...)):
    service = deps.channel_service(request)
    try:
        ok = await service.add_by_identifier(identifier)
    except RuntimeError as exc:
        if str(exc) == "no_client":
            return RedirectResponse(url="/channels?error=no_client", status_code=303)
        ok = False
    except Exception:
        ok = False

    if not ok:
        return RedirectResponse(url="/channels?error=resolve", status_code=303)
    return RedirectResponse(url="/channels?msg=channel_added", status_code=303)


@router.get("/dialogs")
async def get_dialogs(request: Request):
    service = deps.channel_service(request)
    dialogs = await service.get_dialogs_with_added_flags()
    return JSONResponse(content=dialogs)


@router.post("/add-bulk")
async def add_bulk(request: Request):
    form = await request.form()
    service = deps.channel_service(request)
    await service.add_bulk_by_dialog_ids(form.getlist("channel_ids"))
    return RedirectResponse(url="/channels?msg=channels_added", status_code=303)


@router.post("/{pk}/toggle")
async def toggle_channel(request: Request, pk: int):
    await deps.channel_service(request).toggle(pk)
    return RedirectResponse(url="/channels?msg=channel_toggled", status_code=303)


@router.post("/{pk}/delete")
async def delete_channel(request: Request, pk: int):
    await deps.channel_service(request).delete(pk)
    return RedirectResponse(url="/channels?msg=channel_deleted", status_code=303)
