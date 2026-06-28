from __future__ import annotations

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse

from src.models import PhotoSendMode
from src.web import deps
from src.web.photo_loader.forms import (
    PhotoAutoCreateForm,
    PhotoBatchForm,
    PhotoPhoneForm,
    PhotoRefreshForm,
    PhotoScheduleForm,
    PhotoSendForm,
)
from src.web.photo_loader.handlers import (
    handle_photo_auto_create,
    handle_photo_batch,
    handle_photo_cancel_item,
    handle_photo_delete_auto,
    handle_photo_loader_dialogs,
    handle_photo_loader_page,
    handle_photo_loader_refresh,
    handle_photo_publish_batch,
    handle_photo_run_due,
    handle_photo_schedule,
    handle_photo_send,
    handle_photo_toggle_auto,
    handle_photo_update_auto,
)
from src.web.photo_loader.responses import photo_loader_response

router = APIRouter()


@router.get("", response_class=HTMLResponse)
async def photo_loader_page(request: Request, phone: str | None = None):
    return photo_loader_response(request, await handle_photo_loader_page(request, phone))


@router.get("/fragments/dialogs", response_class=HTMLResponse)
async def photo_loader_dialogs_fragment(request: Request, phone: str | None = None):
    return photo_loader_response(request, await handle_photo_loader_dialogs(request, phone))


@router.post("/refresh")
async def photo_loader_refresh(request: Request, phone: str = Form("")):
    result = await handle_photo_loader_refresh(request, PhotoRefreshForm(phone=phone))
    return photo_loader_response(request, result)


@router.post("/send")
async def photo_send(
    request: Request,
    phone: str = Form(""),
    target_dialog_id: str = Form(""),
    target_title: str = Form(""),
    target_type: str = Form(""),
    send_mode: str = Form(PhotoSendMode.SEPARATE.value),
    caption: str = Form(""),
    photos: list[UploadFile] = File(...),
):
    result = await handle_photo_send(
        request,
        PhotoSendForm(
            phone=phone,
            target_dialog_id=target_dialog_id,
            target_title=target_title,
            target_type=target_type,
            send_mode=send_mode,
            caption=caption,
            photos=photos,
        ),
    )
    return photo_loader_response(request, result)


@router.post("/schedule")
async def photo_schedule(
    request: Request,
    phone: str = Form(""),
    target_dialog_id: str = Form(""),
    target_title: str = Form(""),
    target_type: str = Form(""),
    send_mode: str = Form(PhotoSendMode.SEPARATE.value),
    caption: str = Form(""),
    schedule_at: str = Form(""),
    photos: list[UploadFile] = File(...),
):
    result = await handle_photo_schedule(
        request,
        PhotoScheduleForm(
            phone=phone,
            target_dialog_id=target_dialog_id,
            target_title=target_title,
            target_type=target_type,
            send_mode=send_mode,
            caption=caption,
            schedule_at=schedule_at,
            photos=photos,
        ),
    )
    return photo_loader_response(request, result)


@router.post("/batch")
async def photo_batch(
    request: Request,
    phone: str = Form(""),
    target_dialog_id: str = Form(""),
    target_title: str = Form(""),
    target_type: str = Form(""),
    caption: str = Form(""),
    manifest_text: str = Form(""),
):
    result = await handle_photo_batch(
        request,
        PhotoBatchForm(
            phone=phone,
            target_dialog_id=target_dialog_id,
            target_title=target_title,
            target_type=target_type,
            caption=caption,
            manifest_text=manifest_text,
        ),
    )
    return photo_loader_response(request, result)


@router.post("/auto")
async def photo_auto_create(
    request: Request,
    phone: str = Form(""),
    target_dialog_id: str = Form(""),
    target_title: str = Form(""),
    target_type: str = Form(""),
    folder_path: str = Form(""),
    send_mode: str = Form(PhotoSendMode.SEPARATE.value),
    caption: str = Form(""),
    interval_minutes: int | None = Form(None),
):
    result = await handle_photo_auto_create(
        request,
        PhotoAutoCreateForm(
            phone=phone,
            target_dialog_id=target_dialog_id,
            target_title=target_title,
            target_type=target_type,
            folder_path=folder_path,
            send_mode=send_mode,
            caption=caption,
            interval_minutes=interval_minutes,
        ),
    )
    return photo_loader_response(request, result)


@router.post("/run-due")
async def photo_run_due(request: Request, phone: str = Form(""), dry_run: bool = Form(False)):
    result = await handle_photo_run_due(request, PhotoPhoneForm(phone=phone, dry_run=dry_run))
    return photo_loader_response(request, result)


@router.post("/batches/{batch_id}/publish")
async def photo_publish_batch(request: Request, batch_id: int, phone: str = Form("")):
    result = await handle_photo_publish_batch(request, batch_id, PhotoPhoneForm(phone=phone))
    return photo_loader_response(request, result)


@router.post("/items/{item_id}/cancel")
async def photo_cancel_item(request: Request, item_id: int, phone: str = Form("")):
    result = await handle_photo_cancel_item(request, item_id, PhotoPhoneForm(phone=phone))
    return photo_loader_response(request, result)


@router.post("/auto/{job_id}/toggle")
async def photo_toggle_auto(request: Request, job_id: int, phone: str = Form("")):
    result = await handle_photo_toggle_auto(request, job_id, PhotoPhoneForm(phone=phone))
    return photo_loader_response(request, result)


@router.post("/auto/{job_id}/update")
async def photo_update_auto(request: Request, job_id: int):
    return photo_loader_response(request, await handle_photo_update_auto(request, job_id))


@router.post("/auto/{job_id}/delete")
async def photo_delete_auto(request: Request, job_id: int, phone: str = Form("")):
    result = await handle_photo_delete_auto(request, job_id, PhotoPhoneForm(phone=phone))
    return photo_loader_response(request, result)


@router.get("/batches", response_class=JSONResponse)
async def list_photo_batches(request: Request, limit: int = 50):
    """List photo batches as JSON (parity with CLI `photo-loader batch-list`)."""
    batches = await deps.get_photo_task_service(request).list_batches(limit=limit)
    return JSONResponse([b.model_dump(mode="json") for b in batches])


@router.get("/auto", response_class=JSONResponse)
async def list_auto_uploads(request: Request, active_only: bool = False):
    """List auto-upload jobs as JSON (parity with CLI `photo-loader auto-list`)."""
    jobs = await deps.get_photo_auto_upload_service(request).list_jobs(active_only=active_only)
    return JSONResponse([j.model_dump(mode="json") for j in jobs])


@router.get("/items", response_class=JSONResponse)
async def list_photo_items(request: Request, limit: int = 100):
    """List photo batch items as JSON (parity with CLI `photo-loader items`)."""
    items = await deps.get_photo_task_service(request).list_items(limit=limit)
    return JSONResponse([i.model_dump(mode="json") for i in items])
