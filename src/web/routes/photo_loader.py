from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse

from src.models import PhotoAutoUploadJob, PhotoSendMode
from src.services.photo_task_service import PhotoTarget
from src.web import deps

router = APIRouter()

UPLOAD_ROOT = Path("data/photo_uploads")


def _redirect(phone: str, code: str, error: bool = False) -> RedirectResponse:
    key = "error" if error else "msg"
    return RedirectResponse(
        url=f"/my-telegram/photos?phone={quote(phone, safe='')}&{key}={code}",
        status_code=303,
    )


def _ensure_upload_root() -> None:
    UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)


async def _persist_uploads(files: list[UploadFile], folder_name: str) -> list[str]:
    _ensure_upload_root()
    target_dir = UPLOAD_ROOT / folder_name
    target_dir.mkdir(parents=True, exist_ok=True)
    stored: list[str] = []
    for upload in files:
        if not upload.filename:
            continue
        data = await upload.read()
        if not data:
            continue
        safe_name = f"{uuid.uuid4().hex}_{Path(upload.filename).name}"
        path = target_dir / safe_name
        path.write_bytes(data)
        stored.append(str(path))
    return stored


def _parse_target(form, dialogs: list[dict]) -> PhotoTarget:
    dialog_id = int(str(form.get("target_dialog_id", "0")))
    title = str(form.get("target_title", "")).strip() or None
    target_type = str(form.get("target_type", "")).strip() or None
    if not title or not target_type:
        for dialog in dialogs:
            if int(dialog["channel_id"]) == dialog_id:
                title = dialog.get("title")
                target_type = dialog.get("channel_type")
                break
    return PhotoTarget(dialog_id=dialog_id, title=title, target_type=target_type)


def _parse_schedule_at(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.astimezone()
    return dt.astimezone(timezone.utc)


@router.get("", response_class=HTMLResponse)
async def photo_loader_page(request: Request, phone: str | None = None):
    pool = deps.get_pool(request)
    accounts = sorted(pool.clients.keys())
    selected_phone = phone if phone in pool.clients else (accounts[0] if accounts else None)
    dialogs = []
    if selected_phone:
        dialogs = await deps.channel_service(request).get_my_dialogs(selected_phone)
    batches = await deps.get_photo_task_service(request).list_batches(limit=20)
    items = await deps.get_photo_task_service(request).list_items(limit=20)
    auto_jobs = await deps.get_photo_auto_upload_service(request).list_jobs()
    return deps.get_templates(request).TemplateResponse(
        request,
        "photo_loader.html",
        {
            "accounts": accounts,
            "selected_phone": selected_phone,
            "dialogs": dialogs,
            "batches": batches,
            "items": items,
            "auto_jobs": auto_jobs,
        },
    )


@router.post("/send")
async def photo_send(
    request: Request,
    phone: str = Form(...),
    target_dialog_id: int = Form(...),
    target_title: str = Form(""),
    target_type: str = Form(""),
    send_mode: str = Form(PhotoSendMode.ALBUM.value),
    caption: str = Form(""),
    photos: list[UploadFile] = File(...),
):
    target = PhotoTarget(
        dialog_id=target_dialog_id,
        title=target_title or None,
        target_type=target_type or None,
    )
    saved = await _persist_uploads(photos, f"manual_{uuid.uuid4().hex}")
    try:
        await deps.get_photo_task_service(request).send_now(
            phone=phone,
            target=target,
            file_paths=saved,
            mode=send_mode,
            caption=caption or None,
        )
    except Exception:
        return _redirect(phone, "photo_send_failed", error=True)
    return _redirect(phone, "photo_sent")


@router.post("/schedule")
async def photo_schedule(
    request: Request,
    phone: str = Form(...),
    target_dialog_id: int = Form(...),
    target_title: str = Form(""),
    target_type: str = Form(""),
    send_mode: str = Form(PhotoSendMode.ALBUM.value),
    caption: str = Form(""),
    schedule_at: str = Form(...),
    photos: list[UploadFile] = File(...),
):
    target = PhotoTarget(
        dialog_id=target_dialog_id,
        title=target_title or None,
        target_type=target_type or None,
    )
    saved = await _persist_uploads(photos, f"scheduled_{uuid.uuid4().hex}")
    try:
        await deps.get_photo_task_service(request).schedule_send(
            phone=phone,
            target=target,
            file_paths=saved,
            mode=send_mode,
            schedule_at=_parse_schedule_at(schedule_at),
            caption=caption or None,
        )
    except Exception:
        return _redirect(phone, "photo_schedule_failed", error=True)
    return _redirect(phone, "photo_scheduled")


@router.post("/batch")
async def photo_batch(
    request: Request,
    phone: str = Form(...),
    target_dialog_id: int = Form(...),
    target_title: str = Form(""),
    target_type: str = Form(""),
    caption: str = Form(""),
    manifest_text: str = Form(""),
):
    target = PhotoTarget(
        dialog_id=target_dialog_id,
        title=target_title or None,
        target_type=target_type or None,
    )
    try:
        manifest = json.loads(manifest_text)
        await deps.get_photo_task_service(request).create_batch(
            phone=phone,
            target=target,
            entries=manifest,
            caption=caption or None,
        )
    except Exception:
        return _redirect(phone, "photo_batch_failed", error=True)
    return _redirect(phone, "photo_batch_created")


@router.post("/auto")
async def photo_auto_create(
    request: Request,
    phone: str = Form(...),
    target_dialog_id: int = Form(...),
    target_title: str = Form(""),
    target_type: str = Form(""),
    folder_path: str = Form(...),
    send_mode: str = Form(PhotoSendMode.ALBUM.value),
    caption: str = Form(""),
    interval_minutes: int = Form(...),
):
    try:
        await deps.get_photo_auto_upload_service(request).create_job(
            PhotoAutoUploadJob(
                phone=phone,
                target_dialog_id=target_dialog_id,
                target_title=target_title or None,
                target_type=target_type or None,
                folder_path=folder_path,
                send_mode=PhotoSendMode(send_mode),
                caption=caption or None,
                interval_minutes=interval_minutes,
                is_active=True,
            )
        )
    except Exception:
        return _redirect(phone, "photo_auto_failed", error=True)
    return _redirect(phone, "photo_auto_created")


@router.post("/run-due")
async def photo_run_due(request: Request, phone: str = Form("")):
    try:
        await deps.get_photo_task_service(request).run_due()
        await deps.get_photo_auto_upload_service(request).run_due()
    except Exception:
        return _redirect(phone, "photo_run_due_failed", error=True)
    return _redirect(phone, "photo_run_due_ok")


@router.post("/items/{item_id}/cancel")
async def photo_cancel_item(request: Request, item_id: int, phone: str = Form("")):
    ok = await deps.get_photo_task_service(request).cancel_item(item_id)
    return _redirect(phone, "photo_item_cancelled" if ok else "photo_item_cancel_failed", error=not ok)


@router.post("/auto/{job_id}/toggle")
async def photo_toggle_auto(request: Request, job_id: int, phone: str = Form("")):
    service = deps.get_photo_auto_upload_service(request)
    job = await service.get_job(job_id)
    if job is None:
        return _redirect(phone, "photo_auto_failed", error=True)
    await service.update_job(job_id, is_active=not job.is_active)
    return _redirect(phone, "photo_auto_toggled")


@router.post("/auto/{job_id}/delete")
async def photo_delete_auto(request: Request, job_id: int, phone: str = Form("")):
    await deps.get_photo_auto_upload_service(request).delete_job(job_id)
    return _redirect(phone, "photo_auto_deleted")
