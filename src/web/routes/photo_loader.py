from __future__ import annotations

import json
import logging
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
logger = logging.getLogger(__name__)

UPLOAD_ROOT = Path("data/photo_uploads")


def _redirect(
    phone: str,
    code: str,
    error: bool = False,
    *,
    command_id: int | None = None,
) -> RedirectResponse:
    key = "error" if error else "msg"
    suffix = f"&command_id={command_id}" if command_id is not None else ""
    return RedirectResponse(
        url=f"/dialogs/photos?phone={quote(phone, safe='')}&{key}={code}{suffix}",
        status_code=303,
    )


async def _persist_uploads(files: list[UploadFile], folder_name: str) -> list[str]:
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


async def _validate_target(
    request: Request,
    *,
    phone: str,
    target_dialog_id: str,
    target_title: str = "",
    target_type: str = "",
) -> tuple[PhotoTarget | None, str | None]:
    raw_id = target_dialog_id.strip()
    if not raw_id:
        return None, "photo_target_required"
    try:
        dialog_id = int(raw_id)
    except ValueError:
        return None, "photo_target_invalid"

    dialogs = await deps.channel_service(request).get_my_dialogs(phone)
    dialog = next(
        (item for item in dialogs if int(item["channel_id"]) == dialog_id),
        None,
    )
    if dialog is None:
        return None, "photo_target_invalid"
    if str(dialog.get("channel_type", "")).strip() == "bot":
        return None, "photo_target_invalid"
    target = _parse_target(
        {
            "target_dialog_id": str(dialog_id),
            "target_title": target_title,
            "target_type": target_type,
        },
        dialogs,
    )
    if target.title is None or target.target_type is None:
        return None, "photo_target_invalid"
    return target, None


def _parse_schedule_at(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.astimezone()
    return dt.astimezone(timezone.utc)


def _target_label(target_title: str | None, target_dialog_id: int | None) -> str | None:
    if target_title:
        return target_title
    if target_dialog_id is not None:
        return str(target_dialog_id)
    return None


def _build_feedback(
    msg: str | None,
    error: str | None,
    *,
    batches,
    items,
    auto_jobs,
) -> dict | None:
    if msg == "photo_sent":
        item = items[0] if items else None
        target = _target_label(
            getattr(item, "target_title", None),
            getattr(item, "target_dialog_id", None),
        )
        body = "Фото успешно отправлены."
        if target:
            body = f"Фото успешно отправлены в {target}."
        return {
            "variant": "success",
            "title": "Отправка завершена",
            "body": body + " Свежий результат показан в списке photo items ниже.",
            "highlight_kind": "item",
        }

    if msg == "photo_scheduled":
        item = items[0] if items else None
        target = _target_label(
            getattr(item, "target_title", None),
            getattr(item, "target_dialog_id", None),
        )
        body = "Отложенная отправка создана."
        if target:
            body = f"Отложенная отправка создана для {target}."
        return {
            "variant": "success",
            "title": "Отложка создана",
            "body": body + " Свежий item подсвечен ниже.",
            "highlight_kind": "item",
        }

    if msg == "photo_batch_created":
        batch = batches[0] if batches else None
        target = _target_label(
            getattr(batch, "target_title", None),
            getattr(batch, "target_dialog_id", None),
        )
        body = "Batch photo tasks создан."
        if target:
            body = f"Batch photo tasks создан для {target}."
        return {
            "variant": "success",
            "title": "Batch создан",
            "body": body + " Свежий batch подсвечен ниже.",
            "highlight_kind": "batch",
        }

    if msg == "photo_auto_created":
        job = auto_jobs[0] if auto_jobs else None
        target = _target_label(
            getattr(job, "target_title", None),
            getattr(job, "target_dialog_id", None),
        )
        body = "Авто-джоб создан."
        if target:
            body = f"Авто-джоб создан для {target}."
        return {
            "variant": "success",
            "title": "Авто-загрузка настроена",
            "body": body + " Свежий auto job подсвечен ниже.",
            "highlight_kind": "auto",
        }

    if error == "photo_send_failed":
        return {
            "variant": "error",
            "title": "Отправка не выполнена",
            "body": "Не удалось отправить фото. Проверьте аккаунт, диалог и логи сервера.",
            "highlight_kind": "",
        }

    if error == "photo_target_required":
        return {
            "variant": "error",
            "title": "Цель не выбрана",
            "body": "Сначала выберите канал, чат или личный диалог.",
            "highlight_kind": "",
        }

    if error == "photo_target_invalid":
        return {
            "variant": "error",
            "title": "Цель недоступна",
            "body": "Выбранная цель недоступна. Выберите её заново.",
            "highlight_kind": "",
        }

    if error == "photo_schedule_failed":
        return {
            "variant": "error",
            "title": "Отложка не создана",
            "body": "Не удалось создать отложенную отправку. Проверьте дату, аккаунт и логи.",
            "highlight_kind": "",
        }

    if error == "photo_batch_failed":
        return {
            "variant": "error",
            "title": "Batch не создан",
            "body": "Не удалось создать batch photo tasks. Проверьте manifest и логи сервера.",
            "highlight_kind": "",
        }

    if error == "photo_auto_failed":
        return {
            "variant": "error",
            "title": "Авто-загрузка не создана",
            "body": "Не удалось создать auto job. Проверьте папку, аккаунт и логи сервера.",
            "highlight_kind": "",
        }

    return None


@router.get("", response_class=HTMLResponse)
async def photo_loader_page(request: Request, phone: str | None = None):
    pool = deps.get_pool(request)
    accounts = sorted(pool.clients.keys())
    selected_phone = phone if phone in pool.clients else (accounts[0] if accounts else None)
    msg = request.query_params.get("msg")
    error = request.query_params.get("error")
    dialogs = []
    dialogs_cached_at = None
    if selected_phone:
        dialogs = await deps.channel_service(request).get_my_dialogs(selected_phone)
        dialogs_cached_at = await deps.get_db(request).repos.dialog_cache.get_cached_at(
            selected_phone
        )
    batches = await deps.get_photo_task_service(request).list_batches(limit=20)
    items = await deps.get_photo_task_service(request).list_items(limit=20)
    auto_jobs = await deps.get_photo_auto_upload_service(request).list_jobs()
    photo_feedback = _build_feedback(
        msg,
        error,
        batches=batches,
        items=items,
        auto_jobs=auto_jobs,
    )
    return deps.get_templates(request).TemplateResponse(
        request,
        "photo_loader.html",
        {
            "accounts": accounts,
            "selected_phone": selected_phone,
            "dialogs": dialogs,
            "dialogs_cached_at": dialogs_cached_at,
            "batches": batches,
            "items": items,
            "auto_jobs": auto_jobs,
            "photo_feedback": photo_feedback,
        },
    )


@router.post("/refresh")
async def photo_loader_refresh(request: Request, phone: str = Form("")):
    if not phone:
        return _redirect("", "missing_fields", error=True)
    command_id = await deps.telegram_command_service(request).enqueue(
        "dialogs.refresh",
        payload={"phone": phone},
        requested_by="web:photo_loader.refresh",
    )
    return _redirect(phone, "dialogs_refresh_queued", command_id=command_id)


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
    if not phone:
        return _redirect("", "missing_fields", error=True)
    target = None
    saved: list[str] = []
    try:
        target, target_error = await _validate_target(
            request,
            phone=phone,
            target_dialog_id=target_dialog_id,
            target_title=target_title,
            target_type=target_type,
        )
        if target_error:
            return _redirect(phone, target_error, error=True)
        saved = await _persist_uploads(photos, f"manual_{uuid.uuid4().hex}")
        command_id = await deps.telegram_command_service(request).enqueue(
            "photo.send_now",
            payload={
                "phone": phone,
                "target_dialog_id": target.dialog_id,
                "target_title": target.title,
                "target_type": target.target_type,
                "file_paths": saved,
                "mode": send_mode,
                "caption": caption or None,
            },
            requested_by="web:photo_loader.send",
        )
    except Exception:
        logger.exception(
            "Photo send failed: phone=%s target_dialog_id=%s target_title=%r "
            "target_type=%r send_mode=%s files=%d",
            phone,
            target_dialog_id,
            getattr(target, "title", None),
            getattr(target, "target_type", None),
            send_mode,
            len(saved),
        )
        return _redirect(phone, "photo_send_failed", error=True)
    return _redirect(phone, "photo_send_queued", command_id=command_id)


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
    if not phone or not schedule_at:
        return _redirect(phone, "missing_fields", error=True)
    target = None
    saved: list[str] = []
    try:
        target, target_error = await _validate_target(
            request,
            phone=phone,
            target_dialog_id=target_dialog_id,
            target_title=target_title,
            target_type=target_type,
        )
        if target_error:
            return _redirect(phone, target_error, error=True)
        saved = await _persist_uploads(photos, f"scheduled_{uuid.uuid4().hex}")
        parsed_schedule_at = _parse_schedule_at(schedule_at)
        command_id = await deps.telegram_command_service(request).enqueue(
            "photo.schedule_send",
            payload={
                "phone": phone,
                "target_dialog_id": target.dialog_id,
                "target_title": target.title,
                "target_type": target.target_type,
                "file_paths": saved,
                "mode": send_mode,
                "schedule_at": parsed_schedule_at.isoformat(),
                "caption": caption or None,
            },
            requested_by="web:photo_loader.schedule",
        )
    except Exception:
        logger.exception(
            "Photo schedule failed: phone=%s target_dialog_id=%s target_title=%r "
            "target_type=%r send_mode=%s files=%d schedule_at=%r",
            phone,
            target_dialog_id,
            getattr(target, "title", None),
            getattr(target, "target_type", None),
            send_mode,
            len(saved),
            schedule_at,
        )
        return _redirect(phone, "photo_schedule_failed", error=True)
    return _redirect(phone, "photo_schedule_queued", command_id=command_id)


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
    if not phone:
        return _redirect("", "missing_fields", error=True)
    target = None
    try:
        target, target_error = await _validate_target(
            request,
            phone=phone,
            target_dialog_id=target_dialog_id,
            target_title=target_title,
            target_type=target_type,
        )
        if target_error:
            return _redirect(phone, target_error, error=True)
        manifest = json.loads(manifest_text)
        await deps.get_photo_task_service(request).create_batch(
            phone=phone,
            target=target,
            entries=manifest,
            caption=caption or None,
        )
    except Exception:
        manifest_size = len(manifest) if isinstance(locals().get("manifest"), list) else None
        logger.exception(
            "Photo batch creation failed: phone=%s target_dialog_id=%s target_title=%r "
            "target_type=%r manifest_entries=%s",
            phone,
            target_dialog_id,
            getattr(target, "title", None),
            getattr(target, "target_type", None),
            manifest_size,
        )
        return _redirect(phone, "photo_batch_failed", error=True)
    return _redirect(phone, "photo_batch_created")


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
    if not phone or not folder_path or interval_minutes is None:
        return _redirect(phone, "missing_fields", error=True)
    target = None
    try:
        target, target_error = await _validate_target(
            request,
            phone=phone,
            target_dialog_id=target_dialog_id,
            target_title=target_title,
            target_type=target_type,
        )
        if target_error:
            return _redirect(phone, target_error, error=True)
        await deps.get_photo_auto_upload_service(request).create_job(
            PhotoAutoUploadJob(
                phone=phone,
                target_dialog_id=target.dialog_id,
                target_title=target.title,
                target_type=target.target_type,
                folder_path=folder_path,
                send_mode=PhotoSendMode(send_mode),
                caption=caption or None,
                interval_minutes=interval_minutes,
                is_active=True,
            )
        )
    except Exception:
        logger.exception(
            "Photo auto job creation failed: phone=%s target_dialog_id=%s target_title=%r "
            "target_type=%r folder_path=%r send_mode=%s interval_minutes=%s",
            phone,
            target_dialog_id,
            getattr(target, "title", None),
            getattr(target, "target_type", None),
            folder_path,
            send_mode,
            interval_minutes,
        )
        return _redirect(phone, "photo_auto_failed", error=True)
    return _redirect(phone, "photo_auto_created")


@router.post("/run-due")
async def photo_run_due(request: Request, phone: str = Form("")):
    try:
        command_id = await deps.telegram_command_service(request).enqueue(
            "photo.run_due",
            payload={},
            requested_by="web:photo_loader.run_due",
        )
    except Exception:
        logger.exception("Photo run_due failed: phone=%s", phone)
        return _redirect(phone, "photo_run_due_failed", error=True)
    return _redirect(phone, "photo_run_due_queued", command_id=command_id)


@router.post("/items/{item_id}/cancel")
async def photo_cancel_item(request: Request, item_id: int, phone: str = Form("")):
    ok = await deps.get_photo_task_service(request).cancel_item(item_id)
    code = "photo_item_cancelled" if ok else "photo_item_cancel_failed"
    return _redirect(phone, code, error=not ok)


@router.post("/auto/{job_id}/toggle")
async def photo_toggle_auto(request: Request, job_id: int, phone: str = Form("")):
    service = deps.get_photo_auto_upload_service(request)
    job = await service.get_job(job_id)
    if job is None:
        return _redirect(phone, "photo_auto_failed", error=True)
    await service.update_job(job_id, is_active=not job.is_active)
    return _redirect(phone, "photo_auto_toggled")


@router.post("/auto/{job_id}/update")
async def photo_update_auto(request: Request, job_id: int):
    form = await request.form()
    phone = form.get("phone", "")
    service = deps.get_photo_auto_upload_service(request)
    job = await service.get_job(job_id)
    if job is None:
        return _redirect(phone, "photo_auto_failed", error=True)
    kwargs: dict = {}
    if form.get("folder"):
        kwargs["folder_path"] = form["folder"]
    if form.get("mode"):
        from src.models import PhotoSendMode

        kwargs["send_mode"] = PhotoSendMode(form["mode"])
    if form.get("caption") is not None:
        kwargs["caption"] = form["caption"]
    interval = form.get("interval_minutes")
    if interval and str(interval).isdigit():
        kwargs["interval_minutes"] = int(interval)
    if form.get("is_active"):
        kwargs["is_active"] = form["is_active"] in ("1", "true", "on")
    await service.update_job(job_id, **kwargs)
    return _redirect(phone, "photo_auto_updated")


@router.post("/auto/{job_id}/delete")
async def photo_delete_auto(request: Request, job_id: int, phone: str = Form("")):
    await deps.get_photo_auto_upload_service(request).delete_job(job_id)
    return _redirect(phone, "photo_auto_deleted")
