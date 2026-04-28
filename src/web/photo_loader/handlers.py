from __future__ import annotations

import json
import logging
import uuid

from fastapi import Request

from src.models import PhotoAutoUploadJob, PhotoSendMode
from src.services.photo_task_service import PhotoTarget
from src.web import deps
from src.web.photo_loader import forms
from src.web.photo_loader.forms import (
    PhotoAutoCreateForm,
    PhotoAutoUpdateForm,
    PhotoBatchForm,
    PhotoPhoneForm,
    PhotoRefreshForm,
    PhotoScheduleForm,
    PhotoSendForm,
)
from src.web.photo_loader.responses import PhotoLoaderRedirect

logger = logging.getLogger("src.web.routes.photo_loader")


def _target_label(target_title: str | None, target_dialog_id: int | None) -> str | None:
    if target_title:
        return target_title
    if target_dialog_id is not None:
        return str(target_dialog_id)
    return None


def build_feedback(
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


async def handle_photo_loader_page(request: Request, phone: str | None = None) -> dict:
    pool = deps.get_pool(request)
    accounts = sorted(pool.clients.keys())
    selected_phone = phone if phone in pool.clients else (accounts[0] if accounts else None)
    msg = request.query_params.get("msg")
    error = request.query_params.get("error")
    dialogs = []
    dialogs_cached_at = None
    if selected_phone:
        dialogs = await deps.channel_service(request).get_my_dialogs(selected_phone)
        dialogs_cached_at = await deps.get_db(request).repos.dialog_cache.get_cached_at(selected_phone)
    batches = await deps.get_photo_task_service(request).list_batches(limit=20)
    items = await deps.get_photo_task_service(request).list_items(limit=20)
    auto_jobs = await deps.get_photo_auto_upload_service(request).list_jobs()
    photo_feedback = build_feedback(
        msg,
        error,
        batches=batches,
        items=items,
        auto_jobs=auto_jobs,
    )
    return {
        "accounts": accounts,
        "selected_phone": selected_phone,
        "dialogs": dialogs,
        "dialogs_cached_at": dialogs_cached_at,
        "batches": batches,
        "items": items,
        "auto_jobs": auto_jobs,
        "photo_feedback": photo_feedback,
    }


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
    target = forms.parse_target(
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


async def handle_photo_loader_refresh(request: Request, form: PhotoRefreshForm) -> PhotoLoaderRedirect:
    if not form.phone:
        return PhotoLoaderRedirect("", "missing_fields", error=True)
    command_id = await deps.telegram_command_service(request).enqueue(
        "dialogs.refresh",
        payload={"phone": form.phone},
        requested_by="web:photo_loader.refresh",
    )
    return PhotoLoaderRedirect(form.phone, "dialogs_refresh_queued", command_id=command_id)


async def handle_photo_send(request: Request, form: PhotoSendForm) -> PhotoLoaderRedirect:
    if not form.phone:
        return PhotoLoaderRedirect("", "missing_fields", error=True)
    target = None
    saved: list[str] = []
    try:
        target, target_error = await _validate_target(
            request,
            phone=form.phone,
            target_dialog_id=form.target_dialog_id,
            target_title=form.target_title,
            target_type=form.target_type,
        )
        if target_error:
            return PhotoLoaderRedirect(form.phone, target_error, error=True)
        saved = await forms.persist_uploads(form.photos, f"manual_{uuid.uuid4().hex}")
        command_id = await deps.telegram_command_service(request).enqueue(
            "photo.send_now",
            payload={
                "phone": form.phone,
                "target_dialog_id": target.dialog_id,
                "target_title": target.title,
                "target_type": target.target_type,
                "file_paths": saved,
                "mode": form.send_mode,
                "caption": form.caption or None,
            },
            requested_by="web:photo_loader.send",
        )
    except Exception:
        logger.exception(
            "Photo send failed: phone=%s target_dialog_id=%s target_title=%r "
            "target_type=%r send_mode=%s files=%d",
            form.phone,
            form.target_dialog_id,
            getattr(target, "title", None),
            getattr(target, "target_type", None),
            form.send_mode,
            len(saved),
        )
        return PhotoLoaderRedirect(form.phone, "photo_send_failed", error=True)
    return PhotoLoaderRedirect(form.phone, "photo_send_queued", command_id=command_id)


async def handle_photo_schedule(request: Request, form: PhotoScheduleForm) -> PhotoLoaderRedirect:
    if not form.phone or not form.schedule_at:
        return PhotoLoaderRedirect(form.phone, "missing_fields", error=True)
    target = None
    saved: list[str] = []
    try:
        target, target_error = await _validate_target(
            request,
            phone=form.phone,
            target_dialog_id=form.target_dialog_id,
            target_title=form.target_title,
            target_type=form.target_type,
        )
        if target_error:
            return PhotoLoaderRedirect(form.phone, target_error, error=True)
        saved = await forms.persist_uploads(form.photos, f"scheduled_{uuid.uuid4().hex}")
        parsed_schedule_at = forms.parse_schedule_at(form.schedule_at)
        command_id = await deps.telegram_command_service(request).enqueue(
            "photo.schedule_send",
            payload={
                "phone": form.phone,
                "target_dialog_id": target.dialog_id,
                "target_title": target.title,
                "target_type": target.target_type,
                "file_paths": saved,
                "mode": form.send_mode,
                "schedule_at": parsed_schedule_at.isoformat(),
                "caption": form.caption or None,
            },
            requested_by="web:photo_loader.schedule",
        )
    except Exception:
        logger.exception(
            "Photo schedule failed: phone=%s target_dialog_id=%s target_title=%r "
            "target_type=%r send_mode=%s files=%d schedule_at=%r",
            form.phone,
            form.target_dialog_id,
            getattr(target, "title", None),
            getattr(target, "target_type", None),
            form.send_mode,
            len(saved),
            form.schedule_at,
        )
        return PhotoLoaderRedirect(form.phone, "photo_schedule_failed", error=True)
    return PhotoLoaderRedirect(form.phone, "photo_schedule_queued", command_id=command_id)


async def handle_photo_batch(request: Request, form: PhotoBatchForm) -> PhotoLoaderRedirect:
    if not form.phone:
        return PhotoLoaderRedirect("", "missing_fields", error=True)
    target = None
    try:
        target, target_error = await _validate_target(
            request,
            phone=form.phone,
            target_dialog_id=form.target_dialog_id,
            target_title=form.target_title,
            target_type=form.target_type,
        )
        if target_error:
            return PhotoLoaderRedirect(form.phone, target_error, error=True)
        manifest = json.loads(form.manifest_text)
        await deps.get_photo_task_service(request).create_batch(
            phone=form.phone,
            target=target,
            entries=manifest,
            caption=form.caption or None,
        )
    except Exception:
        manifest_size = len(manifest) if isinstance(locals().get("manifest"), list) else None
        logger.exception(
            "Photo batch creation failed: phone=%s target_dialog_id=%s target_title=%r "
            "target_type=%r manifest_entries=%s",
            form.phone,
            form.target_dialog_id,
            getattr(target, "title", None),
            getattr(target, "target_type", None),
            manifest_size,
        )
        return PhotoLoaderRedirect(form.phone, "photo_batch_failed", error=True)
    return PhotoLoaderRedirect(form.phone, "photo_batch_created")


async def handle_photo_auto_create(request: Request, form: PhotoAutoCreateForm) -> PhotoLoaderRedirect:
    if not form.phone or not form.folder_path or form.interval_minutes is None:
        return PhotoLoaderRedirect(form.phone, "missing_fields", error=True)
    target = None
    try:
        target, target_error = await _validate_target(
            request,
            phone=form.phone,
            target_dialog_id=form.target_dialog_id,
            target_title=form.target_title,
            target_type=form.target_type,
        )
        if target_error:
            return PhotoLoaderRedirect(form.phone, target_error, error=True)
        await deps.get_photo_auto_upload_service(request).create_job(
            PhotoAutoUploadJob(
                phone=form.phone,
                target_dialog_id=target.dialog_id,
                target_title=target.title,
                target_type=target.target_type,
                folder_path=form.folder_path,
                send_mode=PhotoSendMode(form.send_mode),
                caption=form.caption or None,
                interval_minutes=form.interval_minutes,
                is_active=True,
            )
        )
    except Exception:
        logger.exception(
            "Photo auto job creation failed: phone=%s target_dialog_id=%s target_title=%r "
            "target_type=%r folder_path=%r send_mode=%s interval_minutes=%s",
            form.phone,
            form.target_dialog_id,
            getattr(target, "title", None),
            getattr(target, "target_type", None),
            form.folder_path,
            form.send_mode,
            form.interval_minutes,
        )
        return PhotoLoaderRedirect(form.phone, "photo_auto_failed", error=True)
    return PhotoLoaderRedirect(form.phone, "photo_auto_created")


async def handle_photo_run_due(request: Request, form: PhotoPhoneForm) -> PhotoLoaderRedirect:
    try:
        command_id = await deps.telegram_command_service(request).enqueue(
            "photo.run_due",
            payload={},
            requested_by="web:photo_loader.run_due",
        )
    except Exception:
        logger.exception("Photo run_due failed: phone=%s", form.phone)
        return PhotoLoaderRedirect(form.phone, "photo_run_due_failed", error=True)
    return PhotoLoaderRedirect(form.phone, "photo_run_due_queued", command_id=command_id)


async def handle_photo_cancel_item(request: Request, item_id: int, form: PhotoPhoneForm) -> PhotoLoaderRedirect:
    ok = await deps.get_photo_task_service(request).cancel_item(item_id)
    code = "photo_item_cancelled" if ok else "photo_item_cancel_failed"
    return PhotoLoaderRedirect(form.phone, code, error=not ok)


async def handle_photo_toggle_auto(request: Request, job_id: int, form: PhotoPhoneForm) -> PhotoLoaderRedirect:
    service = deps.get_photo_auto_upload_service(request)
    job = await service.get_job(job_id)
    if job is None:
        return PhotoLoaderRedirect(form.phone, "photo_auto_failed", error=True)
    await service.update_job(job_id, is_active=not job.is_active)
    return PhotoLoaderRedirect(form.phone, "photo_auto_toggled")


async def handle_photo_update_auto(
    request: Request,
    job_id: int,
    form: PhotoAutoUpdateForm,
) -> PhotoLoaderRedirect:
    service = deps.get_photo_auto_upload_service(request)
    job = await service.get_job(job_id)
    if job is None:
        return PhotoLoaderRedirect(form.phone, "photo_auto_failed", error=True)
    await service.update_job(job_id, **form.values)
    return PhotoLoaderRedirect(form.phone, "photo_auto_updated")


async def handle_photo_delete_auto(request: Request, job_id: int, form: PhotoPhoneForm) -> PhotoLoaderRedirect:
    await deps.get_photo_auto_upload_service(request).delete_job(job_id)
    return PhotoLoaderRedirect(form.phone, "photo_auto_deleted")

