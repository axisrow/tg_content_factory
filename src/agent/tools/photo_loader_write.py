from __future__ import annotations

from typing import Any

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._photo_loader_runtime import (
    photo_auto_upload_service,
    photo_task_service,
    resolve_photo_target_id,
    split_file_paths,
)
from src.agent.tools._registry import (
    _text_response,
    require_confirmation,
    require_phone_permission,
    require_pool,
    resolve_phone,
)
from src.agent.tools.photo_loader_schemas import (
    CANCEL_PHOTO_ITEM_SCHEMA,
    CREATE_AUTO_UPLOAD_SCHEMA,
    CREATE_PHOTO_BATCH_SCHEMA,
    DELETE_AUTO_UPLOAD_SCHEMA,
    RUN_PHOTO_DUE_SCHEMA,
    SCHEDULE_PHOTOS_SCHEMA,
    SEND_PHOTOS_NOW_SCHEMA,
    TOGGLE_AUTO_UPLOAD_SCHEMA,
    UPDATE_AUTO_UPLOAD_SCHEMA,
)
from src.models import PhotoAutoUploadJob, PhotoSendMode
from src.services import photo_task_service as photo_task_module
from src.utils.datetime import parse_required_schedule_datetime


def register_send_tools(db: Any, client_pool: Any) -> list[Any]:
    tools: list[Any] = []

    @tool(
        "send_photos_now",
        "⚠️ Send photos to a Telegram dialog immediately. "
        "target = dialog_id from list_photo_dialogs (or 'me'). "
        "file_paths = comma-separated server-local paths. mode: album/separate. "
        "Ask for confirmation first.",
        SEND_PHOTOS_NOW_SCHEMA,
    )
    async def send_photos_now(args):
        pool_gate = require_pool(client_pool, "Отправка фото")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "send_photos_now")
        if perm_gate:
            return perm_gate
        gate = require_confirmation("отправит фото в Telegram-диалог", args)
        if gate:
            return gate
        try:
            svc = photo_task_service(db, client_pool)
            target = args.get("target", "")
            files = split_file_paths(args.get("file_paths", ""))
            mode = args.get("mode", "album")
            caption = args.get("caption")
            if not phone or not target or not files:
                return _text_response("Ошибка: phone, target и file_paths обязательны.")
            target_id = await resolve_photo_target_id(client_pool, phone, target)
            result = await svc.send_now(
                phone=phone,
                target=photo_task_module.PhotoTarget(dialog_id=target_id),
                file_paths=files,
                mode=mode,
                caption=caption,
            )
            return _text_response(f"Фото отправлены. Item id={result.id}, status={result.status}")
        except Exception as exc:
            return _text_response(f"Ошибка отправки фото: {exc}")

    tools.append(send_photos_now)

    @tool(
        "schedule_photos",
        "⚠️ Schedule photos to be sent at a specific time. "
        "target = dialog_id from list_photo_dialogs. "
        "schedule_at = ISO datetime (e.g. '2025-12-31T10:00:00'). "
        "file_paths = comma-separated server-local paths. Ask for confirmation first.",
        SCHEDULE_PHOTOS_SCHEMA,
    )
    async def schedule_photos(args):
        pool_gate = require_pool(client_pool, "Планирование фото")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "schedule_photos")
        if perm_gate:
            return perm_gate
        gate = require_confirmation("запланирует отправку фото", args)
        if gate:
            return gate
        try:
            svc = photo_task_service(db, client_pool)
            target = args.get("target", "")
            files = split_file_paths(args.get("file_paths", ""))
            schedule_at_str = args.get("schedule_at", "")
            mode = args.get("mode", "album")
            caption = args.get("caption")
            if not phone or not target or not files or not schedule_at_str:
                return _text_response("Ошибка: phone, target, file_paths и schedule_at обязательны.")
            schedule_at = parse_required_schedule_datetime(schedule_at_str)
            result = await svc.schedule_send(
                phone=phone,
                target=photo_task_module.PhotoTarget(dialog_id=int(target)),
                file_paths=files,
                mode=mode,
                schedule_at=schedule_at,
                caption=caption,
            )
            return _text_response(f"Фото запланированы на {schedule_at}. Item id={result.id}")
        except Exception as exc:
            return _text_response(f"Ошибка планирования фото: {exc}")

    tools.append(schedule_photos)

    @tool(
        "cancel_photo_item",
        "⚠️ Cancel a scheduled photo item. item_id from list_photo_items. Ask user for confirmation first.",
        CANCEL_PHOTO_ITEM_SCHEMA,
    )
    async def cancel_photo_item(args):
        item_id = args.get("item_id")
        if item_id is None:
            return _text_response("Ошибка: item_id обязателен.")
        gate = require_confirmation(f"отменит запланированное фото item_id={item_id}", args)
        if gate:
            return gate
        try:
            svc = photo_task_service(db, client_pool)
            ok = await svc.cancel_item(int(item_id))
            if ok:
                return _text_response(f"Фото item_id={item_id} отменено.")
            return _text_response(f"Не удалось отменить item_id={item_id} (возможно, уже отправлено).")
        except Exception as exc:
            return _text_response(f"Ошибка отмены фото: {exc}")

    tools.append(cancel_photo_item)
    return tools


def register_batch_write_tools(db: Any, client_pool: Any) -> list[Any]:
    tools: list[Any] = []

    @tool(
        "create_photo_batch",
        "⚠️ Create a photo batch for sending to a Telegram dialog. "
        "Params: phone, target (dialog_id), file_paths (comma-sep), caption. "
        "Ask user for confirmation first.",
        CREATE_PHOTO_BATCH_SCHEMA,
    )
    async def create_photo_batch(args):
        pool_gate = require_pool(client_pool, "Создание батча фото")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "create_photo_batch")
        if perm_gate:
            return perm_gate
        target = args.get("target", "")
        files = split_file_paths(args.get("file_paths", ""))
        caption = args.get("caption")
        if not target or not files:
            return _text_response("Ошибка: target и file_paths обязательны.")
        gate = require_confirmation(
            f"создаст батч фото для отправки: файлы={files}, target={target}", args
        )
        if gate:
            return gate
        try:
            svc = photo_task_service(db, client_pool)
            entries = [{"file_path": file_path} for file_path in files]
            batch_id = await svc.create_batch(
                phone=phone,
                target=photo_task_module.PhotoTarget(dialog_id=int(target)),
                entries=entries,
                caption=caption,
            )
            return _text_response(f"Батч создан: id={batch_id}")
        except Exception as exc:
            return _text_response(f"Ошибка создания батча: {exc}")

    tools.append(create_photo_batch)

    @tool(
        "run_photo_due",
        "⚠️ Process all due photo items and auto-upload jobs (sends to Telegram). "
        "Ask user for confirmation first.",
        RUN_PHOTO_DUE_SCHEMA,
    )
    async def run_photo_due(args):
        pool_gate = require_pool(client_pool, "Обработка фото")
        if pool_gate:
            return pool_gate
        gate = require_confirmation("отправит все запланированные фото в Telegram", args)
        if gate:
            return gate
        try:
            tasks_svc = photo_task_service(db, client_pool)
            auto_svc = photo_auto_upload_service(db, client_pool)
            items = await tasks_svc.run_due()
            jobs = await auto_svc.run_due()
            return _text_response(f"Обработано: items={items}, auto_jobs={jobs}")
        except Exception as exc:
            return _text_response(f"Ошибка обработки фото: {exc}")

    tools.append(run_photo_due)
    return tools


def register_auto_write_tools(db: Any, client_pool: Any) -> list[Any]:
    tools: list[Any] = []

    @tool(
        "toggle_auto_upload",
        "Toggle an auto-upload job active/paused. job_id from list_auto_uploads.",
        TOGGLE_AUTO_UPLOAD_SCHEMA,
    )
    async def toggle_auto_upload(args):
        job_id = args.get("job_id")
        if job_id is None:
            return _text_response("Ошибка: job_id обязателен.")
        try:
            svc = photo_auto_upload_service(db, client_pool)
            job = await svc.get_job(int(job_id))
            if job is None:
                return _text_response(f"Автозагрузка id={job_id} не найдена.")
            await svc.update_job(int(job_id), is_active=not job.is_active)
            status = "активирована" if not job.is_active else "приостановлена"
            return _text_response(f"Автозагрузка id={job_id} {status}.")
        except Exception as exc:
            return _text_response(f"Ошибка переключения автозагрузки: {exc}")

    tools.append(toggle_auto_upload)

    @tool(
        "delete_auto_upload",
        "⚠️ DANGEROUS: Delete an auto-upload job. job_id from list_auto_uploads. "
        "Always ask user for confirmation first.",
        DELETE_AUTO_UPLOAD_SCHEMA,
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def delete_auto_upload(args):
        job_id = args.get("job_id")
        if job_id is None:
            return _text_response("Ошибка: job_id обязателен.")
        gate = require_confirmation(f"удалит автозагрузку id={job_id}", args)
        if gate:
            return gate
        try:
            svc = photo_auto_upload_service(db, client_pool)
            await svc.delete_job(int(job_id))
            return _text_response(f"Автозагрузка id={job_id} удалена.")
        except Exception as exc:
            return _text_response(f"Ошибка удаления автозагрузки: {exc}")

    tools.append(delete_auto_upload)

    @tool(
        "create_auto_upload",
        "⚠️ Create an auto-upload job to send photos from a server-side folder on a schedule. "
        "target = dialog_id from list_photo_dialogs. mode: album/separate. Ask for confirmation first.",
        CREATE_AUTO_UPLOAD_SCHEMA,
    )
    async def create_auto_upload(args):
        pool_gate = require_pool(client_pool, "Создание автозагрузки")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "create_auto_upload")
        if perm_gate:
            return perm_gate
        folder_path = args.get("folder_path", "")
        target = args.get("target", "")
        interval = int(args.get("interval_minutes", 60))
        mode = args.get("mode", "album")
        caption = args.get("caption")
        if not target or not folder_path:
            return _text_response("Ошибка: target и folder_path обязательны.")
        gate = require_confirmation(
            f"создаст автозагрузку фото: folder={folder_path}, target={target}", args
        )
        if gate:
            return gate
        try:
            svc = photo_auto_upload_service(db, client_pool)
            job_id = await svc.create_job(
                PhotoAutoUploadJob(
                    phone=phone,
                    target_dialog_id=int(target),
                    folder_path=folder_path,
                    send_mode=PhotoSendMode(mode),
                    caption=caption,
                    interval_minutes=interval,
                )
            )
            return _text_response(f"Автозагрузка создана: id={job_id}")
        except Exception as exc:
            return _text_response(f"Ошибка создания автозагрузки: {exc}")

    tools.append(create_auto_upload)

    @tool(
        "update_auto_upload",
        "⚠️ Update an existing auto-upload job settings. job_id from list_auto_uploads. "
        "mode: album/separate. Ask user for confirmation first.",
        UPDATE_AUTO_UPLOAD_SCHEMA,
    )
    async def update_auto_upload(args):
        job_id = args.get("job_id")
        if job_id is None:
            return _text_response("Ошибка: job_id обязателен.")
        changes = []
        if args.get("folder_path"):
            changes.append(f"folder={args['folder_path']}")
        if args.get("mode"):
            changes.append(f"mode={args['mode']}")
        if args.get("interval_minutes") is not None:
            changes.append(f"interval={args['interval_minutes']}m")
        if args.get("is_active") is not None:
            changes.append(f"active={args['is_active']}")
        desc = f"обновит автозагрузку id={job_id}"
        if changes:
            desc += f" ({', '.join(changes)})"
        gate = require_confirmation(desc, args)
        if gate:
            return gate
        try:
            svc = photo_auto_upload_service(db, client_pool)
            existing = await svc.get_job(int(job_id))
            if existing is None:
                return _text_response(f"Автозагрузка id={job_id} не найдена.")
            mode_str = args.get("mode")
            await svc.update_job(
                int(job_id),
                folder_path=args.get("folder_path"),
                send_mode=PhotoSendMode(mode_str) if mode_str else None,
                caption=args.get("caption"),
                interval_minutes=args.get("interval_minutes"),
                is_active=args.get("is_active"),
            )
            return _text_response(f"Автозагрузка id={job_id} обновлена.")
        except Exception as exc:
            return _text_response(f"Ошибка обновления автозагрузки: {exc}")

    tools.append(update_auto_upload)
    return tools
