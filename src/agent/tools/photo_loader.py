"""Agent tools for photo upload and scheduling."""

from __future__ import annotations

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._registry import _text_response, require_confirmation, require_pool


def register(db, client_pool, embedding_service, **kwargs):
    tools = []

    @tool("list_photo_batches", "List photo upload batches", {"limit": int})
    async def list_photo_batches(args):
        try:
            from src.database.bundles import PhotoLoaderBundle
            from src.services.photo_publish_service import PhotoPublishService
            from src.services.photo_task_service import PhotoTaskService

            svc = PhotoTaskService(PhotoLoaderBundle.from_database(db), PhotoPublishService(client_pool))
            limit = int(args.get("limit", 50))
            batches = await svc.list_batches(limit=limit)
            if not batches:
                return _text_response("Батчи фото не найдены.")
            lines = [f"Батчи фото ({len(batches)}):"]
            for b in batches:
                lines.append(
                    f"- batch_id={b.id}, phone={b.phone}, target={b.target_dialog_id}, "
                    f"status={b.status}, items={b.total_items}, created={b.created_at}"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения батчей: {e}")

    tools.append(list_photo_batches)

    @tool("list_photo_items", "List photo batch items with status", {"limit": int})
    async def list_photo_items(args):
        try:
            from src.database.bundles import PhotoLoaderBundle
            from src.services.photo_publish_service import PhotoPublishService
            from src.services.photo_task_service import PhotoTaskService

            svc = PhotoTaskService(PhotoLoaderBundle.from_database(db), PhotoPublishService(client_pool))
            limit = int(args.get("limit", 100))
            items = await svc.list_items(limit=limit)
            if not items:
                return _text_response("Элементы батчей не найдены.")
            lines = [f"Элементы ({len(items)}):"]
            for item in items:
                lines.append(
                    f"- item_id={item.id}, batch={item.batch_id}, "
                    f"status={item.status}, scheduled={item.scheduled_at}"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения элементов: {e}")

    tools.append(list_photo_items)

    @tool(
        "send_photos_now",
        "⚠️ Send photos to a Telegram dialog immediately. "
        "Params: phone, target (dialog_id), file_paths (comma-sep), mode (album/separate), caption. "
        "Ask user for confirmation first.",
        {"phone": str, "target": str, "file_paths": str, "mode": str, "caption": str, "confirm": bool},
    )
    async def send_photos_now(args):
        pool_gate = require_pool(client_pool, "Отправка фото")
        if pool_gate:
            return pool_gate
        gate = require_confirmation("отправит фото в Telegram-диалог", args)
        if gate:
            return gate
        try:
            from src.database.bundles import PhotoLoaderBundle
            from src.services.photo_publish_service import PhotoPublishService
            from src.services.photo_task_service import PhotoTaskService

            svc = PhotoTaskService(PhotoLoaderBundle.from_database(db), PhotoPublishService(client_pool))
            phone = args.get("phone", "")
            target = args.get("target", "")
            files = [f.strip() for f in args.get("file_paths", "").split(",") if f.strip()]
            mode = args.get("mode", "album")
            caption = args.get("caption")
            if not phone or not target or not files:
                return _text_response("Ошибка: phone, target и file_paths обязательны.")
            from src.models import PhotoTarget

            result = await svc.send_now(
                phone=phone,
                target=PhotoTarget(dialog_id=int(target)),
                file_paths=files,
                mode=mode,
                caption=caption,
            )
            return _text_response(f"Фото отправлены. Item id={result.id}, status={result.status}")
        except Exception as e:
            return _text_response(f"Ошибка отправки фото: {e}")

    tools.append(send_photos_now)

    @tool(
        "schedule_photos",
        "⚠️ Schedule photos to be sent at a specific time. "
        "Params: phone, target (dialog_id), file_paths (comma-sep), schedule_at (ISO datetime), "
        "mode (album/separate), caption. Ask user for confirmation first.",
        {
            "phone": str, "target": str, "file_paths": str,
            "schedule_at": str, "mode": str, "caption": str, "confirm": bool,
        },
    )
    async def schedule_photos(args):
        pool_gate = require_pool(client_pool, "Планирование фото")
        if pool_gate:
            return pool_gate
        gate = require_confirmation("запланирует отправку фото", args)
        if gate:
            return gate
        try:
            from datetime import datetime

            from src.database.bundles import PhotoLoaderBundle
            from src.models import PhotoTarget
            from src.services.photo_publish_service import PhotoPublishService
            from src.services.photo_task_service import PhotoTaskService

            svc = PhotoTaskService(PhotoLoaderBundle.from_database(db), PhotoPublishService(client_pool))
            phone = args.get("phone", "")
            target = args.get("target", "")
            files = [f.strip() for f in args.get("file_paths", "").split(",") if f.strip()]
            schedule_at_str = args.get("schedule_at", "")
            mode = args.get("mode", "album")
            caption = args.get("caption")
            if not phone or not target or not files or not schedule_at_str:
                return _text_response("Ошибка: phone, target, file_paths и schedule_at обязательны.")
            schedule_at = datetime.fromisoformat(schedule_at_str)
            result = await svc.schedule_send(
                phone=phone,
                target=PhotoTarget(dialog_id=int(target)),
                file_paths=files,
                mode=mode,
                schedule_at=schedule_at,
                caption=caption,
            )
            return _text_response(f"Фото запланированы на {schedule_at}. Item id={result.id}")
        except Exception as e:
            return _text_response(f"Ошибка планирования фото: {e}")

    tools.append(schedule_photos)

    @tool(
        "cancel_photo_item",
        "⚠️ Cancel a scheduled photo item. Ask user for confirmation first.",
        {"item_id": int, "confirm": bool},
    )
    async def cancel_photo_item(args):
        item_id = args.get("item_id")
        if item_id is None:
            return _text_response("Ошибка: item_id обязателен.")
        gate = require_confirmation(f"отменит запланированное фото item_id={item_id}", args)
        if gate:
            return gate
        try:
            from src.database.bundles import PhotoLoaderBundle
            from src.services.photo_publish_service import PhotoPublishService
            from src.services.photo_task_service import PhotoTaskService

            svc = PhotoTaskService(PhotoLoaderBundle.from_database(db), PhotoPublishService(client_pool))
            ok = await svc.cancel_item(int(item_id))
            if ok:
                return _text_response(f"Фото item_id={item_id} отменено.")
            return _text_response(f"Не удалось отменить item_id={item_id} (возможно, уже отправлено).")
        except Exception as e:
            return _text_response(f"Ошибка отмены фото: {e}")

    tools.append(cancel_photo_item)

    @tool("list_auto_uploads", "List automatic photo upload jobs", {})
    async def list_auto_uploads(args):
        try:
            from src.database.bundles import PhotoLoaderBundle
            from src.services.photo_auto_upload_service import PhotoAutoUploadService
            from src.services.photo_publish_service import PhotoPublishService

            svc = PhotoAutoUploadService(PhotoLoaderBundle.from_database(db), PhotoPublishService(client_pool))
            jobs = await svc.list_jobs()
            if not jobs:
                return _text_response("Автозагрузки не настроены.")
            lines = [f"Автозагрузки ({len(jobs)}):"]
            for j in jobs:
                status = "активна" if j.is_active else "пауза"
                lines.append(
                    f"- id={j.id}, phone={j.phone}, target={j.target_dialog_id}, "
                    f"folder={j.folder_path}, interval={j.interval_minutes}мин, {status}"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения автозагрузок: {e}")

    tools.append(list_auto_uploads)

    @tool("toggle_auto_upload", "Toggle an auto-upload job active/paused", {"job_id": int})
    async def toggle_auto_upload(args):
        job_id = args.get("job_id")
        if job_id is None:
            return _text_response("Ошибка: job_id обязателен.")
        try:
            from src.database.bundles import PhotoLoaderBundle
            from src.services.photo_auto_upload_service import PhotoAutoUploadService
            from src.services.photo_publish_service import PhotoPublishService

            svc = PhotoAutoUploadService(PhotoLoaderBundle.from_database(db), PhotoPublishService(client_pool))
            job = await svc.get_job(int(job_id))
            if job is None:
                return _text_response(f"Автозагрузка id={job_id} не найдена.")
            await svc.update_job(int(job_id), is_active=not job.is_active)
            status = "активирована" if not job.is_active else "приостановлена"
            return _text_response(f"Автозагрузка id={job_id} {status}.")
        except Exception as e:
            return _text_response(f"Ошибка переключения автозагрузки: {e}")

    tools.append(toggle_auto_upload)

    @tool(
        "delete_auto_upload",
        "⚠️ DANGEROUS: Delete an auto-upload job. Always ask user for confirmation first.",
        {"job_id": int, "confirm": bool},
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
            from src.database.bundles import PhotoLoaderBundle
            from src.services.photo_auto_upload_service import PhotoAutoUploadService
            from src.services.photo_publish_service import PhotoPublishService

            svc = PhotoAutoUploadService(PhotoLoaderBundle.from_database(db), PhotoPublishService(client_pool))
            await svc.delete_job(int(job_id))
            return _text_response(f"Автозагрузка id={job_id} удалена.")
        except Exception as e:
            return _text_response(f"Ошибка удаления автозагрузки: {e}")

    tools.append(delete_auto_upload)

    @tool(
        "create_photo_batch",
        "⚠️ Create a photo batch for sending to a Telegram dialog. "
        "Params: phone, target (dialog_id), file_paths (comma-sep), caption. "
        "Ask user for confirmation first.",
        {"phone": str, "target": str, "file_paths": str, "caption": str, "confirm": bool},
    )
    async def create_photo_batch(args):
        pool_gate = require_pool(client_pool, "Создание батча фото")
        if pool_gate:
            return pool_gate
        phone = args.get("phone", "")
        target = args.get("target", "")
        files = [f.strip() for f in args.get("file_paths", "").split(",") if f.strip()]
        caption = args.get("caption")
        if not phone or not target or not files:
            return _text_response("Ошибка: phone, target и file_paths обязательны.")
        gate = require_confirmation(
            f"создаст батч фото для отправки: файлы={files}, target={target}", args
        )
        if gate:
            return gate
        try:
            from src.database.bundles import PhotoLoaderBundle
            from src.services.photo_publish_service import PhotoPublishService
            from src.services.photo_task_service import PhotoTaskService

            svc = PhotoTaskService(PhotoLoaderBundle.from_database(db), PhotoPublishService(client_pool))
            entries = [{"file_path": f} for f in files]
            from src.models import PhotoTarget

            batch_id = await svc.create_batch(
                phone=phone,
                target=PhotoTarget(dialog_id=int(target)),
                entries=entries,
                caption=caption,
            )
            return _text_response(f"Батч создан: id={batch_id}")
        except Exception as e:
            return _text_response(f"Ошибка создания батча: {e}")

    tools.append(create_photo_batch)

    @tool(
        "run_photo_due",
        "⚠️ Process all due photo items and auto-upload jobs (sends to Telegram). "
        "Ask user for confirmation first.",
        {"confirm": bool},
    )
    async def run_photo_due(args):
        pool_gate = require_pool(client_pool, "Обработка фото")
        if pool_gate:
            return pool_gate
        gate = require_confirmation("отправит все запланированные фото в Telegram", args)
        if gate:
            return gate
        try:
            from src.database.bundles import PhotoLoaderBundle
            from src.services.photo_auto_upload_service import PhotoAutoUploadService
            from src.services.photo_publish_service import PhotoPublishService
            from src.services.photo_task_service import PhotoTaskService

            publish_svc = PhotoPublishService(client_pool)
            bundle = PhotoLoaderBundle.from_database(db)
            tasks_svc = PhotoTaskService(bundle, publish_svc)
            auto_svc = PhotoAutoUploadService(bundle, publish_svc)
            items = await tasks_svc.run_due()
            jobs = await auto_svc.run_due()
            return _text_response(f"Обработано: items={items}, auto_jobs={jobs}")
        except Exception as e:
            return _text_response(f"Ошибка обработки фото: {e}")

    tools.append(run_photo_due)

    @tool(
        "create_auto_upload",
        "⚠️ Create an auto-upload job to send photos from a folder on a schedule. "
        "Ask user for confirmation first.",
        {
            "phone": str, "target": str, "folder_path": str,
            "interval_minutes": int, "mode": str, "caption": str, "confirm": bool,
        },
    )
    async def create_auto_upload(args):
        pool_gate = require_pool(client_pool, "Создание автозагрузки")
        if pool_gate:
            return pool_gate
        gate = require_confirmation("создаст задачу автозагрузки фото", args)
        if gate:
            return gate
        try:
            from src.database.bundles import PhotoLoaderBundle
            from src.models import PhotoAutoUploadJob, PhotoSendMode
            from src.services.photo_auto_upload_service import PhotoAutoUploadService
            from src.services.photo_publish_service import PhotoPublishService

            svc = PhotoAutoUploadService(PhotoLoaderBundle.from_database(db), PhotoPublishService(client_pool))
            phone = args.get("phone", "")
            target = args.get("target", "")
            folder_path = args.get("folder_path", "")
            interval = int(args.get("interval_minutes", 60))
            mode = args.get("mode", "album")
            caption = args.get("caption")
            if not phone or not target or not folder_path:
                return _text_response("Ошибка: phone, target и folder_path обязательны.")
            job_id = await svc.create_job(PhotoAutoUploadJob(
                phone=phone,
                target_dialog_id=int(target),
                folder_path=folder_path,
                send_mode=PhotoSendMode(mode),
                caption=caption,
                interval_minutes=interval,
            ))
            return _text_response(f"Автозагрузка создана: id={job_id}")
        except Exception as e:
            return _text_response(f"Ошибка создания автозагрузки: {e}")

    tools.append(create_auto_upload)

    @tool(
        "update_auto_upload",
        "⚠️ Update an existing auto-upload job settings. Ask user for confirmation first.",
        {
            "job_id": int, "folder_path": str, "mode": str,
            "caption": str, "interval_minutes": int, "is_active": bool, "confirm": bool,
        },
    )
    async def update_auto_upload(args):
        job_id = args.get("job_id")
        if job_id is None:
            return _text_response("Ошибка: job_id обязателен.")
        gate = require_confirmation(f"обновит автозагрузку id={job_id}", args)
        if gate:
            return gate
        try:
            from src.database.bundles import PhotoLoaderBundle
            from src.models import PhotoSendMode
            from src.services.photo_auto_upload_service import PhotoAutoUploadService
            from src.services.photo_publish_service import PhotoPublishService

            svc = PhotoAutoUploadService(PhotoLoaderBundle.from_database(db), PhotoPublishService(client_pool))
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
        except Exception as e:
            return _text_response(f"Ошибка обновления автозагрузки: {e}")

    tools.append(update_auto_upload)

    return tools
