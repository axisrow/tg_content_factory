from __future__ import annotations

from typing import Any

from claude_agent_sdk import tool

from src.agent.tools._photo_loader_runtime import photo_auto_upload_service, photo_task_service
from src.agent.tools._registry import _text_response, require_phone_permission, require_pool, resolve_phone
from src.agent.tools.photo_loader_schemas import (
    LIST_AUTO_UPLOADS_SCHEMA,
    LIST_PHOTO_BATCHES_SCHEMA,
    LIST_PHOTO_DIALOGS_SCHEMA,
    LIST_PHOTO_ITEMS_SCHEMA,
    REFRESH_PHOTO_DIALOGS_SCHEMA,
)


def register_batch_read_tools(db: Any, client_pool: Any) -> list[Any]:
    tools: list[Any] = []

    @tool(
        "list_photo_batches",
        "List photo upload batches with batch_id, phone, target, status, and item count.",
        LIST_PHOTO_BATCHES_SCHEMA,
    )
    async def list_photo_batches(args):
        try:
            svc = photo_task_service(db, client_pool)
            limit = int(args.get("limit", 50))
            batches = await svc.list_batches(limit=limit)
            if not batches:
                return _text_response("Батчи фото не найдены.")
            lines = [f"Батчи фото ({len(batches)}):"]
            for batch in batches:
                lines.append(
                    f"- batch_id={batch.id}, phone={batch.phone}, target={batch.target_dialog_id}, "
                    f"status={batch.status}, items={batch.total_items}, created={batch.created_at}"
                )
            return _text_response("\n".join(lines))
        except Exception as exc:
            return _text_response(f"Ошибка получения батчей: {exc}")

    tools.append(list_photo_batches)

    @tool(
        "list_photo_items",
        "List photo batch items with item_id, batch_id, status, and scheduled time. "
        "Use item_id with cancel_photo_item.",
        LIST_PHOTO_ITEMS_SCHEMA,
    )
    async def list_photo_items(args):
        try:
            svc = photo_task_service(db, client_pool)
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
        except Exception as exc:
            return _text_response(f"Ошибка получения элементов: {exc}")

    tools.append(list_photo_items)
    return tools


def register_auto_read_tools(db: Any, client_pool: Any) -> list[Any]:
    tools: list[Any] = []

    @tool(
        "list_auto_uploads",
        "List automatic photo upload jobs with job_id, folder, target, interval, and status. "
        "Use job_id with toggle_auto_upload / delete_auto_upload.",
        LIST_AUTO_UPLOADS_SCHEMA,
    )
    async def list_auto_uploads(args):
        try:
            svc = photo_auto_upload_service(db, client_pool)
            jobs = await svc.list_jobs()
            if not jobs:
                return _text_response("Автозагрузки не настроены.")
            lines = [f"Автозагрузки ({len(jobs)}):"]
            for job in jobs:
                status = "активна" if job.is_active else "пауза"
                lines.append(
                    f"- id={job.id}, phone={job.phone}, target={job.target_dialog_id}, "
                    f"folder={job.folder_path}, interval={job.interval_minutes}мин, {status}"
                )
            return _text_response("\n".join(lines))
        except Exception as exc:
            return _text_response(f"Ошибка получения автозагрузок: {exc}")

    tools.append(list_auto_uploads)
    return tools


def register_dialog_tools(db: Any, client_pool: Any) -> list[Any]:
    tools: list[Any] = []

    @tool(
        "list_photo_dialogs",
        "List Telegram dialogs available as photo upload targets (channels, groups, chats).",
        LIST_PHOTO_DIALOGS_SCHEMA,
    )
    async def list_photo_dialogs(args):
        pool_gate = require_pool(client_pool, "Список диалогов для фото")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "list_photo_dialogs")
        if perm_gate:
            return perm_gate
        try:
            from src.services.channel_service import ChannelService

            svc = ChannelService(db, client_pool, None)
            dialogs = await svc.get_my_dialogs(phone)
            if not dialogs:
                return _text_response(f"Диалоги для {phone} не найдены.")
            lines = [f"Диалоги ({len(dialogs)}):"]
            for dialog in dialogs:
                lines.append(
                    f"- id={dialog['channel_id']}, type={dialog.get('channel_type', '?')}: "
                    f"{dialog.get('title', '?')}"
                )
            return _text_response("\n".join(lines))
        except Exception as exc:
            return _text_response(f"Ошибка получения диалогов: {exc}")

    tools.append(list_photo_dialogs)

    @tool(
        "refresh_photo_dialogs",
        "Refresh the Telegram dialog cache for photo upload targeting. "
        "Use when new channels/groups are not appearing in the list.",
        REFRESH_PHOTO_DIALOGS_SCHEMA,
    )
    async def refresh_photo_dialogs(args):
        pool_gate = require_pool(client_pool, "Обновление кэша диалогов")
        if pool_gate:
            return pool_gate
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        perm_gate = await require_phone_permission(db, phone, "refresh_photo_dialogs")
        if perm_gate:
            return perm_gate
        from src.agent.tools._registry import require_confirmation

        gate = require_confirmation(f"обновит кэш диалогов для {phone}", args)
        if gate:
            return gate
        try:
            from src.services.channel_service import ChannelService

            svc = ChannelService(db, client_pool, None)
            dialogs = await svc.get_my_dialogs(phone, refresh=True)
            return _text_response(f"Кэш диалогов обновлён: {len(dialogs)} диалогов.")
        except Exception as exc:
            return _text_response(f"Ошибка обновления кэша диалогов: {exc}")

    tools.append(refresh_photo_dialogs)
    return tools
