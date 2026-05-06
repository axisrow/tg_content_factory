from __future__ import annotations

from typing import Any

from src.database import bundles as database_bundles
from src.services import photo_auto_upload_service as photo_auto_upload_module
from src.services import photo_publish_service as photo_publish_module
from src.services import photo_task_service as photo_task_module


def photo_bundle(db: Any) -> Any:
    return database_bundles.PhotoLoaderBundle.from_database(db)


def photo_publish_service(client_pool: Any) -> Any:
    return photo_publish_module.PhotoPublishService(client_pool)


def photo_task_service(db: Any, client_pool: Any) -> Any:
    return photo_task_module.PhotoTaskService(photo_bundle(db), photo_publish_service(client_pool))


def photo_auto_upload_service(db: Any, client_pool: Any) -> Any:
    return photo_auto_upload_module.PhotoAutoUploadService(photo_bundle(db), photo_publish_service(client_pool))


def split_file_paths(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


async def resolve_photo_target_id(client_pool: Any, phone: str, target: str) -> int:
    if target.strip().lower() != "me":
        return int(target)
    client_result = await client_pool.get_client_by_phone(phone)
    if not client_result:
        raise LookupError(f"Клиент для {phone} не найден.")
    session, _ = client_result
    me = await session.fetch_me()
    return int(me.id)
