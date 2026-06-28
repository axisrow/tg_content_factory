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


_SELF_TARGET_ALIASES = {"me", "self"}


async def resolve_photo_target(client_pool: Any, phone: str, target: str) -> Any:
    """Resolve a photo target literal to a full PhotoTarget.

    "me"/"self" → the account's own Saved Messages: the own user-id with
    target_type="saved" so PhotoPublishService → resolve_dialog_entity maps it to
    PeerUser (Saved Messages). Without target_type="saved" a cleared/stale cache makes
    the own user-id fall through to PeerChannel(abs(id)), mis-resolving Saved Messages
    as an unrelated channel — parity with the CLI `_resolve_self_target` (audit #838/10).
    Any other value is a raw numeric dialog id (target_type=None, resolved via cache).
    """
    if target.strip().lower() not in _SELF_TARGET_ALIASES:
        return photo_task_module.PhotoTarget(dialog_id=int(target))
    client_result = await client_pool.get_client_by_phone(phone)
    if not client_result:
        raise LookupError(f"Клиент для {phone} не найден.")
    session, acquired_phone = client_result
    try:
        me = await session.fetch_me()
        return photo_task_module.PhotoTarget(dialog_id=int(me.id), title="Saved Messages", target_type="saved")
    finally:
        await client_pool.release_client(acquired_phone)


async def resolve_photo_target_id(client_pool: Any, phone: str, target: str) -> int:
    """Backward-compatible shim returning only the dialog id.

    Prefer resolve_photo_target(), which also carries target_type="saved" for "me"/"self".
    """
    resolved = await resolve_photo_target(client_pool, phone, target)
    return resolved.dialog_id
