from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

from src.config import AppConfig
from src.database import Database
from src.models import Account, RuntimeSnapshot, TelegramCommandStatus
from src.scheduler.service import SchedulerManager
from src.services.notification_service import NotificationService
from src.services.notification_target_service import NotificationTargetService
from src.services.photo_auto_upload_service import PhotoAutoUploadService
from src.services.photo_task_service import PhotoTarget, PhotoTaskService
from src.telegram.auth import TelegramAuth
from src.telegram.client_pool import ClientPool
from src.telegram.collector import Collector
from src.telegram.flood_wait import HandledFloodWaitError, run_with_flood_wait
from src.telegram.notifier import Notifier

logger = logging.getLogger(__name__)


class TelegramCommandDispatcher:
    def __init__(
        self,
        db: Database,
        pool: ClientPool,
        config: AppConfig | None = None,
        collector: Collector | None = None,
        *,
        scheduler: SchedulerManager | None = None,
        auth: TelegramAuth | None = None,
    ):
        self._db = db
        self._pool = pool
        self._config = config
        self._collector = collector
        self._scheduler = scheduler
        self._auth = auth
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop(), name="telegram_command_dispatcher")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            command = await self._db.repos.telegram_commands.claim_next_command()
            if command is None:
                await asyncio.sleep(1.0)
                continue
            try:
                result = await self._dispatch(command.command_type, command.payload)
            except asyncio.CancelledError:
                await self._db.repos.telegram_commands.update_command(
                    command.id,
                    status=TelegramCommandStatus.PENDING,
                    error="cancelled while running; reset for retry",
                )
                raise
            except Exception as exc:
                logger.exception("Telegram command failed: id=%s type=%s", command.id, command.command_type)
                await self._db.repos.telegram_commands.update_command(
                    command.id,
                    status=TelegramCommandStatus.FAILED,
                    error=str(exc),
                    payload=command.payload,
                )
            else:
                await self._db.repos.telegram_commands.update_command(
                    command.id,
                    status=TelegramCommandStatus.SUCCEEDED,
                    result_payload=result.get("result") or {},
                    payload=result.get("payload_update"),
                )

    async def _dispatch(self, command_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        handler_name = f"_handle_{command_type.replace('.', '_')}"
        handler = getattr(self, handler_name, None)
        if not callable(handler):
            raise RuntimeError(f"Unsupported telegram command: {command_type}")
        return await handler(payload)

    async def _get_client(self, phone: str):
        result = await self._pool.get_native_client_by_phone(phone)
        if result is None:
            raise RuntimeError("client unavailable")
        return result

    def _photo_task_service(self) -> PhotoTaskService:
        from src.database.bundles import PhotoLoaderBundle
        from src.services.photo_publish_service import PhotoPublishService

        return PhotoTaskService(PhotoLoaderBundle.from_database(self._db), PhotoPublishService(self._pool))

    def _photo_auto_upload_service(self) -> PhotoAutoUploadService:
        from src.database.bundles import PhotoLoaderBundle
        from src.services.photo_publish_service import PhotoPublishService

        return PhotoAutoUploadService(PhotoLoaderBundle.from_database(self._db), PhotoPublishService(self._pool))

    def _notification_service(self) -> NotificationService:
        from src.database.bundles import NotificationBundle

        target_service = NotificationTargetService(NotificationBundle.from_database(self._db), self._pool)
        kwargs: dict[str, Any] = {}
        if self._config is not None:
            kwargs["bot_name_prefix"] = self._config.notifications.bot_name_prefix
            kwargs["bot_username_prefix"] = self._config.notifications.bot_username_prefix
        return NotificationService(self._db, target_service, **kwargs)

    def _notification_target_service(self) -> NotificationTargetService:
        from src.database.bundles import NotificationBundle

        return NotificationTargetService(NotificationBundle.from_database(self._db), self._pool)

    async def _handle_auth_send_code(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._auth is None or not self._auth.is_configured:
            raise RuntimeError("auth_not_configured")
        phone = str(payload["phone"]).strip()
        result = await self._auth.send_code(phone)
        return {"phone": phone, **result}

    async def _handle_auth_resend_code(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._auth is None or not self._auth.is_configured:
            raise RuntimeError("auth_not_configured")
        phone = str(payload["phone"]).strip()
        result = await self._auth.resend_code(phone)
        return {"phone": phone, **result}

    async def _handle_auth_verify_code(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._auth is None or not self._auth.is_configured:
            raise RuntimeError("auth_not_configured")
        phone = str(payload["phone"]).strip()
        password_2fa = str(payload.get("password_2fa", "")).strip() or None
        payload["password_2fa"] = ""
        session_string = await self._auth.verify_code(
            phone,
            str(payload["code"]),
            str(payload["phone_code_hash"]),
            password_2fa,
        )
        existing = await self._db.get_accounts()
        account = Account(
            phone=phone,
            session_string=session_string,
            is_primary=not any(acc.phone == phone for acc in existing) and len(existing) == 0,
            is_premium=False,
        )
        await self._db.add_account(account)
        connect_result = await self._handle_accounts_connect({"phone": phone})
        return {"result": {"phone": phone, **connect_result}, "payload_update": {**payload}}

    async def _handle_scheduler_reconcile(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._scheduler is None:
            raise RuntimeError("scheduler_unavailable")
        autostart = await self._db.get_setting("scheduler_autostart")
        desired_running = autostart == "1"
        if not desired_running:
            await self._scheduler.stop()
            await self._scheduler.load_settings()
            return {"running": False}
        if self._scheduler.is_running:
            await self._scheduler.stop()
        await self._scheduler.load_settings()
        await self._scheduler.start()
        return {"running": True, "interval_minutes": self._scheduler.interval_minutes}

    async def _handle_scheduler_trigger_warm(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._scheduler is None:
            raise RuntimeError("scheduler_unavailable")
        await self._scheduler.trigger_warm_background()
        return {"started": True}

    async def _handle_dialogs_refresh(self, payload: dict[str, Any]) -> dict[str, Any]:
        phone = str(payload["phone"])
        dialogs = await self._pool.get_dialogs_for_phone(phone, include_dm=True, mode="full", refresh=True)
        await self._db.repos.dialog_cache.replace_dialogs(phone, dialogs)
        return {"phone": phone, "dialogs_count": len(dialogs)}

    async def _handle_dialogs_cache_clear(self, payload: dict[str, Any]) -> dict[str, Any]:
        phone = str(payload.get("phone") or "").strip()
        invalidate = getattr(self._pool, "invalidate_dialogs_cache", None)
        if phone:
            if callable(invalidate):
                invalidate(phone)
            await self._db.repos.dialog_cache.clear_dialogs(phone)
        else:
            if callable(invalidate):
                invalidate()
            await self._db.repos.dialog_cache.clear_all_dialogs()
        return {"phone": phone}

    async def _handle_dialogs_leave(self, payload: dict[str, Any]) -> dict[str, Any]:
        phone = str(payload["phone"])
        dialogs = [(int(item["dialog_id"]), str(item["title"])) for item in payload.get("dialogs", [])]
        results = await self._pool.leave_channels(phone, dialogs)
        left = sum(1 for ok in results.values() if ok)
        failed = len(results) - left
        return {"left": left, "failed": failed}

    async def _handle_dialogs_send(self, payload: dict[str, Any]) -> dict[str, Any]:
        client, phone = await self._get_client(str(payload["phone"]))
        try:
            entity = await client.get_entity(payload["recipient"])
            message = await client.send_message(entity, payload["text"])
            return {"phone": phone, "message_id": getattr(message, "id", None)}
        finally:
            await self._pool.release_client(phone)

    async def _handle_dialogs_edit_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        client, phone = await self._get_client(str(payload["phone"]))
        try:
            entity = await client.get_entity(payload["chat_id"])
            await client.edit_message(entity, int(payload["message_id"]), payload["text"])
            return {"phone": phone}
        finally:
            await self._pool.release_client(phone)

    async def _handle_dialogs_delete_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        client, phone = await self._get_client(str(payload["phone"]))
        try:
            entity = await client.get_entity(payload["chat_id"])
            ids = [int(value) for value in payload["message_ids"]]
            await client.delete_messages(entity, ids)
            return {"phone": phone, "deleted": len(ids)}
        finally:
            await self._pool.release_client(phone)

    async def _handle_dialogs_forward_messages(self, payload: dict[str, Any]) -> dict[str, Any]:
        client, phone = await self._get_client(str(payload["phone"]))
        try:
            from_entity = await client.get_entity(payload["from_chat"])
            to_entity = await client.get_entity(payload["to_chat"])
            ids = [int(value) for value in payload["message_ids"]]
            await client.forward_messages(to_entity, ids, from_entity)
            return {"phone": phone, "forwarded": len(ids)}
        finally:
            await self._pool.release_client(phone)

    async def _handle_dialogs_pin_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        client, phone = await self._get_client(str(payload["phone"]))
        try:
            entity = await client.get_entity(payload["chat_id"])
            await client.pin_message(entity, int(payload["message_id"]), notify=bool(payload.get("notify", False)))
            return {"phone": phone}
        finally:
            await self._pool.release_client(phone)

    async def _handle_dialogs_unpin_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        client, phone = await self._get_client(str(payload["phone"]))
        try:
            entity = await client.get_entity(payload["chat_id"])
            message_id = payload.get("message_id")
            await client.unpin_message(entity, int(message_id) if message_id is not None else None)
            return {"phone": phone}
        finally:
            await self._pool.release_client(phone)

    async def _handle_dialogs_participants(self, payload: dict[str, Any]) -> dict[str, Any]:
        client, phone = await self._get_client(str(payload["phone"]))
        try:
            entity = await client.get_entity(payload["chat_id"])
            participants = await client.get_participants(
                entity,
                limit=int(payload.get("limit", 200)),
                search=str(payload.get("search", "")),
            )
            data = [
                {
                    "id": p.id,
                    "first_name": getattr(p, "first_name", None) or "",
                    "last_name": getattr(p, "last_name", None) or "",
                    "username": getattr(p, "username", None) or "",
                }
                for p in participants
            ]
            scope = f"dialogs_participants:{phone}:{payload['chat_id']}"
            search_value = str(payload.get("search", ""))
            # Only cache unfiltered (full) participant lists. A search-filtered
            # result would otherwise overwrite the shared snapshot, so later
            # no-search GETs would return only the filtered subset.
            if not search_value:
                await self._db.repos.runtime_snapshots.upsert_snapshot(
                    RuntimeSnapshot(
                        snapshot_type="dialogs_participants",
                        scope=scope,
                        payload={"participants": data, "total": len(data)},
                    )
                )
            result = {"phone": phone, "scope": scope, "total": len(data)}
            if search_value:
                # Search results are intentionally not cached; return them
                # inline so the client can read them via GET /telegram-commands/{id}.
                result["participants"] = data
            return result
        finally:
            await self._pool.release_client(phone)

    async def _handle_dialogs_broadcast_stats(self, payload: dict[str, Any]) -> dict[str, Any]:
        client, phone = await self._get_client(str(payload["phone"]))
        try:
            entity = await client.get_entity(payload["chat_id"])
            stats = await client.get_broadcast_stats(entity)
            fields: dict[str, Any] = {}
            for attr in ("followers", "views_per_post", "shares_per_post", "reactions_per_post", "forwards_per_post"):
                val = getattr(stats, attr, None)
                if val is not None:
                    current = getattr(val, "current", None)
                    previous = getattr(val, "previous", None)
                    if current is not None:
                        fields[attr] = {"current": current, "previous": previous}
                    else:
                        fields[attr] = str(val)
            period = getattr(stats, "period", None)
            if period is not None:
                fields["period"] = {
                    "min_date": period.min_date.isoformat() if getattr(period, "min_date", None) else None,
                    "max_date": period.max_date.isoformat() if getattr(period, "max_date", None) else None,
                }
            enabled_notifications = getattr(stats, "enabled_notifications", None)
            if enabled_notifications is not None:
                fields["enabled_notifications"] = enabled_notifications
            if not fields:
                fields["raw"] = str(stats)
            scope = f"dialogs_broadcast_stats:{phone}:{payload['chat_id']}"
            await self._db.repos.runtime_snapshots.upsert_snapshot(
                RuntimeSnapshot(
                    snapshot_type="dialogs_broadcast_stats",
                    scope=scope,
                    payload={"stats": fields},
                )
            )
            return {"phone": phone, "scope": scope}
        finally:
            await self._pool.release_client(phone)

    async def _handle_dialogs_archive(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._set_dialog_folder(payload, folder_id=1)

    async def _handle_dialogs_unarchive(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._set_dialog_folder(payload, folder_id=0)

    async def _set_dialog_folder(self, payload: dict[str, Any], *, folder_id: int) -> dict[str, Any]:
        client, phone = await self._get_client(str(payload["phone"]))
        try:
            entity = await client.get_entity(payload["chat_id"])
            await client.edit_folder(entity, folder_id)
            return {"phone": phone, "folder_id": folder_id}
        finally:
            await self._pool.release_client(phone)

    async def _handle_dialogs_mark_read(self, payload: dict[str, Any]) -> dict[str, Any]:
        client, phone = await self._get_client(str(payload["phone"]))
        try:
            entity = await client.get_entity(payload["chat_id"])
            max_id = payload.get("max_id")
            await client.send_read_acknowledge(entity, max_id=int(max_id) if max_id is not None else None)
            return {"phone": phone}
        finally:
            await self._pool.release_client(phone)

    async def _handle_dialogs_edit_admin(self, payload: dict[str, Any]) -> dict[str, Any]:
        client, phone = await self._get_client(str(payload["phone"]))
        try:
            entity = await client.get_entity(payload["chat_id"])
            user = await client.get_entity(payload["user_id"])
            kwargs = {"is_admin": bool(payload.get("is_admin", False))}
            if payload.get("title"):
                kwargs["title"] = payload["title"]
            await client.edit_admin(entity, user, **kwargs)
            return {"phone": phone}
        finally:
            await self._pool.release_client(phone)

    async def _handle_dialogs_edit_permissions(self, payload: dict[str, Any]) -> dict[str, Any]:
        from datetime import datetime

        client, phone = await self._get_client(str(payload["phone"]))
        try:
            entity = await client.get_entity(payload["chat_id"])
            user = await client.get_entity(payload["user_id"])
            kwargs: dict[str, Any] = {}
            if payload.get("until_date"):
                kwargs["until_date"] = datetime.fromisoformat(str(payload["until_date"]))
            if "send_messages" in payload:
                kwargs["send_messages"] = bool(payload["send_messages"])
            if "send_media" in payload:
                kwargs["send_media"] = bool(payload["send_media"])
            await client.edit_permissions(entity, user, **kwargs)
            return {"phone": phone}
        finally:
            await self._pool.release_client(phone)

    async def _handle_dialogs_kick(self, payload: dict[str, Any]) -> dict[str, Any]:
        client, phone = await self._get_client(str(payload["phone"]))
        try:
            entity = await client.get_entity(payload["chat_id"])
            user = await client.get_entity(payload["user_id"])
            await client.kick_participant(entity, user)
            return {"phone": phone}
        finally:
            await self._pool.release_client(phone)

    async def _handle_dialogs_create_channel(self, payload: dict[str, Any]) -> dict[str, Any]:
        from telethon.tl.functions.channels import CreateChannelRequest, UpdateUsernameRequest

        client, phone = await self._get_client(str(payload["phone"]))
        try:
            result = await client(
                CreateChannelRequest(
                    title=str(payload["title"]).strip(),
                    about=str(payload.get("about", "")).strip(),
                    broadcast=True,
                    megagroup=False,
                )
            )
            channel = result.chats[0] if result.chats else None
            if channel is None:
                raise RuntimeError("Telegram returned empty response")
            channel_id = getattr(channel, "id", None)
            channel_username = getattr(channel, "username", None) or ""
            requested_username = str(payload.get("username", "")).strip()
            if requested_username and channel_id:
                try:
                    await client(UpdateUsernameRequest(channel, requested_username))
                    channel_username = requested_username
                except Exception:
                    logger.warning("Could not set username %r for new channel id=%s", requested_username, channel_id)
            return {
                "phone": phone,
                "channel_id": channel_id,
                "channel_title": payload["title"],
                "channel_username": channel_username,
                "invite_link": f"https://t.me/{channel_username}" if channel_username else "",
            }
        finally:
            await self._pool.release_client(phone)

    async def _handle_agent_forum_topics_refresh(self, payload: dict[str, Any]) -> dict[str, Any]:
        channel_id = int(payload["channel_id"])
        topics = await self._pool.get_forum_topics(channel_id)
        if topics:
            await self._db.upsert_forum_topics(channel_id, topics)
            await self._db.set_channel_type(channel_id, "forum")
        return {"channel_id": channel_id, "count": len(topics)}

    async def _handle_channels_add_identifier(self, payload: dict[str, Any]) -> dict[str, Any]:
        identifier = str(payload["identifier"]).strip()
        info = await self._pool.resolve_channel(identifier)
        if not info:
            raise RuntimeError("resolve failed")
        meta = await self._pool.fetch_channel_meta(info["channel_id"], info.get("channel_type"))
        from src.models import Channel

        await self._db.add_channel(
            Channel(
                channel_id=info["channel_id"],
                title=info["title"],
                username=info["username"],
                channel_type=info.get("channel_type"),
                is_active=not info.get("deactivate", False),
                about=meta.get("about") if meta else None,
                linked_chat_id=meta.get("linked_chat_id") if meta else None,
                has_comments=meta.get("has_comments", False) if meta else False,
                created_at=info.get("created_at"),
            )
        )
        return {"channel_id": info["channel_id"]}

    async def _handle_channels_collect_stats(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._collector is None:
            raise RuntimeError("collector_unavailable")
        channel_pk = int(payload["channel_pk"])
        channel = await self._db.get_channel_by_pk(channel_pk)
        if channel is None:
            raise RuntimeError("channel_not_found")
        result = await self._collector.collect_channel_stats(channel)
        return {"channel_id": channel.channel_id, "collected": bool(result)}

    async def _handle_channels_refresh_types(self, payload: dict[str, Any]) -> dict[str, Any]:
        channels = await self._db.get_channels(active_only=True)
        updated = 0
        failed = 0
        for ch in channels:
            identifier = ch.username or str(ch.channel_id)
            try:
                info = await self._pool.resolve_channel(identifier)
            except Exception:
                info = None
            if info is False:
                await self._db.set_channel_active(ch.id, False)
                await self._db.set_channel_type(ch.channel_id, "unavailable")
                failed += 1
                continue
            if not info or info.get("channel_type") is None:
                failed += 1
                continue
            await self._db.set_channel_type(ch.channel_id, info["channel_type"])
            updated += 1
        return {"updated": updated, "failed": failed}

    async def _handle_channels_refresh_meta(self, payload: dict[str, Any]) -> dict[str, Any]:
        channels = await self._db.get_channels(active_only=True)
        updated = 0
        failed = 0
        for ch in channels:
            try:
                meta = await self._pool.fetch_channel_meta(ch.channel_id, ch.channel_type)
            except Exception:
                meta = None
            if not meta:
                failed += 1
                continue
            await self._db.update_channel_full_meta(
                ch.channel_id,
                about=meta["about"],
                linked_chat_id=meta["linked_chat_id"],
                has_comments=meta["has_comments"],
            )
            updated += 1
        return {"updated": updated, "failed": failed}

    async def _handle_channels_import_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        from src.models import Channel

        identifiers = [str(item).strip() for item in payload.get("identifiers", []) if str(item).strip()]
        existing = await self._db.get_channels()
        existing_ids = {channel.channel_id for channel in existing}
        added = 0
        skipped = 0
        failed = 0
        details: list[dict[str, Any]] = []
        for ident in identifiers:
            try:
                info = await self._pool.resolve_channel(ident)
            except Exception:
                info = None
            if not info:
                failed += 1
                details.append({"identifier": ident, "status": "failed"})
                continue
            if info["channel_id"] in existing_ids:
                skipped += 1
                details.append({"identifier": ident, "status": "skipped"})
                continue
            await self._db.add_channel(
                Channel(
                    channel_id=info["channel_id"],
                    title=info["title"],
                    username=info["username"],
                    channel_type=info.get("channel_type"),
                    is_active=not info.get("deactivate", False),
                    created_at=info.get("created_at"),
                )
            )
            existing_ids.add(info["channel_id"])
            added += 1
            details.append({"identifier": ident, "status": "added"})
        return {"added": added, "skipped": skipped, "failed": failed, "details": details}

    async def _handle_dialogs_download_media(self, payload: dict[str, Any]) -> dict[str, Any]:
        client, phone = await self._get_client(str(payload["phone"]))
        try:
            entity = await client.get_entity(payload["chat_id"])
            msg = None

            async def _lookup_message() -> None:
                nonlocal msg
                async for item in client.iter_messages(
                    entity, ids=int(payload["message_id"])
                ):
                    msg = item
                    break

            try:
                await run_with_flood_wait(
                    _lookup_message(),
                    operation="dispatcher_dialogs_download_media_lookup",
                    phone=phone,
                    pool=self._pool,
                )
            except HandledFloodWaitError as exc:
                raise RuntimeError(f"flood_wait:{exc.info.wait_seconds}") from exc
            if msg is None:
                raise RuntimeError("message_not_found")
            output_dir = Path(__file__).resolve().parents[2] / "data" / "downloads"
            output_dir.mkdir(parents=True, exist_ok=True)
            output_dir_resolved = output_dir.resolve()
            path = await client.download_media(msg, file=str(output_dir_resolved))
            if not path:
                raise RuntimeError("no_media")
            resolved = Path(path).resolve()
            if output_dir_resolved not in resolved.parents:
                raise RuntimeError("path_escape")
            return {"phone": phone, "path": str(resolved)}
        finally:
            await self._pool.release_client(phone)

    async def _handle_accounts_connect(self, payload: dict[str, Any]) -> dict[str, Any]:
        phone = str(payload["phone"])
        accounts = await self._db.get_accounts()
        account = next((a for a in accounts if a.phone == phone), None)
        if account is None:
            raise RuntimeError(f"account_not_found:{phone}")
        await self._pool.add_client(phone, account.session_string)
        result = await self._pool.get_client_by_phone(phone)
        is_premium = False
        if result is not None:
            session, acquired_phone = result
            try:
                me = await session.fetch_me()
                is_premium = bool(getattr(me, "premium", False))
            finally:
                await self._pool.release_client(acquired_phone)
        await self._db.update_account_premium(phone, is_premium)
        return {"phone": phone, "is_premium": is_premium}

    async def _handle_accounts_toggle(self, payload: dict[str, Any]) -> dict[str, Any]:
        account_id = int(payload["account_id"])
        accounts = await self._db.get_accounts()
        account = next((a for a in accounts if a.id == account_id), None)
        if account is None:
            raise RuntimeError(f"account_not_found:{account_id}")
        new_active = not account.is_active
        await self._db.set_account_active(account_id, new_active)
        if new_active:
            try:
                await self._pool.add_client(account.phone, account.session_string)
            except Exception as exc:
                logger.warning("accounts.toggle: failed to add client %s: %s", account.phone, exc)
        else:
            try:
                await self._pool.remove_client(account.phone)
            except Exception as exc:
                logger.warning("accounts.toggle: failed to remove client %s: %s", account.phone, exc)
        return {"account_id": account_id, "is_active": new_active}

    async def _handle_accounts_delete(self, payload: dict[str, Any]) -> dict[str, Any]:
        account_id = int(payload["account_id"])
        accounts = await self._db.get_accounts()
        account = next((a for a in accounts if a.id == account_id), None)
        if account is not None:
            try:
                await self._pool.remove_client(account.phone)
            except Exception as exc:
                logger.warning("accounts.delete: failed to remove client %s: %s", account.phone, exc)
        await self._db.delete_account(account_id)
        return {"account_id": account_id, "deleted": True}

    async def _handle_notifications_setup_bot(self, payload: dict[str, Any]) -> dict[str, Any]:
        bot = await self._notification_service().setup_bot()
        return {"bot_username": bot.bot_username, "bot_id": bot.bot_id}

    async def _handle_notifications_delete_bot(self, payload: dict[str, Any]) -> dict[str, Any]:
        await self._notification_service().teardown_bot()
        return {"deleted": True}

    async def _handle_notifications_test(self, payload: dict[str, Any]) -> dict[str, Any]:
        from src.database.bundles import NotificationBundle

        target_service = self._notification_target_service()
        notifier = Notifier(target_service, None, NotificationBundle.from_database(self._db))
        ok = await notifier.notify("✅ Тест уведомлений: соединение установлено")
        if not ok:
            raise RuntimeError("notification_test_failed")
        return {"sent": True}

    async def _handle_photo_send_now(self, payload: dict[str, Any]) -> dict[str, Any]:
        item = await self._photo_task_service().send_now(
            phone=str(payload["phone"]),
            target=PhotoTarget(
                dialog_id=int(payload["target_dialog_id"]),
                title=payload.get("target_title"),
                target_type=payload.get("target_type"),
            ),
            file_paths=[str(path) for path in payload.get("file_paths", [])],
            mode=str(payload.get("mode", "separate")),
            caption=payload.get("caption"),
        )
        return {"item_id": item.id, "batch_id": item.batch_id}

    async def _handle_photo_schedule_send(self, payload: dict[str, Any]) -> dict[str, Any]:
        from datetime import datetime

        item = await self._photo_task_service().schedule_send(
            phone=str(payload["phone"]),
            target=PhotoTarget(
                dialog_id=int(payload["target_dialog_id"]),
                title=payload.get("target_title"),
                target_type=payload.get("target_type"),
            ),
            file_paths=[str(path) for path in payload.get("file_paths", [])],
            mode=str(payload.get("mode", "separate")),
            schedule_at=datetime.fromisoformat(str(payload["schedule_at"])),
            caption=payload.get("caption"),
        )
        return {"item_id": item.id, "batch_id": item.batch_id}

    async def _handle_photo_run_due(self, payload: dict[str, Any]) -> dict[str, Any]:
        items = await self._photo_task_service().run_due()
        jobs = await self._photo_auto_upload_service().run_due()
        return {"processed_items": items, "processed_jobs": jobs}

    async def _handle_moderation_publish_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        from src.services.pipeline_service import PipelineService
        from src.services.publish_service import PublishService

        run_id = int(payload["run_id"])
        run = await self._db.repos.generation_runs.get(run_id)
        if run is None:
            raise RuntimeError("run_not_found")
        pipeline = await PipelineService(self._db).get(int(payload["pipeline_id"]))
        if pipeline is None:
            raise RuntimeError("pipeline_invalid")
        results = await PublishService(self._db, self._pool).publish_run(run, pipeline)
        if not results or not all(result.success for result in results):
            raise RuntimeError("pipeline_run_failed")
        return {"run_id": run_id, "published": len(results)}
