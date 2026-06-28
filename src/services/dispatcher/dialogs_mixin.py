"""Dialog action command handlers (#1047).

Domain: ``dialogs.*`` — message send/edit/delete/forward, pin/unpin, reactions
(with the per-phone rate-limit gate), participants, broadcast stats, admin and
permission edits, kick, archive/unarchive, mark-read, channel creation, cache
refresh/clear, leave, and resolve.

Two ``dialogs.*`` handlers deliberately stay on the facade class, not here:
``dialogs.join`` (the suite patches ``TelegramActionService`` through the facade
module namespace) and ``dialogs.download_media`` (it reads ``mod.__file__`` to
locate ``data/downloads`` and the suite monkeypatches that). Keeping them on the
facade is the only way those patches reach the call site.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from inspect import isawaitable
from typing import TYPE_CHECKING, Any

from src.models import RuntimeSnapshot
from src.services.dispatcher._constants import (
    DEFAULT_REACTION_MIN_INTERVAL_SEC,
    REACTION_MIN_INTERVAL_CEILING_SEC,
    REACTION_MIN_INTERVAL_FLOOR_SEC,
    REACTION_MIN_INTERVAL_SETTING,
)
from src.services.dispatcher._errors import TelegramCommandRetryLaterError
from src.services.telegram_actions import TelegramActionService
from src.settings_utils import parse_float_setting
from src.telegram.reactions import normalize_outgoing_reaction_emoji
from src.telegram.utils import normalize_utc
from src.utils.datetime import parse_required_datetime

if TYPE_CHECKING:
    from src.services.dispatcher._base import _DispatcherProtocol

    _Base = _DispatcherProtocol
else:
    _Base = object

logger = logging.getLogger(__name__)


def _payload_channel_type(item: dict[str, Any]) -> str:
    """Channel type for a leave/delete payload item.

    The web form parses ``channel_ids`` as ``"id:type"`` and forwards the type
    here. Older payloads only carried ``title``; for those (and any missing
    type) fall back to ``"channel"`` → ``PeerChannel``, mirroring the CLI/agent
    convention (a bare-positive id is a channel, never a user).
    """
    channel_type = item.get("channel_type")
    if isinstance(channel_type, str) and channel_type:
        return channel_type
    return "channel"


class DialogsCommandsMixin(_Base):
    """``dialogs.*`` command handlers and the reaction rate-limit machinery."""

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
        dialogs = [
            (int(item["dialog_id"]), _payload_channel_type(item))
            for item in payload.get("dialogs", [])
        ]
        result = await TelegramActionService(self._pool).leave_dialogs(phone=phone, dialogs=dialogs)
        return {"left": result.success_count, "failed": result.failed_count}

    async def _handle_dialogs_delete(self, payload: dict[str, Any]) -> dict[str, Any]:
        phone = str(payload["phone"])
        dialogs = [
            (int(item["dialog_id"]), _payload_channel_type(item))
            for item in payload.get("dialogs", [])
        ]
        result = await TelegramActionService(self._pool).delete_dialogs(phone=phone, dialogs=dialogs)
        return {"deleted": result.success_count, "failed": result.failed_count}

    async def _handle_dialogs_send(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await TelegramActionService(self._pool).send_message(
            phone=str(payload["phone"]),
            recipient=payload["recipient"],
            text=payload["text"],
        )
        return {"phone": result.phone, "message_id": result.message_id}

    async def _handle_dialogs_resolve(self, payload: dict[str, Any]) -> dict[str, Any]:
        identifier = str(payload["identifier"])
        entity = await self._pool.resolve_any_entity(
            identifier, phone=str(payload.get("phone") or "") or None
        )
        if not entity:
            raise RuntimeError(f"resolve failed: {identifier!r} not found")
        return {"entity": entity}

    async def _handle_dialogs_edit_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await TelegramActionService(self._pool).edit_message(
            phone=str(payload["phone"]),
            chat_id=payload["chat_id"],
            message_id=int(payload["message_id"]),
            text=payload["text"],
        )
        return {"phone": result.phone}

    async def _handle_dialogs_delete_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await TelegramActionService(self._pool).delete_messages(
            phone=str(payload["phone"]),
            chat_id=payload["chat_id"],
            message_ids=[int(value) for value in payload["message_ids"]],
        )
        return {"phone": result.phone, "deleted": result.count}

    async def _handle_dialogs_forward_messages(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await TelegramActionService(self._pool).forward_messages(
            phone=str(payload["phone"]),
            from_chat=payload["from_chat"],
            to_chat=payload["to_chat"],
            message_ids=[int(value) for value in payload["message_ids"]],
        )
        return {"phone": result.phone, "forwarded": result.count}

    async def _account_flood_until(self, phone: str) -> datetime | None:
        accounts: list[Any] = []
        for getter_name in ("get_account_summaries", "get_accounts"):
            getter = getattr(self._db, getter_name, None)
            if not callable(getter):
                continue
            try:
                result = getter(active_only=True)
            except TypeError:
                result = getter()
            if isawaitable(result):
                result = await result
            if isinstance(result, (list, tuple)):
                accounts = list(result)
                break
        now = datetime.now(timezone.utc)
        for account in accounts:
            if str(getattr(account, "phone", "")) != phone:
                continue
            flood_until = normalize_utc(getattr(account, "flood_wait_until", None))
            if flood_until is not None and flood_until > now:
                return flood_until
        return None

    async def _reaction_min_interval(self) -> float:
        """Per-phone minimum seconds between reactions, read live from DB settings.

        Clamped to a non-zero floor because Telegram rate-limits reactions
        server-side; values outside the range or unparseable fall back to the
        default.
        """
        raw = await self._db.get_setting(REACTION_MIN_INTERVAL_SETTING)
        value = parse_float_setting(
            raw,
            setting_name=REACTION_MIN_INTERVAL_SETTING,
            default=DEFAULT_REACTION_MIN_INTERVAL_SEC,
            logger=logger,
        )
        return max(REACTION_MIN_INTERVAL_FLOOR_SEC, min(REACTION_MIN_INTERVAL_CEILING_SEC, value))

    async def _ensure_reaction_can_run(self, phone: str) -> None:
        is_warming = self._pool_method("is_warming")
        if callable(is_warming):
            try:
                warming = bool(is_warming())
            except Exception:
                warming = False
            if warming:
                run_after = datetime.now(timezone.utc) + timedelta(seconds=5)
                raise TelegramCommandRetryLaterError(
                    run_after=run_after,
                    reason="account dialog warm-up is still running",
                    result_payload={
                        "state": "waiting_warmup",
                        "phone": phone,
                        "next_available_at_utc": run_after.isoformat(),
                    },
                )

        flood_until = await self._account_flood_until(phone)
        if flood_until is not None:
            raise TelegramCommandRetryLaterError(
                run_after=flood_until + timedelta(seconds=1),
                reason=f"account {phone} is flood-waited until {flood_until.isoformat()}",
                result_payload={
                    "state": "waiting_flood_wait",
                    "phone": phone,
                    "next_available_at_utc": flood_until.isoformat(),
                },
            )

        last = self._last_reaction_at_monotonic.get(phone)
        if last is None:
            return
        min_interval = await self._reaction_min_interval()
        elapsed = time.monotonic() - last
        remaining = min_interval - elapsed
        if remaining > 0:
            run_after = datetime.now(timezone.utc) + timedelta(seconds=remaining)
            raise TelegramCommandRetryLaterError(
                run_after=run_after,
                reason=f"reaction rate limit for {phone}; waiting {int(remaining) + 1}s",
                result_payload={
                    "state": "waiting_rate_limit",
                    "phone": phone,
                    "retry_after_sec": int(remaining) + 1,
                    "next_available_at_utc": run_after.isoformat(),
                },
            )

    async def _record_reaction(self, *phones: str) -> None:
        """Stamp the last-reaction time for every phone a reaction went out for,
        then evict entries that are older than the rate-limit window.

        Two phones are passed when the pool normalises the requested phone
        (``"+1"`` requested, ``"1"`` acquired): ``_ensure_reaction_can_run`` reads
        under the *requested* phone, so we must record under it too — recording
        only under the acquired phone would let the next reaction for ``"+1"``
        skip the gate entirely (#1030). Recording under both keys keeps the gate
        correct whichever form the next command carries.

        The map is in-memory only and keyed by phone, so without eviction a
        long-lived worker reacting across many accounts grows it without bound.
        Entries older than the interval carry no information — the gate would let
        that phone react regardless — so pruning them changes no behaviour and is
        not an idempotency ledger.

        This runs *after* the (irreversible) Telegram send, so the stamping —
        plain in-memory writes that cannot fail — happens unconditionally, while
        the prune is best-effort: it needs a live DB settings read for the
        interval, and a transient failure there must not bubble up and flip an
        already-sent reaction to FAILED (which would re-send it on retry, #1030).
        """
        now = time.monotonic()
        for phone in phones:
            if phone:
                self._last_reaction_at_monotonic[phone] = now
        try:
            min_interval = await self._reaction_min_interval()
        except Exception as exc:  # noqa: BLE001 — bookkeeping must not fail the send
            logger.warning("reaction timestamp prune skipped (interval read failed): %s", exc)
            return
        stale_before = now - min_interval
        stale = [
            phone
            for phone, stamp in self._last_reaction_at_monotonic.items()
            if stamp < stale_before
        ]
        for phone in stale:
            del self._last_reaction_at_monotonic[phone]

    async def _handle_dialogs_pin_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await TelegramActionService(self._pool).pin_message(
            phone=str(payload["phone"]),
            chat_id=payload["chat_id"],
            message_id=int(payload["message_id"]),
            notify=bool(payload.get("notify", False)),
        )
        return {"phone": result.phone}

    async def _handle_dialogs_react(self, payload: dict[str, Any]) -> dict[str, Any]:
        phone = str(payload["phone"])
        await self._ensure_reaction_can_run(phone)
        emoji = normalize_outgoing_reaction_emoji(str(payload.get("emoji") or ""))
        result = await TelegramActionService(self._pool).send_reaction(
            phone=phone,
            chat_id=payload["chat_id"],
            message_id=int(payload["message_id"]),
            emoji=emoji,
            native=True,
            resolve_entity=True,
        )
        # Record under both the requested phone (what the gate reads) and the
        # acquired phone the pool handed out, then prune stale entries (#1030).
        await self._record_reaction(phone, result.phone)
        return {"phone": result.phone}

    async def _handle_dialogs_unpin_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        message_id = payload.get("message_id")
        result = await TelegramActionService(self._pool).unpin_message(
            phone=str(payload["phone"]),
            chat_id=payload["chat_id"],
            message_id=int(message_id) if message_id is not None else None,
        )
        return {"phone": result.phone}

    async def _handle_dialogs_participants(self, payload: dict[str, Any]) -> dict[str, Any]:
        action_result = await TelegramActionService(self._pool).get_participants(
            phone=str(payload["phone"]),
            chat_id=payload["chat_id"],
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
            for p in action_result.participants
        ]
        scope = f"dialogs_participants:{action_result.phone}:{payload['chat_id']}"
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
        result = {"phone": action_result.phone, "scope": scope, "total": len(data)}
        if search_value:
            # Search results are intentionally not cached; return them
            # inline so the client can read them via GET /telegram-commands/{id}.
            result["participants"] = data
        return result

    async def _handle_dialogs_broadcast_stats(self, payload: dict[str, Any]) -> dict[str, Any]:
        action_result = await TelegramActionService(self._pool).get_broadcast_stats(
            phone=str(payload["phone"]),
            chat_id=payload["chat_id"],
        )
        stats = action_result.stats
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
        scope = f"dialogs_broadcast_stats:{action_result.phone}:{payload['chat_id']}"
        await self._db.repos.runtime_snapshots.upsert_snapshot(
            RuntimeSnapshot(
                snapshot_type="dialogs_broadcast_stats",
                scope=scope,
                payload={"stats": fields},
            )
        )
        return {"phone": action_result.phone, "scope": scope}

    async def _handle_dialogs_archive(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._set_dialog_folder(payload, folder_id=1)

    async def _handle_dialogs_unarchive(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._set_dialog_folder(payload, folder_id=0)

    async def _set_dialog_folder(self, payload: dict[str, Any], *, folder_id: int) -> dict[str, Any]:
        result = await TelegramActionService(self._pool).set_dialog_folder(
            phone=str(payload["phone"]),
            chat_id=payload["chat_id"],
            folder_id=folder_id,
        )
        return {"phone": result.phone, "folder_id": folder_id}

    async def _handle_dialogs_mark_read(self, payload: dict[str, Any]) -> dict[str, Any]:
        max_id = payload.get("max_id")
        result = await TelegramActionService(self._pool).mark_read(
            phone=str(payload["phone"]),
            chat_id=payload["chat_id"],
            max_id=int(max_id) if max_id is not None else None,
        )
        return {"phone": result.phone}

    async def _handle_dialogs_edit_admin(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await TelegramActionService(self._pool).edit_admin(
            phone=str(payload["phone"]),
            chat_id=payload["chat_id"],
            user_id=payload["user_id"],
            is_admin=bool(payload.get("is_admin", False)),
            title=payload.get("title") or None,
        )
        return {"phone": result.phone}

    async def _handle_dialogs_edit_permissions(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await TelegramActionService(self._pool).edit_permissions(
            phone=str(payload["phone"]),
            chat_id=payload["chat_id"],
            user_id=payload["user_id"],
            until_date=parse_required_datetime(str(payload["until_date"])) if payload.get("until_date") else None,
            send_messages=bool(payload["send_messages"]) if "send_messages" in payload else None,
            send_media=bool(payload["send_media"]) if "send_media" in payload else None,
        )
        return {"phone": result.phone}

    async def _handle_dialogs_kick(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await TelegramActionService(self._pool).kick_participant(
            phone=str(payload["phone"]),
            chat_id=payload["chat_id"],
            user_id=payload["user_id"],
        )
        return {"phone": result.phone}

    async def _handle_dialogs_create_channel(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await TelegramActionService(self._pool).create_channel(
            phone=str(payload["phone"]),
            title=str(payload["title"]).strip(),
            about=str(payload.get("about", "")).strip(),
            username=str(payload.get("username", "")).strip(),
        )
        return {
            "phone": result.phone,
            "channel_id": result.channel_id,
            "channel_title": result.channel_title,
            "channel_username": result.channel_username,
            "invite_link": result.invite_link,
        }
