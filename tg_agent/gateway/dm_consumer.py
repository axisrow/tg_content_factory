"""Deliver gateway DM events into the project's existing DM storage atomically.

The gateway journals every private message of the shared accounts; this
consumer pulls the project's events by cursor. Since #1868 the gateway pushes a
notice over the relay the moment it journals one, so the pull happens at once
(the 30 s timeout is only a safety net), and history recovery reads natively
over the same relay.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import cast

from tg_agent.database import Database
from tg_agent.database.repositories.dm_messages import DmMessagesRepository
from tg_agent.database.repositories.incoming_dms import IncomingDmsRepository
from tg_agent.database.repositories.messages import _TransactionWrites
from tg_agent.gateway.catalog import ProjectAccountCatalog
from tg_agent.gateway.client import GatewayRequestError
from tg_agent.models import DmMessage, IncomingDm
from tg_agent.telegram.dm_catchup import load_dm_catchup_settings
from tg_agent.telegram.dm_history import read_dialog_history_since

logger = logging.getLogger(__name__)

_EVENT_POLL_SEC = 30.0
_CATALOG_REFRESH_SEC = 60.0
# ponytail: one fixed pause after a failed recovery; make it grow if a long
# outage should cost fewer dialog sweeps than four an hour.
_RECOVERY_RETRY_SEC = 900.0


class GatewayDmConsumer:
    def __init__(self, pool, db, client):
        self.pool = pool
        self.db = db
        self.client = client
        self._task = None
        self._resync_required = False
        self._recover_not_before = 0.0
        self._events_waiting = asyncio.Event()

    async def start(self):
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="project-gateway-dm-consumer")

    async def stop(self):
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    def status(self):
        return {"running": self._task is not None and not self._task.done(), "accounts": {},
                "resync_required": self._resync_required}

    async def _run(self):
        try:
            await self.client.subscribe_events(self._events_waiting.set)
        except Exception as exc:  # the poll below still delivers
            logger.warning("Gateway DM push unavailable: %s", type(exc).__name__)
        loop = asyncio.get_running_loop()
        next_refresh = 0.0
        while True:
            self._events_waiting.clear()
            try:
                if loop.time() >= next_refresh:
                    await ProjectAccountCatalog(self.db, self.client).refresh()
                    next_refresh = loop.time() + _CATALOG_REFRESH_SEC
                await self.consume()
            except GatewayRequestError as exc:
                self._resync_required = exc.status == 410
                logger.warning("Gateway DM synchronization delayed: status %s", exc.status)
            except Exception as exc:
                logger.warning("Gateway DM synchronization delayed: %s", type(exc).__name__)
            try:
                async with asyncio.timeout(_EVENT_POLL_SEC):
                    await self._events_waiting.wait()
            except TimeoutError:
                pass

    async def consume(self):
        cursor = int(await self.db.get_setting("gateway_event_cursor") or 0)
        try:
            events = await self.client.events(cursor)
        except GatewayRequestError as exc:
            if exc.status != 410:
                raise
            self._resync_required = True
            loop = asyncio.get_running_loop()
            if loop.time() < self._recover_not_before:
                return
            try:
                await self.recover()
            except Exception:
                # A recovery sweeps every account's dialog list, so a failing one
                # waits before the next try instead of repeating that on each poll.
                self._recover_not_before = loop.time() + _RECOVERY_RETRY_SEC
                raise
            self._resync_required = False
            return
        if not events:
            return
        accounts = {row["id"]: row["phone"] for row in await self.client.accounts()}
        for event in events:
            payload = event["payload"]
            phone = accounts.get(event["account_id"])
            if phone is None:
                raise ValueError("Gateway event references an unavailable account")
            async with self.db.transaction() as conn:
                await self._save(conn, phone, payload)
                await conn.execute("INSERT INTO settings(key,value) VALUES ('gateway_event_cursor',?) "
                                   "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(event["id"]),))
            await self.client.acknowledge(event["id"])

    async def _save(self, conn, phone, payload, *, processed=False):
        date = datetime.fromisoformat(payload["date"]) if payload.get("date") else None
        fields = {"phone": phone, "chat_id": payload["dialog_id"], "message_id": payload["message_id"],
                  "text": payload.get("text"), "message_date": date, "received_at": datetime.now(timezone.utc)}
        writer = cast("Database", _TransactionWrites(conn))
        await DmMessagesRepository(conn, database=writer).record(DmMessage(**fields, out=payload["out"]))
        if not payload["out"]:
            await IncomingDmsRepository(conn, database=writer).record(IncomingDm(**fields), processed=processed)

    async def recover(self):
        # Capture the retained floor BEFORE reading history. Events arriving during
        # recovery are subsequently replayed and deduplicated by local unique keys.
        floor = await self.client.event_floor()
        settings = await load_dm_catchup_settings(self.db)
        for account in await self.client.accounts():
            if not account["active"]:
                continue
            phone = account["phone"]
            dialogs = await self.pool.get_dialogs_for_phone(phone, include_dm=True, refresh=True)
            for dialog in dialogs:
                if dialog["channel_type"] not in {"dm", "user", "bot"}:
                    continue
                chat = dialog.get("dialog_id") or dialog["channel_id"]
                mode, staleness = settings.resolve(phone, chat)
                if mode == "ignore":
                    continue
                await self._recover_dialog(phone, chat, mode, staleness)
        await self.db.set_setting("gateway_event_cursor", str(floor))
        await self.client.acknowledge(floor)

    async def _recover_dialog(self, phone, chat, mode, staleness):
        cursor = await self.db.repos.dm_messages.max_message_id(phone, chat)
        acquired = await self.pool.get_client_by_phone(phone)
        if acquired is None:
            return
        session, acquired_phone = acquired
        try:
            while True:
                rows = await read_dialog_history_since(
                    session.raw_client, api_id=0, api_hash="", peer=chat, min_id=cursor, limit=200)
                if not rows:
                    break
                async with self.db.transaction() as conn:
                    for message in rows:
                        date = message.date if message.date.tzinfo else message.date.replace(tzinfo=timezone.utc)
                        stale = datetime.now(timezone.utc) - date > timedelta(seconds=staleness)
                        payload = {"dialog_id": chat, "message_id": message.id, "text": message.text,
                                   "out": message.out, "date": date.isoformat()}
                        await self._save(conn, phone, payload, processed=mode == "journal_only" or stale)
                next_id = max(message.id for message in rows)
                if next_id <= cursor:
                    raise ValueError("Gateway DM history returned a non-advancing page")
                cursor = next_id
        finally:
            await self.pool.release_client(acquired_phone)
