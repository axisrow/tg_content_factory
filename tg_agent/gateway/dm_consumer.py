"""Deliver gateway events into the project's existing DM storage atomically."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import cast
from uuid import UUID, uuid4

import httpx

from tg_agent.database import Database
from tg_agent.database.repositories.dm_messages import DmMessagesRepository
from tg_agent.database.repositories.incoming_dms import IncomingDmsRepository
from tg_agent.database.repositories.messages import _TransactionWrites
from tg_agent.gateway.protocol import DmHistory
from tg_agent.models import DmMessage, IncomingDm
from tg_agent.telegram.dm_catchup import load_dm_catchup_settings

logger = logging.getLogger(__name__)


class GatewayDmConsumer:
    def __init__(self, pool, db):
        self.pool = pool
        self.db = db
        self._task = None
        self._resync_required = False

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
        while True:
            try:
                await self.consume()
            except httpx.HTTPStatusError as exc:
                self._resync_required = exc.response.status_code == 410
                logger.warning("Gateway DM synchronization delayed: HTTP %s", exc.response.status_code)
            except Exception as exc:
                logger.warning("Gateway DM synchronization delayed: %s", type(exc).__name__)
            await asyncio.sleep(1)

    async def consume(self):
        client = self.pool.client
        if client is None:
            return
        cursor = int(await self.db.get_setting("gateway_event_cursor") or 0)
        try:
            events = await client.events(cursor)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 410:
                raise
            self._resync_required = True
            await self.recover()
            self._resync_required = False
            return
        accounts = {row["id"]: row["phone"] for row in await client.accounts()}
        for event in events:
            payload = event["payload"]
            phone = accounts.get(event["account_id"])
            if phone is None:
                raise ValueError("Gateway event references an unavailable account")
            async with self.db.transaction() as conn:
                await self._save(conn, phone, payload)
                await conn.execute("INSERT INTO settings(key,value) VALUES ('gateway_event_cursor',?) "
                                   "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(event["id"]),))
            await client.acknowledge(event["id"])

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
        response = await self.pool.client.http.get("/v1/events/recovery")
        response.raise_for_status()
        floor = response.json()["floor"]
        settings = await load_dm_catchup_settings(self.db)
        for account in await self.pool.client.accounts():
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
                cursor = await self.db.repos.dm_messages.max_message_id(phone, chat)
                while True:
                    operation = DmHistory(kind="dm_history", account_id=UUID(account["id"]),
                                          dialog_id=chat, after_id=cursor)
                    data = await self.pool.request(operation, f"dm-recovery:{uuid4()}")
                    rows = data["messages"]
                    if not rows:
                        break
                    async with self.db.transaction() as conn:
                        for payload in rows:
                            date = datetime.fromisoformat(payload["date"]) if payload.get("date") else None
                            if date is not None and date.tzinfo is None:
                                date = date.replace(tzinfo=timezone.utc)
                            stale = date is None or datetime.now(timezone.utc) - date > timedelta(seconds=staleness)
                            await self._save(conn, phone, payload, processed=mode == "journal_only" or stale)
                    next_id = max(row["message_id"] for row in rows)
                    if next_id <= cursor:
                        raise ValueError("Gateway DM history returned a non-advancing page")
                    cursor = next_id
        await self.db.set_setting("gateway_event_cursor", str(floor))
        await self.pool.client.acknowledge(floor)
