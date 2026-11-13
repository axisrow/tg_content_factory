"""Persist transport request identities in each project's existing SQLite DB."""

from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from tg_agent.database import Database
from tg_agent.gateway.client import TelegramGatewayClient
from tg_agent.gateway.protocol import Operation

logger = logging.getLogger(__name__)

# #1773 retention windows. Receipted rows carry a terminal receipt and only
# matter for audit; pending rows (NULL receipt — uncertain writes, deferred
# reads, in-flight intents) keep the longer window so a live retry or an
# operator verifying an uncertain delivery always finds its intent in place.
RECEIPTED_RETENTION_DAYS = 7
PENDING_RETENTION_DAYS = 30
PRUNE_INTERVAL_SEC = 3600


class ProjectOutbox:
    """An action key names one intent, including across worker restarts."""

    def __init__(self, db: Database):
        self.db = db
        self._last_prune = 0.0

    async def initialize(self) -> None:
        async with self.db.transaction() as connection:
            await connection.execute(
                "CREATE TABLE IF NOT EXISTS gateway_outbox ("
                "action_key TEXT PRIMARY KEY, request_id TEXT UNIQUE NOT NULL, "
                "payload TEXT NOT NULL, receipt TEXT, created_at TEXT)"
            )
            cursor = await connection.execute("PRAGMA table_info(gateway_outbox)")
            columns = {row[1] for row in await cursor.fetchall()}
            if "created_at" not in columns:
                # Pre-retention rows have no known age: stamp them at the
                # migration moment so they ride the longest window instead of
                # being treated as ancient and dropped on the first prune.
                await connection.execute("ALTER TABLE gateway_outbox ADD COLUMN created_at TEXT")
                await connection.execute("UPDATE gateway_outbox SET created_at=? WHERE created_at IS NULL",
                                         (datetime.now(timezone.utc).isoformat(),))

    async def prepare(self, action_key: str, operation: Operation) -> UUID:
        serialized = json.dumps(operation.model_dump(mode="json"), sort_keys=True)
        async with self.db.transaction() as connection:
            await connection.execute(
                "INSERT OR IGNORE INTO gateway_outbox (action_key, request_id, payload, receipt, created_at) "
                "VALUES (?,?,?,?,?)",
                (action_key, str(uuid4()), serialized, None, datetime.now(timezone.utc).isoformat()))
            cursor = await connection.execute("SELECT request_id,payload FROM gateway_outbox WHERE action_key=?",
                                              (action_key,))
            row = await cursor.fetchone()
            if row[1] != serialized:
                raise ValueError("Gateway action key already identifies a different intent")
            return UUID(row[0])

    async def record(self, action_key: str, receipt: dict) -> None:
        async with self.db.transaction() as connection:
            await connection.execute("UPDATE gateway_outbox SET receipt=? WHERE action_key=?",
                                     (json.dumps(receipt), action_key))

    async def execute(self, client: TelegramGatewayClient, action_key: str, operation: Operation) -> dict:
        request = await self.prepare(action_key, operation)
        await client.submit(request, operation)
        receipt = await client.wait(request)
        await self.record(action_key, receipt)
        return receipt

    async def prune(self, *, force: bool = False) -> dict[str, int]:
        """Delete aged rows so gateway_outbox stays bounded (#1773).

        Only rows past their retention window go; a row still inside it —
        receipted or pending — is never touched, and pending rows are kept
        the longest because they back idempotent retries and uncertain
        delivery verification. Throttled to once per PRUNE_INTERVAL_SEC:
        GatewayPool.initialize runs every few seconds in the refresh loop.
        """
        now = time.time()
        if not force and now - self._last_prune < PRUNE_INTERVAL_SEC:
            return {"receipted": 0, "pending": 0}
        receipted_cutoff = (datetime.now(timezone.utc) - timedelta(days=RECEIPTED_RETENTION_DAYS)).isoformat()
        pending_cutoff = (datetime.now(timezone.utc) - timedelta(days=PENDING_RETENTION_DAYS)).isoformat()
        async with self.db.transaction() as connection:
            cursor = await connection.execute(
                "DELETE FROM gateway_outbox WHERE receipt IS NOT NULL AND created_at < ?", (receipted_cutoff,))
            receipted = cursor.rowcount
            cursor = await connection.execute(
                "DELETE FROM gateway_outbox WHERE receipt IS NULL AND created_at < ?", (pending_cutoff,))
            pending = cursor.rowcount
        self._last_prune = now
        if receipted or pending:
            logger.info("gateway_outbox pruned: receipted=%d pending=%d", receipted, pending)
        return {"receipted": receipted, "pending": pending}
