"""Persist transport request identities in each project's existing SQLite DB."""

from __future__ import annotations

import json
from uuid import UUID, uuid4

from tg_agent.database import Database
from tg_agent.gateway.client import TelegramGatewayClient
from tg_agent.gateway.protocol import Operation


class ProjectOutbox:
    """An action key names one intent, including across worker restarts."""

    def __init__(self, db: Database):
        self.db = db

    async def initialize(self) -> None:
        async with self.db.transaction() as connection:
            await connection.execute(
                "CREATE TABLE IF NOT EXISTS gateway_outbox ("
                "action_key TEXT PRIMARY KEY, request_id TEXT UNIQUE NOT NULL, "
                "payload TEXT NOT NULL, receipt TEXT)"
            )

    async def prepare(self, action_key: str, operation: Operation) -> UUID:
        serialized = json.dumps(operation.model_dump(mode="json"), sort_keys=True)
        async with self.db.transaction() as connection:
            await connection.execute("INSERT OR IGNORE INTO gateway_outbox VALUES (?,?,?,NULL)",
                                     (action_key, str(uuid4()), serialized))
            async with connection.execute("SELECT request_id,payload FROM gateway_outbox WHERE action_key=?",
                                          (action_key,)) as cursor:
                row = await cursor.fetchone()
            if row[1] != serialized:
                raise ValueError("Gateway action key already identifies a different intent")
            return UUID(row[0])

    async def execute(self, client: TelegramGatewayClient, action_key: str, operation: Operation) -> dict:
        request = await self.prepare(action_key, operation)
        await client.submit(request, operation)
        receipt = await client.wait(request)
        async with self.db.transaction() as connection:
            await connection.execute("UPDATE gateway_outbox SET receipt=? WHERE action_key=?",
                                     (json.dumps(receipt), action_key))
        return receipt
