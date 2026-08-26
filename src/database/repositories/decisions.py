"""Repository for the append-only decision provenance journal."""

from __future__ import annotations

from typing import TYPE_CHECKING

import aiosqlite

from src.database.pool import ReadConnection
from src.models import Decision
from src.utils.datetime import parse_datetime

if TYPE_CHECKING:
    from src.database.facade import Database


class DecisionsRepository:
    """Record and query who made a decision, and why."""

    def __init__(
        self,
        db: ReadConnection,
        *,
        database: "Database | None" = None,
    ):
        self._db = db
        self._database = database

    @staticmethod
    def _to_decision(row: aiosqlite.Row) -> Decision:
        keys = row.keys()
        return Decision(
            id=row["id"],
            entity=row["entity"],
            entity_key=row["entity_key"] if "entity_key" in keys else None,
            entity_name=row["entity_name"] if "entity_name" in keys else None,
            field=row["field"],
            old_value=row["old_value"] if "old_value" in keys else None,
            new_value=row["new_value"],
            origin=row["origin"],
            actor=row["actor"] if "actor" in keys else None,
            reason=row["reason"] if "reason" in keys else None,
            created_at=parse_datetime(row["created_at"] if "created_at" in keys else None),
        )

    async def record(
        self,
        *,
        entity: str,
        field: str,
        new_value: str,
        origin: str,
        entity_key: int | None = None,
        entity_name: str | None = None,
        old_value: str | None = None,
        actor: str | None = None,
        reason: str | None = None,
        commit: bool = True,
    ) -> int:
        """Append a decision and return its row id.

        With ``commit=False`` the caller must own an open ``Database.transaction``;
        the insert then uses that transaction's write connection and is committed
        together with the caller's other changes.
        """
        assert self._database is not None, (
            "DecisionsRepository.record requires a Database reference"
        )
        params = (
            entity,
            entity_key,
            entity_name,
            field,
            old_value,
            new_value,
            origin,
            actor,
            reason,
        )
        sql = (
            "INSERT INTO decisions "
            "(entity, entity_key, entity_name, field, old_value, new_value, origin, actor, reason) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        if commit:
            cur = await self._database.execute_write(sql, params)
            return cur.lastrowid or 0

        write_conn = self._database.db
        assert write_conn is not None, (
            "DecisionsRepository.record(commit=False) requires an active Database connection"
        )
        assert write_conn.in_transaction, (
            "DecisionsRepository.record(commit=False) must run inside a caller-owned "
            "Database.transaction() block"
        )
        cur = await write_conn.execute(sql, params)
        return cur.lastrowid or 0

    async def history(
        self,
        entity: str,
        entity_key: int,
        field: str | None = None,
        limit: int = 50,
    ) -> list[Decision]:
        """Return newest decisions for an entity, optionally limited to a field."""
        conditions = ["entity = ?", "entity_key = ?"]
        params: list[object] = [entity, entity_key]
        if field is not None:
            conditions.append("field = ?")
            params.append(field)
        params.append(limit)
        cur = await self._db.execute(
            "SELECT * FROM decisions WHERE "
            + " AND ".join(conditions)
            + " ORDER BY id DESC LIMIT ?",
            tuple(params),
        )
        return [self._to_decision(row) for row in await cur.fetchall()]

    async def last_human_decision(
        self,
        entity: str,
        entity_key: int,
        field: str,
    ) -> Decision | None:
        """Return the newest human decision for an entity field, if any."""
        cur = await self._db.execute(
            "SELECT * FROM decisions "
            "WHERE entity = ? AND entity_key = ? AND field = ? AND origin = 'human' "
            "ORDER BY id DESC LIMIT 1",
            (entity, entity_key, field),
        )
        row = await cur.fetchone()
        return self._to_decision(row) if row else None
