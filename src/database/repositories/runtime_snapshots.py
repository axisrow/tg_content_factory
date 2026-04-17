from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import aiosqlite

from src.models import RuntimeSnapshot


def _parse_json(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


class RuntimeSnapshotsRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    @staticmethod
    def _to_snapshot(row: aiosqlite.Row) -> RuntimeSnapshot:
        return RuntimeSnapshot(
            snapshot_type=row["snapshot_type"],
            scope=row["scope"],
            payload=_parse_json(row["payload"]),
            updated_at=(datetime.fromisoformat(row["updated_at"]) if row["updated_at"] else None),
        )

    async def upsert_snapshot(self, snapshot: RuntimeSnapshot) -> None:
        await self._db.execute(
            """
            INSERT INTO runtime_snapshots (snapshot_type, scope, payload, updated_at)
            VALUES (?, ?, ?, COALESCE(?, datetime('now')))
            ON CONFLICT(snapshot_type, scope) DO UPDATE SET
                payload = excluded.payload,
                updated_at = COALESCE(excluded.updated_at, datetime('now'))
            """,
            (
                snapshot.snapshot_type,
                snapshot.scope,
                json.dumps(snapshot.payload),
                snapshot.updated_at.isoformat() if snapshot.updated_at is not None else None,
            ),
        )
        await self._db.commit()

    async def get_snapshot(self, snapshot_type: str, scope: str = "global") -> RuntimeSnapshot | None:
        cur = await self._db.execute(
            """
            SELECT * FROM runtime_snapshots
            WHERE snapshot_type = ? AND scope = ?
            """,
            (snapshot_type, scope),
        )
        row = await cur.fetchone()
        return self._to_snapshot(row) if row else None
