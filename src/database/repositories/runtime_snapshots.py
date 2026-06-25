"""Снимки состояния воркера для чтения web-стороной (канал worker → web)."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import aiosqlite

from src.models import RuntimeSnapshot
from src.utils.datetime import parse_datetime
from src.utils.json import safe_json_dumps


def _parse_json(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


if TYPE_CHECKING:
    from src.database.facade import Database


class RuntimeSnapshotsRepository:
    """Снимки живого состояния воркера, публикуемые для web-стороны.

    Воркер пишет (`upsert_snapshot`) heartbeat, статусы аккаунтов/планировщика и
    т.п., а web-контейнер (который не держит Telegram-соединений) читает их
    (`get_snapshot`), чтобы отрисовать статус. Идентичность снимка —
    `(snapshot_type, scope)`; запись — upsert по этой паре.
    """

    def __init__(
        self,
        db: aiosqlite.Connection,
        *,
        database: "Database | None" = None,
    ):
        self._db = db
        self._database = database

    @staticmethod
    def _to_snapshot(row: aiosqlite.Row) -> RuntimeSnapshot:
        return RuntimeSnapshot(
            snapshot_type=row["snapshot_type"],
            scope=row["scope"],
            payload=_parse_json(row["payload"]),
            updated_at=parse_datetime(row["updated_at"]),
        )

    async def upsert_snapshot(self, snapshot: RuntimeSnapshot) -> None:
        """Записать/обновить снимок по паре (snapshot_type, scope).

        `updated_at` берётся из снимка либо проставляется текущим временем БД.
        """
        await self._database.execute_write(
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
                safe_json_dumps(snapshot.payload),
                snapshot.updated_at.isoformat() if snapshot.updated_at is not None else None,
            ),
        )

    async def get_snapshot(self, snapshot_type: str, scope: str = "global") -> RuntimeSnapshot | None:
        """Снимок по типу и области (по умолчанию глобальной), либо None."""
        cur = await self._db.execute(
            """
            SELECT * FROM runtime_snapshots
            WHERE snapshot_type = ? AND scope = ?
            """,
            (snapshot_type, scope),
        )
        row = await cur.fetchone()
        return self._to_snapshot(row) if row else None
