"""Репозиторий пинг-мониторов затихших DM-контактов (issue #1415).

Доступ через `db.repos.ping_monitors`. Таблица `ping_monitors` — по строке на
1:1 диалог, поставленный владельцем под наблюдение: базовый порог тишины, текст
пинга, счётчик затухающей прогрессии и отметки последней активности. Повторная
постановка того же диалога того же аккаунта исключена `UNIQUE(phone, dialog_id)`.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

import aiosqlite

from tg_agent.models import PingMonitor
from tg_agent.utils.datetime import parse_datetime

if TYPE_CHECKING:
    from tg_agent.database.facade import Database


class PingMonitorRepository:
    """CRUD пинг-мониторов (таблица `ping_monitors`)."""

    def __init__(
        self,
        db,
        *,
        database: "Database | None" = None,
    ):
        self._db = db
        self._database = database

    @staticmethod
    def _to_monitor(row: aiosqlite.Row) -> PingMonitor:
        return PingMonitor(
            id=row["id"],
            phone=row["phone"],
            dialog_id=row["dialog_id"],
            dialog_title=row["dialog_title"],
            is_active=bool(row["is_active"]),
            silence_threshold_days=row["silence_threshold_days"],
            nudge_text=row["nudge_text"],
            cycle_count=row["cycle_count"],
            last_checked_at=parse_datetime(row["last_checked_at"]),
            last_incoming_message_at=parse_datetime(row["last_incoming_message_at"]),
            last_outgoing_message_at=parse_datetime(row["last_outgoing_message_at"]),
            last_nudge_sent_at=parse_datetime(row["last_nudge_sent_at"]),
            error=row["error"],
            created_at=parse_datetime(row["created_at"]),
        )

    async def create(self, monitor: PingMonitor) -> int:
        """Создать монитор; вернуть id. Дубль (phone, dialog_id) — IntegrityError."""
        assert self._database is not None, (
            "PingMonitorRepository.create requires a Database reference"
        )
        cur = await self._database.execute_write(
            """
            INSERT INTO ping_monitors (
                phone, dialog_id, dialog_title, is_active,
                silence_threshold_days, nudge_text, cycle_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                monitor.phone,
                monitor.dialog_id,
                monitor.dialog_title,
                1 if monitor.is_active else 0,
                monitor.silence_threshold_days,
                monitor.nudge_text,
                monitor.cycle_count,
            ),
        )
        return cur.lastrowid or 0

    async def get(self, monitor_id: int) -> PingMonitor | None:
        """Один монитор по id, либо ``None``."""
        cur = await self._db.execute("SELECT * FROM ping_monitors WHERE id = ?", (monitor_id,))
        row = await cur.fetchone()
        return self._to_monitor(row) if row else None

    async def list(self, active_only: bool = False) -> list[PingMonitor]:
        """Все мониторы по порядку id; с ``active_only`` — только активные."""
        sql = "SELECT * FROM ping_monitors"
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY id"
        cur = await self._db.execute(sql)
        return [self._to_monitor(row) for row in await cur.fetchall()]

    async def update(
        self,
        monitor_id: int,
        *,
        nudge_text: str | None = None,
        silence_threshold_days: int | None = None,
        is_active: bool | None = None,
        cycle_count: int | None = None,
        last_checked_at: datetime | None = None,
        last_incoming_message_at: datetime | None = None,
        last_outgoing_message_at: datetime | None = None,
        last_nudge_sent_at: datetime | None = None,
        error: str | None = None,
    ) -> None:
        """Частично обновить монитор (текст, порог, прогрессия, отметки); пусто — no-op."""
        assert self._database is not None, (
            "PingMonitorRepository.update requires a Database reference"
        )
        sets: list[str] = []
        params: list[object] = []
        if nudge_text is not None:
            sets.append("nudge_text = ?")
            params.append(nudge_text)
        if silence_threshold_days is not None:
            sets.append("silence_threshold_days = ?")
            params.append(silence_threshold_days)
        if is_active is not None:
            sets.append("is_active = ?")
            params.append(1 if is_active else 0)
        if cycle_count is not None:
            sets.append("cycle_count = ?")
            params.append(cycle_count)
        if last_checked_at is not None:
            sets.append("last_checked_at = ?")
            params.append(last_checked_at.astimezone(timezone.utc).isoformat())
        if last_incoming_message_at is not None:
            sets.append("last_incoming_message_at = ?")
            params.append(last_incoming_message_at.astimezone(timezone.utc).isoformat())
        if last_outgoing_message_at is not None:
            sets.append("last_outgoing_message_at = ?")
            params.append(last_outgoing_message_at.astimezone(timezone.utc).isoformat())
        if last_nudge_sent_at is not None:
            sets.append("last_nudge_sent_at = ?")
            params.append(last_nudge_sent_at.astimezone(timezone.utc).isoformat())
        if error is not None:
            sets.append("error = ?")
            params.append(error)
        if not sets:
            return
        params.append(monitor_id)
        await self._database.execute_write(
            f"UPDATE ping_monitors SET {', '.join(sets)} WHERE id = ?",
            tuple(params),
        )

    async def delete(self, monitor_id: int) -> bool:
        """Удалить монитор по id; вернуть, была ли удалена строка (#1933)."""
        assert self._database is not None, (
            "PingMonitorRepository.delete requires a Database reference"
        )
        cur = await self._database.execute_write("DELETE FROM ping_monitors WHERE id = ?", (monitor_id,))
        return cur.rowcount > 0
