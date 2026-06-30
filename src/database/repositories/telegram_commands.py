"""Репозиторий очереди команд Telegram-воркеру (постановка, claim, обновление статуса)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import aiosqlite

from src.models import TelegramCommand, TelegramCommandStatus
from src.utils.datetime import parse_datetime
from src.utils.json import safe_json_dumps


def _parse_json(raw: str | None) -> dict[str, Any] | None:
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


if TYPE_CHECKING:
    from src.database.facade import Database


class TelegramCommandsRepository:
    """Очередь команд Telegram-воркеру (BotFather, отправка, auth и т.п.).

    Web/CLI ставят команды в очередь (`create_command`), воркер атомарно
    забирает их (`claim_next_command`: PENDING → RUNNING под транзакцией) и
    обновляет статус/результат (`update_command`). Поддерживает фильтрацию по
    типу/статусу/телефону и счётчики для панели мониторинга.
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
    def _to_command(row: aiosqlite.Row) -> TelegramCommand:
        return TelegramCommand(
            id=row["id"],
            command_type=row["command_type"],
            payload=_parse_json(row["payload"]) or {},
            status=TelegramCommandStatus(row["status"]),
            requested_by=row["requested_by"],
            created_at=parse_datetime(row["created_at"]),
            started_at=parse_datetime(row["started_at"]),
            run_after=parse_datetime(row["run_after"]),
            finished_at=parse_datetime(row["finished_at"]),
            error=row["error"],
            result_payload=_parse_json(row["result_payload"]),
        )

    async def create_command(self, command: TelegramCommand) -> int:
        """Поставить команду в очередь воркеру. Возвращает id новой строки."""
        assert self._database is not None, (
            "TelegramCommandsRepository.create_command requires a Database reference"
        )
        cur = await self._database.execute_write(
            """
            INSERT INTO telegram_commands (
                command_type, payload, status, requested_by, run_after, result_payload
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                command.command_type,
                safe_json_dumps(command.payload),
                command.status.value,
                command.requested_by,
                command.run_after.astimezone(timezone.utc).isoformat()
                if command.run_after is not None
                else None,
                safe_json_dumps(command.result_payload) if command.result_payload is not None else None,
            ),
        )
        return cur.lastrowid or 0

    async def get_command(self, command_id: int) -> TelegramCommand | None:
        """Прочитать команду по id, либо None если её нет."""
        cur = await self._db.execute(
            "SELECT * FROM telegram_commands WHERE id = ?",
            (command_id,),
        )
        row = await cur.fetchone()
        return self._to_command(row) if row else None

    @staticmethod
    def _filtered_query(
        *,
        command_type: str | None = None,
        status: TelegramCommandStatus | None = None,
        phone: str | None = None,
    ) -> tuple[str, list[Any]]:
        where: list[str] = []
        params: list[Any] = []
        if command_type:
            where.append("command_type = ?")
            params.append(command_type)
        if status is not None:
            where.append("status = ?")
            params.append(status.value)
        if phone:
            where.append("json_extract(payload, '$.phone') = ?")
            params.append(phone)
        clause = f"WHERE {' AND '.join(where)}" if where else ""
        return clause, params

    async def list_commands(
        self,
        *,
        limit: int = 100,
        command_type: str | None = None,
        status: TelegramCommandStatus | None = None,
        phone: str | None = None,
    ) -> list[TelegramCommand]:
        """Список команд (новые сверху) с опциональным фильтром по типу/статусу/телефону."""
        where, params = self._filtered_query(command_type=command_type, status=status, phone=phone)
        cur = await self._db.execute(
            f"SELECT * FROM telegram_commands {where} ORDER BY id DESC LIMIT ?",
            (*params, limit),
        )
        rows = await cur.fetchall()
        return [self._to_command(row) for row in rows]

    async def count_by_status(
        self,
        *,
        command_type: str | None = None,
        status: TelegramCommandStatus | None = None,
        phone: str | None = None,
    ) -> dict[TelegramCommandStatus, int]:
        """Сколько команд в каждом статусе (под фильтр). Все статусы присутствуют, нулевые — с 0."""
        where, params = self._filtered_query(command_type=command_type, status=status, phone=phone)
        cur = await self._db.execute(
            f"""
            SELECT status, COUNT(*) AS count
            FROM telegram_commands
            {where}
            GROUP BY status
            """,
            tuple(params),
        )
        rows = await cur.fetchall()
        result = {status_value: 0 for status_value in TelegramCommandStatus}
        for row in rows:
            result[TelegramCommandStatus(row["status"])] = int(row["count"] or 0)
        return result

    async def count_result_states(
        self,
        *,
        command_type: str | None = None,
        status: TelegramCommandStatus | None = None,
        phone: str | None = None,
    ) -> dict[str, int]:
        """Распределение по `result_payload.$.state` (под фильтр) — детализация исходов команд.

        Пустые состояния опускаются; ключ — значение `$.state` из JSON-результата.
        """
        where, params = self._filtered_query(command_type=command_type, status=status, phone=phone)
        cur = await self._db.execute(
            f"""
            SELECT COALESCE(json_extract(result_payload, '$.state'), '') AS state, COUNT(*) AS count
            FROM telegram_commands
            {where}
            GROUP BY state
            """,
            tuple(params),
        )
        rows = await cur.fetchall()
        return {str(row["state"]): int(row["count"] or 0) for row in rows if row["state"]}

    async def find_active_by_type(
        self, command_type: str, *, payload: dict[str, Any] | None = None
    ) -> TelegramCommand | None:
        """Return the oldest PENDING/RUNNING command of the given type (and payload, if provided)."""
        cur = await self._db.execute(
            """
            SELECT * FROM telegram_commands
            WHERE command_type = ? AND status IN (?, ?)
            ORDER BY id ASC
            """,
            (
                command_type,
                TelegramCommandStatus.PENDING.value,
                TelegramCommandStatus.RUNNING.value,
            ),
        )
        rows = await cur.fetchall()
        if not rows:
            return None
        if payload is None:
            return self._to_command(rows[0])
        for row in rows:
            row_payload = _parse_json(row["payload"]) or {}
            if row_payload == payload:
                return self._to_command(row)
        return None

    async def cancel_command(self, command_id: int) -> bool:
        """Cancel a single PENDING command.

        Returns True if the row was transitioned PENDING → CANCELLED. RUNNING
        commands are not cancelled here — the dispatcher may already be in the
        middle of a Telegram API call and the row would be overwritten when
        it finishes; cancel from the UI is only safe for not-yet-claimed work.
        """
        assert self._database is not None, (
            "TelegramCommandsRepository.cancel_command requires a Database reference"
        )
        finished_at = datetime.now(timezone.utc).isoformat()
        cur = await self._database.execute_write(
            """
            UPDATE telegram_commands
            SET status = ?, finished_at = ?, run_after = NULL
            WHERE id = ? AND status = ?
            """,
            (
                TelegramCommandStatus.CANCELLED.value,
                finished_at,
                command_id,
                TelegramCommandStatus.PENDING.value,
            ),
        )
        return (cur.rowcount or 0) > 0

    async def cancel_pending_commands(
        self,
        *,
        command_type: str | None = None,
        phone: str | None = None,
    ) -> int:
        """Bulk-cancel PENDING commands matching the filters. Returns count."""
        assert self._database is not None, (
            "TelegramCommandsRepository.cancel_pending_commands requires a Database reference"
        )
        finished_at = datetime.now(timezone.utc).isoformat()
        where = ["status = ?"]
        params: list[Any] = [TelegramCommandStatus.PENDING.value]
        if command_type:
            where.append("command_type = ?")
            params.append(command_type)
        if phone:
            where.append("json_extract(payload, '$.phone') = ?")
            params.append(phone)
        sql = (
            "UPDATE telegram_commands "
            "SET status = ?, finished_at = ?, run_after = NULL "
            f"WHERE {' AND '.join(where)}"
        )
        cur = await self._database.execute_write(
            sql,
            (
                TelegramCommandStatus.CANCELLED.value,
                finished_at,
                *params,
            ),
        )
        return cur.rowcount or 0

    async def reset_running_on_startup(self) -> int:
        """Move RUNNING commands back to PENDING on worker startup.

        Commands can be left in RUNNING if the worker was killed mid-dispatch
        (asyncio.CancelledError re-raise, SIGTERM, etc). Without this reset
        they would stay claimed forever, since claim_next_command only picks
        PENDING rows.
        """
        assert self._database is not None, (
            "TelegramCommandsRepository.reset_running_on_startup requires a Database reference"
        )
        cur = await self._database.execute_write(
            """
            UPDATE telegram_commands
            SET status = ?, started_at = NULL
            WHERE status = ?
            """,
            (TelegramCommandStatus.PENDING.value, TelegramCommandStatus.RUNNING.value),
        )
        return cur.rowcount or 0

    async def claim_next_command(self) -> TelegramCommand | None:
        """Атомарно забрать следующую готовую команду: PENDING → RUNNING.

        Под транзакцией выбирает старейшую PENDING-строку, у которой `run_after`
        не в будущем, и помечает её RUNNING со `started_at`, чтобы два воркера не
        взяли одну команду. Возвращает обновлённую команду либо None, если очередь
        пуста.
        """
        assert self._database is not None, (
            "TelegramCommandsRepository.claim_next_command requires a Database reference"
        )
        now = datetime.now(timezone.utc).isoformat()
        claimed_id: int | None = None
        async with self._database.transaction() as conn:
            cur = await conn.execute(
                """
                SELECT * FROM telegram_commands
                WHERE status = ?
                  AND (run_after IS NULL OR run_after <= ?)
                ORDER BY COALESCE(run_after, ''), id ASC
                LIMIT 1
                """,
                (TelegramCommandStatus.PENDING.value, now),
            )
            row = await cur.fetchone()
            if row is None:
                return None
            await conn.execute(
                """
                UPDATE telegram_commands
                SET status = ?, started_at = ?
                WHERE id = ? AND status = ?
                """,
                (
                    TelegramCommandStatus.RUNNING.value,
                    now,
                    row["id"],
                    TelegramCommandStatus.PENDING.value,
                ),
            )
            claimed_id = row["id"]
        return await self.get_command(claimed_id) if claimed_id is not None else None

    async def update_command(
        self,
        command_id: int,
        *,
        status: TelegramCommandStatus,
        error: str | None = None,
        result_payload: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        run_after: datetime | None = None,
    ) -> None:
        """Обновить статус команды и сопутствующие поля.

        Терминальные статусы (SUCCEEDED/FAILED/CANCELLED) проставляют `finished_at`.
        Возврат в PENDING (повторная постановка) сбрасывает started_at/finished_at,
        чтобы ретрай показывал свежий запуск, а не прерванную попытку. `result_payload`
        и `finished_at` пишутся через COALESCE — терминальный апдейт без свежего payload
        сохраняет прежнюю диагностику, а не затирает её в NULL (audit #835/15).
        """
        assert self._database is not None, (
            "TelegramCommandsRepository.update_command requires a Database reference"
        )
        finished_at = None
        if status in {
            TelegramCommandStatus.SUCCEEDED,
            TelegramCommandStatus.FAILED,
            TelegramCommandStatus.CANCELLED,
        }:
            finished_at = datetime.now(timezone.utc).isoformat()
        payload_json = safe_json_dumps(payload) if payload is not None else None
        run_after_iso = run_after.astimezone(timezone.utc).isoformat() if run_after is not None else None
        if status == TelegramCommandStatus.PENDING:
            # Reset started_at when re-queueing so a retried command shows
            # a fresh run timestamp rather than the interrupted attempt's.
            sets = [
                "status = ?",
                "error = ?",
                "result_payload = ?",
                "run_after = ?",
                "started_at = NULL",
                "finished_at = NULL",
            ]
            params: list[Any] = [
                status.value,
                error,
                safe_json_dumps(result_payload) if result_payload is not None else None,
                run_after_iso,
            ]
        else:
            sets = [
                "status = ?",
                "error = ?",
                # COALESCE so a terminal update without a fresh payload (e.g. a
                # FAILED retry) preserves earlier diagnostics instead of wiping
                # them to NULL (audit #835/15) — mirrors the payload/run_after guards.
                "result_payload = COALESCE(?, result_payload)",
                "run_after = NULL",
                "finished_at = COALESCE(?, finished_at)",
            ]
            params = [
                status.value,
                error,
                safe_json_dumps(result_payload) if result_payload is not None else None,
                finished_at,
            ]
        if payload_json is not None:
            sets.append("payload = ?")
            params.append(payload_json)
        params.append(command_id)
        await self._database.execute_write(
            f"UPDATE telegram_commands SET {', '.join(sets)} WHERE id = ?",
            tuple(params),
        )
