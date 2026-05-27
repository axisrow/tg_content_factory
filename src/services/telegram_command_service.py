from __future__ import annotations

from datetime import datetime

from src.database import Database
from src.models import TelegramCommand, TelegramCommandStatus


class TelegramCommandService:
    def __init__(self, db: Database):
        self._db = db

    async def enqueue(
        self,
        command_type: str,
        *,
        payload: dict,
        requested_by: str | None = None,
        deduplicate: bool = True,
        run_after: datetime | None = None,
    ) -> int:
        """Enqueue a telegram command.

        When ``deduplicate`` is True (default), returns the id of an existing
        PENDING/RUNNING command with the same ``command_type`` and identical
        ``payload`` instead of creating a duplicate. This prevents accidental
        UI-driven fan-out (e.g. clicking the same action multiple times) from
        producing a backlog of identical Telegram API calls.
        """
        if deduplicate:
            existing = await self._db.repos.telegram_commands.find_active_by_type(
                command_type, payload=payload
            )
            if existing is not None and existing.id is not None:
                return existing.id
        return await self._db.repos.telegram_commands.create_command(
            TelegramCommand(
                command_type=command_type,
                payload=payload,
                requested_by=requested_by,
                run_after=run_after,
            )
        )

    async def get(self, command_id: int):
        return await self._db.repos.telegram_commands.get_command(command_id)

    async def list(
        self,
        *,
        command_type: str | None = None,
        phone: str | None = None,
        status: TelegramCommandStatus | None = None,
        limit: int = 100,
    ) -> list[TelegramCommand]:
        return await self._db.repos.telegram_commands.list_commands(
            command_type=command_type,
            phone=phone,
            status=status,
            limit=limit,
        )

    async def summary(
        self,
        *,
        command_type: str | None = None,
        phone: str | None = None,
        status: TelegramCommandStatus | None = None,
    ) -> dict[TelegramCommandStatus, int]:
        return await self._db.repos.telegram_commands.count_by_status(
            command_type=command_type,
            phone=phone,
            status=status,
        )

    async def result_state_summary(
        self,
        *,
        command_type: str | None = None,
        phone: str | None = None,
        status: TelegramCommandStatus | None = None,
    ) -> dict[str, int]:
        return await self._db.repos.telegram_commands.count_result_states(
            command_type=command_type,
            phone=phone,
            status=status,
        )
