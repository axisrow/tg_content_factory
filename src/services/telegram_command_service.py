from __future__ import annotations

from src.database import Database
from src.models import TelegramCommand


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
            )
        )

    async def get(self, command_id: int):
        return await self._db.repos.telegram_commands.get_command(command_id)
