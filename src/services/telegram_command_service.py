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
    ) -> int:
        return await self._db.repos.telegram_commands.create_command(
            TelegramCommand(
                command_type=command_type,
                payload=payload,
                requested_by=requested_by,
            )
        )

    async def get(self, command_id: int):
        return await self._db.repos.telegram_commands.get_command(command_id)
