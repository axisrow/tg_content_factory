"""Crash-safe execution of dialog operations, one ledger item at a time."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from uuid import uuid4

from src.database.repositories.dialog_batch import DialogBatchRepository
from src.models import DialogBatchItem, DialogBatchOperation, DialogBatchStatus

DialogExecutor = Callable[[DialogBatchItem], Awaitable[None]]


class DialogBatchService:
    _locks: dict[tuple[int, int], asyncio.Lock] = {}

    def __init__(self, repository: DialogBatchRepository):
        self._repo = repository

    async def create(self, *, phone: str, op_type: str, dialogs: list[tuple[int, str]]) -> int:
        """Persist the complete input before any Telegram request is made."""
        if not dialogs:
            raise ValueError("dialogs must not be empty")
        return await self._repo.create_batch(
            DialogBatchOperation(phone=phone, op_type=op_type), dialogs
        )

    async def run(self, batch_id: int, executor: DialogExecutor) -> DialogBatchOperation:
        lock = self._locks.setdefault((id(self._repo), batch_id), asyncio.Lock())
        async with lock:
            batch = await self._repo.get_batch(batch_id)
            if batch is None:
                raise ValueError(f"Unknown dialog batch: {batch_id}")
            owner = uuid4().hex
            if not await self._repo.acquire_lease(batch_id, owner, datetime.now(timezone.utc)):
                return batch
            await self._repo.recover_running(batch_id)
            while (item := await self._repo.claim_next(batch_id)) is not None:
                try:
                    await executor(item)
                except Exception as exc:
                    await self._repo.update_item(item.id, DialogBatchStatus.FAILED, str(exc))  # type: ignore[arg-type]
                else:
                    await self._repo.update_item(item.id, DialogBatchStatus.COMPLETED)  # type: ignore[arg-type]
            items = await self._repo.list_items(batch_id)
            status = (DialogBatchStatus.FAILED if any(i.status == DialogBatchStatus.FAILED for i in items)
                      else DialogBatchStatus.COMPLETED)
            await self._repo.finish_batch(batch_id, status, owner)
            return (await self._repo.get_batch(batch_id)) or batch

    async def resume(self, batch_id: int, executor: DialogExecutor) -> DialogBatchOperation:
        return await self.run(batch_id, executor)
