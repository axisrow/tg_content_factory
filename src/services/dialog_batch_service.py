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

    async def _release_lease(self, batch_id: int, owner: str) -> None:
        """Persist cancellation cleanup even through transient SQLite contention."""
        async def release_with_retries() -> None:
            while True:
                try:
                    await self._repo.release_lease(batch_id, owner)
                    return
                except Exception:
                    await asyncio.sleep(1)

        task = asyncio.create_task(release_with_retries())
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                # Keep waiting: the retry loop is intentionally detached from
                # repeated cancellation of the worker task.
                continue
        await task

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
            try:
                acquired = await self._repo.acquire_lease(batch_id, owner, datetime.now(timezone.utc))
            except asyncio.CancelledError:
                await self._release_lease(batch_id, owner)
                raise
            if not acquired:
                return batch
            try:
                await self._repo.recover_running(batch_id)
            except asyncio.CancelledError:
                await self._release_lease(batch_id, owner)
                raise
            stop_heartbeat = asyncio.Event()
            lease_lost = asyncio.Event()

            async def heartbeat() -> None:
                while not stop_heartbeat.is_set():
                    await asyncio.sleep(30)
                    for _ in range(3):
                        try:
                            renewed = await self._repo.renew_lease(batch_id, owner, datetime.now(timezone.utc))
                        except Exception:
                            await asyncio.sleep(1)
                            continue
                        if renewed:
                            break
                        lease_lost.set()
                        return
                    else:
                        lease_lost.set()
                        return

            heartbeat_task = asyncio.create_task(heartbeat())
            try:
                while not lease_lost.is_set() and (item := await self._repo.claim_next(batch_id, owner)) is not None:
                    execution = asyncio.create_task(executor(item))
                    lost_waiter = asyncio.create_task(lease_lost.wait())
                    try:
                        done, _ = await asyncio.wait(
                            (execution, lost_waiter), return_when=asyncio.FIRST_COMPLETED
                        )
                        if lost_waiter in done and execution not in done:
                            execution.cancel()
                            await asyncio.gather(execution, return_exceptions=True)
                            break
                        await execution
                    except asyncio.CancelledError:
                        execution.cancel()
                        await asyncio.gather(execution, return_exceptions=True)
                        raise
                    except Exception as exc:
                        await self._repo.update_item(item.id, DialogBatchStatus.FAILED, str(exc), owner)  # type: ignore[arg-type]
                    else:
                        await self._repo.update_item(item.id, DialogBatchStatus.COMPLETED, owner=owner)  # type: ignore[arg-type]
                    finally:
                        lost_waiter.cancel()
                        await asyncio.gather(lost_waiter, return_exceptions=True)
                if lease_lost.is_set():
                    # Do not finalize: pending work must remain resumable by a
                    # fresh owner after the lease is recovered.
                    return (await self._repo.get_batch(batch_id)) or batch
                items = await self._repo.list_items(batch_id)
                status = (DialogBatchStatus.FAILED if any(i.status == DialogBatchStatus.FAILED for i in items)
                          else DialogBatchStatus.COMPLETED)
                await self._repo.finish_batch(batch_id, status, owner)
                return (await self._repo.get_batch(batch_id)) or batch
            except asyncio.CancelledError:
                await self._release_lease(batch_id, owner)
                raise
            finally:
                stop_heartbeat.set()
                heartbeat_task.cancel()
                await asyncio.gather(heartbeat_task, return_exceptions=True)

    async def resume(self, batch_id: int, executor: DialogExecutor) -> DialogBatchOperation:
        return await self.run(batch_id, executor)
