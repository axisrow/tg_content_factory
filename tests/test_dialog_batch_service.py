from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import DialogBatchOperation, DialogBatchStatus
from src.services.dialog_batch_service import DialogBatchService


@pytest.mark.anyio
async def test_batch_resume_and_concurrent_run_are_idempotent(db):
    service = DialogBatchService(db.repos.dialog_batch)
    batch_id = await service.create(phone="+1", op_type="delete", dialogs=[(1, "channel")])
    calls = 0

    async def execute(_item):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0)

    await asyncio.gather(service.run(batch_id, execute), service.resume(batch_id, execute))
    await service.resume(batch_id, execute)
    assert calls == 1


@pytest.mark.anyio
async def test_run_cancellation_cancels_executor(db):
    service = DialogBatchService(db.repos.dialog_batch)
    batch_id = await service.create(phone="+1", op_type="delete", dialogs=[(1, "channel")])
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async def execute(_item):
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    task = asyncio.create_task(service.run(batch_id, execute))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cancelled.is_set()

    await service.run(batch_id, lambda _item: asyncio.sleep(0))
    assert (await db.repos.dialog_batch.get_batch(batch_id)).status.value == "completed"


@pytest.mark.anyio
async def test_lease_release_retries_through_double_cancellation():
    repository = MagicMock()
    repository.release_lease = AsyncMock(side_effect=[RuntimeError("busy"), RuntimeError("busy"), None])
    service = DialogBatchService(repository)

    task = asyncio.create_task(service._release_lease(1, "owner"))
    await asyncio.sleep(0)
    task.cancel()
    task.cancel()
    await task

    assert repository.release_lease.await_count == 3


@pytest.mark.anyio
async def test_heartbeat_stop_survives_double_cancellation():
    async def heartbeat():
        await asyncio.Event().wait()

    heartbeat_task = asyncio.create_task(heartbeat())
    await asyncio.sleep(0)
    stop_task = asyncio.create_task(DialogBatchService._stop_heartbeat(heartbeat_task))
    await asyncio.sleep(0)
    stop_task.cancel()
    stop_task.cancel()
    await stop_task
    assert heartbeat_task.cancelled()


@pytest.mark.anyio
async def test_repository_claims_and_fences_items(db):
    repository = db.repos.dialog_batch
    batch_id = await repository.create_batch(
        DialogBatchOperation(phone="+1", op_type="delete"), [(7, "channel")]
    )
    owner = "test-owner"
    assert await repository.acquire_lease(batch_id, owner, datetime.now(timezone.utc))
    item = await repository.claim_next(batch_id, owner)
    assert item is not None and item.dialog_id == 7 and item.attempts == 1
    await repository.update_item(item.id, DialogBatchStatus.COMPLETED, owner=owner)
    await repository.finish_batch(batch_id, DialogBatchStatus.COMPLETED, owner)
    stored = await repository.get_batch(batch_id)
    assert stored is not None and stored.status == DialogBatchStatus.COMPLETED
