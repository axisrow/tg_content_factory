from __future__ import annotations

import asyncio

import pytest

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
