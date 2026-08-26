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
