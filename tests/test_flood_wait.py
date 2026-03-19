from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from telethon.errors import FloodWaitError

from src.telegram.flood_wait import HandledFloodWaitError, handle_flood_wait, run_with_flood_wait


@pytest.mark.asyncio
async def test_run_with_flood_wait_returns_success_value():
    result = await run_with_flood_wait(
        AsyncMock(return_value="ok")(),
        operation="test_operation",
    )

    assert result == "ok"


@pytest.mark.asyncio
async def test_handle_flood_wait_reports_pool_and_builds_info():
    err = FloodWaitError(request=None, capture=0)
    err.seconds = 33
    pool = AsyncMock()

    info = await handle_flood_wait(
        err,
        operation="test_operation",
        phone="+7000",
        pool=pool,
    )

    assert info.operation == "test_operation"
    assert info.phone == "+7000"
    assert info.wait_seconds == 33
    assert "Flood wait 33s" in info.detail
    pool.report_flood.assert_awaited_once_with("+7000", 33)


@pytest.mark.asyncio
async def test_run_with_flood_wait_raises_handled_error_with_info():
    err = FloodWaitError(request=None, capture=0)
    err.seconds = 17

    async def _raise():
        raise err

    pool = AsyncMock()

    with pytest.raises(HandledFloodWaitError) as exc_info:
        await run_with_flood_wait(
            _raise(),
            operation="test_operation",
            phone="+7999",
            pool=pool,
        )

    assert exc_info.value.info.operation == "test_operation"
    assert exc_info.value.info.phone == "+7999"
    assert exc_info.value.info.wait_seconds == 17
    assert "Flood wait 17s" in exc_info.value.info.detail
    pool.report_flood.assert_awaited_once_with("+7999", 17)
