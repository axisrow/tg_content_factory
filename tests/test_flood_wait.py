from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from telethon.errors import FloodWaitError

from src.telegram.flood_wait import (
    HandledFloodWaitError,
    format_flood_wait_detail,
    handle_flood_wait,
    run_with_flood_wait,
)


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


# === format_flood_wait_detail tests ===


def test_format_flood_wait_detail_without_phone():
    """Detail string without phone."""
    from datetime import datetime, timezone

    next_time = datetime(2024, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
    detail = format_flood_wait_detail(
        wait_seconds=30,
        next_available_at_utc=next_time,
    )

    assert "Flood wait 30s" in detail
    assert "2024-03-15T12:00:00" in detail
    assert "UTC" in detail
    assert "for " not in detail  # No phone


def test_format_flood_wait_detail_with_phone():
    """Detail string includes phone."""
    from datetime import datetime, timezone

    next_time = datetime(2024, 3, 15, 14, 30, 0, tzinfo=timezone.utc)
    detail = format_flood_wait_detail(
        wait_seconds=45,
        next_available_at_utc=next_time,
        phone="+1234567890",
    )

    assert "Flood wait 45s" in detail
    assert "for +1234567890" in detail


# === handle_flood_wait edge cases ===


@pytest.mark.asyncio
async def test_handle_flood_wait_zero_seconds_clamped_to_one():
    """Zero seconds is clamped to 1."""
    err = FloodWaitError(request=None, capture=0)
    err.seconds = 0

    info = await handle_flood_wait(
        err,
        operation="test_op",
    )

    assert info.wait_seconds == 1  # Clamped


@pytest.mark.asyncio
async def test_handle_flood_wait_none_seconds_clamped_to_one():
    """None seconds is clamped to 1."""
    err = FloodWaitError(request=None, capture=0)
    err.seconds = None

    info = await handle_flood_wait(
        err,
        operation="test_op",
    )

    assert info.wait_seconds == 1  # Clamped


@pytest.mark.asyncio
async def test_handle_flood_wait_without_pool():
    """No pool means no report_flood call."""
    err = FloodWaitError(request=None, capture=0)
    err.seconds = 10

    info = await handle_flood_wait(
        err,
        operation="test_op",
        phone="+7000",
        pool=None,
    )

    assert info.wait_seconds == 10


@pytest.mark.asyncio
async def test_run_with_flood_wait_with_timeout():
    """Timeout parameter is forwarded to wait_for."""
    import asyncio

    err = FloodWaitError(request=None, capture=0)
    err.seconds = 5

    async def _raise_after_delay():
        await asyncio.sleep(0.1)
        raise err

    with pytest.raises(HandledFloodWaitError):
        await run_with_flood_wait(
            _raise_after_delay(),
            operation="test_op",
            timeout=0.5,
        )
