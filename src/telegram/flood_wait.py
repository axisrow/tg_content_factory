from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, TypeVar

from pydantic import BaseModel
from telethon.errors import FloodWaitError

logger = logging.getLogger(__name__)

T = TypeVar("T")


class FloodWaitInfo(BaseModel):
    operation: str
    phone: str | None = None
    wait_seconds: int
    next_available_at_utc: datetime
    detail: str


class HandledFloodWaitError(RuntimeError):
    def __init__(self, info: FloodWaitInfo):
        super().__init__(info.detail)
        self.info = info


def format_flood_wait_detail(
    *,
    wait_seconds: int,
    next_available_at_utc: datetime,
    phone: str | None = None,
) -> str:
    detail = (
        f"Flood wait {wait_seconds}s until {next_available_at_utc.isoformat()} UTC"
    )
    if phone:
        detail += f" for {phone}"
    return detail


async def handle_flood_wait(
    exc: FloodWaitError,
    *,
    operation: str,
    phone: str | None = None,
    pool: Any | None = None,
    logger_: logging.Logger | None = None,
) -> FloodWaitInfo:
    wait_seconds = max(1, int(getattr(exc, "seconds", 0) or 0))
    next_available_at_utc = datetime.now(timezone.utc) + timedelta(seconds=wait_seconds)
    detail = format_flood_wait_detail(
        wait_seconds=wait_seconds,
        next_available_at_utc=next_available_at_utc,
        phone=phone,
    )

    if pool is not None and phone:
        reporter = getattr(pool, "report_flood", None)
        if callable(reporter):
            await reporter(phone, wait_seconds)

    active_logger = logger_ or logger
    active_logger.warning("%s: %s", operation, detail)

    return FloodWaitInfo(
        operation=operation,
        phone=phone,
        wait_seconds=wait_seconds,
        next_available_at_utc=next_available_at_utc,
        detail=detail,
    )


async def run_with_flood_wait(
    awaitable: Awaitable[T],
    *,
    operation: str,
    phone: str | None = None,
    pool: Any | None = None,
    logger_: logging.Logger | None = None,
    timeout: float | None = None,
) -> T:
    try:
        if timeout is None:
            return await awaitable
        return await asyncio.wait_for(awaitable, timeout=timeout)
    except FloodWaitError as exc:
        info = await handle_flood_wait(
            exc,
            operation=operation,
            phone=phone,
            pool=pool,
            logger_=logger_,
        )
        raise HandledFloodWaitError(info) from exc
