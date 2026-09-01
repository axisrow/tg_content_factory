"""Compat shim: the canonical home of this module is the ``telethon_floodgate``
PyPI package. The bodies were extracted 1:1 and live there now; this file
re-exports the same objects so existing imports, ``isinstance`` checks and
test doubles keep working unchanged.

Deliberately does NOT import ``asyncio``: a leftover test patching
``src.telegram.flood_wait.asyncio.sleep`` must fail loudly with
AttributeError at setup instead of silently sleeping for real. Patch
``telethon_floodgate.flood_wait.asyncio.sleep`` instead.
"""
from telethon_floodgate.flood_wait import (
    FLOOD_WAIT_RETRY_BUFFER_SEC,
    TRANSIENT_FLOOD_WAIT_MAX_SEC,
    TRANSIENT_FLOOD_WAIT_RETRY_BUDGET_SEC,
    FloodWaitInfo,
    HandledFloodWaitError,
    coerce_flood_wait_seconds,
    flood_wait_remaining_seconds,
    format_flood_wait_detail,
    handle_flood_wait,
    is_blocking_flood_wait_until,
    is_transient_flood_wait_seconds,
    is_transient_flood_wait_until,
    run_with_flood_wait,
    run_with_flood_wait_retry,
    sleep_for_flood_wait_seconds,
    sleep_for_handled_flood_wait,
)

__all__ = [
    "FLOOD_WAIT_RETRY_BUFFER_SEC",
    "TRANSIENT_FLOOD_WAIT_MAX_SEC",
    "TRANSIENT_FLOOD_WAIT_RETRY_BUDGET_SEC",
    "FloodWaitInfo",
    "HandledFloodWaitError",
    "coerce_flood_wait_seconds",
    "flood_wait_remaining_seconds",
    "format_flood_wait_detail",
    "handle_flood_wait",
    "is_blocking_flood_wait_until",
    "is_transient_flood_wait_seconds",
    "is_transient_flood_wait_until",
    "run_with_flood_wait",
    "run_with_flood_wait_retry",
    "sleep_for_flood_wait_seconds",
    "sleep_for_handled_flood_wait",
]
