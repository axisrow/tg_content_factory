"""Compat shim: the canonical home of this module is the ``telethon_floodgate``
PyPI package. The bodies were extracted 1:1 and live there now; this file
re-exports the same objects so existing imports, ``isinstance`` checks and
test doubles keep working unchanged.
"""
from telethon_floodgate.flood_breaker import (
    DEFAULT_COOLDOWN_SECONDS,
    DEFAULT_FLOOD_THRESHOLD,
    FloodCircuitBreaker,
    TelegramOperationSuspendedError,
)

__all__ = [
    "DEFAULT_COOLDOWN_SECONDS",
    "DEFAULT_FLOOD_THRESHOLD",
    "FloodCircuitBreaker",
    "TelegramOperationSuspendedError",
]
