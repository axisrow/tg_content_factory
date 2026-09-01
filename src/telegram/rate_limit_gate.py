"""Compat shim: the canonical home of this module is the ``telethon_floodgate``
PyPI package. The bodies were extracted 1:1 and live there now; this file
re-exports the same objects so existing imports, ``isinstance`` checks and
test doubles keep working unchanged.
"""
from telethon_floodgate.rate_limit_gate import (
    DIALOG_PAGE_MAX_CALLS,
    DIALOG_SWEEP_MAX_CALLS,
    RateLimitSpec,
    TelegramPeerRateLimitedError,
    TelegramRateLimitedError,
    TelegramRateLimitGate,
)

__all__ = [
    "DIALOG_PAGE_MAX_CALLS",
    "DIALOG_SWEEP_MAX_CALLS",
    "RateLimitSpec",
    "TelegramPeerRateLimitedError",
    "TelegramRateLimitedError",
    "TelegramRateLimitGate",
]
