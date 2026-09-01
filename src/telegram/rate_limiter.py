"""Compat shim: the canonical home of this module is the ``telethon_floodgate``
PyPI package. The bodies were extracted 1:1 and live there now; this file
re-exports the same objects so existing imports keep working unchanged.
"""
from telethon_floodgate.rate_limiter import (
    DEFAULT_JITTER_SEC,
    DEFAULT_MAX_CALLS,
    DEFAULT_WINDOW_SEC,
    GLOBAL_RESOLVE_BACKOFF_THRESHOLD_SEC,
    RESOLVE_USERNAME_BACKOFF_BUFFER_SEC,
    ResolveRateLimiter,
    UsernameResolveFloodWaitDeferredError,
    UsernameResolveRateLimitedError,
)

__all__ = [
    "DEFAULT_JITTER_SEC",
    "DEFAULT_MAX_CALLS",
    "DEFAULT_WINDOW_SEC",
    "GLOBAL_RESOLVE_BACKOFF_THRESHOLD_SEC",
    "RESOLVE_USERNAME_BACKOFF_BUFFER_SEC",
    "ResolveRateLimiter",
    "UsernameResolveFloodWaitDeferredError",
    "UsernameResolveRateLimitedError",
]
