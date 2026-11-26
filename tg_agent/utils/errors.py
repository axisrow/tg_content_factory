"""Reason text for a stored failure."""
from __future__ import annotations


def error_text(exc: BaseException, *, limit: int = 500) -> str:
    """The reason a failure is stored with — never empty.

    An exception raised without a message (``asyncio.TimeoutError()``, a bare
    ``RuntimeError()``) has an empty ``str()``. The stored reason is then its
    class name, so a FAILED row always says why as far as the code knows
    (#1968) — an empty reason reads as "no error" wherever ``""`` is the
    success value.
    """
    return str(exc).strip()[:limit] or type(exc).__name__
