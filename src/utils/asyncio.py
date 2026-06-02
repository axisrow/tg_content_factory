from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable


def make_log_task_exception_callback(
    logger: logging.Logger,
    *,
    level: str,
    message: str,
    include_exception_in_message: bool = False,
    exc_info: bool = True,
) -> Callable[[asyncio.Task], None]:
    """Build a done-callback that logs unhandled fire-and-forget task failures."""

    def _log_task_exception(task: asyncio.Task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return

        args = (task.get_name(), exc) if include_exception_in_message else (task.get_name(),)
        getattr(logger, level)(message, *args, exc_info=exc if exc_info else None)

    return _log_task_exception
