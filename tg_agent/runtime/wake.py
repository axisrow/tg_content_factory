"""Wake the worker's queue loops the moment work is enqueued (#1868).

The worker drains three DB-backed queues — ``telegram_commands`` and the
``collection_tasks`` table (read by both the collection queue and the unified
dispatcher). Their loops used to sleep a fixed poll interval (1–5 s) between
empty claims, so every enqueue waited for the next tick. Now an insert notifies
the queue: consumers in the same process wake at once, and a CLI process sends
the notification to the daemon over its relay. The poll stays as a safety net.

Consumers subscribe one :class:`asyncio.Event` each, clear it before a claim and
wait on it (with the poll interval as timeout) when the claim came back empty.
"""

from __future__ import annotations

import asyncio
import functools
import logging
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

TELEGRAM_COMMANDS = "telegram_commands"
TASKS = "tasks"
QUEUES = frozenset({TELEGRAM_COMMANDS, TASKS})

_subscribers: dict[str, list[asyncio.Event]] = {queue: [] for queue in QUEUES}
_remote: Callable[[str], Awaitable[None]] | None = None


def subscribe(queue: str) -> asyncio.Event:
    """A fresh event set whenever ``queue`` receives work."""
    event = asyncio.Event()
    _subscribers[queue].append(event)
    return event


def unsubscribe(queue: str, event: asyncio.Event) -> None:
    try:
        _subscribers[queue].remove(event)
    except (KeyError, ValueError):
        pass


def set_remote(hook: Callable[[str], Awaitable[None]] | None) -> None:
    """Forward notifications to another process (the CLI → the daemon's relay)."""
    global _remote
    _remote = hook


def notify_local(queue: str) -> None:
    for event in tuple(_subscribers.get(queue, ())):
        event.set()


async def notify(queue: str) -> None:
    """Wake every consumer of ``queue``: in this process and, if wired, the daemon."""
    notify_local(queue)
    hook = _remote
    if hook is not None:
        try:
            await hook(queue)
        except Exception:  # best effort: the consumer's poll still picks the work up
            logger.debug("remote wake for %s failed", queue, exc_info=True)


_F = TypeVar("_F", bound=Callable[..., Awaitable[Any]])


def notifies(queue: str) -> Callable[[_F], _F]:
    """Decorate an async enqueue: once it returned a row id, wake ``queue``."""

    def decorate(fn: _F) -> _F:
        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            result = await fn(*args, **kwargs)
            if result:
                await notify(queue)
            return result

        return wrapper  # type: ignore[return-value]

    return decorate


async def wait(event: asyncio.Event, timeout: float) -> None:
    """Return when ``event`` is set or ``timeout`` elapses.

    Uses the ``asyncio.timeout`` context form: on 3.11 the ``wait_for`` wrapper
    around an event can leak a waiter when the event fires as the timeout lands
    (#1857).
    """
    try:
        async with asyncio.timeout(timeout):
            await event.wait()
    except TimeoutError:
        pass
