"""Shared runtime context for agent tool backends."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from concurrent.futures import CancelledError as FutureCancelledError
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from typing import Literal, TypeVar

_T = TypeVar("_T")
RuntimeKind = Literal["live", "snapshot", "none"]
DEFAULT_SYNC_TIMEOUT_SEC = 120.0


class AgentToolRuntimeError(RuntimeError):
    """Public runtime failure for sync tool adapters."""

    def __init__(self, message: str, *, retryable: bool = True) -> None:
        super().__init__(message)
        self.retryable = retryable


def detect_runtime_kind(client_pool: object | None) -> RuntimeKind:
    """Classify the Telegram runtime attached to an agent backend."""
    if client_pool is None:
        return "none"
    if client_pool.__class__.__name__ == "SnapshotClientPool":
        return "snapshot"
    return "live"


@dataclass(slots=True)
class AgentRuntimeContext:
    """Runtime dependencies shared by async and thread-based agent tools."""

    db: object
    config: object | None = None
    client_pool: object | None = None
    scheduler_manager: object | None = None
    runtime_kind: RuntimeKind = "none"
    owner_loop: asyncio.AbstractEventLoop | None = None
    sync_timeout_sec: float | None = DEFAULT_SYNC_TIMEOUT_SEC

    @classmethod
    def build(
        cls,
        *,
        db: object,
        config: object | None = None,
        client_pool: object | None = None,
        scheduler_manager: object | None = None,
        runtime_kind: RuntimeKind | None = None,
        owner_loop: asyncio.AbstractEventLoop | None = None,
    ) -> "AgentRuntimeContext":
        if owner_loop is None:
            try:
                owner_loop = asyncio.get_running_loop()
            except RuntimeError:
                owner_loop = None
        return cls(
            db=db,
            config=config,
            client_pool=client_pool,
            scheduler_manager=scheduler_manager,
            runtime_kind=runtime_kind or detect_runtime_kind(client_pool),
            owner_loop=owner_loop,
        )

    @property
    def has_live_telegram(self) -> bool:
        return self.runtime_kind == "live" and self.client_pool is not None

    def bind_owner_loop(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """Bind the asyncio loop that owns live Telegram resources."""
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                return
        self.owner_loop = loop

    def run_sync(self, tool_name: str, operation: Callable[[], Awaitable[_T]]) -> _T:
        """Run an async operation from a sync tool thread."""
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        if self.has_live_telegram and self.owner_loop is not None and self.owner_loop.is_running():
            if current_loop is self.owner_loop:
                raise AgentToolRuntimeError(
                    f"Agent tool '{tool_name}' cannot synchronously block the live Telegram owner event loop.",
                    retryable=True,
                )
            future = asyncio.run_coroutine_threadsafe(operation(), self.owner_loop)
            cancel_event = None
            permission_wait_tracker = None
            try:
                from src.agent.permission_gate import get_request_context

                request_context = get_request_context()
                if request_context is not None:
                    cancel_event = request_context.cancel_event
                    permission_wait_tracker = request_context.permission_wait_tracker
            except (AttributeError, LookupError):
                cancel_event = None
                permission_wait_tracker = None

            deadline = (
                time.monotonic() + self.sync_timeout_sec
                if self.sync_timeout_sec is not None
                else None
            )
            while True:
                if cancel_event is not None and cancel_event.is_set():
                    future.cancel()
                    raise AgentToolRuntimeError(
                        f"Agent tool '{tool_name}' was cancelled while waiting for the live Telegram runtime.",
                        retryable=True,
                    )

                permission_waiting = (
                    permission_wait_tracker is not None
                    and permission_wait_tracker.is_waiting()
                )
                poll_timeout = 0.5
                started_at = time.monotonic()
                if deadline is not None and not permission_waiting:
                    remaining = deadline - started_at
                    if remaining <= 0:
                        future.cancel()
                        raise AgentToolRuntimeError(
                            f"Agent tool '{tool_name}' timed out while waiting for the live Telegram runtime.",
                            retryable=True,
                        )
                    poll_timeout = min(poll_timeout, remaining)

                try:
                    return future.result(timeout=poll_timeout)
                except FutureCancelledError as exc:
                    # The live operation was cancelled (e.g. a parallel flood-wait
                    # batch tore down sibling futures). Surface it as a retryable
                    # tool error instead of a raw CancelledError so the model can
                    # retry/report it as a transient condition rather than a crash (#597).
                    raise AgentToolRuntimeError(
                        f"Agent tool '{tool_name}' was cancelled while running on the live "
                        "Telegram runtime; retry shortly.",
                        retryable=True,
                    ) from exc
                except FutureTimeoutError as exc:
                    permission_waiting = (
                        permission_wait_tracker is not None
                        and permission_wait_tracker.is_waiting()
                    )
                    if deadline is not None and permission_waiting:
                        deadline += time.monotonic() - started_at
                    elif deadline is not None and deadline <= time.monotonic():
                        future.cancel()
                        raise AgentToolRuntimeError(
                            f"Agent tool '{tool_name}' timed out while waiting for the live Telegram runtime.",
                            retryable=True,
                        ) from exc

        if current_loop is None:
            return asyncio.run(operation())

        raise AgentToolRuntimeError(
            f"Agent tool '{tool_name}' cannot run inside an active event loop without a sync bridge.",
            retryable=True,
        )
