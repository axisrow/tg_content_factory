"""Shared runtime context for agent tool backends."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal, TypeVar

_T = TypeVar("_T")
RuntimeKind = Literal["live", "snapshot", "none"]


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

    @classmethod
    def build(
        cls,
        *,
        db: object,
        config: object | None = None,
        client_pool: object | None = None,
        scheduler_manager: object | None = None,
        runtime_kind: RuntimeKind | None = None,
    ) -> "AgentRuntimeContext":
        return cls(
            db=db,
            config=config,
            client_pool=client_pool,
            scheduler_manager=scheduler_manager,
            runtime_kind=runtime_kind or detect_runtime_kind(client_pool),
        )

    @property
    def has_live_telegram(self) -> bool:
        return self.runtime_kind == "live" and self.client_pool is not None

    def run_sync(self, tool_name: str, operation: Callable[[], Awaitable[_T]]) -> _T:
        """Run an async operation from a sync tool thread."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(operation())
        raise RuntimeError(f"Agent tool '{tool_name}' cannot run inside an active event loop")

