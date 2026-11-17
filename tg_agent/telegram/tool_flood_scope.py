"""Tool-call flood attribution (#1851).

The agent tool boundary marks the ``(tool, phone)`` pair it is dispatching;
the pool's flood reporters (:meth:`FloodRotationMixin.report_flood` /
``report_family_flood``) read the mark so a real FloodWait lands in the
tool-keyed breaker even when the tool handler converts the flood into a
text result. The scope object is shared by reference across task copies of
the context, so a spawned report task (``rpc_guard._report``) flags the
same object the boundary settles on — the same bridge the upstream
floodgate builds with its ``_FLOOD_SEEN`` contextvar.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass


@dataclass(slots=True)
class ToolFloodScope:
    """Mutable mark for the tool call currently dispatching Telegram RPCs."""

    tool_name: str
    phone: str
    flooded: bool = False


_tool_flood_scope: ContextVar[ToolFloodScope | None] = ContextVar(
    "tool_flood_scope", default=None
)


def current_tool_flood_scope() -> ToolFloodScope | None:
    """The tool call scope active in this context, or None outside the boundary."""
    return _tool_flood_scope.get()


@contextmanager
def tool_flood_scope(tool_name: str, phone: str) -> Iterator[ToolFloodScope]:
    """Mark the (tool, phone) pair for flood attribution; restore on exit."""
    scope = ToolFloodScope(tool_name=tool_name, phone=phone)
    token = _tool_flood_scope.set(scope)
    try:
        yield scope
    finally:
        _tool_flood_scope.reset(token)
