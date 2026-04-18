"""Base class for pipeline node handlers."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class NodeContext:
    """Context passed between nodes during pipeline execution."""

    def __init__(self) -> None:
        self._data: dict[str, Any] = {}

    def set(self, node_id: str, key: str, value: Any) -> None:
        self._data[f"{node_id}.{key}"] = value

    def get(self, node_id: str, key: str, default: Any = None) -> Any:
        return self._data.get(f"{node_id}.{key}", default)

    def get_last(self, key: str, default: Any = None) -> Any:
        """Get the last-set value for a given key across all nodes."""
        for node_id_key, value in reversed(list(self._data.items())):
            if node_id_key.endswith(f".{key}"):
                return value
        return default

    def set_global(self, key: str, value: Any) -> None:
        self._data[f"__global__.{key}"] = value

    def get_global(self, key: str, default: Any = None) -> Any:
        return self._data.get(f"__global__.{key}", default)

    def record_error(
        self,
        *,
        node_id: str,
        code: str,
        detail: str,
        retry_after: int | None = None,
    ) -> None:
        """Record a structured error raised while executing a node.

        Errors are surfaced via ``get_errors()`` and propagated up through
        ``PipelineExecutor.execute`` into ``metadata["node_errors"]`` so that
        CLI/web can show why an action-only pipeline produced zero actions.
        """
        entry: dict[str, Any] = {
            "node_id": node_id,
            "code": code,
            "detail": detail,
        }
        if retry_after is not None:
            entry["retry_after"] = int(retry_after)
        key = "__node_errors__"
        current = self._data.get(key)
        if not isinstance(current, list):
            current = []
            self._data[key] = current
        current.append(entry)

    def get_errors(self) -> list[dict[str, Any]]:
        """Return a *copy* of the errors recorded so far."""
        current = self._data.get("__node_errors__")
        if not isinstance(current, list):
            return []
        return list(current)


class BaseNodeHandler(ABC):
    """Abstract base class for pipeline node handlers."""

    @abstractmethod
    async def execute(self, node_config: dict, context: NodeContext, services: dict) -> None:
        """Execute the node logic, writing results to context."""
