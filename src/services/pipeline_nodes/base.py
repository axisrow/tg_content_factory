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


class BaseNodeHandler(ABC):
    """Abstract base class for pipeline node handlers."""

    @abstractmethod
    async def execute(self, node_config: dict, context: NodeContext, services: dict) -> None:
        """Execute the node logic, writing results to context."""
