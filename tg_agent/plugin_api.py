"""Public API for trusted, pip-installed TG Agent plugins (API major 1)."""
from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from importlib.metadata import entry_points
from typing import Any

from claude_agent_sdk import SdkMcpTool
from claude_agent_sdk import tool as sdk_tool

from tg_agent.agent.tools._categories import ToolCategory, ToolMeta
from tg_agent.agent.tools._registry import AgentToolContext, _text_response
from tg_agent.agent.tools._registry import require_confirmation as require_confirmation

text_response = _text_response

PLUGIN_API_VERSION = 1
# Seconds granted to plugin setup and shutdown alike; a hung lifecycle callback
# is cancelled and the plugin skipped/failed without blocking startup or exit.
_LIFECYCLE_TIMEOUT = 10.0
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PluginContext:
    db: Any
    client_pool: Any
    config: Any
    logger: logging.Logger
    extras: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Plugin:
    id: str
    version: str
    requires_api: int
    register: Callable[[PluginContext], list[SdkMcpTool]]
    setup: Callable[[PluginContext], Awaitable[None]] | None = None
    shutdown: Callable[[], Awaitable[None]] | None = None


def tool(name, description, input_schema, *, category, phone_bound=False, **kwargs):
    """SDK tool decorator with mandatory permission metadata."""
    meta = ToolMeta(ToolCategory(category), phone_bound=phone_bound)

    def decorate(handler):
        result = sdk_tool(name, description, input_schema, **kwargs)(handler)
        result.plugin_meta = meta
        return result

    return decorate


# Metadata is process-wide, like the built-in permission catalogue. Runtime
# contributions are reference counted so stopping web cannot erase worker tools.
_catalogues: dict[PluginRuntime, dict[str, dict[str, ToolMeta]]] = {}


def permission_groups():
    groups: dict[str, dict[str, ToolMeta]] = {}
    for catalogue in _catalogues.values():
        groups.update(catalogue)
    return groups


def _invalidate_permissions():
    from tg_agent.agent.tools import permissions

    permissions._metadata_cache = None
    permissions._permissions_cache = None
    permissions._access_policy_cache = None
    # TOOL_CATEGORIES is dynamic once plugins exist, so the allow-list cache in
    # get_all_allowed_tools() must be rebuilt too.
    permissions._all_allowed_tools_cache = None


class PluginRuntime:
    """One activation per database/container, independent of registry rebuilds."""

    def __init__(self, db, config, client_pool=None, scheduler_manager=None):
        self.db = db
        self.config = config
        self.client_pool = client_pool
        self.scheduler_manager = scheduler_manager
        self.tools: list[SdkMcpTool] = []
        self.active: list[Plugin] = []
        self._lock = asyncio.Lock()
        self._started = False
        self._closed = False

    async def start(self):
        async with self._lock:
            if self._started or self._closed:
                return
            self._started = True
            from tg_agent.agent.tools.permissions import TOOL_CATEGORIES

            reserved = set(TOOL_CATEGORIES)
            catalogue: dict[str, dict[str, ToolMeta]] = {}
            seen = set()
            disabled = set(getattr(self.config, "plugins_disabled", ()))
            try:
                entries = sorted(entry_points(group="tg_agent.plugins"), key=lambda ep: (ep.name, ep.value))
            except Exception:
                logger.exception("Plugin discovery failed")
                return
            try:
                for ep in entries:
                    if ep.name in disabled:
                        continue
                    plugin = None
                    setup_started = False
                    try:
                        plugin = ep.load()
                        if not isinstance(plugin, Plugin) or not re.fullmatch(r"[a-z][a-z0-9_]*", plugin.id):
                            raise ValueError("Invalid plugin contract or id")
                        # Entry point names are stable ids, so disabled plugins need not be imported.
                        if ep.name != plugin.id or plugin.id in seen:
                            raise ValueError("Entry point must match unique plugin id")
                        seen.add(plugin.id)
                        if plugin.requires_api != PLUGIN_API_VERSION:
                            logger.warning("Skipping incompatible plugin %s (API %s)", plugin.id, plugin.requires_api)
                            continue
                        from tg_agent.services.embedding_service import EmbeddingService

                        embedding_service = EmbeddingService(self.db, config=self.config)
                        context = AgentToolContext.build(
                            db=self.db, config=self.config, client_pool=self.client_pool,
                            scheduler_manager=self.scheduler_manager, embedding_service=embedding_service,
                        )
                        ctx = PluginContext(
                            self.db, self.client_pool, self.config,
                            logging.getLogger(f"tg_agent.plugins.{plugin.id}"),
                            {"tool_context": context, "runtime_context": context.runtime_context,
                             "scheduler_manager": self.scheduler_manager, "embedding_service": embedding_service},
                        )
                        setup_started = True
                        if plugin.setup:
                            await asyncio.wait_for(plugin.setup(ctx), timeout=_LIFECYCLE_TIMEOUT)
                        tools = list(plugin.register(ctx))
                        metadata = {}
                        # Another container may have finished activation while setup awaited.
                        from tg_agent.agent.tools.permissions import TOOL_CATEGORIES

                        reserved.update(TOOL_CATEGORIES)
                        for item in tools:
                            meta = getattr(item, "plugin_meta", None)
                            if not isinstance(item, SdkMcpTool) or not isinstance(meta, ToolMeta):
                                raise ValueError("Plugin tools must use plugin_api.tool")
                            # The same plugin may be active in both web and worker.
                            own_existing = permission_groups().get(f"Plugin: {plugin.id}", {})
                            if item.name in metadata or (item.name in reserved and item.name not in own_existing):
                                raise ValueError(f"Duplicate tool name: {item.name}")
                            if item.name in own_existing and own_existing[item.name] != meta:
                                raise ValueError(f"Inconsistent tool metadata: {item.name}")
                            metadata[item.name] = meta
                        own_existing = permission_groups().get(f"Plugin: {plugin.id}")
                        if own_existing is not None and own_existing != metadata:
                            raise ValueError("Plugin tools must have consistent metadata across containers")
                        self.tools.extend(tools)
                        self.active.append(plugin)
                        reserved.update(metadata)
                        catalogue[f"Plugin: {plugin.id}"] = metadata
                        _catalogues[self] = catalogue
                        _invalidate_permissions()
                    except BaseException as exc:
                        if setup_started and plugin is not None:
                            await self._shutdown(plugin)
                        if not isinstance(exc, Exception):
                            raise
                        logger.exception("Skipping failed plugin %s", ep.name)
            except BaseException:
                await self._close()
                raise

    async def _shutdown(self, plugin):
        if plugin.shutdown:
            try:
                await asyncio.wait_for(plugin.shutdown(), timeout=_LIFECYCLE_TIMEOUT)
            except Exception:
                logger.exception("Plugin shutdown failed: %s", plugin.id)

    async def close(self):
        async with self._lock:
            await self._close()

    async def _close(self):
        if self._closed:
            return
        self._closed = True
        try:
            for plugin in reversed(self.active):
                await self._shutdown(plugin)
        finally:
            self.active.clear()
            self.tools.clear()
            _catalogues.pop(self, None)
            _invalidate_permissions()


async def start_plugins(db, config, client_pool=None, scheduler_manager=None):
    """Initialize once; the database owns final cleanup on every exit path."""
    runtime = getattr(db, "plugins", None)
    if not isinstance(runtime, PluginRuntime):
        runtime = PluginRuntime(db, config, client_pool, scheduler_manager)
        db.plugins = runtime
    await runtime.start()
    return runtime
