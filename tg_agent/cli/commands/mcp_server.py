"""Standalone stdio MCP server exposing the agent tool registry.

External agents that speak MCP over stdio — notably the Codex CLI, which the
``CodexSdkBackend`` spawns as a subprocess — connect here to use the project's
tools (channels/search/pipelines/messaging/…). The in-process path
(``ClaudeSdkBackend``) consumes the same registry directly via
``make_mcp_server``; this command serves it out-of-process over JSON-RPC.

The tool registry and its JSON-Schema conversion are reused verbatim from
``create_sdk_mcp_server`` (it already builds a real ``mcp.server.Server`` and
puts it in ``cfg["instance"]``), so there is no second copy of the tool wiring.

stdout is the JSON-RPC channel — logging must never write there. We send logs
to stderr so the protocol stream stays clean.
"""

from __future__ import annotations

import argparse
import logging
import sys
from contextlib import AsyncExitStack

from tg_agent.cli.runtime import init_db, init_pool
from tg_agent.cli.typer_app import run_async

logger = logging.getLogger(__name__)


def _route_logging_to_stderr() -> None:
    """Ensure no log handler writes to stdout (the MCP JSON-RPC channel)."""
    root = logging.getLogger()
    for handler in root.handlers[:]:
        stream = getattr(handler, "stream", None)
        if stream is sys.stdout:
            handler.stream = sys.stderr  # type: ignore[attr-defined]


async def _serve(config_path: str, *, with_pool: bool) -> None:
    from mcp.server.stdio import stdio_server

    from tg_agent.agent.tools import make_mcp_server
    from tg_agent.agent.tools.permissions import load_tool_access_policy

    config, db = await init_db(config_path)
    from tg_agent.plugin_api import start_plugins

    async with AsyncExitStack() as cleanup:
        cleanup.push_async_callback(db.close)
        pool = None
        if with_pool:
            try:
                # Legacy long-lived surface (frozen): keeps its pre-#1521 behaviour.
                auth, pool = await init_pool(config, db, allow_live_daemon=True)
                if auth is not None:
                    cleanup.push_async_callback(auth.cleanup)
                if pool is not None:
                    cleanup.push_async_callback(pool.disconnect_all)
            except Exception:
                logger.warning("Telegram pool init failed; pool-dependent tools will error", exc_info=True)
                pool = None

        plugins = await start_plugins(db, config, pool)
        cleanup.push_async_callback(plugins.close)
        # Metadata must exist before loading ACL. A headless MCP process cannot
        # prompt for grants, so denied and requestable tools stay hidden.
        access_policy = await load_tool_access_policy(db, use_cache=False)
        server_config = make_mcp_server(db, client_pool=pool, config=config, access_policy=access_policy)
        server = server_config["instance"]
        logger.info("MCP server ready over stdio (pool=%s, ACL enforced)", "on" if pool else "off")

        async with stdio_server() as (read_stream, write_stream):
            await server.run(read_stream, write_stream, server.create_initialization_options())


def serve_mcp(config_path: str, *, no_pool: bool = False) -> None:
    """Run the stdio MCP server until interrupted.

    Shared body for both CLI entry points — the argparse ``run`` wrapper below
    and the Typer ``mcp-server`` command (``tg_agent/cli/typer_commands.py``). Routes
    logging off stdout (the JSON-RPC channel) first, then drives the long-lived
    ``_serve`` coroutine through the single async-bridge ``run_async`` (replacing
    the former local ``asyncio.run``). ``KeyboardInterrupt`` exits quietly.
    """
    _route_logging_to_stderr()
    with_pool = not no_pool
    try:
        run_async(_serve(config_path, with_pool=with_pool))
    except KeyboardInterrupt:
        pass


def run(args: argparse.Namespace) -> None:
    serve_mcp(args.config, no_pool=getattr(args, "no_pool", False))
