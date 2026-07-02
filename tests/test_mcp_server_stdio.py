"""The ``mcp-server`` CLI exposes the agent tool registry over stdio JSON-RPC.

Spawns the real entrypoint as a subprocess and performs a genuine MCP handshake
(initialize + tools/list) through the standard ``mcp`` client, proving an
external agent (e.g. the Codex CLI) can discover and call the project's tools.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.anyio


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


async def _list_tool_names(config_path: Path) -> list[str]:
    from mcp import ClientSession
    from mcp.client.stdio import StdioServerParameters, stdio_client

    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "src.main", "--config", str(config_path), "mcp-server", "--no-pool"],
        cwd=str(await asyncio.to_thread(_repo_root)),
    )
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.list_tools()
            return [t.name for t in result.tools]


@pytest.fixture
def _mcp_config(tmp_path: Path) -> Path:
    """Minimal config.yaml pointing the server at a throwaway DB."""
    db_path = tmp_path / "mcp_test.db"
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        f'database:\n  path: "{db_path.as_posix()}"\n',
        encoding="utf-8",
    )
    return cfg


async def test_mcp_server_lists_project_tools(_mcp_config: Path):
    names = await _list_tool_names(_mcp_config)
    # A representative sample from across the tool modules — the handshake works
    # and the registry is served, not an empty server.
    assert "search_messages" in names
    assert "list_channels" in names
    assert len(names) > 50


async def _seed_tool_acl(db_path: Path, acl: dict[str, bool]) -> None:
    """Persist the agent_tool_permissions ACL into the throwaway DB, then close it.

    Closing matters: the subprocess opens its own connection, so the write must
    be committed and the WAL flushed before the server starts.
    """
    import json

    from src.database import Database

    db = Database(str(db_path))
    await db.initialize()
    try:
        await db.repos.settings.set_setting("agent_tool_permissions", json.dumps(acl))
    finally:
        await db.close()


async def test_mcp_server_enforces_tool_acl(_mcp_config: Path):
    """A DENIED tool is not registered in the out-of-process server.

    The call-time session gate (a ContextVar) cannot cross the process boundary,
    so registration-time ACL filtering is the only thing standing between an
    external agent (Codex) and a write/delete tool the admin disabled. Here we
    deny one destructive tool and allow two read tools, then assert the denied
    one is absent from tools/list while the allowed ones remain.
    """
    db_path = _mcp_config.parent / "mcp_test.db"
    await _seed_tool_acl(
        db_path,
        {"delete_pipeline": False, "list_channels": True, "search_messages": True},
    )

    names = await _list_tool_names(_mcp_config)
    assert "delete_pipeline" not in names, "DENIED tool leaked into the standalone MCP server"
    assert "list_channels" in names
    assert "search_messages" in names
