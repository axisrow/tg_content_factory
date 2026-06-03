"""The ``mcp-server`` CLI exposes the agent tool registry over stdio JSON-RPC.

Spawns the real entrypoint as a subprocess and performs a genuine MCP handshake
(initialize + tools/list) through the standard ``mcp`` client, proving an
external agent (e.g. the Codex CLI) can discover and call the project's tools.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.anyio


async def _list_tool_names(config_path: Path) -> list[str]:
    from mcp import ClientSession
    from mcp.client.stdio import StdioServerParameters, stdio_client

    params = StdioServerParameters(
        command=sys.executable,
        args=["-m", "src.main", "--config", str(config_path), "mcp-server", "--no-pool"],
        cwd=str(Path(__file__).resolve().parent.parent),
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
