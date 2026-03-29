"""Shared helpers for agent tools tests."""
from __future__ import annotations

from unittest.mock import patch


def _get_tool_handlers(mock_db, client_pool=None, config=None, **kwargs):
    """Build MCP tools and return their handlers keyed by name."""
    captured_tools = []
    with patch(
        "src.agent.tools.create_sdk_mcp_server",
        side_effect=lambda **kw: captured_tools.extend(kw.get("tools", [])),
    ):
        from src.agent.tools import make_mcp_server

        make_mcp_server(mock_db, client_pool=client_pool, config=config, **kwargs)
    return {t.name: t.handler for t in captured_tools}


def _text(result: dict) -> str:
    """Extract text from MCP tool result payload."""
    return result["content"][0]["text"]
