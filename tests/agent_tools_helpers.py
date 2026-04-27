"""Shared helpers for agent tools tests."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch


def _normalize_get_setting_mock(mock_db) -> None:
    """Default unconfigured Database.get_setting() mocks to None."""
    get_setting = getattr(mock_db, "get_setting", None)
    if not isinstance(get_setting, AsyncMock):
        return
    if get_setting.side_effect is not None:
        return
    if isinstance(get_setting.return_value, (AsyncMock, MagicMock)):
        get_setting.return_value = None


def _get_tool_handlers(mock_db, client_pool=None, config=None, **kwargs):
    """Build MCP tools and return their handlers keyed by name."""
    _normalize_get_setting_mock(mock_db)
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


async def assert_tool_text(handler, payload: dict, expected: str) -> dict:
    """Run an agent tool handler and assert that its text payload contains expected text."""
    result = await handler(payload)
    assert expected in _text(result)
    return result
