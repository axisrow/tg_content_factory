"""Tests for src/agent/tools.py - MCP tools for Agent.

These tests call the actual tool handler functions via the @tool decorator's
.handler attribute, ensuring argument parsing, formatting, and error handling
are all exercised.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.database import Database


@pytest.fixture
def mock_db():
    """Create a mock Database for testing tools."""
    return MagicMock(spec=Database)


def _get_tool_handlers(mock_db):
    """Build MCP tools and return their handlers keyed by name."""
    captured_tools = []

    with patch(
        "src.agent.tools.create_sdk_mcp_server",
        side_effect=lambda **kwargs: captured_tools.extend(kwargs.get("tools", [])),
    ):
        from src.agent.tools import make_mcp_server

        make_mcp_server(mock_db)

    return {t.name: t.handler for t in captured_tools}


def _text(result: dict) -> str:
    """Extract text from tool result payload."""
    return result["content"][0]["text"]


# ---------------------------------------------------------------------------
# search_messages tool
# ---------------------------------------------------------------------------


class TestSearchMessagesTool:
    """Tests for the search_messages tool handler."""

    @pytest.mark.asyncio
    async def test_empty_result(self, mock_db):
        mock_db.search_messages = AsyncMock(return_value=([], 0))
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["search_messages"]({"query": "nonexistent", "limit": 20})

        assert result["content"][0]["type"] == "text"
        assert "Ничего не найдено" in _text(result)
        assert "nonexistent" in _text(result)
        mock_db.search_messages.assert_awaited_once_with(query="nonexistent", limit=20)

    @pytest.mark.asyncio
    async def test_with_results(self, mock_db):
        mock_messages = [
            SimpleNamespace(
                channel_id=100, message_id=1,
                text="Test message one", date="2025-01-01",
            ),
            SimpleNamespace(
                channel_id=200, message_id=2,
                text="Another test message", date="2025-01-02",
            ),
        ]
        mock_db.search_messages = AsyncMock(return_value=(mock_messages, 2))
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["search_messages"]({"query": "test", "limit": 10})

        text = _text(result)
        assert "Найдено 2 сообщений" in text
        assert "channel_id=100" in text
        assert "channel_id=200" in text
        mock_db.search_messages.assert_awaited_once_with(query="test", limit=10)

    @pytest.mark.asyncio
    async def test_text_truncation(self, mock_db):
        """Tool truncates message text to 300 chars."""
        long_text = "x" * 500
        mock_messages = [
            SimpleNamespace(channel_id=100, message_id=1, text=long_text, date="2025-01-01"),
        ]
        mock_db.search_messages = AsyncMock(return_value=(mock_messages, 1))
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["search_messages"]({"query": "x", "limit": 20})

        text = _text(result)
        # Preview is capped at 300 chars
        assert "x" * 300 in text
        assert "x" * 301 not in text

    @pytest.mark.asyncio
    async def test_none_text_handled(self, mock_db):
        """Tool handles messages with None text without crashing."""
        mock_messages = [
            SimpleNamespace(channel_id=100, message_id=1, text=None, date="2025-01-01"),
        ]
        mock_db.search_messages = AsyncMock(return_value=(mock_messages, 1))
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["search_messages"]({"query": "test", "limit": 20})

        text = _text(result)
        assert "channel_id=100" in text

    @pytest.mark.asyncio
    async def test_default_limit(self, mock_db):
        """Tool applies default limit=20 when not provided."""
        mock_db.search_messages = AsyncMock(return_value=([], 0))
        handlers = _get_tool_handlers(mock_db)

        await handlers["search_messages"]({"query": "test"})

        mock_db.search_messages.assert_awaited_once_with(query="test", limit=20)

    @pytest.mark.asyncio
    async def test_custom_limit(self, mock_db):
        mock_db.search_messages = AsyncMock(return_value=([], 0))
        handlers = _get_tool_handlers(mock_db)

        await handlers["search_messages"]({"query": "test", "limit": 50})

        mock_db.search_messages.assert_awaited_once_with(query="test", limit=50)

    @pytest.mark.asyncio
    async def test_error_returns_text_not_exception(self, mock_db):
        """Tool catches DB errors and returns error text (no exception raised)."""
        mock_db.search_messages = AsyncMock(side_effect=Exception("DB connection error"))
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["search_messages"]({"query": "test", "limit": 20})

        text = _text(result)
        assert "Ошибка поиска сообщений" in text
        assert "DB connection error" in text


class TestSemanticSearchTool:
    @pytest.mark.asyncio
    async def test_with_results(self, mock_db):
        mock_messages = [
            SimpleNamespace(
                channel_id=100,
                message_id=1,
                text="Semantic result",
                date="2025-01-01",
            ),
        ]
        mock_db.search_semantic_messages = AsyncMock(return_value=(mock_messages, 1))

        class FakeEmbeddingService:
            def __init__(self, _db):
                pass

            async def index_pending_messages(self):
                return 0

            async def embed_query(self, query):
                assert query == "semantic"
                return [1.0, 0.0]

        with patch("src.agent.tools.EmbeddingService", FakeEmbeddingService):
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["semantic_search"]({"query": "semantic", "limit": 5})

        text = _text(result)
        assert "Семантически найдено 1 сообщений" in text
        assert "Semantic result" in text
        mock_db.search_semantic_messages.assert_awaited_once_with([1.0, 0.0], limit=5)

    @pytest.mark.asyncio
    async def test_error_returns_text_not_exception(self, mock_db):
        class BrokenEmbeddingService:
            def __init__(self, _db):
                pass

            async def embed_query(self, query):
                raise RuntimeError("vec unavailable")

        with patch("src.agent.tools.EmbeddingService", BrokenEmbeddingService):
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["semantic_search"]({"query": "semantic", "limit": 5})

        text = _text(result)
        assert "Ошибка семантического поиска" in text
        assert "vec unavailable" in text


# ---------------------------------------------------------------------------
# get_channels tool
# ---------------------------------------------------------------------------


class TestGetChannelsTool:
    """Tests for the get_channels tool handler."""

    @pytest.mark.asyncio
    async def test_empty(self, mock_db):
        mock_db.get_channels = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["get_channels"]({})

        assert "Каналы не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_channels(self, mock_db):
        mock_channels = [
            SimpleNamespace(
                channel_id=100, title="Active Channel",
                username="active_ch", is_active=True, is_filtered=False,
            ),
            SimpleNamespace(
                channel_id=200, title="Inactive Channel",
                username="inactive_ch", is_active=False, is_filtered=True,
            ),
        ]
        mock_db.get_channels = AsyncMock(return_value=mock_channels)
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["get_channels"]({})

        text = _text(result)
        assert "Доступные каналы (2)" in text
        assert "@active_ch" in text
        assert "активен" in text
        assert "неактивен" in text
        assert "[отфильтрован]" in text

    @pytest.mark.asyncio
    async def test_none_username(self, mock_db):
        """Channel with username=None renders @None (known pre-existing issue)."""
        mock_channels = [
            SimpleNamespace(
                channel_id=100, title="Private Channel",
                username=None, is_active=True, is_filtered=False,
            ),
        ]
        mock_db.get_channels = AsyncMock(return_value=mock_channels)
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["get_channels"]({})

        text = _text(result)
        assert "Private Channel" in text
        # Known issue: renders @None for channels without username
        assert "@None" in text

    @pytest.mark.asyncio
    async def test_error_returns_text_not_exception(self, mock_db):
        """Tool catches DB errors and returns error text."""
        mock_db.get_channels = AsyncMock(side_effect=Exception("DB query failed"))
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["get_channels"]({})

        text = _text(result)
        assert "Ошибка получения каналов" in text
        assert "DB query failed" in text


# ---------------------------------------------------------------------------
# make_mcp_server factory
# ---------------------------------------------------------------------------


class TestMakeMcpServer:
    """Tests for make_mcp_server factory function."""

    def test_creates_server_returns_dict(self, mock_db):
        from src.agent.tools import make_mcp_server

        server = make_mcp_server(mock_db)
        assert server is not None
        assert isinstance(server, dict)
        assert server["name"] == "telegram_db"

    def test_server_has_instance(self, mock_db):
        from src.agent.tools import make_mcp_server

        server = make_mcp_server(mock_db)
        assert "instance" in server
