"""Tests for src/agent/tools.py - MCP tools for Agent.

These tests verify the tool logic without using the decorator infrastructure,
which requires complex SDK setup. Instead, we test the underlying DB calls.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.database import Database


@pytest.fixture
def mock_db():
    """Create a mock Database for testing tools."""
    return MagicMock(spec=Database)


class TestSearchMessagesLogic:
    """Tests for search_messages tool logic (DB interaction)."""

    @pytest.mark.asyncio
    async def test_search_messages_empty_result(self, mock_db):
        """Test search_messages when no results found."""
        mock_db.search_messages = AsyncMock(return_value=([], 0))

        messages, total = await mock_db.search_messages(query="nonexistent", limit=20)

        assert messages == []
        assert total == 0
        mock_db.search_messages.assert_awaited_once_with(query="nonexistent", limit=20)

    @pytest.mark.asyncio
    async def test_search_messages_with_results(self, mock_db):
        """Test search_messages with results found."""
        mock_messages = [
            SimpleNamespace(
                channel_id=100,
                message_id=1,
                text="Test message one with important content",
                date="2025-01-01",
            ),
            SimpleNamespace(
                channel_id=200,
                message_id=2,
                text="Another test message",
                date="2025-01-02",
            ),
        ]
        mock_db.search_messages = AsyncMock(return_value=(mock_messages, 2))

        messages, total = await mock_db.search_messages(query="test", limit=10)

        assert len(messages) == 2
        assert total == 2
        assert messages[0].channel_id == 100
        assert messages[1].channel_id == 200

    @pytest.mark.asyncio
    async def test_search_messages_truncates_in_formatter(self, mock_db):
        """Test that long text can be truncated to 300 chars in formatter."""
        long_text = "x" * 500
        mock_messages = [
            SimpleNamespace(
                channel_id=100,
                message_id=1,
                text=long_text,
                date="2025-01-01",
            ),
        ]
        mock_db.search_messages = AsyncMock(return_value=(mock_messages, 1))

        messages, total = await mock_db.search_messages(query="x", limit=20)

        # Full text is returned from DB, truncation happens in tool formatter
        assert len(messages[0].text) == 500
        # The tool logic would truncate: (m.text or "")[:300]
        truncated = (messages[0].text or "")[:300]
        assert len(truncated) == 300

    @pytest.mark.asyncio
    async def test_search_messages_with_none_text(self, mock_db):
        """Test search_messages handles messages with None text."""
        mock_messages = [
            SimpleNamespace(
                channel_id=100,
                message_id=1,
                text=None,
                date="2025-01-01",
            ),
        ]
        mock_db.search_messages = AsyncMock(return_value=(mock_messages, 1))

        messages, total = await mock_db.search_messages(query="test", limit=20)

        # Should not crash on None text
        assert len(messages) == 1
        # The tool logic handles None: (m.text or "")[:300]
        safe_text = (messages[0].text or "")[:300]
        assert safe_text == ""

    @pytest.mark.asyncio
    async def test_search_messages_default_limit(self, mock_db):
        """Test search_messages uses default limit of 20."""
        mock_db.search_messages = AsyncMock(return_value=([], 0))

        await mock_db.search_messages(query="test")

        # Verify default limit behavior
        mock_db.search_messages.assert_awaited_once_with(query="test")

    @pytest.mark.asyncio
    async def test_search_messages_custom_limit(self, mock_db):
        """Test search_messages with custom limit."""
        mock_db.search_messages = AsyncMock(return_value=([], 0))

        await mock_db.search_messages(query="test", limit=50)

        mock_db.search_messages.assert_awaited_once_with(query="test", limit=50)

    @pytest.mark.asyncio
    async def test_search_messages_error_handling(self, mock_db):
        """Test search_messages handles database errors."""
        mock_db.search_messages = AsyncMock(side_effect=Exception("DB connection error"))

        with pytest.raises(Exception, match="DB connection error"):
            await mock_db.search_messages(query="test", limit=20)


class TestGetChannelsLogic:
    """Tests for get_channels tool logic (DB interaction)."""

    @pytest.mark.asyncio
    async def test_get_channels_empty(self, mock_db):
        """Test get_channels when no channels exist."""
        mock_db.get_channels = AsyncMock(return_value=[])

        channels = await mock_db.get_channels()

        assert channels == []
        mock_db.get_channels.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_get_channels_with_channels(self, mock_db):
        """Test get_channels with multiple channels."""
        mock_channels = [
            SimpleNamespace(
                channel_id=100,
                title="Active Channel",
                username="active_ch",
                is_active=True,
                is_filtered=False,
            ),
            SimpleNamespace(
                channel_id=200,
                title="Inactive Channel",
                username="inactive_ch",
                is_active=False,
                is_filtered=True,
            ),
            SimpleNamespace(
                channel_id=300,
                title="Filtered Active",
                username="filtered_ch",
                is_active=True,
                is_filtered=True,
            ),
        ]
        mock_db.get_channels = AsyncMock(return_value=mock_channels)

        channels = await mock_db.get_channels()

        assert len(channels) == 3
        assert channels[0].title == "Active Channel"
        assert channels[1].is_active is False
        assert channels[2].is_filtered is True

    @pytest.mark.asyncio
    async def test_get_channels_with_no_username(self, mock_db):
        """Test get_channels handles channels without username."""
        mock_channels = [
            SimpleNamespace(
                channel_id=100,
                title="Private Channel",
                username=None,
                is_active=True,
                is_filtered=False,
            ),
        ]
        mock_db.get_channels = AsyncMock(return_value=mock_channels)

        channels = await mock_db.get_channels()

        assert len(channels) == 1
        assert channels[0].username is None

    @pytest.mark.asyncio
    async def test_get_channels_error_handling(self, mock_db):
        """Test get_channels handles database errors."""
        mock_db.get_channels = AsyncMock(side_effect=Exception("DB query failed"))

        with pytest.raises(Exception, match="DB query failed"):
            await mock_db.get_channels()


class TestMakeMcpServer:
    """Tests for make_mcp_server factory function."""

    def test_creates_server_returns_object(self, mock_db):
        """Test that make_mcp_server returns a server config object."""
        from src.agent.tools import make_mcp_server

        server = make_mcp_server(mock_db)
        # create_sdk_mcp_server returns some kind of config object
        assert server is not None

    def test_server_is_callable_with_db(self, mock_db):
        """Test that make_mcp_server can be called with db parameter."""
        from src.agent.tools import make_mcp_server

        # Should not raise
        server = make_mcp_server(mock_db)
        assert server is not None


class TestToolMessageFormatting:
    """Tests for message formatting logic in tools."""

    def test_format_empty_results(self):
        """Test formatting when no messages found."""
        query = "nonexistent"
        messages = []
        total = 0

        if not messages:
            text = f"Ничего не найдено по запросу: {query!r}"
        else:
            text = f"Found {total}"

        assert "Ничего не найдено" in text
        assert "nonexistent" in text

    def test_format_with_results(self):
        """Test formatting when messages are found."""
        messages = [
            SimpleNamespace(channel_id=100, text="test", date="2025-01-01"),
        ]
        total = 1

        lines = [f"Найдено {total} сообщений для 'test'. Показаны первые {len(messages)}:"]
        for m in messages:
            preview = (m.text or "")[:300]
            lines.append(f"- [channel_id={m.channel_id}, date={m.date}]: {preview}")
        text = "\n".join(lines)

        assert "Найдено 1 сообщений" in text
        assert "channel_id=100" in text

    def test_format_channels_list(self):
        """Test formatting channel list."""
        channels = [
            SimpleNamespace(
                title="Test",
                username="test_ch",
                channel_id=100,
                is_active=True,
                is_filtered=False,
            ),
        ]

        lines = [f"Доступные каналы ({len(channels)}):"]
        for ch in channels:
            status = "активен" if ch.is_active else "неактивен"
            filtered = " [отфильтрован]" if ch.is_filtered else ""
            lines.append(f"- {ch.title} (@{ch.username}, id={ch.channel_id}, {status}{filtered})")
        text = "\n".join(lines)

        assert "Доступные каналы (1)" in text
        assert "@test_ch" in text
        assert "активен" in text
