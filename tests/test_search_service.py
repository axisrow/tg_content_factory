"""Tests for SearchService."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import SearchResult
from src.services.search_service import SearchService


def _make_search_result(query="test"):
    """Create a valid SearchResult."""
    return SearchResult(messages=[], total=0, query=query)


@pytest.mark.asyncio
async def test_search_ai_mode():
    """Test search with AI mode."""
    engine = MagicMock()
    ai_search = MagicMock()
    ai_search.search = AsyncMock(return_value=_make_search_result())

    service = SearchService(engine, ai_search)
    result = await service.search(mode="ai", query="test", limit=10)

    ai_search.search.assert_called_once_with("test")
    assert result.total == 0


@pytest.mark.asyncio
async def test_search_ai_mode_without_ai_search():
    """Test search with AI mode but no AI search engine."""
    engine = MagicMock()
    engine.search_local = AsyncMock(return_value=_make_search_result())

    service = SearchService(engine, ai_search=None)
    result = await service.search(mode="ai", query="test", limit=10)

    # Falls through to default (local)
    engine.search_local.assert_called_once()


@pytest.mark.asyncio
async def test_search_telegram_mode():
    """Test search with telegram mode."""
    engine = MagicMock()
    engine.search_telegram = AsyncMock(return_value=_make_search_result())

    service = SearchService(engine)
    result = await service.search(mode="telegram", query="test", limit=10)

    engine.search_telegram.assert_called_once_with("test", limit=10)


@pytest.mark.asyncio
async def test_search_my_chats_mode():
    """Test search with my_chats mode."""
    engine = MagicMock()
    engine.search_my_chats = AsyncMock(return_value=_make_search_result())

    service = SearchService(engine)
    result = await service.search(mode="my_chats", query="test", limit=10)

    engine.search_my_chats.assert_called_once_with("test", limit=10)


@pytest.mark.asyncio
async def test_search_channel_mode():
    """Test search with channel mode."""
    engine = MagicMock()
    engine.search_in_channel = AsyncMock(return_value=_make_search_result())

    service = SearchService(engine)
    result = await service.search(mode="channel", query="test", limit=10, channel_id=100)

    engine.search_in_channel.assert_called_once_with(100, "test", limit=10)


@pytest.mark.asyncio
async def test_search_semantic_mode():
    """Test search with semantic mode."""
    engine = MagicMock()
    engine.search_semantic = AsyncMock(return_value=_make_search_result())

    service = SearchService(engine)
    result = await service.search(
        mode="semantic",
        query="test",
        limit=10,
        channel_id=100,
        date_from="2024-01-01",
        date_to="2024-12-31",
        offset=0,
        min_length=None,
        max_length=None,
    )

    engine.search_semantic.assert_called_once()


@pytest.mark.asyncio
async def test_search_hybrid_mode():
    """Test search with hybrid mode."""
    engine = MagicMock()
    engine.search_hybrid = AsyncMock(return_value=_make_search_result())

    service = SearchService(engine)
    result = await service.search(
        mode="hybrid",
        query="test",
        limit=10,
        channel_id=100,
        date_from="2024-01-01",
        date_to="2024-12-31",
        offset=0,
        is_fts=True,
        min_length=None,
        max_length=None,
    )

    engine.search_hybrid.assert_called_once()


@pytest.mark.asyncio
async def test_search_local_mode_default():
    """Test search falls back to local for unknown mode."""
    engine = MagicMock()
    engine.search_local = AsyncMock(return_value=_make_search_result())

    service = SearchService(engine)
    result = await service.search(mode="unknown", query="test", limit=10)

    engine.search_local.assert_called_once()


@pytest.mark.asyncio
async def test_check_quota():
    """Test check_quota delegates to engine."""
    engine = MagicMock()
    engine.check_search_quota = AsyncMock(return_value={"allowed": True})

    service = SearchService(engine)
    result = await service.check_quota("test")

    engine.check_search_quota.assert_called_once_with("test")
    assert result == {"allowed": True}
