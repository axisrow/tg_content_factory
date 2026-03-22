"""Tests for LocalSearch."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import Message, SearchResult
from src.search.local_search import LocalSearch


@pytest.fixture
def mock_search_bundle():
    """Mock SearchBundle."""
    bundle = MagicMock()
    bundle.search_messages = AsyncMock(return_value=([], 0))
    bundle.messages = MagicMock()
    bundle.messages.search_semantic_messages = AsyncMock(return_value=([], 0))
    bundle.messages.search_hybrid_messages = AsyncMock(return_value=([], 0))
    return bundle


@pytest.fixture
def mock_embedding_service():
    """Mock EmbeddingService."""
    service = MagicMock()
    service.embed_query = AsyncMock(return_value=[0.1, 0.2, 0.3])
    return service


def make_message(**kwargs) -> Message:
    """Create a valid Message instance for tests."""
    defaults = {
        "channel_id": 1,
        "message_id": 1,
        "text": "test message",
        "date": "2024-01-01",
    }
    defaults.update(kwargs)
    return Message(**defaults)


# === search tests ===


@pytest.mark.asyncio
async def test_search_basic(mock_search_bundle):
    """Basic search returns SearchResult."""
    msg = make_message(text="test message", channel_id=1, message_id=1)
    mock_search_bundle.search_messages.return_value = ([msg], 1)

    local_search = LocalSearch(mock_search_bundle)
    result = await local_search.search(query="test")

    assert isinstance(result, SearchResult)
    assert result.total == 1
    assert result.query == "test"
    mock_search_bundle.search_messages.assert_called_once()


@pytest.mark.asyncio
async def test_search_with_channel_filter(mock_search_bundle):
    """Search with channel_id filter."""
    mock_search_bundle.search_messages.return_value = ([], 0)

    local_search = LocalSearch(mock_search_bundle)
    await local_search.search(query="test", channel_id=123)

    args, kwargs = mock_search_bundle.search_messages.call_args
    assert kwargs["channel_id"] == 123


@pytest.mark.asyncio
async def test_search_with_date_range(mock_search_bundle):
    """Search with date range."""
    mock_search_bundle.search_messages.return_value = ([], 0)

    local_search = LocalSearch(mock_search_bundle)
    await local_search.search(query="test", date_from="2024-01-01", date_to="2024-12-31")

    args, kwargs = mock_search_bundle.search_messages.call_args
    assert kwargs["date_from"] == "2024-01-01"
    assert kwargs["date_to"] == "2024-12-31"


@pytest.mark.asyncio
async def test_search_with_length_filters(mock_search_bundle):
    """Search with min/max length filters."""
    mock_search_bundle.search_messages.return_value = ([], 0)

    local_search = LocalSearch(mock_search_bundle)
    await local_search.search(query="test", min_length=100, max_length=500)

    args, kwargs = mock_search_bundle.search_messages.call_args
    assert kwargs["min_length"] == 100
    assert kwargs["max_length"] == 500


@pytest.mark.asyncio
async def test_search_with_fts_flag(mock_search_bundle):
    """Search with FTS flag."""
    mock_search_bundle.search_messages.return_value = ([], 0)

    local_search = LocalSearch(mock_search_bundle)
    await local_search.search(query="test", is_fts=True)

    args, kwargs = mock_search_bundle.search_messages.call_args
    assert kwargs["is_fts"] is True


@pytest.mark.asyncio
async def test_search_with_pagination(mock_search_bundle):
    """Search with limit and offset."""
    mock_search_bundle.search_messages.return_value = ([], 0)

    local_search = LocalSearch(mock_search_bundle)
    await local_search.search(query="test", limit=10, offset=20)

    args, kwargs = mock_search_bundle.search_messages.call_args
    assert kwargs["limit"] == 10
    assert kwargs["offset"] == 20


# === search_semantic tests ===


@pytest.mark.asyncio
async def test_search_semantic_basic(mock_search_bundle, mock_embedding_service):
    """Basic semantic search returns SearchResult."""
    msg = make_message(text="semantic match")
    mock_search_bundle.messages.search_semantic_messages.return_value = ([msg], 1)

    local_search = LocalSearch(mock_search_bundle, mock_embedding_service)
    result = await local_search.search_semantic(query="test")

    assert isinstance(result, SearchResult)
    assert result.total == 1
    mock_embedding_service.embed_query.assert_called_once_with("test")


@pytest.mark.asyncio
async def test_search_semantic_with_filters(mock_search_bundle, mock_embedding_service):
    """Semantic search with filters."""
    mock_search_bundle.messages.search_semantic_messages.return_value = ([], 0)

    local_search = LocalSearch(mock_search_bundle, mock_embedding_service)
    await local_search.search_semantic(
        query="test",
        channel_id=5,
        date_from="2024-01-01",
        date_to="2024-12-31",
        limit=5,
        offset=10,
        min_length=50,
        max_length=200,
    )

    args, kwargs = mock_search_bundle.messages.search_semantic_messages.call_args
    assert kwargs["channel_id"] == 5
    assert kwargs["date_from"] == "2024-01-01"
    assert kwargs["date_to"] == "2024-12-31"
    assert kwargs["limit"] == 5
    assert kwargs["offset"] == 10
    assert kwargs["min_length"] == 50
    assert kwargs["max_length"] == 200


@pytest.mark.asyncio
async def test_search_semantic_no_embedding_service(mock_search_bundle):
    """Semantic search without embedding service raises."""
    local_search = LocalSearch(mock_search_bundle, embedding_service=None)

    with pytest.raises(RuntimeError) as exc_info:
        await local_search.search_semantic(query="test")

    assert "unavailable" in str(exc_info.value)


# === search_hybrid tests ===


@pytest.mark.asyncio
async def test_search_hybrid_basic(mock_search_bundle, mock_embedding_service):
    """Basic hybrid search returns SearchResult."""
    msg = make_message(text="hybrid match")
    mock_search_bundle.messages.search_hybrid_messages.return_value = ([msg], 1)

    local_search = LocalSearch(mock_search_bundle, mock_embedding_service)
    result = await local_search.search_hybrid(query="test")

    assert isinstance(result, SearchResult)
    assert result.total == 1
    mock_embedding_service.embed_query.assert_called_once_with("test")


@pytest.mark.asyncio
async def test_search_hybrid_with_filters(mock_search_bundle, mock_embedding_service):
    """Hybrid search with filters."""
    mock_search_bundle.messages.search_hybrid_messages.return_value = ([], 0)

    local_search = LocalSearch(mock_search_bundle, mock_embedding_service)
    await local_search.search_hybrid(
        query="test",
        channel_id=7,
        date_from="2024-06-01",
        date_to="2024-06-30",
        limit=15,
        offset=5,
        is_fts=True,
        min_length=20,
        max_length=100,
    )

    args, kwargs = mock_search_bundle.messages.search_hybrid_messages.call_args
    assert kwargs["query"] == "test"
    assert kwargs["channel_id"] == 7
    assert kwargs["date_from"] == "2024-06-01"
    assert kwargs["date_to"] == "2024-06-30"
    assert kwargs["limit"] == 15
    assert kwargs["offset"] == 5
    assert kwargs["is_fts"] is True
    assert kwargs["min_length"] == 20
    assert kwargs["max_length"] == 100


@pytest.mark.asyncio
async def test_search_hybrid_no_embedding_service(mock_search_bundle):
    """Hybrid search without embedding service raises."""
    local_search = LocalSearch(mock_search_bundle, embedding_service=None)

    with pytest.raises(RuntimeError) as exc_info:
        await local_search.search_hybrid(query="test")

    assert "unavailable" in str(exc_info.value)
