"""Tests for SearchLogRepository."""
from __future__ import annotations

import pytest

from src.database.repositories.search_log import SearchLogRepository


@pytest.fixture
async def repo(db):
    """Create repository instance."""
    return SearchLogRepository(db.db)


async def test_log_search(repo):
    """Test logging a search."""
    await repo.log_search("+1234567890", "test query", 10)


async def test_get_recent_searches_empty(repo):
    """Test getting searches when none exist."""
    result = await repo.get_recent_searches()
    assert result == []


async def test_get_recent_searches(repo):
    """Test getting recent searches."""
    await repo.log_search("+1111111111", "query1", 5)
    await repo.log_search("+2222222222", "query2", 10)

    result = await repo.get_recent_searches()
    assert len(result) == 2

    # Should be ordered by id DESC
    assert result[0]["query"] == "query2"
    assert result[1]["query"] == "query1"


async def test_get_recent_searches_limit(repo):
    """Test limit parameter."""
    for i in range(30):
        await repo.log_search("+1234567890", f"query{i}", i)

    result = await repo.get_recent_searches(limit=10)
    assert len(result) == 10

    # Should get most recent
    assert result[0]["query"] == "query29"


async def test_search_log_fields(repo):
    """Test that all fields are returned correctly."""
    await repo.log_search("+1234567890", "test query", 42)

    result = await repo.get_recent_searches(limit=1)
    assert len(result) == 1

    entry = result[0]
    assert entry["phone"] == "+1234567890"
    assert entry["query"] == "test query"
    assert entry["results_count"] == 42
    assert "id" in entry
    assert "created_at" in entry


async def test_search_log_zero_results(repo):
    """Test logging search with zero results."""
    await repo.log_search("+1234567890", "nonexistent", 0)

    result = await repo.get_recent_searches(limit=1)
    assert result[0]["results_count"] == 0
