"""Tests for SearchLogRepository."""

from __future__ import annotations


async def test_log_search(search_log_repo):
    """Test logging a search."""
    await search_log_repo.log_search("+1234567890", "test query", 10)

    result = await search_log_repo.get_recent_searches(limit=1)
    assert len(result) == 1
    assert result[0]["query"] == "test query"
    assert result[0]["results_count"] == 10


async def test_get_recent_searches_empty(search_log_repo):
    """Test getting searches when none exist."""
    result = await search_log_repo.get_recent_searches()
    assert result == []


async def test_get_recent_searches(search_log_repo):
    """Test getting recent searches."""
    await search_log_repo.log_search("+1111111111", "query1", 5)
    await search_log_repo.log_search("+2222222222", "query2", 10)

    result = await search_log_repo.get_recent_searches()
    assert len(result) == 2

    # Should be ordered by id DESC
    assert result[0]["query"] == "query2"
    assert result[1]["query"] == "query1"


async def test_get_recent_searches_limit(search_log_repo):
    """Test limit parameter."""
    for i in range(30):
        await search_log_repo.log_search("+1234567890", f"query{i}", i)

    result = await search_log_repo.get_recent_searches(limit=10)
    assert len(result) == 10

    # Should get most recent
    assert result[0]["query"] == "query29"


async def test_search_log_fields(search_log_repo):
    """Test that all fields are returned correctly."""
    await search_log_repo.log_search("+1234567890", "test query", 42)

    result = await search_log_repo.get_recent_searches(limit=1)
    assert len(result) == 1

    entry = result[0]
    assert entry["phone"] == "+1234567890"
    assert entry["query"] == "test query"
    assert entry["results_count"] == 42
    assert "id" in entry
    assert "created_at" in entry


async def test_search_log_zero_results(search_log_repo):
    """Test logging search with zero results."""
    await search_log_repo.log_search("+1234567890", "nonexistent", 0)

    result = await search_log_repo.get_recent_searches(limit=1)
    assert result[0]["results_count"] == 0
