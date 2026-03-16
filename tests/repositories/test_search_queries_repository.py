"""Tests for SearchQueriesRepository."""
from __future__ import annotations

from datetime import datetime

import pytest

from src.database.repositories.search_queries import SearchQueriesRepository
from src.models import SearchQuery


@pytest.fixture
async def repo(db):
    """Create repository instance."""
    return SearchQueriesRepository(db.db)


def make_query(query: str = "test query", **kwargs) -> SearchQuery:
    """Create a test SearchQuery."""
    defaults = {
        "is_regex": False,
        "is_fts": False,
        "is_active": True,
        "notify_on_collect": False,
        "track_stats": True,
        "interval_minutes": 60,
    }
    defaults.update(kwargs)
    return SearchQuery(query=query, **defaults)


# add tests

async def test_add_basic(repo):
    """Test adding a basic search query."""
    sq = make_query("hello world")
    pk = await repo.add(sq)
    assert pk > 0

    result = await repo.get_by_id(pk)
    assert result is not None
    assert result.query == "hello world"


async def test_add_with_all_fields(repo):
    """Test adding query with all fields."""
    sq = SearchQuery(
        query="test",
        is_regex=True,
        is_fts=False,
        is_active=False,
        notify_on_collect=True,
        track_stats=False,
        interval_minutes=30,
        exclude_patterns="spam\njunk",
        max_length=500,
    )
    pk = await repo.add(sq)

    result = await repo.get_by_id(pk)
    assert result.is_regex is True
    assert result.is_active is False
    assert result.notify_on_collect is True
    assert result.track_stats is False
    assert result.interval_minutes == 30
    assert result.exclude_patterns == "spam\njunk"
    assert result.max_length == 500


# get_all tests

async def test_get_all_empty(repo):
    """Test getting all queries when none exist."""
    result = await repo.get_all()
    assert result == []


async def test_get_all_multiple(repo):
    """Test getting all queries."""
    await repo.add(make_query("query1"))
    await repo.add(make_query("query2"))
    await repo.add(make_query("query3"))

    result = await repo.get_all()
    assert len(result) == 3
    queries = {q.query for q in result}
    assert queries == {"query1", "query2", "query3"}


async def test_get_all_active_only(repo):
    """Test getting only active queries."""
    await repo.add(make_query("active1", is_active=True))
    await repo.add(make_query("inactive", is_active=False))
    await repo.add(make_query("active2", is_active=True))

    result = await repo.get_all(active_only=True)
    assert len(result) == 2
    assert all(q.is_active for q in result)


async def test_get_all_ordered_by_id(repo):
    """Test that queries are ordered by id."""
    id1 = await repo.add(make_query("first"))
    id2 = await repo.add(make_query("second"))
    id3 = await repo.add(make_query("third"))

    result = await repo.get_all()
    assert result[0].id == id1
    assert result[1].id == id2
    assert result[2].id == id3


# get_by_id tests

async def test_get_by_id_found(repo):
    """Test getting query by id."""
    sq = make_query("test query")
    pk = await repo.add(sq)

    result = await repo.get_by_id(pk)
    assert result is not None
    assert result.query == "test query"


async def test_get_by_id_not_found(repo):
    """Test getting non-existent query."""
    result = await repo.get_by_id(999)
    assert result is None


# set_active tests

async def test_set_active_true(repo):
    """Test activating a query."""
    pk = await repo.add(make_query("test", is_active=False))
    await repo.set_active(pk, True)

    result = await repo.get_by_id(pk)
    assert result.is_active is True


async def test_set_active_false(repo):
    """Test deactivating a query."""
    pk = await repo.add(make_query("test", is_active=True))
    await repo.set_active(pk, False)

    result = await repo.get_by_id(pk)
    assert result.is_active is False


# update tests

async def test_update_query(repo):
    """Test updating a query."""
    pk = await repo.add(make_query("old query"))

    updated = make_query("new query", is_regex=True, interval_minutes=120)
    await repo.update(pk, updated)

    result = await repo.get_by_id(pk)
    assert result.query == "new query"
    assert result.is_regex is True
    assert result.interval_minutes == 120


async def test_update_preserves_id(repo):
    """Test that update preserves the original id."""
    pk = await repo.add(make_query("test"))
    await repo.update(pk, make_query("updated"))

    result = await repo.get_by_id(pk)
    assert result.id == pk


# delete tests

async def test_delete(repo):
    """Test deleting a query."""
    pk = await repo.add(make_query("test"))
    await repo.delete(pk)

    result = await repo.get_by_id(pk)
    assert result is None


async def test_delete_cascades_stats(repo):
    """Test that deleting a query also deletes its stats."""
    pk = await repo.add(make_query("test"))
    await repo.record_stat(pk, 10)

    await repo.delete(pk)

    # Verify stats are gone
    stats = await repo.get_daily_stats(pk)
    assert stats == []


# record_stat tests

async def test_record_stat(repo):
    """Test recording a stat."""
    pk = await repo.add(make_query("test"))
    await repo.record_stat(pk, 42)

    stats = await repo.get_daily_stats(pk, days=1)
    assert len(stats) == 1
    assert stats[0].count == 42


async def test_record_stat_replaces_same_day(repo):
    """Test that recording stat for same day replaces previous."""
    pk = await repo.add(make_query("test"))
    await repo.record_stat(pk, 10)
    await repo.record_stat(pk, 20)  # Should replace

    stats = await repo.get_daily_stats(pk, days=1)
    assert len(stats) == 1
    assert stats[0].count == 20


# get_daily_stats tests

async def test_get_daily_stats_empty(repo):
    """Test getting daily stats when none exist."""
    pk = await repo.add(make_query("test"))
    stats = await repo.get_daily_stats(pk)
    assert stats == []


async def test_get_daily_stats_multiple_days(repo):
    """Test getting stats across multiple days."""
    pk = await repo.add(make_query("test"))

    # Insert stats for different days
    await repo._db.executemany(
        "INSERT INTO search_query_stats (query_id, match_count, recorded_at) VALUES (?, ?, ?)",
        [
            (pk, 10, "2026-03-14 12:00:00"),
            (pk, 20, "2026-03-15 12:00:00"),
            (pk, 30, "2026-03-16 12:00:00"),
        ],
    )
    await repo._db.commit()

    stats = await repo.get_daily_stats(pk, days=7)
    assert len(stats) == 3
    counts = {s.count for s in stats}
    assert counts == {10, 20, 30}


async def test_get_daily_stats_aggregates_by_day(repo):
    """Test that stats are aggregated by day."""
    pk = await repo.add(make_query("test"))

    # Insert multiple stats for same day
    await repo._db.executemany(
        "INSERT INTO search_query_stats (query_id, match_count, recorded_at) VALUES (?, ?, ?)",
        [
            (pk, 10, "2026-03-16 10:00:00"),
            (pk, 20, "2026-03-16 15:00:00"),
        ],
    )
    await repo._db.commit()

    stats = await repo.get_daily_stats(pk, days=7)
    assert len(stats) == 1
    assert stats[0].count == 30  # Sum of both


# get_stats_for_all tests

async def test_get_stats_for_all_empty(repo):
    """Test getting all stats when none exist."""
    stats = await repo.get_stats_for_all()
    assert stats == {}


async def test_get_stats_for_all_multiple_queries(repo):
    """Test getting stats for multiple queries."""
    pk1 = await repo.add(make_query("query1"))
    pk2 = await repo.add(make_query("query2"))

    await repo._db.executemany(
        "INSERT INTO search_query_stats (query_id, match_count, recorded_at) VALUES (?, ?, ?)",
        [
            (pk1, 10, "2026-03-16 12:00:00"),
            (pk2, 20, "2026-03-16 12:00:00"),
        ],
    )
    await repo._db.commit()

    stats = await repo.get_stats_for_all(days=7)
    assert pk1 in stats
    assert pk2 in stats
    assert stats[pk1][0].count == 10
    assert stats[pk2][0].count == 20


# get_last_recorded_at tests

async def test_get_last_recorded_at_none(repo):
    """Test getting last recorded time when no stats exist."""
    pk = await repo.add(make_query("test"))
    result = await repo.get_last_recorded_at(pk)
    assert result is None


async def test_get_last_recorded_at(repo):
    """Test getting last recorded time."""
    pk = await repo.add(make_query("test"))

    await repo._db.executemany(
        "INSERT INTO search_query_stats (query_id, match_count, recorded_at) VALUES (?, ?, ?)",
        [
            (pk, 10, "2026-03-14 12:00:00"),
            (pk, 20, "2026-03-16 18:00:00"),
            (pk, 30, "2026-03-15 10:00:00"),
        ],
    )
    await repo._db.commit()

    result = await repo.get_last_recorded_at(pk)
    assert result == "2026-03-16 18:00:00"


# get_last_recorded_at_all tests

async def test_get_last_recorded_at_all_empty(repo):
    """Test getting all last recorded times when no stats exist."""
    result = await repo.get_last_recorded_at_all()
    assert result == {}


async def test_get_last_recorded_at_all(repo):
    """Test getting last recorded times for all queries."""
    pk1 = await repo.add(make_query("query1"))
    pk2 = await repo.add(make_query("query2"))

    await repo._db.executemany(
        "INSERT INTO search_query_stats (query_id, match_count, recorded_at) VALUES (?, ?, ?)",
        [
            (pk1, 10, "2026-03-15 12:00:00"),
            (pk2, 20, "2026-03-16 18:00:00"),
        ],
    )
    await repo._db.commit()

    result = await repo.get_last_recorded_at_all()
    assert result[pk1] == "2026-03-15 12:00:00"
    assert result[pk2] == "2026-03-16 18:00:00"


# get_notification_queries tests

async def test_get_notification_queries_empty(repo):
    """Test getting notification queries when none exist."""
    result = await repo.get_notification_queries()
    assert result == []


async def test_get_notification_queries(repo):
    """Test getting queries with notify_on_collect."""
    await repo.add(make_query("notify1", notify_on_collect=True))
    await repo.add(make_query("no_notify", notify_on_collect=False))
    await repo.add(make_query("notify2", notify_on_collect=True))

    result = await repo.get_notification_queries()
    assert len(result) == 2
    queries = {q.query for q in result}
    assert queries == {"notify1", "notify2"}


async def test_get_notification_queries_active_only(repo):
    """Test that notification queries can filter by active."""
    await repo.add(make_query("active_notify", notify_on_collect=True, is_active=True))
    await repo.add(make_query("inactive_notify", notify_on_collect=True, is_active=False))

    result = await repo.get_notification_queries(active_only=True)
    assert len(result) == 1
    assert result[0].query == "active_notify"


async def test_get_notification_queries_all(repo):
    """Test getting all notification queries including inactive."""
    await repo.add(make_query("active_notify", notify_on_collect=True, is_active=True))
    await repo.add(make_query("inactive_notify", notify_on_collect=True, is_active=False))

    result = await repo.get_notification_queries(active_only=False)
    assert len(result) == 2


# _row_to_model tests

async def test_row_to_model_handles_null_is_fts(repo):
    """Test that null is_fts is handled correctly."""
    # Directly insert with null is_fts
    await repo._db.execute(
        "INSERT INTO search_queries"
        " (query, name, is_regex, is_fts, is_active,"
        " notify_on_collect, track_stats, interval_minutes)"
        " VALUES (?, ?, ?, NULL, ?, ?, ?, ?)",
        ("test", "test", 0, 1, 0, 1, 60),
    )
    await repo._db.commit()

    result = await repo.get_all()
    assert len(result) == 1
    assert result[0].is_fts is False  # NULL -> False


async def test_row_to_model_handles_null_exclude_patterns(repo):
    """Test that null exclude_patterns is handled correctly."""
    await repo._db.execute(
        "INSERT INTO search_queries"
        " (query, name, is_regex, is_fts, is_active,"
        " notify_on_collect, track_stats, interval_minutes,"
        " exclude_patterns)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL)",
        ("test", "test", 0, 0, 1, 0, 1, 60),
    )
    await repo._db.commit()

    result = await repo.get_all()
    assert len(result) == 1
    assert result[0].exclude_patterns == ""


async def test_row_to_model_created_at(repo):
    """Test that created_at is properly parsed."""
    pk = await repo.add(make_query("test"))
    result = await repo.get_by_id(pk)
    assert result.created_at is not None
    assert isinstance(result.created_at, datetime)
