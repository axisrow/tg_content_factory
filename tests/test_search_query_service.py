"""Tests for src/services/search_query_service.py"""

from __future__ import annotations

from datetime import date as date_cls
from datetime import timedelta

import pytest

from src.models import SearchQuery, SearchQueryDailyStat
from src.services.search_query_service import SearchQueryService


class FakeBundle:
    """Fake SearchQueryBundle for testing."""

    def __init__(self):
        self._queries: dict[int, SearchQuery] = {}
        self._next_id = 1
        self._stats: dict[int, list[SearchQueryDailyStat]] = {}
        self._last_recorded: dict[int, str] = {}
        self.record_stat_calls: list[tuple[int, int]] = []

    async def add(self, sq: SearchQuery) -> int:
        sq_id = self._next_id
        self._next_id += 1
        sq.id = sq_id
        self._queries[sq_id] = sq
        return sq_id

    async def get_all(self, active_only: bool = False) -> list[SearchQuery]:
        queries = list(self._queries.values())
        if active_only:
            queries = [q for q in queries if q.is_active]
        return queries

    async def get_by_id(self, sq_id: int) -> SearchQuery | None:
        return self._queries.get(sq_id)

    async def set_active(self, sq_id: int, active: bool) -> None:
        if sq_id in self._queries:
            self._queries[sq_id].is_active = active

    async def update(self, sq_id: int, sq: SearchQuery) -> None:
        sq.id = sq_id
        self._queries[sq_id] = sq

    async def delete(self, sq_id: int) -> None:
        self._queries.pop(sq_id, None)

    async def record_stat(self, sq_id: int, count: int) -> None:
        self.record_stat_calls.append((sq_id, count))

    async def get_fts_daily_stats_for_query(
        self, sq: SearchQuery, days: int = 30
    ) -> list[SearchQueryDailyStat]:
        return self._stats.get(sq.id or 0, [])

    async def get_fts_daily_stats_batch(
        self, queries: list[SearchQuery], days: int = 30
    ) -> dict[int, list[SearchQueryDailyStat]]:
        return {q.id: self._stats.get(q.id or 0, []) for q in queries if q.id}

    async def get_last_recorded_at_all(self) -> dict[int, str]:
        return self._last_recorded.copy()

    async def get_daily_stats(
        self, sq_id: int, days: int = 30
    ) -> list[SearchQueryDailyStat]:
        return self._stats.get(sq_id, [])

    def set_stats(self, sq_id: int, stats: list[SearchQueryDailyStat]) -> None:
        self._stats[sq_id] = stats


@pytest.fixture
def bundle():
    return FakeBundle()


@pytest.fixture
def service(bundle):
    return SearchQueryService(bundle)


# === add/list/get tests ===


@pytest.mark.anyio
async def test_add_creates_search_query(service, bundle):
    """Add delegates to bundle.add() with correct fields."""
    sq_id = await service.add(
        query="test query",
        interval_minutes=30,
        is_regex=True,
        is_fts=False,
        notify_on_collect=True,
        track_stats=True,
        exclude_patterns="spam",
        max_length=500,
    )

    assert sq_id == 1
    stored = await bundle.get_by_id(sq_id)
    assert stored is not None
    assert stored.query == "test query"
    assert stored.interval_minutes == 30
    assert stored.is_regex is True
    assert stored.notify_on_collect is True
    assert stored.exclude_patterns == "spam"
    assert stored.max_length == 500


@pytest.mark.anyio
async def test_list_returns_all_queries(service, bundle):
    """List delegates to bundle.get_all()."""
    await service.add("query1")
    await service.add("query2")

    result = await service.list()

    assert len(result) == 2


@pytest.mark.anyio
async def test_list_active_only(service, bundle):
    """List with active_only filters inactive queries."""
    id1 = await service.add("active query")
    id2 = await service.add("inactive query")
    await bundle.set_active(id2, False)

    result = await service.list(active_only=True)

    assert len(result) == 1
    assert result[0].id == id1


@pytest.mark.anyio
async def test_get_returns_query_by_id(service, bundle):
    """Get delegates to bundle.get_by_id()."""
    sq_id = await service.add("test")

    result = await service.get(sq_id)

    assert result is not None
    assert result.query == "test"


@pytest.mark.anyio
async def test_get_nonexistent_returns_none(service):
    """Get returns None for nonexistent query."""
    result = await service.get(999)

    assert result is None


# === toggle tests ===


@pytest.mark.anyio
async def test_toggle_switches_active_state(service, bundle):
    """Toggle switches is_active on existing query."""
    sq_id = await service.add("test", is_regex=False)

    await service.toggle(sq_id)

    stored = await bundle.get_by_id(sq_id)
    assert stored is not None
    assert stored.is_active is False  # Started as True, toggled to False


@pytest.mark.anyio
async def test_toggle_nonexistent_does_nothing(service):
    """Toggle is a no-op when query not found."""
    # Should not raise
    await service.toggle(999)


# === update tests ===


@pytest.mark.anyio
async def test_update_modifies_query(service, bundle):
    """Update delegates to bundle.update() preserving is_active."""
    sq_id = await service.add("old query")
    await bundle.set_active(sq_id, False)  # Make it inactive

    result = await service.update(
        sq_id,
        query="new query",
        interval_minutes=120,
        is_regex=True,
        is_fts=False,
        notify_on_collect=False,
        track_stats=False,
        exclude_patterns="",
        max_length=None,
    )

    assert result is True
    stored = await bundle.get_by_id(sq_id)
    assert stored is not None
    assert stored.query == "new query"
    assert stored.interval_minutes == 120
    assert stored.is_active is False  # Preserved


@pytest.mark.anyio
async def test_update_nonexistent_returns_false(service):
    """Update returns False when query not found."""
    result = await service.update(
        999,
        query="new query",
        interval_minutes=60,
    )

    assert result is False


# === run_once tests ===


@pytest.mark.anyio
async def test_run_once_regex_query_returns_zero(service, bundle):
    """Regex queries return 0 (not FTS-countable)."""
    sq_id = await service.add("test", is_regex=True)

    result = await service.run_once(sq_id)

    assert result == 0


@pytest.mark.anyio
async def test_run_once_fts_query_with_stats(service, bundle):
    """FTS query records stats when track_stats=True."""
    sq_id = await service.add("test", is_fts=True, track_stats=True)
    today = date_cls.today().isoformat()
    bundle.set_stats(sq_id, [SearchQueryDailyStat(day=today, count=5)])

    result = await service.run_once(sq_id)

    assert result == 5
    assert bundle.record_stat_calls == [(sq_id, 5)]


# === _fill_missing_days static method tests ===


def test_fill_missing_days_empty_stats():
    """Empty stats generates full days+1 empty entries."""
    result = SearchQueryService._fill_missing_days([], days=7)

    assert len(result) == 8  # 7 days + today
    for stat in result:
        assert stat.count == 0


def test_fill_missing_days_fills_gaps():
    """Existing days preserved, missing days filled with 0."""
    today = date_cls.today()
    day3 = (today - timedelta(days=3)).isoformat()
    day1 = (today - timedelta(days=1)).isoformat()

    existing = [
        SearchQueryDailyStat(day=day3, count=10),
        SearchQueryDailyStat(day=day1, count=5),
    ]

    result = SearchQueryService._fill_missing_days(existing, days=5)

    assert len(result) == 6  # 5 days + today
    # Check that existing values are preserved
    by_day = {s.day: s.count for s in result}
    assert by_day[day3] == 10
    assert by_day[day1] == 5
    # Check that missing days are filled
    day2 = (today - timedelta(days=2)).isoformat()
    assert by_day[day2] == 0


def test_fill_missing_days_none_stats():
    """None stats returns empty list."""
    result = SearchQueryService._fill_missing_days(None, days=7)

    assert result == []


# === get_with_stats tests ===


@pytest.mark.anyio
async def test_get_with_stats_excludes_regex_from_fts(service, bundle):
    """Regex queries are excluded from FTS stats batch."""
    await service.add("regex.*", is_regex=True, track_stats=True)
    await service.add("fts query", is_fts=True, track_stats=True)

    result = await service.get_with_stats(days=7)

    assert len(result) == 2
    # Both should have daily_stats (empty for regex, potentially populated for FTS)
    for item in result:
        assert "daily_stats" in item
        assert "total_30d" in item
