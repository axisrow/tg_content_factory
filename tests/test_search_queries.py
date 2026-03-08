from __future__ import annotations

import pytest

from src.database.bundles import SearchQueryBundle
from src.models import SearchQuery
from src.services.search_query_service import SearchQueryService


@pytest.fixture
def bundle(db):
    return SearchQueryBundle.from_database(db)


@pytest.fixture
def svc(bundle):
    return SearchQueryService(bundle)


@pytest.mark.asyncio
async def test_add_and_list(svc):
    sq_id = await svc.add("test", "аренда квартир", 30)
    assert sq_id > 0

    items = await svc.list()
    assert len(items) == 1
    assert items[0].name == "test"
    assert items[0].interval_minutes == 30


@pytest.mark.asyncio
async def test_toggle(svc):
    sq_id = await svc.add("q1", "query", 60)
    items = await svc.list()
    assert items[0].is_active is True

    await svc.toggle(sq_id)
    items = await svc.list()
    assert items[0].is_active is False


@pytest.mark.asyncio
async def test_delete(svc):
    sq_id = await svc.add("q1", "query", 60)
    await svc.delete(sq_id)
    items = await svc.list()
    assert len(items) == 0


@pytest.mark.asyncio
async def test_record_stat_and_daily_stats(bundle, svc):
    sq_id = await svc.add("q1", "query", 60)
    await bundle.record_stat(sq_id, 42)
    stats = await svc.get_daily_stats(sq_id, days=30)
    assert len(stats) == 1
    assert stats[0].count == 42


@pytest.mark.asyncio
async def test_record_stat_deduplication(bundle, svc):
    sq_id = await svc.add("q1", "query", 60)
    await bundle.record_stat(sq_id, 10)
    await bundle.record_stat(sq_id, 20)
    stats = await svc.get_daily_stats(sq_id, days=30)
    assert len(stats) == 1
    assert stats[0].count == 20  # overwritten, not accumulated


@pytest.mark.asyncio
async def test_get_with_stats_no_n_plus_one(svc, bundle):
    for i in range(5):
        sq_id = await svc.add(f"q{i}", f"query{i}", 60)
        await bundle.record_stat(sq_id, i * 10)

    items = await svc.get_with_stats()
    assert len(items) == 5
    assert items[2]["total_30d"] == 20
    assert items[0]["last_run"] is not None


@pytest.mark.asyncio
async def test_get_last_recorded_at_all(bundle):
    repo = bundle.search_queries
    sq = SearchQuery(name="q1", query="test")
    sq_id = await repo.add(sq)
    await repo.record_stat(sq_id, 5)

    result = await repo.get_last_recorded_at_all()
    assert sq_id in result
    assert result[sq_id] is not None


@pytest.mark.asyncio
async def test_interval_minutes_validation():
    with pytest.raises(Exception):
        SearchQuery(name="bad", query="q", interval_minutes=0)
