from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from src.database.bundles import SearchQueryBundle
from src.models import Channel, Message, SearchQuery
from src.services.search_query_service import SearchQueryService


@pytest.fixture
def bundle(db):
    return SearchQueryBundle.from_database(db)


@pytest.fixture
def svc(bundle):
    return SearchQueryService(bundle)


async def _insert_messages(db, texts, channel_id=100, base_date=None):
    """Helper: add a channel and insert messages with given texts."""
    if base_date is None:
        base_date = datetime.now()
    ch = Channel(channel_id=channel_id, title="test")
    await db.repos.channels.add_channel(ch)
    for i, text in enumerate(texts):
        msg = Message(
            channel_id=channel_id,
            message_id=i + 1,
            text=text,
            date=base_date + timedelta(seconds=i),
        )
        await db.repos.messages.insert_message(msg)


@pytest.mark.asyncio
async def test_add_and_list(svc):
    sq_id = await svc.add("аренда квартир", 30)
    assert sq_id > 0

    items = await svc.list()
    assert len(items) == 1
    assert items[0].query == "аренда квартир"
    assert items[0].interval_minutes == 30


@pytest.mark.asyncio
async def test_toggle(svc):
    sq_id = await svc.add("query", 60)
    items = await svc.list()
    assert items[0].is_active is True

    await svc.toggle(sq_id)
    items = await svc.list()
    assert items[0].is_active is False


@pytest.mark.asyncio
async def test_delete(svc):
    sq_id = await svc.add("query", 60)
    await svc.delete(sq_id)
    items = await svc.list()
    assert len(items) == 0


@pytest.mark.asyncio
async def test_record_stat_and_daily_stats(bundle, svc):
    sq_id = await svc.add("query", 60)
    await bundle.record_stat(sq_id, 42)
    stats = await svc.get_daily_stats(sq_id, days=30)
    assert len(stats) == 1
    assert stats[0].count == 42


@pytest.mark.asyncio
async def test_record_stat_deduplication(bundle, svc):
    sq_id = await svc.add("query", 60)
    await bundle.record_stat(sq_id, 10)
    await bundle.record_stat(sq_id, 20)
    stats = await svc.get_daily_stats(sq_id, days=30)
    assert len(stats) == 1
    assert stats[0].count == 20  # overwritten, not accumulated


@pytest.mark.asyncio
async def test_get_with_stats_from_fts(svc, bundle, db):
    await _insert_messages(db, ["hello world", "hello again", "goodbye"])

    await svc.add("hello", 60)
    # Also add a non-tracking query to ensure it gets empty stats
    await svc.add("goodbye", 60, track_stats=False)

    items = await svc.get_with_stats()
    assert len(items) == 2
    # "hello" matches 2 messages
    hello_item = next(i for i in items if i["query"].query == "hello")
    assert hello_item["total_30d"] == 2
    assert len(hello_item["daily_stats"]) >= 1
    # non-tracking query should have empty stats
    goodbye_item = next(i for i in items if i["query"].query == "goodbye")
    assert goodbye_item["total_30d"] == 0
    assert goodbye_item["daily_stats"] == []


@pytest.mark.asyncio
async def test_get_last_recorded_at_all(bundle):
    repo = bundle.search_queries
    sq = SearchQuery(query="test")
    sq_id = await repo.add(sq)
    await repo.record_stat(sq_id, 5)

    result = await repo.get_last_recorded_at_all()
    assert sq_id in result
    assert result[sq_id] is not None


@pytest.mark.asyncio
async def test_fts_daily_stats_groups_by_day(bundle, db):
    now = datetime.now()
    yesterday = now - timedelta(days=1)
    ch = Channel(channel_id=200, title="ch")
    await db.repos.channels.add_channel(ch)
    # 2 messages today, 1 yesterday
    for i, (text, dt) in enumerate([
        ("alpha one", now),
        ("alpha two", now),
        ("alpha old", yesterday),
    ]):
        msg = Message(channel_id=200, message_id=i + 1, text=text, date=dt)
        await db.repos.messages.insert_message(msg)

    stats = await bundle.get_fts_daily_stats("alpha", days=30)
    assert len(stats) == 2
    total = sum(s.count for s in stats)
    assert total == 3


@pytest.mark.asyncio
async def test_run_once_counts_today_only(svc, db):
    now = datetime.now()
    old = now - timedelta(days=5)
    await _insert_messages(db, ["target msg"], channel_id=300, base_date=now)
    ch2 = Channel(channel_id=301, title="ch2")
    await db.repos.channels.add_channel(ch2)
    msg = Message(channel_id=301, message_id=1, text="target old", date=old)
    await db.repos.messages.insert_message(msg)

    sq_id = await svc.add("target", 60)
    count = await svc.run_once(sq_id)
    # run_once uses days=1, so only today's message counts
    assert count == 1


@pytest.mark.asyncio
async def test_update_query(svc):
    sq_id = await svc.add("original", 60)
    result = await svc.update(
        sq_id, "updated", 30,
        is_regex=True, notify_on_collect=True, track_stats=False,
    )
    assert result is True

    sq = await svc.get(sq_id)
    assert sq.query == "updated"
    assert sq.interval_minutes == 30
    assert sq.is_regex is True
    assert sq.notify_on_collect is True
    assert sq.track_stats is False


@pytest.mark.asyncio
async def test_update_nonexistent(svc):
    result = await svc.update(999, "q", 60)
    assert result is False


@pytest.mark.asyncio
async def test_fts_boolean_query(bundle, db):
    """FTS5 boolean query with OR/AND should match correctly."""
    await _insert_messages(db, [
        "аренда мотобайк на месяц",
        "продажа скутер недорого",
        "снять байк в Пхукете",
        "ремонт велосипеда",
    ])
    sq = SearchQuery(query="(байк OR мотобайк OR скутер) AND (аренда OR снять)", is_fts=True)
    sq_id = await bundle.add(sq)
    sq = await bundle.get_by_id(sq_id)
    count = await bundle.count_fts_matches_for_query(sq)
    # "аренда мотобайк" and "снять байк" match; "продажа скутер" has no rent word
    assert count == 2


@pytest.mark.asyncio
async def test_exclude_patterns(bundle, db):
    """Exclude patterns should filter out matching messages."""
    await _insert_messages(db, [
        "аренда байка дешево",
        "аренда байка СПАМ реклама",
        "аренда байка нормальное объявление",
    ])
    sq = SearchQuery(query="аренда", exclude_patterns="СПАМ\nреклама")
    sq_id = await bundle.add(sq)
    sq = await bundle.get_by_id(sq_id)
    count = await bundle.count_fts_matches_for_query(sq)
    # "СПАМ реклама" message excluded (contains both patterns, but one is enough)
    assert count == 2


@pytest.mark.asyncio
async def test_max_length_filter(bundle, db):
    """max_length should filter out messages with text >= max_length."""
    await _insert_messages(db, [
        "short",
        "a" * 500,
    ])
    sq = SearchQuery(query="short OR " + "a" * 10, is_fts=True, max_length=100)
    sq_id = await bundle.add(sq)
    sq = await bundle.get_by_id(sq_id)
    stats = await bundle.get_fts_daily_stats_for_query(sq, days=30)
    total = sum(s.count for s in stats)
    assert total == 1  # only "short" passes length filter


@pytest.mark.asyncio
async def test_fts_collector_matching():
    """Test the _fts_query_matches static method."""
    from src.telegram.collector import Collector

    assert Collector._fts_query_matches(
        "(байк OR мотобайк) AND (аренда OR снять)",
        "хочу снять байк на неделю",
    )
    assert not Collector._fts_query_matches(
        "(байк OR мотобайк) AND (аренда OR снять)",
        "продажа байка",
    )
    assert Collector._fts_query_matches("аренда квартир", "аренда квартир в центре")
    assert not Collector._fts_query_matches("аренда квартир", "продажа домов")


@pytest.mark.asyncio
async def test_exclude_patterns_list_property():
    sq = SearchQuery(query="test", exclude_patterns="foo\nbar\n  baz  \n\n")
    assert sq.exclude_patterns_list == ["foo", "bar", "baz"]

    sq2 = SearchQuery(query="test", exclude_patterns="")
    assert sq2.exclude_patterns_list == []


@pytest.mark.asyncio
async def test_interval_minutes_validation():
    with pytest.raises(ValueError):
        SearchQuery(query="q", interval_minutes=0)
