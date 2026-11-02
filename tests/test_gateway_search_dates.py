"""Gateway search date plumbing (#1750): wire payload and server-side routing."""

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from tg_agent.gateway.protocol import Search
from tg_agent.gateway.reads import _read_search
from tg_agent.gateway.search import GatewaySearch


def test_search_payload_accepts_date_bounds():
    op = Search(
        kind="search",
        account_id=uuid4(),
        mode="telegram",
        query="key",
        limit=50,
        min_date=datetime(2026, 10, 19, tzinfo=timezone.utc),
        max_date=datetime(2026, 11, 3, tzinfo=timezone.utc),
    )
    assert op.min_date == datetime(2026, 10, 19, tzinfo=timezone.utc)
    assert op.max_date == datetime(2026, 11, 3, tzinfo=timezone.utc)
    assert Search(kind="search", account_id=uuid4(), mode="telegram").min_date is None


@pytest.mark.anyio
async def test_gateway_search_carries_dates_in_payload():
    captured: dict = {}

    async def fake_request(operation, request_id):
        captured["operation"] = operation
        return {"total": 0, "query": "key", "messages": [], "channels": []}

    pool = SimpleNamespace(
        select=lambda phone=None: {"id": str(uuid4()), "phone": "+100"},
        clients={},
        request=fake_request,
    )
    persistence = SimpleNamespace(cache_messages_and_channels=None, cache_search_results=None)
    search = GatewaySearch(pool, persistence)
    # _search pops "channels" and validates a SearchResult, then routes telegram-mode
    # caching through cache_search_results — fake both caches out.
    async def fake_cache(channels, messages, phone, query):
        return messages

    persistence.cache_search_results = fake_cache

    min_d = datetime(2026, 10, 19, tzinfo=timezone.utc)
    max_d = datetime(2026, 11, 3, tzinfo=timezone.utc)
    await search.search_telegram("key", 50, min_date=min_d, max_date=max_d)

    op = captured["operation"]
    assert op.min_date == min_d
    assert op.max_date == max_d


@pytest.mark.anyio
async def test_read_search_passes_dates_to_telegram_search(monkeypatch):
    captured: dict = {}

    class FakeTelegramSearch:
        def __init__(self, pool, persistence):
            pass

        async def search_telegram(self, query, limit=50, min_date=None, max_date=None):
            captured.update(query=query, limit=limit, min_date=min_date, max_date=max_date)
            return SimpleNamespace(
                model_dump=lambda mode="python": {"messages": [], "total": 0, "query": query}
            )

    monkeypatch.setattr("tg_agent.gateway.reads.TelegramSearch", FakeTelegramSearch)
    op = Search(
        kind="search",
        account_id=uuid4(),
        mode="telegram",
        query="key",
        limit=30,
        min_date=datetime(2026, 10, 19, tzinfo=timezone.utc),
        max_date=datetime(2026, 11, 3, tzinfo=timezone.utc),
    )
    account = str(op.account_id)
    access = SimpleNamespace(
        _pool=SimpleNamespace(clients={account: {}}),
        auth=None,
    )

    async def fake_connection(project, acct):
        return object()

    access.connection = fake_connection
    result = await _read_search(access, account, object(), op)
    assert captured == {
        "query": "key",
        "limit": 30,
        "min_date": datetime(2026, 10, 19, tzinfo=timezone.utc),
        "max_date": datetime(2026, 11, 3, tzinfo=timezone.utc),
    }
    assert result["total"] == 0


@pytest.mark.anyio
async def test_read_search_rejects_dates_on_non_telegram_modes():
    # Review guard (#1750): the CLI rejects windowless live modes loudly; the
    # gateway must not silently swallow the wire fields either (#1350 class).
    op = Search(
        kind="search",
        account_id=uuid4(),
        mode="my_chats",
        query="key",
        min_date=datetime(2026, 10, 19, tzinfo=timezone.utc),
    )
    access = SimpleNamespace(_pool=SimpleNamespace(clients={"acc": {}}), auth=None)

    async def fake_connection(project, acct):
        return object()

    access.connection = fake_connection
    with pytest.raises(ValueError, match="takes no date bounds"):
        await _read_search(access, "acc", object(), op)
