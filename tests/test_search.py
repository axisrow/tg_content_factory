from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from telethon.errors import FloodWaitError

from src.models import Message
from src.search.engine import SearchEngine
from src.services.embedding_service import EmbeddingService
from src.web.routes.search import _extract_length
from tests.helpers import AsyncIterMessages, FakeCliTelethonClient


@pytest.mark.anyio
async def test_search_local_empty(db):
    engine = SearchEngine(db)
    result = await engine.search_local("test")
    assert result.total == 0
    assert result.messages == []
    assert result.query == "test"


@pytest.mark.anyio
async def test_search_local_with_results(db):
    messages = [
        Message(
            channel_id=-100123,
            message_id=1,
            text="Important news about crypto",
            date=datetime.now(timezone.utc),
        ),
        Message(
            channel_id=-100123,
            message_id=2,
            text="Weather forecast today",
            date=datetime.now(timezone.utc),
        ),
    ]
    await db.insert_messages_batch(messages)

    engine = SearchEngine(db)
    result = await engine.search_local("crypto")
    assert result.total == 1
    assert "crypto" in (result.messages[0].text or "")


@pytest.mark.anyio
async def test_search_local_pagination(db):
    messages = [
        Message(
            channel_id=-100123,
            message_id=i,
            text=f"Test message number {i}",
            date=datetime.now(timezone.utc),
        )
        for i in range(20)
    ]
    await db.insert_messages_batch(messages)

    engine = SearchEngine(db)
    result = await engine.search_local("Test", limit=5, offset=0)
    assert len(result.messages) == 5
    assert result.total == 20


@pytest.mark.anyio
async def test_search_local_maps_channel_title(db):
    from src.models import Channel

    await db.add_channel(Channel(channel_id=-100123, title="Crypto News", username="crypto_news"))
    messages = [
        Message(
            channel_id=-100123,
            message_id=1,
            text="Bitcoin update",
            date=datetime.now(timezone.utc),
        ),
    ]
    await db.insert_messages_batch(messages)

    engine = SearchEngine(db)
    result = await engine.search_local("Bitcoin")
    assert result.total == 1
    assert result.messages[0].channel_title == "Crypto News"
    assert result.messages[0].channel_username == "crypto_news"


@pytest.mark.anyio
async def test_search_semantic_with_results(db, monkeypatch):
    await db.insert_messages_batch(
        [
            Message(
                channel_id=-100123,
                message_id=1,
                text="Bitcoin sentiment is rising",
                date=datetime.now(timezone.utc),
            ),
            Message(
                channel_id=-100123,
                message_id=2,
                text="Rainy weather all week",
                date=datetime.now(timezone.utc),
            ),
        ]
    )
    rows = await db.execute_fetchall("SELECT id, text FROM messages ORDER BY id")
    ids_by_text = {row["text"]: int(row["id"]) for row in rows}
    embeddings = [
        (ids_by_text["Bitcoin sentiment is rising"], [1.0, 0.0]),
        (ids_by_text["Rainy weather all week"], [0.0, 1.0]),
    ]
    await db.repos.messages.upsert_message_embeddings(embeddings)
    await db.repos.messages.upsert_message_embedding_json(embeddings)
    monkeypatch.setattr(
        EmbeddingService,
        "index_pending_messages",
        AsyncMock(return_value=0),
    )
    monkeypatch.setattr(
        EmbeddingService,
        "embed_query",
        AsyncMock(return_value=[1.0, 0.0]),
    )

    engine = SearchEngine(db)
    result = await engine.search_semantic("bitcoin")

    assert result.total >= 1
    assert result.messages[0].text == "Bitcoin sentiment is rising"


def _make_mock_api_message(channel_id=100123, msg_id=42, text="Test message about AI"):
    from telethon.tl.types import PeerChannel

    msg = MagicMock()
    msg.peer_id = PeerChannel(channel_id=channel_id)
    msg.id = msg_id
    msg.from_id = None
    msg.message = text
    msg.date = datetime.now(timezone.utc)
    msg.media = None
    return msg


def _make_search_response(messages, chats=None, users=None):
    response = MagicMock()
    response.messages = messages
    response.chats = chats or []
    response.users = users or []
    response.next_rate = None
    return response


def _make_resolved_message(
    chat_id=100123,
    chat_title="My Chat",
    chat_username="my_chat",
    msg_id=42,
    text="resolved message",
    sender_id=999,
    sender_first="John",
    sender_last="Doe",
):
    chat = MagicMock()
    chat.id = chat_id
    chat.title = chat_title
    chat.username = chat_username

    sender = MagicMock()
    sender.id = sender_id
    sender.first_name = sender_first
    sender.last_name = sender_last
    sender.title = ""

    msg = MagicMock()
    msg.id = msg_id
    msg.chat = chat
    msg.sender = sender
    msg.message = text
    msg.text = text
    msg.date = datetime.now(timezone.utc)
    msg.media = None
    return msg


async def _connect_search_account(
    harness,
    *,
    phone: str,
    session_string: str,
    client: FakeCliTelethonClient,
    is_premium: bool = False,
):
    harness.queue_cli_client(phone=phone, client=client)
    await harness.add_account(
        phone=phone,
        session_string=session_string,
        is_primary=True,
        is_premium=is_premium,
    )
    await harness.initialize_connected_accounts()


@pytest.mark.anyio
async def test_search_telegram_returns_results(db, real_pool_harness_factory):
    mock_msg = _make_mock_api_message()
    mock_chat = MagicMock()
    mock_chat.id = 100123
    mock_chat.title = "Test Channel"
    mock_chat.username = "test_channel"

    response = _make_search_response([mock_msg], chats=[mock_chat])
    harness = real_pool_harness_factory()
    await _connect_search_account(
        harness,
        phone="+1234567890",
        session_string="premium-session",
        is_premium=True,
        client=FakeCliTelethonClient(
            me=SimpleNamespace(premium=True),
            invoke_side_effect=lambda request: response,
        ),
    )

    engine = SearchEngine(db, pool=harness.pool)
    result = await engine.search_telegram("AI", limit=10)

    assert result.total == 1
    assert result.query == "AI"
    assert result.messages[0].message_id == 42
    assert result.messages[0].text == "Test message about AI"
    assert result.messages[0].channel_title == "Test Channel"


@pytest.mark.anyio
async def test_search_telegram_ignores_quota_probe_failure_when_search_succeeds(
    db,
    real_pool_harness_factory,
):
    mock_msg = _make_mock_api_message()
    mock_chat = MagicMock()
    mock_chat.id = 100123
    mock_chat.title = "Test Channel"
    mock_chat.username = "test_channel"

    response = _make_search_response([mock_msg], chats=[mock_chat])

    def _invoke(request):
        if request.__class__.__name__ == "CheckSearchPostsFloodRequest":
            raise RuntimeError("quota unavailable")
        return response

    harness = real_pool_harness_factory()
    await _connect_search_account(
        harness,
        phone="+1234567890",
        session_string="premium-session",
        is_premium=True,
        client=FakeCliTelethonClient(
            me=SimpleNamespace(premium=True),
            invoke_side_effect=_invoke,
        ),
    )

    engine = SearchEngine(db, pool=harness.pool)
    result = await engine.search_telegram("AI", limit=10)

    assert result.total == 1
    assert result.error is None
    assert result.messages[0].message_id == 42


@pytest.mark.anyio
async def test_search_telegram_caches_to_db(db, real_pool_harness_factory):
    mock_msg = _make_mock_api_message(channel_id=100456, msg_id=7, text="cached search result")
    mock_chat = MagicMock()
    mock_chat.id = 100456
    mock_chat.title = "Cache Channel"
    mock_chat.username = None

    response = _make_search_response([mock_msg], chats=[mock_chat])
    harness = real_pool_harness_factory()
    await _connect_search_account(
        harness,
        phone="+1234567890",
        session_string="premium-session",
        is_premium=True,
        client=FakeCliTelethonClient(
            me=SimpleNamespace(premium=True),
            invoke_side_effect=lambda request: response,
        ),
    )

    engine = SearchEngine(db, pool=harness.pool)
    await engine.search_telegram("cached", limit=5)

    messages, total = await db.search_messages(query="cached", limit=10, offset=0)
    assert total == 1
    assert messages[0].text == "cached search result"


@pytest.mark.anyio
async def test_search_telegram_no_pool(db):
    engine = SearchEngine(db, pool=None)
    result = await engine.search_telegram("anything")

    assert result.total == 0
    assert result.messages == []
    assert result.query == "anything"
    assert result.error == "Нет подключённых Telegram-аккаунтов."


@pytest.mark.anyio
async def test_search_telegram_no_premium(db, real_pool_harness_factory):
    harness = real_pool_harness_factory()
    engine = SearchEngine(db, pool=harness.pool)
    result = await engine.search_telegram("query")

    assert result.total == 0
    assert result.messages == []
    assert "Premium" in result.error


@pytest.mark.anyio
async def test_search_telegram_flood_wait_does_not_mark_generic_account_flood(
    db,
    real_pool_harness_factory,
):
    def _raise_flood(_request):
        err = FloodWaitError(request=None, capture=0)
        err.seconds = 45
        raise err

    harness = real_pool_harness_factory()
    phone = "+1234567890"
    await _connect_search_account(
        harness,
        phone=phone,
        session_string="premium-session",
        is_premium=True,
        client=FakeCliTelethonClient(
            me=SimpleNamespace(premium=True),
            invoke_side_effect=_raise_flood,
        ),
    )

    engine = SearchEngine(db, pool=harness.pool)
    result = await engine.search_telegram("query")

    assert result.total == 0
    assert result.flood_wait is not None
    accounts = await db.get_accounts(active_only=True)
    flooded = next(account for account in accounts if account.phone == phone)
    assert flooded.flood_wait_until is None
    unavailable_reason = await harness.pool.get_premium_unavailability_reason()
    assert "Flood Wait" in unavailable_reason


@pytest.mark.anyio
async def test_check_search_quota_flood_wait_returns_none_and_marks_premium_flood(
    db,
    real_pool_harness_factory,
):
    def _raise_flood(_request):
        err = FloodWaitError(request=None, capture=0)
        err.seconds = 21
        raise err

    harness = real_pool_harness_factory()
    phone = "+1234567890"
    await _connect_search_account(
        harness,
        phone=phone,
        session_string="premium-session",
        is_premium=True,
        client=FakeCliTelethonClient(
            me=SimpleNamespace(premium=True),
            invoke_side_effect=_raise_flood,
        ),
    )

    engine = SearchEngine(db, pool=harness.pool)
    quota = await engine.check_search_quota("query")

    assert quota is None
    accounts = await db.get_accounts(active_only=True)
    flooded = next(account for account in accounts if account.phone == phone)
    assert flooded.flood_wait_until is None
    unavailable_reason = await harness.pool.get_premium_unavailability_reason()
    assert "Flood Wait" in unavailable_reason


@pytest.mark.anyio
async def test_search_my_chats_runtime_error_returns_search_result(
    db,
    real_pool_harness_factory,
):
    async def _broken_iter(*args, **kwargs):
        raise RuntimeError("telegram api failure")
        yield

    harness = real_pool_harness_factory()
    await _connect_search_account(
        harness,
        phone="+1234567890",
        session_string="session-1",
        client=FakeCliTelethonClient(
            dialogs=[],
            iter_messages_factory=_broken_iter,
        ),
    )

    engine = SearchEngine(db, pool=harness.pool)
    result = await engine.search_my_chats("query")

    assert result.total == 0
    assert "telegram api failure" in result.error


@pytest.mark.anyio
async def test_search_my_chats_returns_results(db, real_pool_harness_factory):
    mock_msg = _make_resolved_message()
    harness = real_pool_harness_factory()
    await _connect_search_account(
        harness,
        phone="+1234567890",
        session_string="session-1",
        client=FakeCliTelethonClient(
            dialogs=[],
            iter_messages_factory=lambda *args, **kwargs: AsyncIterMessages([mock_msg]),
        ),
    )

    engine = SearchEngine(db, pool=harness.pool)
    result = await engine.search_my_chats("resolved", limit=10)

    assert result.total == 1
    assert result.messages[0].message_id == 42
    assert result.messages[0].text == "resolved message"
    assert result.messages[0].channel_title == "My Chat"


@pytest.mark.anyio
async def test_search_my_chats_skips_warm_dialog_cache_when_already_fetched(
    db,
    real_pool_harness_factory,
):
    mock_msg = _make_resolved_message()
    harness = real_pool_harness_factory()
    phone = "+1234567890"
    client = FakeCliTelethonClient(
        dialogs=[],
        iter_messages_factory=lambda *args, **kwargs: AsyncIterMessages([mock_msg]),
    )
    await _connect_search_account(
        harness,
        phone=phone,
        session_string="session-1",
        client=client,
    )
    harness.pool.mark_dialogs_fetched(phone)

    engine = SearchEngine(db, pool=harness.pool)
    result = await engine.search_my_chats("resolved", limit=10)

    assert result.total == 1
    client.get_dialogs.assert_not_awaited()


@pytest.mark.anyio
async def test_search_my_chats_reports_flood_wait_and_marks_account(
    db,
    real_pool_harness_factory,
):
    async def _flood_iter(*args, **kwargs):
        err = FloodWaitError(request=None, capture=0)
        err.seconds = 33
        raise err
        yield

    harness = real_pool_harness_factory()
    phone = "+1234567890"
    await _connect_search_account(
        harness,
        phone=phone,
        session_string="session-1",
        client=FakeCliTelethonClient(
            dialogs=[],
            iter_messages_factory=_flood_iter,
        ),
    )

    engine = SearchEngine(db, pool=harness.pool)
    result = await engine.search_my_chats("query")

    assert result.total == 0
    assert result.flood_wait is not None
    assert result.flood_wait.wait_seconds == 33
    assert "Flood wait 33s" in (result.error or "")
    accounts = await db.get_accounts(active_only=True)
    flooded = next(account for account in accounts if account.phone == phone)
    assert flooded.flood_wait_until is not None


@pytest.mark.anyio
async def test_search_my_chats_no_pool(db):
    engine = SearchEngine(db, pool=None)
    result = await engine.search_my_chats("anything")

    assert result.total == 0
    assert result.error == "Нет подключённых Telegram-аккаунтов."


@pytest.mark.anyio
async def test_search_my_chats_no_clients(db, real_pool_harness_factory):
    harness = real_pool_harness_factory()
    engine = SearchEngine(db, pool=harness.pool)
    result = await engine.search_my_chats("query")

    assert result.total == 0
    assert result.error == "Нет доступных Telegram-аккаунтов. Проверьте подключение."


@pytest.mark.anyio
async def test_search_in_channel_returns_results(db, real_pool_harness_factory):
    mock_msg = _make_resolved_message(chat_id=200456, chat_title="Target Channel")
    harness = real_pool_harness_factory()
    await _connect_search_account(
        harness,
        phone="+1234567890",
        session_string="session-1",
        client=FakeCliTelethonClient(
            dialogs=[],
            entity_resolver=lambda _peer: MagicMock(),
            iter_messages_factory=lambda *args, **kwargs: AsyncIterMessages([mock_msg]),
        ),
    )

    engine = SearchEngine(db, pool=harness.pool)
    result = await engine.search_in_channel(200456, "resolved", limit=10)

    assert result.total == 1
    assert result.messages[0].channel_id == 200456
    assert result.messages[0].channel_title == "Target Channel"


@pytest.mark.anyio
async def test_search_in_channel_all_channels(db, real_pool_harness_factory):
    mock_msg = _make_resolved_message(chat_id=300789, chat_title="All Chats Result")
    harness = real_pool_harness_factory()
    await _connect_search_account(
        harness,
        phone="+1234567890",
        session_string="session-1",
        client=FakeCliTelethonClient(
            dialogs=[],
            iter_messages_factory=lambda *args, **kwargs: AsyncIterMessages([mock_msg]),
        ),
    )

    engine = SearchEngine(db, pool=harness.pool)
    result = await engine.search_in_channel(None, "query", limit=10)

    assert result.total == 1
    assert result.error is None
    assert result.messages[0].channel_title == "All Chats Result"


@pytest.mark.anyio
async def test_search_in_channel_entity_not_found(db, real_pool_harness_factory):
    harness = real_pool_harness_factory()
    await _connect_search_account(
        harness,
        phone="+1234567890",
        session_string="session-1",
        client=FakeCliTelethonClient(
            dialogs=[],
            entity_resolver=lambda _peer: ValueError("entity not found"),
        ),
    )

    engine = SearchEngine(db, pool=harness.pool)
    result = await engine.search_in_channel(999999, "query")

    assert result.total == 0
    assert "Не удалось найти канал" in result.error


@pytest.mark.anyio
async def test_search_in_channel_runtime_error_returns_search_result(
    db,
    real_pool_harness_factory,
):
    async def _broken_iter(*args, **kwargs):
        raise RuntimeError("channel search failure")
        yield

    harness = real_pool_harness_factory()
    await _connect_search_account(
        harness,
        phone="+1234567890",
        session_string="session-1",
        client=FakeCliTelethonClient(
            dialogs=[],
            entity_resolver=lambda _peer: MagicMock(),
            iter_messages_factory=_broken_iter,
        ),
    )

    engine = SearchEngine(db, pool=harness.pool)
    result = await engine.search_in_channel(999999, "query")

    assert result.total == 0
    assert "channel search failure" in result.error


@pytest.mark.parametrize(
    "query, expected",
    [
        ("crypto len<400", ("crypto", None, 400)),
        ("crypto len>100 len<400", ("crypto", 100, 400)),
        ("len>100", ("", 100, None)),
        ("crypto", ("crypto", None, None)),
        ("", ("", None, None)),
        ("lenovo", ("lenovo", None, None)),
    ],
    ids=[
        "max_only",
        "range",
        "min_only",
        "no_len",
        "empty",
        "lenovo_no_match",
    ],
)
def test_extract_length(query, expected):
    assert _extract_length(query) == expected
