from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from telethon.errors import FloodWaitError

from tests.helpers import AsyncIterMessages, FakeCliTelethonClient
from tg_agent.models import Message
from tg_agent.search import telegram_search as telegram_search_module
from tg_agent.search.engine import SearchEngine
from tg_agent.services.embedding_service import EmbeddingService


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
    # total is a lower bound since #766 (offset + page size), has_more marks
    # the remaining pages instead of an exact COUNT.
    assert result.total == 5
    assert result.has_more is True

    last_page = await engine.search_local("Test", limit=5, offset=15)
    assert len(last_page.messages) == 5
    assert last_page.has_more is False
    assert last_page.total == 20


@pytest.mark.anyio
async def test_search_local_maps_channel_title(db):
    from tg_agent.models import Channel

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
    msg.views = 123
    msg.forwards = 4
    msg.replies = SimpleNamespace(replies=5)
    msg.reactions = None
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
    msg.views = 123
    msg.forwards = 4
    msg.replies = SimpleNamespace(replies=5)
    msg.reactions = None
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
    assert result.messages[0].id is not None
    assert result.messages[0].views == 123
    assert result.messages[0].forwards == 4
    assert result.messages[0].reply_count == 5


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
async def test_search_telegram_quota_timeout_returns_diagnostic_error_and_releases_client(
    db,
    real_pool_harness_factory,
    monkeypatch,
    caplog,
):
    monkeypatch.setattr(telegram_search_module, "PREMIUM_SEARCH_RPC_TIMEOUT_SEC", 0.01)
    caplog.set_level(logging.WARNING, logger="tg_agent.search.telegram_search")

    request_names: list[str] = []

    async def _invoke(request):
        request_names.append(request.__class__.__name__)
        if request.__class__.__name__ == "CheckSearchPostsFloodRequest":
            await asyncio.sleep(0.1)
        return _make_search_response([])

    harness = real_pool_harness_factory()
    phone = "+1234567890"
    client = FakeCliTelethonClient(me=SimpleNamespace(premium=True))
    client.invoke = AsyncMock(side_effect=_invoke)
    await _connect_search_account(
        harness,
        phone=phone,
        session_string="premium-session",
        is_premium=True,
        client=client,
    )

    engine = SearchEngine(db, pool=harness.pool)
    result = await engine.search_telegram("slow quota", limit=10)

    assert result.total == 0
    assert result.error is not None
    assert "stage=quota" in result.error
    assert request_names == ["CheckSearchPostsFloodRequest"]
    assert "premium_search timeout stage=quota" in caplog.text
    assert "slow quota" not in caplog.text
    assert phone not in caplog.text
    assert "+12...7890" in caplog.text

    leased = await harness.pool.get_premium_client()
    assert leased is not None
    _, leased_phone = leased
    await harness.pool.release_client(leased_phone)


@pytest.mark.anyio
async def test_search_telegram_rpc_timeout_returns_diagnostic_error_and_releases_client(
    db,
    real_pool_harness_factory,
    monkeypatch,
    caplog,
):
    monkeypatch.setattr(telegram_search_module, "PREMIUM_SEARCH_RPC_TIMEOUT_SEC", 0.01)
    caplog.set_level(logging.WARNING, logger="tg_agent.search.telegram_search")

    async def _invoke(request):
        if request.__class__.__name__ == "CheckSearchPostsFloodRequest":
            return SimpleNamespace(total_daily=100, remains=10, wait_till=None, query_is_free=False)
        await asyncio.sleep(0.1)
        return _make_search_response([])

    harness = real_pool_harness_factory()
    phone = "+1234567890"
    client = FakeCliTelethonClient(me=SimpleNamespace(premium=True))
    client.invoke = AsyncMock(side_effect=_invoke)
    await _connect_search_account(
        harness,
        phone=phone,
        session_string="premium-session",
        is_premium=True,
        client=client,
    )

    engine = SearchEngine(db, pool=harness.pool)
    result = await engine.search_telegram("slow rpc", limit=10)

    assert result.total == 0
    assert result.error is not None
    assert "stage=telegram_rpc" in result.error
    assert "premium_search timeout stage=telegram_rpc" in caplog.text
    assert "slow rpc" not in caplog.text
    assert phone not in caplog.text
    assert "+12...7890" in caplog.text

    leased = await harness.pool.get_premium_client()
    assert leased is not None
    _, leased_phone = leased
    await harness.pool.release_client(leased_phone)


@pytest.mark.anyio
async def test_search_telegram_success_logs_safe_diagnostics(
    db,
    real_pool_harness_factory,
    caplog,
):
    mock_msg = _make_mock_api_message()
    mock_chat = MagicMock()
    mock_chat.id = 100123
    mock_chat.title = "Test Channel"
    mock_chat.username = "test_channel"

    response = _make_search_response([mock_msg], chats=[mock_chat])
    harness = real_pool_harness_factory()
    phone = "+1234567890"
    await _connect_search_account(
        harness,
        phone=phone,
        session_string="premium-session",
        is_premium=True,
        client=FakeCliTelethonClient(
            me=SimpleNamespace(premium=True),
            invoke_side_effect=lambda request: response,
        ),
    )

    caplog.set_level(logging.INFO, logger="tg_agent.search.telegram_search")
    engine = SearchEngine(db, pool=harness.pool)
    result = await engine.search_telegram("AI", limit=10)

    assert result.total == 1
    assert "premium_search start mode=telegram" in caplog.text
    assert "premium_search success" in caplog.text
    assert "query_hash=" in caplog.text
    assert "AI" not in caplog.text
    assert phone not in caplog.text
    assert "+12...7890" in caplog.text


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
async def test_search_telegram_preserves_sender_identity(db, real_pool_harness_factory):
    from telethon.tl.types import PeerUser

    mock_msg = _make_mock_api_message(channel_id=100457, msg_id=8, text="identity search result")
    mock_msg.from_id = PeerUser(user_id=999)
    mock_chat = MagicMock()
    mock_chat.id = 100457
    mock_chat.title = "Identity Channel"
    mock_chat.username = None
    mock_user = SimpleNamespace(id=999, first_name="John", last_name="Doe", username="@jdoe")

    response = _make_search_response([mock_msg], chats=[mock_chat], users=[mock_user])
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
    result = await engine.search_telegram("identity", limit=5)

    assert result.total == 1
    assert result.messages[0].sender_id == 999
    assert result.messages[0].sender_name == "John Doe"
    assert result.messages[0].sender_first_name == "John"
    assert result.messages[0].sender_last_name == "Doe"
    assert result.messages[0].sender_username == "jdoe"
    messages, total = await db.search_messages(query="identity", limit=10, offset=0)
    assert total == 1
    assert messages[0].sender_username == "jdoe"


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


@pytest.mark.anyio
async def test_search_semantic_unavailable_returns_friendly_error(db):
    engine = SearchEngine(db)
    engine._search_bundle = SimpleNamespace(vec_available=False, numpy_available=False)
    result = await engine.search_semantic("paris")
    assert result.messages == []
    assert result.total == 0
    assert result.error and "Семантический поиск недоступен" in result.error


@pytest.mark.anyio
async def test_search_hybrid_unavailable_returns_friendly_error(db):
    engine = SearchEngine(db)
    engine._search_bundle = SimpleNamespace(vec_available=False, numpy_available=False)
    result = await engine.search_hybrid("paris")
    assert result.messages == []
    assert result.total == 0
    assert result.error and "Семантический поиск недоступен" in result.error


def _set_semantic_backend(engine, *, vec=False, numpy=False):
    # SearchBundle is a frozen dataclass — rebuild it with the desired availability flags
    # instead of mutating in place.
    engine._search_bundle = replace(engine._search_bundle, vec_available=vec, numpy_available=numpy)


@pytest.mark.anyio
async def test_has_semantic_index_false_when_backend_unavailable(db):
    # No semantic backend at all → never index-usable, even with embeddings present.
    await db.insert_messages_batch(
        [Message(channel_id=-100123, message_id=1, text="hi", date=datetime.now(timezone.utc))]
    )
    await db.repos.messages.upsert_message_embeddings([(1, [0.1, 0.2, 0.3])])
    engine = SearchEngine(db)
    _set_semantic_backend(engine, vec=False, numpy=False)
    assert await engine.has_semantic_index() is False


@pytest.mark.anyio
async def test_has_semantic_index_false_when_no_embeddings_indexed(db):
    # Backend importable (numpy) but nothing indexed → must report False so retrieval
    # falls back to local search and never embeds the query via an external provider.
    engine = SearchEngine(db)
    _set_semantic_backend(engine, numpy=True)
    assert await db.repos.messages.count_embeddings() == 0
    assert await engine.has_semantic_index() is False


@pytest.mark.anyio
async def test_has_semantic_index_true_after_embeddings_indexed(db):
    await db.insert_messages_batch(
        [Message(channel_id=-100123, message_id=1, text="hi", date=datetime.now(timezone.utc))]
    )
    await db.repos.messages.upsert_message_embeddings([(1, [0.1, 0.2, 0.3])])
    engine = SearchEngine(db)
    _set_semantic_backend(engine, numpy=True)
    assert await db.repos.messages.count_embeddings() == 1
    assert await engine.has_semantic_index() is True


# ── Embeddings API key at rest (issue #1597) ─────────────────────────────


@pytest.mark.anyio
async def test_runtime_config_decrypts_stored_api_key(db):
    """The embeddings key is stored enc:v2; the service must read it back as plaintext."""
    from tg_agent.security import SessionCipher
    from tg_agent.services.embedding_service import EMBEDDINGS_API_KEY_SETTING

    cipher = SessionCipher("test-session-encryption-key")
    await db.set_setting(EMBEDDINGS_API_KEY_SETTING, cipher.encrypt("sk-plain-123"))
    service = EmbeddingService(db)
    cfg = await service._runtime_config()
    assert cfg.api_key == "sk-plain-123"


@pytest.mark.anyio
async def test_runtime_config_encrypted_key_without_cipher_raises(tmp_path):
    """An encrypted stored key without SESSION_ENCRYPTION_KEY must fail loudly, not send ciphertext."""
    from tg_agent.database import Database
    from tg_agent.security import SessionCipher
    from tg_agent.services.embedding_service import EMBEDDINGS_API_KEY_SETTING

    raw_cipher = SessionCipher("test-session-encryption-key")
    database = Database(str(tmp_path / "nockey.db"), session_encryption_secret=None)
    await database.initialize()
    try:
        await database.set_setting(
            EMBEDDINGS_API_KEY_SETTING, raw_cipher.encrypt("sk-plain-123")
        )
        service = EmbeddingService(database)
        with pytest.raises(RuntimeError, match="SESSION_ENCRYPTION_KEY"):
            await service._runtime_config()
    finally:
        await database.close()


@pytest.mark.anyio
async def test_runtime_config_plaintext_key_still_read(tmp_path):
    """Legacy plaintext keys keep working (decrypt passthrough)."""
    from tg_agent.database import Database
    from tg_agent.services.embedding_service import EMBEDDINGS_API_KEY_SETTING

    database = Database(str(tmp_path / "plain.db"), session_encryption_secret=None)
    await database.initialize()
    try:
        await database.set_setting(EMBEDDINGS_API_KEY_SETTING, "sk-legacy-plain")
        service = EmbeddingService(database)
        cfg = await service._runtime_config()
        assert cfg.api_key == "sk-legacy-plain"
    finally:
        await database.close()


# ---------------------------------------------------------------------------
# Date-window filter on the premium global search (#1750)
# ---------------------------------------------------------------------------


def _fake_post(msg_id: int, date, *, channel: bool = True):
    """A minimal Telethon-like channel post for _search_posts_global."""
    from types import SimpleNamespace

    from telethon.tl.types import PeerChannel

    return SimpleNamespace(
        id=msg_id,
        date=date,
        peer_id=PeerChannel(channel_id=100) if channel else None,
        message=f"post {msg_id}",
        from_id=None,
        views=10,
        forwards=1,
        replies=None,
        reactions=None,
        media=None,
    )


class _FakeSearchSession:
    """Serves queued batches from search_posts_batch, recording the raw calls."""

    def __init__(self, batches):
        from types import SimpleNamespace

        # SUBCLASS_OF_ID = crc32(b'InputPeer') — makes telethon's get_input_peer
        # accept the fake chat so pagination can build the offset peer.
        chat = SimpleNamespace(id=100, title="Test", username="testch", SUBCLASS_OF_ID=0xC91C90B6)
        self._batches = [SimpleNamespace(messages=b, chats=[chat], users=[], next_rate=i + 1)
                         for i, b in enumerate(batches[:-1])]
        self._batches.append(SimpleNamespace(messages=batches[-1], chats=[chat], users=[], next_rate=None))
        self.calls: list[dict] = []

    async def search_posts_batch(self, query, *, offset_rate, offset_peer, offset_id, limit):
        self.calls.append({"offset_rate": offset_rate, "offset_id": offset_id, "limit": limit})
        return self._batches.pop(0)


@pytest.mark.anyio
async def test_search_posts_global_window_filter_and_pagination():
    from datetime import datetime
    from datetime import timezone as tz

    from tg_agent.search.telegram_search import TelegramSearch

    def d(day, hour=0):
        return datetime(2026, 11, day, hour, tzinfo=tz.utc)

    # Batch sizes must match batch_limit (=limit - kept) so pagination continues.
    session = _FakeSearchSession([
        [_fake_post(1, d(1, 10), channel=False), _fake_post(2, d(1, 12))],  # both dropped
        [_fake_post(3, d(3, 9))],
    ])
    search = TelegramSearch(None, None)
    messages, _channels = await search._search_posts_global(
        session, "key", limit=2, min_date=d(2), max_date=d(6),
    )
    assert [m.message_id for m in messages] == [3]
    assert len(session.calls) == 2  # pagination continued after the fully-filtered batch


@pytest.mark.anyio
async def test_search_posts_global_without_window_keeps_everything():
    from datetime import datetime
    from datetime import timezone as tz

    from tg_agent.search.telegram_search import TelegramSearch

    d = lambda day: datetime(2026, 11, day, tzinfo=tz.utc)  # noqa: E731
    session = _FakeSearchSession([[_fake_post(1, d(1)), _fake_post(2, d(9))]])
    search = TelegramSearch(None, None)
    messages, _ = await search._search_posts_global(session, "key", limit=10)
    assert [m.message_id for m in messages] == [1, 2]


def test_live_date_bounds_semantics():
    from datetime import datetime
    from datetime import timezone as tz

    from tg_agent.search.engine import _live_date_bounds

    min_d, max_d = _live_date_bounds("2026-10-19", "2026-11-2")
    assert min_d == datetime(2026, 10, 19, tzinfo=tz.utc)
    # date-only "to" is inclusive of the whole day (mirrors DB-mode #1593 semantics)
    assert max_d == datetime(2026, 11, 3, tzinfo=tz.utc)

    iso_min, iso_max = _live_date_bounds("2026-10-20T13:00:00+03:00", "2026-10-21T00:00:00Z")
    assert iso_min == datetime(2026, 10, 20, 10, tzinfo=tz.utc)
    assert iso_max == datetime(2026, 10, 21, tzinfo=tz.utc)

    assert _live_date_bounds(None, None) == (None, None)
    with pytest.raises(ValueError):
        _live_date_bounds("not-a-date", None)


@pytest.mark.anyio
async def test_engine_search_telegram_routes_parsed_bounds(monkeypatch):
    from datetime import datetime
    from datetime import timezone as tz

    from tg_agent.search.engine import SearchEngine

    engine = object.__new__(SearchEngine)  # skip __init__: no DB/pool needed for routing
    captured: dict = {}

    async def fake_search(query, limit, min_date=None, max_date=None):
        captured.update(query=query, limit=limit, min_date=min_date, max_date=max_date)

    engine._telegram = SimpleNamespace(search_telegram=fake_search)  # routing target only
    await engine.search_telegram("key", limit=7, date_from="2026-10-19", date_to="2026-11-02")
    assert captured == {
        "query": "key",
        "limit": 7,
        "min_date": datetime(2026, 10, 19, tzinfo=tz.utc),
        "max_date": datetime(2026, 11, 3, tzinfo=tz.utc),
    }
