from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import Message
from src.search.engine import SearchEngine


@pytest.mark.asyncio
async def test_search_local_empty(db):
    engine = SearchEngine(db)
    result = await engine.search_local("test")
    assert result.total == 0
    assert result.messages == []
    assert result.query == "test"


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


def _make_mock_api_message(
    channel_id=100123, msg_id=42, text="Test message about AI",
):
    """Create a mock raw API message for SearchPostsRequest response."""
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
    """Create a mock SearchPostsRequest response."""
    r = MagicMock()
    r.messages = messages
    r.chats = chats or []
    r.users = users or []
    r.next_rate = None
    return r


def _make_premium_client(call_return):
    """Create a mock client with Premium and __call__ returning search results."""
    me = MagicMock()
    me.premium = True

    client = MagicMock()
    client.get_me = AsyncMock(return_value=me)
    client.__call__ = AsyncMock(return_value=call_return)
    # MagicMock needs __call__ to be async for `await client(...)`
    client.side_effect = client.__call__.side_effect
    return client


@pytest.mark.asyncio
async def test_search_telegram_returns_results(db):
    mock_msg = _make_mock_api_message()
    mock_chat = MagicMock()
    mock_chat.id = 100123
    mock_chat.title = "Test Channel"
    mock_chat.username = "test_channel"

    response = _make_search_response([mock_msg], chats=[mock_chat])

    mock_client = AsyncMock()
    mock_client.return_value = response

    pool = MagicMock()
    pool.get_premium_client = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()

    engine = SearchEngine(db, pool=pool)
    result = await engine.search_telegram("AI", limit=10)

    assert result.total == 1
    assert result.query == "AI"
    assert result.messages[0].message_id == 42
    assert result.messages[0].text == "Test message about AI"
    assert result.messages[0].channel_title == "Test Channel"
    pool.release_client.assert_called_with("+1234567890")


@pytest.mark.asyncio
async def test_search_telegram_caches_to_db(db):
    mock_msg = _make_mock_api_message(channel_id=100456, msg_id=7, text="cached search result")
    mock_chat = MagicMock()
    mock_chat.id = 100456
    mock_chat.title = "Cache Channel"
    mock_chat.username = None

    response = _make_search_response([mock_msg], chats=[mock_chat])

    mock_client = AsyncMock()
    mock_client.return_value = response

    pool = MagicMock()
    pool.get_premium_client = AsyncMock(return_value=(mock_client, "+1234567890"))
    pool.release_client = AsyncMock()

    engine = SearchEngine(db, pool=pool)
    await engine.search_telegram("cached", limit=5)

    messages, total = await db.search_messages(query="cached", limit=10, offset=0)
    assert total == 1
    assert messages[0].text == "cached search result"
    pool.release_client.assert_called_with("+1234567890")


@pytest.mark.asyncio
async def test_search_telegram_no_pool(db):
    engine = SearchEngine(db, pool=None)
    result = await engine.search_telegram("anything")

    assert result.total == 0
    assert result.messages == []
    assert result.query == "anything"
    assert result.error == "Нет подключённых Telegram-аккаунтов."


@pytest.mark.asyncio
async def test_search_telegram_no_premium(db):
    # get_premium_client returns None when no premium accounts are available
    pool = MagicMock()
    pool.get_premium_client = AsyncMock(return_value=None)

    engine = SearchEngine(db, pool=pool)
    result = await engine.search_telegram("query")

    assert result.total == 0
    assert result.messages == []
    assert "Premium" in result.error


# ---- Helpers for resolved Telethon messages (iter_messages) ----

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
    """Create a mock resolved Telethon message (with .chat, .sender, .text)."""
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


def _make_iter_messages_client(messages_list):
    """Create a mock client whose iter_messages yields the given messages."""
    client = AsyncMock()
    client.get_dialogs = AsyncMock(return_value=[])

    async def _iter(*args, **kwargs):
        for m in messages_list:
            yield m

    client.iter_messages = _iter
    return client


# ---- search_my_chats tests ----

@pytest.mark.asyncio
async def test_search_my_chats_returns_results(db):
    mock_msg = _make_resolved_message()
    client = _make_iter_messages_client([mock_msg])

    pool = MagicMock()
    pool.get_available_client = AsyncMock(return_value=(client, "+1234567890"))
    pool.release_client = AsyncMock()

    engine = SearchEngine(db, pool=pool)
    result = await engine.search_my_chats("resolved", limit=10)

    assert result.total == 1
    assert result.messages[0].message_id == 42
    assert result.messages[0].text == "resolved message"
    assert result.messages[0].channel_title == "My Chat"
    pool.release_client.assert_called_once_with("+1234567890")


@pytest.mark.asyncio
async def test_search_my_chats_no_pool(db):
    engine = SearchEngine(db, pool=None)
    result = await engine.search_my_chats("anything")

    assert result.total == 0
    assert result.error == "Нет подключённых Telegram-аккаунтов."


@pytest.mark.asyncio
async def test_search_my_chats_no_clients(db):
    pool = MagicMock()
    pool.get_available_client = AsyncMock(return_value=None)

    engine = SearchEngine(db, pool=pool)
    result = await engine.search_my_chats("query")

    assert result.total == 0
    assert result.error == "Нет доступных Telegram-аккаунтов. Проверьте подключение."


# ---- search_in_channel tests ----

@pytest.mark.asyncio
async def test_search_in_channel_returns_results(db):
    mock_msg = _make_resolved_message(chat_id=200456, chat_title="Target Channel")
    client = _make_iter_messages_client([mock_msg])
    client.get_entity = AsyncMock(return_value=MagicMock())

    pool = MagicMock()
    pool.get_available_client = AsyncMock(return_value=(client, "+1234567890"))
    pool.release_client = AsyncMock()

    engine = SearchEngine(db, pool=pool)
    result = await engine.search_in_channel(200456, "resolved", limit=10)

    assert result.total == 1
    assert result.messages[0].channel_id == 200456
    assert result.messages[0].channel_title == "Target Channel"
    pool.release_client.assert_called_once_with("+1234567890")


@pytest.mark.asyncio
async def test_search_in_channel_all_channels(db):
    """When channel_id is None, search across all user's chats."""
    mock_msg = _make_resolved_message(chat_id=300789, chat_title="All Chats Result")
    client = _make_iter_messages_client([mock_msg])
    client.get_dialogs = AsyncMock(return_value=[])

    pool = MagicMock()
    pool.get_available_client = AsyncMock(return_value=(client, "+1234567890"))
    pool.release_client = AsyncMock()

    engine = SearchEngine(db, pool=pool)
    result = await engine.search_in_channel(None, "query", limit=10)

    assert result.total == 1
    assert result.error is None
    assert result.messages[0].channel_title == "All Chats Result"
    pool.release_client.assert_called_once_with("+1234567890")


@pytest.mark.asyncio
async def test_search_in_channel_entity_not_found(db):
    client = AsyncMock()
    client.get_entity = AsyncMock(side_effect=ValueError("entity not found"))

    pool = MagicMock()
    pool.get_available_client = AsyncMock(return_value=(client, "+1234567890"))
    pool.release_client = AsyncMock()

    engine = SearchEngine(db, pool=pool)
    result = await engine.search_in_channel(999999, "query")

    assert result.total == 0
    assert "Не удалось найти канал" in result.error
    pool.release_client.assert_called_once_with("+1234567890")
