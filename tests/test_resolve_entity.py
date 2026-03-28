"""Tests for resolve_any_entity (ClientPool) and resolve_entity agent tool."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.database import Database

# ---------------------------------------------------------------------------
# Helpers (same pattern as test_coverage_batch4.py)
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_db():
    db = MagicMock(spec=Database)
    db.get_setting = AsyncMock(return_value=None)
    db.repos = MagicMock()
    db.repos.settings = MagicMock()
    db.repos.settings.get = AsyncMock(return_value=None)
    return db


def _get_tool_handlers(mock_db, client_pool=None, **kwargs):
    captured = []
    with patch(
        "src.agent.tools.create_sdk_mcp_server",
        side_effect=lambda **kw: captured.extend(kw.get("tools", [])),
    ):
        from src.agent.tools import make_mcp_server
        make_mcp_server(mock_db, client_pool=client_pool, **kwargs)
    return {t.name: t.handler for t in captured}


def _text(result: dict) -> str:
    return result["content"][0]["text"]


async def _fake_flood_wait(coro, **kw):
    return await coro


def _make_pool():
    """Bare ClientPool with mocked get_available_client / release_client."""
    from src.telegram.client_pool import ClientPool
    pool = ClientPool.__new__(ClientPool)
    pool.release_client = AsyncMock()
    return pool


def _channel_entity(channel_id: int, title: str = "Test Channel", username: str = "testchan",
                    broadcast=True, megagroup=False, gigagroup=False,
                    forum=False, monoforum=False, restricted=False,
                    scam=False, fake=False) -> MagicMock:
    """Build a MagicMock that looks like a Telethon Channel entity."""
    e = MagicMock()
    e.id = channel_id
    e.title = title
    e.username = username
    e.broadcast = broadcast
    e.megagroup = megagroup
    e.gigagroup = gigagroup
    e.forum = forum
    e.monoforum = monoforum
    e.restricted = restricted
    e.scam = scam
    e.fake = fake
    return e


def _user_entity(user_id: int, first_name: str = "First", last_name: str = "Last",
                 username: str = "alxz500", bot: bool = False) -> MagicMock:
    """Build a MagicMock that looks like a Telethon User entity (no title attr)."""
    e = MagicMock(spec=["id", "first_name", "last_name", "username", "bot"])
    e.id = user_id
    e.first_name = first_name
    e.last_name = last_name
    e.username = username
    e.bot = bot
    return e


# ===========================================================================
# 1. ClientPool.resolve_any_entity
# ===========================================================================


class TestResolveAnyEntityClientPool:
    """Tests for ClientPool.resolve_any_entity()."""

    @pytest.mark.asyncio
    async def test_resolve_user_by_username(self):
        pool = _make_pool()
        entity = _user_entity(111, "Alex", "Z", "alxz500")
        mock_session = AsyncMock()
        mock_session.resolve_entity = AsyncMock(return_value=entity)
        pool.get_available_client = AsyncMock(return_value=(mock_session, "+1"))

        with patch("src.telegram.client_pool.adapt_transport_session", return_value=mock_session), \
             patch("src.telegram.client_pool.run_with_flood_wait", side_effect=_fake_flood_wait):
            result = await pool.resolve_any_entity("@alxz500")

        assert result is not None
        assert result["channel_id"] == 111
        assert result["channel_type"] == "dm"
        assert result["title"] == "Alex Z"
        assert result["username"] == "alxz500"

    @pytest.mark.asyncio
    async def test_resolve_bot_by_username(self):
        pool = _make_pool()
        entity = _user_entity(222, "My", "Bot", "mybot", bot=True)
        mock_session = AsyncMock()
        mock_session.resolve_entity = AsyncMock(return_value=entity)
        pool.get_available_client = AsyncMock(return_value=(mock_session, "+1"))

        with patch("src.telegram.client_pool.adapt_transport_session", return_value=mock_session), \
             patch("src.telegram.client_pool.run_with_flood_wait", side_effect=_fake_flood_wait):
            result = await pool.resolve_any_entity("@mybot")

        assert result is not None
        assert result["channel_type"] == "bot"

    @pytest.mark.asyncio
    async def test_resolve_channel_by_username(self):
        pool = _make_pool()
        entity = _channel_entity(333, "News Channel", "newschan", broadcast=True)
        mock_session = AsyncMock()
        mock_session.resolve_entity = AsyncMock(return_value=entity)
        pool.get_available_client = AsyncMock(return_value=(mock_session, "+1"))

        with patch("src.telegram.client_pool.adapt_transport_session", return_value=mock_session), \
             patch("src.telegram.client_pool.run_with_flood_wait", side_effect=_fake_flood_wait):
            result = await pool.resolve_any_entity("@newschan")

        assert result is not None
        assert result["channel_type"] == "channel"
        assert result["title"] == "News Channel"

    @pytest.mark.asyncio
    async def test_resolve_group_by_username(self):
        pool = _make_pool()
        entity = _channel_entity(444, "My Group", "mygroup", broadcast=False, megagroup=True)
        mock_session = AsyncMock()
        mock_session.resolve_entity = AsyncMock(return_value=entity)
        pool.get_available_client = AsyncMock(return_value=(mock_session, "+1"))

        with patch("src.telegram.client_pool.adapt_transport_session", return_value=mock_session), \
             patch("src.telegram.client_pool.run_with_flood_wait", side_effect=_fake_flood_wait):
            result = await pool.resolve_any_entity("@mygroup")

        assert result is not None
        assert result["channel_type"] == "supergroup"

    @pytest.mark.asyncio
    async def test_resolve_by_tme_link(self):
        pool = _make_pool()
        entity = _user_entity(555, "Alex", "", "alxz500")
        mock_session = AsyncMock()
        mock_session.resolve_entity = AsyncMock(return_value=entity)
        pool.get_available_client = AsyncMock(return_value=(mock_session, "+1"))

        with patch("src.telegram.client_pool.adapt_transport_session", return_value=mock_session), \
             patch("src.telegram.client_pool.run_with_flood_wait", side_effect=_fake_flood_wait):
            result = await pool.resolve_any_entity("https://t.me/alxz500")

        assert result is not None
        assert result["channel_id"] == 555

    @pytest.mark.asyncio
    async def test_tme_post_link_stripped_to_channel(self):
        """t.me/chan/123 → resolve t.me/chan, not the post number."""
        pool = _make_pool()
        entity = _channel_entity(666, "Chan", "chan")
        mock_session = AsyncMock()
        mock_session.resolve_entity = AsyncMock(return_value=entity)
        pool.get_available_client = AsyncMock(return_value=(mock_session, "+1"))

        with patch("src.telegram.client_pool.adapt_transport_session", return_value=mock_session), \
             patch("src.telegram.client_pool.run_with_flood_wait", side_effect=_fake_flood_wait):
            result = await pool.resolve_any_entity("https://t.me/chan/123")

        assert result is not None
        # Verify the peer passed to resolve_entity was the stripped link, not "123"
        call_args = mock_session.resolve_entity.call_args[0][0]
        assert "123" not in str(call_args)

    @pytest.mark.asyncio
    async def test_positive_numeric_id_uses_peer_user(self):
        """Positive numeric ID → PeerUser."""
        from telethon.tl.types import PeerUser
        pool = _make_pool()
        entity = _user_entity(12345)
        mock_session = AsyncMock()
        mock_session.resolve_entity = AsyncMock(return_value=entity)
        pool.get_available_client = AsyncMock(return_value=(mock_session, "+1"))

        with patch("src.telegram.client_pool.adapt_transport_session", return_value=mock_session), \
             patch("src.telegram.client_pool.run_with_flood_wait", side_effect=_fake_flood_wait):
            await pool.resolve_any_entity("12345")

        peer_arg = mock_session.resolve_entity.call_args[0][0]
        assert isinstance(peer_arg, PeerUser)
        assert peer_arg.user_id == 12345

    @pytest.mark.asyncio
    async def test_negative_numeric_id_uses_peer_channel(self):
        """Negative numeric ID → PeerChannel."""
        from telethon.tl.types import PeerChannel
        pool = _make_pool()
        entity = _channel_entity(1001234567890, "Chan")
        mock_session = AsyncMock()
        mock_session.resolve_entity = AsyncMock(return_value=entity)
        pool.get_available_client = AsyncMock(return_value=(mock_session, "+1"))

        with patch("src.telegram.client_pool.adapt_transport_session", return_value=mock_session), \
             patch("src.telegram.client_pool.run_with_flood_wait", side_effect=_fake_flood_wait):
            await pool.resolve_any_entity("-1001234567890")

        peer_arg = mock_session.resolve_entity.call_args[0][0]
        assert isinstance(peer_arg, PeerChannel)
        assert peer_arg.channel_id == 1001234567890

    @pytest.mark.asyncio
    async def test_username_not_found_returns_none(self):
        from telethon.errors import UsernameNotOccupiedError
        pool = _make_pool()
        mock_session = AsyncMock()
        pool.get_available_client = AsyncMock(return_value=(mock_session, "+1"))

        async def raise_not_found(coro, **kw):
            raise UsernameNotOccupiedError(request=None)

        with patch("src.telegram.client_pool.adapt_transport_session", return_value=mock_session), \
             patch("src.telegram.client_pool.run_with_flood_wait", side_effect=raise_not_found):
            result = await pool.resolve_any_entity("@nonexistent_skdjfsdlkjf")

        assert result is None

    @pytest.mark.asyncio
    async def test_username_invalid_returns_none(self):
        from telethon.errors import UsernameInvalidError
        pool = _make_pool()
        mock_session = AsyncMock()
        pool.get_available_client = AsyncMock(return_value=(mock_session, "+1"))

        async def raise_invalid(coro, **kw):
            raise UsernameInvalidError(request=None)

        with patch("src.telegram.client_pool.adapt_transport_session", return_value=mock_session), \
             patch("src.telegram.client_pool.run_with_flood_wait", side_effect=raise_invalid):
            result = await pool.resolve_any_entity("@!!!")

        assert result is None

    @pytest.mark.asyncio
    async def test_timeout_returns_none(self):
        pool = _make_pool()
        mock_session = AsyncMock()
        pool.get_available_client = AsyncMock(return_value=(mock_session, "+1"))

        with patch("src.telegram.client_pool.adapt_transport_session", return_value=mock_session), \
             patch("src.telegram.client_pool.run_with_flood_wait", side_effect=asyncio.TimeoutError):
            result = await pool.resolve_any_entity("@alxz500")

        assert result is None

    @pytest.mark.asyncio
    async def test_no_available_client_raises(self):
        pool = _make_pool()
        pool.get_available_client = AsyncMock(return_value=None)

        with pytest.raises(RuntimeError, match="no_client"):
            await pool.resolve_any_entity("@alxz500")

    @pytest.mark.asyncio
    async def test_channel_forbidden_returns_none(self):
        from telethon.tl.types import ChannelForbidden
        pool = _make_pool()
        entity = MagicMock(spec=ChannelForbidden)
        mock_session = AsyncMock()
        mock_session.resolve_entity = AsyncMock(return_value=entity)
        pool.get_available_client = AsyncMock(return_value=(mock_session, "+1"))

        with patch("src.telegram.client_pool.adapt_transport_session", return_value=mock_session), \
             patch("src.telegram.client_pool.run_with_flood_wait", side_effect=_fake_flood_wait), \
             patch("src.telegram.client_pool.ChannelForbidden", ChannelForbidden):
            result = await pool.resolve_any_entity("@private_chan")

        assert result is None

    @pytest.mark.asyncio
    async def test_preferred_phone_tried_first(self):
        """If phone given, get_client_by_phone is called before get_available_client."""
        pool = _make_pool()
        entity = _user_entity(111)
        mock_session = AsyncMock()
        mock_session.resolve_entity = AsyncMock(return_value=entity)
        pool.get_client_by_phone = AsyncMock(return_value=(mock_session, "+123"))
        pool.get_available_client = AsyncMock(return_value=(mock_session, "+999"))

        with patch("src.telegram.client_pool.adapt_transport_session", return_value=mock_session), \
             patch("src.telegram.client_pool.run_with_flood_wait", side_effect=_fake_flood_wait):
            await pool.resolve_any_entity("@alxz500", phone="+123")

        pool.get_client_by_phone.assert_called_once_with("+123")
        pool.get_available_client.assert_not_called()

    @pytest.mark.asyncio
    async def test_preferred_phone_fallback_to_any(self):
        """If preferred phone unavailable, falls back to any available client."""
        pool = _make_pool()
        entity = _user_entity(111)
        mock_session = AsyncMock()
        mock_session.resolve_entity = AsyncMock(return_value=entity)
        pool.get_client_by_phone = AsyncMock(return_value=None)
        pool.get_available_client = AsyncMock(return_value=(mock_session, "+999"))

        with patch("src.telegram.client_pool.adapt_transport_session", return_value=mock_session), \
             patch("src.telegram.client_pool.run_with_flood_wait", side_effect=_fake_flood_wait):
            result = await pool.resolve_any_entity("@alxz500", phone="+123")

        assert result is not None
        pool.get_available_client.assert_called()

    @pytest.mark.asyncio
    async def test_user_with_no_last_name(self):
        """User with only first_name → title is just first_name."""
        pool = _make_pool()
        entity = _user_entity(999, "Alex", "", "alxz500")
        mock_session = AsyncMock()
        mock_session.resolve_entity = AsyncMock(return_value=entity)
        pool.get_available_client = AsyncMock(return_value=(mock_session, "+1"))

        with patch("src.telegram.client_pool.adapt_transport_session", return_value=mock_session), \
             patch("src.telegram.client_pool.run_with_flood_wait", side_effect=_fake_flood_wait):
            result = await pool.resolve_any_entity("@alxz500")

        assert result["title"] == "Alex"

    @pytest.mark.asyncio
    async def test_generic_exception_returns_none(self):
        """Unexpected exception → returns None gracefully."""
        pool = _make_pool()
        mock_session = AsyncMock()
        pool.get_available_client = AsyncMock(return_value=(mock_session, "+1"))

        async def raise_generic(coro, **kw):
            raise ValueError("unexpected")

        with patch("src.telegram.client_pool.adapt_transport_session", return_value=mock_session), \
             patch("src.telegram.client_pool.run_with_flood_wait", side_effect=raise_generic):
            result = await pool.resolve_any_entity("@whatever")

        assert result is None

    @pytest.mark.asyncio
    async def test_random_garbage_input_returns_none(self):
        """Arbitrary garbage text → treated as username string, not found."""
        from telethon.errors import UsernameInvalidError
        pool = _make_pool()
        mock_session = AsyncMock()
        pool.get_available_client = AsyncMock(return_value=(mock_session, "+1"))

        async def raise_invalid(coro, **kw):
            raise UsernameInvalidError(request=None)

        with patch("src.telegram.client_pool.adapt_transport_session", return_value=mock_session), \
             patch("src.telegram.client_pool.run_with_flood_wait", side_effect=raise_invalid):
            result = await pool.resolve_any_entity("ывдлаоывладо ывдлао")

        assert result is None


# ===========================================================================
# 2. resolve_entity agent tool
# ===========================================================================


class TestResolveEntityTool:
    """Tests for the resolve_entity MCP tool in my_telegram.py."""

    @pytest.mark.asyncio
    async def test_resolve_user_success(self, mock_db):
        pool = MagicMock()
        pool.resolve_any_entity = AsyncMock(return_value={
            "channel_id": 111,
            "title": "Alex Z",
            "channel_type": "dm",
            "username": "alxz500",
        })
        mock_db.repos.settings.get = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=pool)

        result = await handlers["resolve_entity"]({"identifier": "@alxz500"})
        text = _text(result)

        assert "Alex Z" in text
        assert "dm" in text
        assert "111" in text
        assert "@alxz500" in text

    @pytest.mark.asyncio
    async def test_resolve_bot_shows_bot_type(self, mock_db):
        pool = MagicMock()
        pool.resolve_any_entity = AsyncMock(return_value={
            "channel_id": 222,
            "title": "My Bot",
            "channel_type": "bot",
            "username": "mybot",
        })
        handlers = _get_tool_handlers(mock_db, client_pool=pool)

        result = await handlers["resolve_entity"]({"identifier": "@mybot"})
        assert "bot" in _text(result)

    @pytest.mark.asyncio
    async def test_resolve_channel_shows_channel_type(self, mock_db):
        pool = MagicMock()
        pool.resolve_any_entity = AsyncMock(return_value={
            "channel_id": 333,
            "title": "News",
            "channel_type": "channel",
            "username": "newschan",
        })
        handlers = _get_tool_handlers(mock_db, client_pool=pool)

        result = await handlers["resolve_entity"]({"identifier": "@newschan"})
        assert "channel" in _text(result)

    @pytest.mark.asyncio
    async def test_empty_identifier_returns_error(self, mock_db):
        pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)

        result = await handlers["resolve_entity"]({"identifier": ""})
        assert "обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_missing_identifier_returns_error(self, mock_db):
        pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)

        result = await handlers["resolve_entity"]({})
        assert "обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)

        result = await handlers["resolve_entity"]({"identifier": "@alxz500"})
        # require_pool returns a message about Telegram client not available
        assert "telegram" in _text(result).lower() or "cli" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_entity_not_found_returns_not_found(self, mock_db):
        pool = MagicMock()
        pool.resolve_any_entity = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=pool)

        result = await handlers["resolve_entity"]({"identifier": "@gibberish_nonexistent"})
        assert "не найдена" in _text(result)

    @pytest.mark.asyncio
    async def test_no_client_raises_runtime_error(self, mock_db):
        pool = MagicMock()
        pool.resolve_any_entity = AsyncMock(side_effect=RuntimeError("no_client"))
        handlers = _get_tool_handlers(mock_db, client_pool=pool)

        result = await handlers["resolve_entity"]({"identifier": "@alxz500"})
        assert "аккаунт" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_generic_exception_returns_error(self, mock_db):
        pool = MagicMock()
        pool.resolve_any_entity = AsyncMock(side_effect=Exception("network error"))
        handlers = _get_tool_handlers(mock_db, client_pool=pool)

        result = await handlers["resolve_entity"]({"identifier": "@alxz500"})
        assert "Ошибка resolve" in _text(result)

    @pytest.mark.asyncio
    async def test_random_garbage_text_attempts_resolve(self, mock_db):
        """Arbitrary text → tool attempts resolve (returns not found)."""
        pool = MagicMock()
        pool.resolve_any_entity = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=pool)

        result = await handlers["resolve_entity"]({"identifier": "ывдлаоывладо ывдлао"})
        # Should not crash — returns graceful not-found
        assert _text(result)
        pool.resolve_any_entity.assert_called_once()

    @pytest.mark.asyncio
    async def test_at_nonexistent_username_attempts_resolve(self, mock_db):
        """@skdjfsdlkjf → tool calls resolve_any_entity with the identifier."""
        pool = MagicMock()
        pool.resolve_any_entity = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=pool)

        result = await handlers["resolve_entity"]({"identifier": "@skdjfsdlkjf"})
        assert "не найдена" in _text(result)
        pool.resolve_any_entity.assert_called_once()
        call_identifier = pool.resolve_any_entity.call_args[0][0]
        assert "skdjfsdlkjf" in call_identifier

    @pytest.mark.asyncio
    async def test_entity_without_username_no_username_line(self, mock_db):
        """Entity with no username → username line not shown."""
        pool = MagicMock()
        pool.resolve_any_entity = AsyncMock(return_value={
            "channel_id": 999,
            "title": "Anonymous",
            "channel_type": "dm",
            "username": None,
        })
        handlers = _get_tool_handlers(mock_db, client_pool=pool)

        result = await handlers["resolve_entity"]({"identifier": "999"})
        assert "Username" not in _text(result)

    @pytest.mark.asyncio
    async def test_with_phone_param_passed_to_resolve(self, mock_db):
        """phone param is forwarded to resolve_any_entity."""
        pool = MagicMock()
        pool.resolve_any_entity = AsyncMock(return_value={
            "channel_id": 123,
            "title": "User",
            "channel_type": "dm",
            "username": "user",
        })
        mock_db.repos.settings.get = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=pool)

        await handlers["resolve_entity"]({"identifier": "@alxz500", "phone": "+66982102247"})

        call_kwargs = pool.resolve_any_entity.call_args
        assert call_kwargs is not None
        # phone should be passed as keyword argument
        assert call_kwargs.kwargs.get("phone") is not None or (
            len(call_kwargs.args) > 1 and call_kwargs.args[1] is not None
        )
