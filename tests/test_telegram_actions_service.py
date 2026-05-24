"""Tests for the shared TelegramActionService contract."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.services.telegram_actions import TelegramActionClientUnavailableError, TelegramActionService


def _pool_with_client(client):
    pool = MagicMock()
    pool.get_native_client_by_phone = AsyncMock(return_value=(client, "+1"))
    pool.get_available_client = AsyncMock(return_value=(client, "+1"))
    pool.get_client_by_phone = AsyncMock(return_value=(client, "+1"))
    pool.release_client = AsyncMock()
    return pool


@pytest.mark.anyio
async def test_send_reaction_resolves_entity_and_releases_client():
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value="entity")
    client.send_reaction = AsyncMock()
    pool = _pool_with_client(client)

    result = await TelegramActionService(pool).send_reaction(
        phone="+1",
        chat_id="@chat",
        message_id=42,
        emoji="🔥",
    )

    assert result.phone == "+1"
    client.get_entity.assert_awaited_once_with("@chat")
    client.send_reaction.assert_awaited_once_with("entity", 42, "🔥")
    pool.release_client.assert_awaited_once_with("+1")


@pytest.mark.anyio
async def test_send_reaction_clear_passes_none_and_releases_client():
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value="entity")
    client.send_reaction = AsyncMock()
    pool = _pool_with_client(client)

    result = await TelegramActionService(pool).send_reaction(
        phone="+1",
        chat_id="@chat",
        message_id=42,
        emoji=None,
    )

    assert result.phone == "+1"
    client.get_entity.assert_awaited_once_with("@chat")
    client.send_reaction.assert_awaited_once_with("entity", 42, None)
    pool.release_client.assert_awaited_once_with("+1")


@pytest.mark.anyio
async def test_create_channel_sets_username_and_returns_link():
    client = AsyncMock()
    channel = SimpleNamespace(id=123, username="")
    client.create_channel = AsyncMock(return_value=SimpleNamespace(chats=[channel]))
    client.update_channel_username = AsyncMock()
    pool = _pool_with_client(client)

    result = await TelegramActionService(pool).create_channel(
        phone="+1",
        title="Title",
        about="About",
        username="public_name",
    )

    assert result.channel_id == 123
    assert result.channel_username == "public_name"
    assert result.invite_link == "https://t.me/public_name"
    client.create_channel.assert_awaited_once_with(
        title="Title",
        about="About",
        broadcast=True,
        megagroup=False,
    )
    client.update_channel_username.assert_awaited_once_with(channel, "public_name")


@pytest.mark.anyio
async def test_download_media_looks_up_message_checks_path_and_releases_client(tmp_path):
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value="entity")
    message = SimpleNamespace(id=7)

    async def _iter_messages(entity, ids):
        assert entity == "entity"
        assert ids == 7
        yield message

    media_path = tmp_path / "media.jpg"
    media_path.write_text("x")
    client.iter_messages = MagicMock(return_value=_iter_messages("entity", 7))
    client.download_media = AsyncMock(return_value=str(media_path))
    pool = _pool_with_client(client)

    result = await TelegramActionService(pool).download_media(
        phone="+1",
        chat_id="@chat",
        message_id=7,
        output_dir=tmp_path,
    )

    assert result.path == str(media_path)
    client.download_media.assert_awaited_once_with(message, file=str(tmp_path.resolve()))
    pool.release_client.assert_awaited_once_with("+1")


@pytest.mark.anyio
async def test_leave_dialogs_delegates_to_pool_leave_channels():
    pool = MagicMock()
    pool.leave_channels = AsyncMock(return_value={100: True, 200: False})

    result = await TelegramActionService(pool).leave_dialogs(
        phone="+1",
        dialogs=[(100, "channel"), (200, "supergroup")],
    )

    assert result.success_count == 1
    assert result.failed_count == 1
    pool.leave_channels.assert_awaited_once_with("+1", [(100, "channel"), (200, "supergroup")])


@pytest.mark.anyio
async def test_action_service_raises_when_client_unavailable():
    pool = MagicMock()
    pool.get_native_client_by_phone = AsyncMock(return_value=None)

    with pytest.raises(TelegramActionClientUnavailableError):
        await TelegramActionService(pool).send_message(phone="+1", recipient="@u", text="hi")
