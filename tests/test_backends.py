"""Tests for TelegramTransportSession — full telethon-cli coverage."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.telegram.backends import TelegramTransportSession
from tests.helpers import AsyncIterMessages, FakeCliTelethonClient


def _session(**kwargs) -> TelegramTransportSession:
    return TelegramTransportSession(FakeCliTelethonClient(**kwargs))


# --- Batch 1: Messages (#184, #185, #186, #190) ---


@pytest.mark.asyncio
async def test_forward_messages():
    result = SimpleNamespace(id=99)
    session = _session(forward_messages_side_effect=lambda *a: result)
    got = await session.forward_messages("target", [1, 2], "source")
    assert got is result
    session.raw_client.forward_messages.assert_awaited_once_with("target", [1, 2], "source")


@pytest.mark.asyncio
async def test_edit_message():
    result = SimpleNamespace(id=1, message="edited")
    session = _session(edit_message_side_effect=lambda *a, **kw: result)
    got = await session.edit_message("chat", 1, "edited")
    assert got is result
    session.raw_client.edit_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_pin_message():
    session = _session(pin_message_side_effect=lambda *a, **kw: None)
    await session.pin_message("chat", 42, notify=True)
    session.raw_client.pin_message.assert_awaited_once_with("chat", 42, notify=True)


@pytest.mark.asyncio
async def test_unpin_message():
    session = _session(unpin_message_side_effect=lambda *a: None)
    await session.unpin_message("chat", 42)
    session.raw_client.unpin_message.assert_awaited_once_with("chat", 42)


@pytest.mark.asyncio
async def test_unpin_message_all():
    session = _session(unpin_message_side_effect=lambda *a: None)
    await session.unpin_message("chat")
    session.raw_client.unpin_message.assert_awaited_once_with("chat", None)


@pytest.mark.asyncio
async def test_delete_messages():
    session = _session(delete_messages_side_effect=lambda *a: [SimpleNamespace(pts_count=2)])
    result = await session.delete_messages("chat", [1, 2])
    session.raw_client.delete_messages.assert_awaited_once_with("chat", [1, 2])
    assert result is not None


# --- Batch 2: Media (#187) ---


@pytest.mark.asyncio
async def test_download_media():
    session = _session(download_media_side_effect=lambda *a, **kw: "/tmp/photo.jpg")
    result = await session.download_media(SimpleNamespace(id=1), file="/tmp/photo.jpg")
    assert result == "/tmp/photo.jpg"
    session.raw_client.download_media.assert_awaited_once()


# --- Batch 3: Participants (#188, #189) ---


@pytest.mark.asyncio
async def test_get_participants():
    users = [SimpleNamespace(id=1, username="alice"), SimpleNamespace(id=2, username="bob")]
    session = _session(get_participants_side_effect=lambda *a, **kw: users)
    result = await session.get_participants("chat", limit=10, search="a")
    assert len(result) == 2
    session.raw_client.get_participants.assert_awaited_once()


@pytest.mark.asyncio
async def test_stream_participants():
    users = [SimpleNamespace(id=1), SimpleNamespace(id=2)]
    session = _session(iter_participants_factory=lambda *a, **kw: AsyncIterMessages(users))
    collected = []
    async for u in session.stream_participants("chat"):
        collected.append(u)
    assert len(collected) == 2


@pytest.mark.asyncio
async def test_edit_admin():
    session = _session(edit_admin_side_effect=lambda *a, **kw: None)
    await session.edit_admin("chat", "user", is_admin=True)
    session.raw_client.edit_admin.assert_awaited_once()


@pytest.mark.asyncio
async def test_edit_permissions():
    session = _session(edit_permissions_side_effect=lambda *a, **kw: None)
    await session.edit_permissions("chat", "user", view_messages=False)
    session.raw_client.edit_permissions.assert_awaited_once()


@pytest.mark.asyncio
async def test_kick_participant():
    session = _session(kick_participant_side_effect=lambda *a: None)
    await session.kick_participant("chat", "user")
    session.raw_client.kick_participant.assert_awaited_once_with("chat", "user")


# --- Batch 4: Stats, Folder, Read Acknowledge (#191, #192, #193) ---


@pytest.mark.asyncio
async def test_edit_folder():
    session = _session(edit_folder_side_effect=lambda *a: None)
    await session.edit_folder("chat", 1)
    session.raw_client.edit_folder.assert_awaited_once_with("chat", 1)


@pytest.mark.asyncio
async def test_send_read_acknowledge():
    session = _session(send_read_acknowledge_side_effect=lambda *a, **kw: True)
    result = await session.send_read_acknowledge("chat", max_id=100)
    assert result is True
    session.raw_client.send_read_acknowledge.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_read_acknowledge_no_max_id():
    session = _session(send_read_acknowledge_side_effect=lambda *a, **kw: True)
    await session.send_read_acknowledge("chat")
    session.raw_client.send_read_acknowledge.assert_awaited_once()


# --- base ---


def test_set_proxy():
    session = _session()
    session.set_proxy({"proxy_type": "socks5", "addr": "127.0.0.1", "port": 1080})
    session.raw_client.set_proxy.assert_called_once()


# --- uploads ---


@pytest.mark.asyncio
async def test_upload_file():
    result = SimpleNamespace(id=42)
    session = _session(upload_file_side_effect=lambda *a, **kw: result)
    got = await session.upload_file("/tmp/photo.jpg")
    assert got is result
    session.raw_client.upload_file.assert_awaited_once()


# --- downloads ---


@pytest.mark.asyncio
async def test_download_file():
    session = _session(download_file_side_effect=lambda *a, **kw: b"data")
    result = await session.download_file(SimpleNamespace(id=1), "/tmp/out.bin")
    assert result == b"data"


@pytest.mark.asyncio
async def test_stream_download():
    chunks = [b"chunk1", b"chunk2"]
    session = _session(iter_download_factory=lambda *a, **kw: AsyncIterMessages(chunks))
    collected = []
    async for chunk in session.stream_download(SimpleNamespace(id=1)):
        collected.append(chunk)
    assert len(collected) == 2


# --- dialogs ---


@pytest.mark.asyncio
async def test_stream_drafts():
    drafts = [SimpleNamespace(text="draft1")]
    session = _session(iter_drafts_factory=lambda: AsyncIterMessages(drafts))
    collected = []
    async for d in session.stream_drafts():
        collected.append(d)
    assert len(collected) == 1


@pytest.mark.asyncio
async def test_get_drafts():
    drafts = [SimpleNamespace(text="hi")]
    session = _session(get_drafts_side_effect=lambda: drafts)
    result = await session.get_drafts()
    assert len(result) == 1


def test_conversation():
    mock_conv = MagicMock()
    session = _session(conversation_factory=lambda *a, **kw: mock_conv)
    result = session.conversation("bot")
    assert result is mock_conv


# --- users ---


@pytest.mark.asyncio
async def test_is_bot():
    session = _session(is_bot_result=False)
    result = await session.is_bot()
    assert result is False


@pytest.mark.asyncio
async def test_is_user_authorized():
    session = _session(authorized=True)
    result = await session.is_user_authorized()
    assert result is True


@pytest.mark.asyncio
async def test_get_peer_id():
    session = _session(get_peer_id_side_effect=lambda p: 123456)
    result = await session.get_peer_id("@username")
    assert result == 123456


# --- chats ---


@pytest.mark.asyncio
async def test_stream_admin_log():
    events = [SimpleNamespace(id=1), SimpleNamespace(id=2)]
    session = _session(iter_admin_log_factory=lambda *a, **kw: AsyncIterMessages(events))
    collected = []
    async for e in session.stream_admin_log("chat"):
        collected.append(e)
    assert len(collected) == 2


@pytest.mark.asyncio
async def test_get_admin_log():
    events = [SimpleNamespace(id=1)]
    session = _session(get_admin_log_side_effect=lambda *a, **kw: events)
    result = await session.get_admin_log("chat")
    assert len(result) == 1


@pytest.mark.asyncio
async def test_stream_profile_photos():
    photos = [SimpleNamespace(id=1)]
    session = _session(iter_profile_photos_factory=lambda *a, **kw: AsyncIterMessages(photos))
    collected = []
    async for p in session.stream_profile_photos("user"):
        collected.append(p)
    assert len(collected) == 1


@pytest.mark.asyncio
async def test_get_profile_photos():
    photos = [SimpleNamespace(id=1), SimpleNamespace(id=2)]
    session = _session(get_profile_photos_side_effect=lambda *a, **kw: photos)
    result = await session.get_profile_photos("user")
    assert len(result) == 2


def test_action():
    session = _session(action_side_effect=lambda *a, **kw: MagicMock())
    result = session.action("chat", "typing")
    assert result is not None


@pytest.mark.asyncio
async def test_get_permissions():
    perms = SimpleNamespace(send_messages=True, send_media=False)
    session = _session(get_permissions_side_effect=lambda *a: perms)
    result = await session.get_permissions("chat", "user")
    assert result.send_messages is True


# --- updates ---


@pytest.mark.asyncio
async def test_set_receive_updates():
    session = _session(set_receive_updates_side_effect=lambda *a: None)
    await session.set_receive_updates(True)
    session.raw_client.set_receive_updates.assert_awaited_once()


@pytest.mark.asyncio
async def test_run_until_disconnected():
    session = _session(run_until_disconnected_side_effect=lambda: None)
    await session.run_until_disconnected()
    session.raw_client.run_until_disconnected.assert_awaited_once()


def test_on():
    session = _session()
    session.on("event")
    session.raw_client.on.assert_called_once_with("event")


def test_add_event_handler():
    session = _session()

    def _cb():
        pass

    session.add_event_handler(_cb, "event")
    session.raw_client.add_event_handler.assert_called_once_with(_cb, "event")


def test_remove_event_handler():
    session = _session()

    def _cb():
        pass

    result = session.remove_event_handler(_cb, "event")
    session.raw_client.remove_event_handler.assert_called_once_with(_cb, "event")
    assert result is True


def test_list_event_handlers():
    session = _session()
    result = session.list_event_handlers()
    assert result == []


@pytest.mark.asyncio
async def test_catch_up():
    session = _session(catch_up_side_effect=lambda: None)
    await session.catch_up()
    session.raw_client.catch_up.assert_awaited_once()


# --- bots ---


@pytest.mark.asyncio
async def test_inline_query():
    results = [SimpleNamespace(id="1", title="result")]
    session = _session(inline_query_side_effect=lambda *a, **kw: results)
    got = await session.inline_query("@bot", "query")
    assert len(got) == 1


# --- buttons ---


def test_build_reply_markup():
    markup = SimpleNamespace(rows=[])
    session = _session(build_reply_markup_side_effect=lambda b: markup)
    result = session.build_reply_markup([["btn1"]])
    assert result is markup


# --- account ---


def test_takeout():
    ctx = MagicMock()
    session = _session(takeout_side_effect=lambda **kw: ctx)
    result = session.takeout(finalize=True)
    assert result is ctx


@pytest.mark.asyncio
async def test_end_takeout():
    session = _session(end_takeout_side_effect=lambda s: None)
    await session.end_takeout(True)
    session.raw_client.end_takeout.assert_awaited_once_with(True)


# --- stats (invoke_request) ---


@pytest.mark.asyncio
async def test_get_broadcast_stats():
    stats = SimpleNamespace(period=SimpleNamespace(min_date=0, max_date=0))
    session = _session(invoke_side_effect=lambda req: stats)
    result = await session.get_broadcast_stats("channel")
    assert result is stats
    session.raw_client.invoke.assert_awaited_once()
