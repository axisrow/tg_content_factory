"""Tests for TelegramTransportSession — full telethon-cli coverage."""

from __future__ import annotations

import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from telethon.errors import FloodWaitError, TypeNotFoundError
from telethon_floodgate import HandledFloodWaitError, TelegramRateLimitGate

from tests.helpers import AsyncIterMessages, FakeCliTelethonClient
from tg_agent.models import Account
from tg_agent.telegram.backends import (
    NativeTelethonBackend,
    TelegramTransportSession,
    TelethonCliBackend,
    fetch_message_reaction_users_raw,
)
from tg_agent.telegram.collector_types import unknown_tl_type_note
from tg_agent.telegram.reactions import TelegramReactionInvalidError
from tg_agent.telegram.schema_drift import SchemaDriftError


def _session(**kwargs) -> TelegramTransportSession:
    return TelegramTransportSession(FakeCliTelethonClient(**kwargs))


def test_with_flood_context_returns_distinct_session_without_mutating_original():
    pool = object()
    original = _session()

    contextual = original.with_flood_context(
        phone="+7000",
        pool=pool,
        report_flood_wait=False,
    )

    assert contextual is not original
    assert contextual.raw_client is original.raw_client
    assert original._phone is None
    assert original._pool is None
    assert original._report_flood_wait is True
    assert contextual._phone == "+7000"
    assert contextual._pool is pool
    assert contextual._report_flood_wait is False


# --- Batch 1: Messages (#184, #185, #186, #190) ---


@pytest.mark.anyio
async def test_forward_messages():
    result = SimpleNamespace(id=99)
    session = _session(forward_messages_side_effect=lambda *a: result)
    got = await session.forward_messages("target", [1, 2], "source")
    assert got is result
    session.raw_client.forward_messages.assert_awaited_once_with("target", [1, 2], "source")


@pytest.mark.anyio
async def test_edit_message():
    result = SimpleNamespace(id=1, message="edited")
    session = _session(edit_message_side_effect=lambda *a, **kw: result)
    got = await session.edit_message("chat", 1, "edited")
    assert got is result
    session.raw_client.edit_message.assert_awaited_once()


@pytest.mark.anyio
async def test_publish_files_silent_reaches_send_file():
    sent = SimpleNamespace(id=1)
    session = _session(send_file_side_effect=lambda *a, **kw: sent)
    got = await session.publish_files("chat", "p.png", silent=True)
    assert got is sent
    session.raw_client.send_file.assert_awaited_once_with(
        "chat", "p.png", caption=None, schedule=None, silent=True
    )


@pytest.mark.anyio
async def test_publish_files_without_silent_keeps_kwargs_unchanged():
    sent = SimpleNamespace(id=2)
    session = _session(send_file_side_effect=lambda *a, **kw: sent)
    got = await session.publish_files("chat", "p.png")
    assert got is sent
    session.raw_client.send_file.assert_awaited_once_with("chat", "p.png", caption=None, schedule=None)


@pytest.mark.anyio
async def test_pin_message():
    session = _session(pin_message_side_effect=lambda *a, **kw: None)
    await session.pin_message("chat", 42, notify=True)
    session.raw_client.pin_message.assert_awaited_once_with("chat", 42, notify=True)


@pytest.mark.anyio
async def test_unpin_message():
    session = _session(unpin_message_side_effect=lambda *a: None)
    await session.unpin_message("chat", 42)
    session.raw_client.unpin_message.assert_awaited_once_with("chat", 42)


@pytest.mark.anyio
async def test_unpin_message_all():
    session = _session(unpin_message_side_effect=lambda *a: None)
    await session.unpin_message("chat")
    session.raw_client.unpin_message.assert_awaited_once_with("chat", None)


@pytest.mark.anyio
async def test_delete_messages():
    session = _session(delete_messages_side_effect=lambda *a: [SimpleNamespace(pts_count=2)])
    result = await session.delete_messages("chat", [1, 2])
    session.raw_client.delete_messages.assert_awaited_once_with("chat", [1, 2])
    assert result is not None


@pytest.mark.anyio
async def test_send_reaction_invokes_raw_api_with_emoji():
    session = _session()

    await session.send_reaction("chat", 42, "❤️")

    request = session.raw_client.invoke.await_args.args[0]
    assert request.peer == "chat"
    assert request.msg_id == 42
    assert [reaction.emoticon for reaction in request.reaction] == ["❤"]


@pytest.mark.anyio
async def test_send_reaction_invokes_raw_api_with_empty_vector_to_clear():
    session = _session()

    await session.send_reaction("chat", 42, None)

    request = session.raw_client.invoke.await_args.args[0]
    assert request.peer == "chat"
    assert request.msg_id == 42
    assert request.reaction == []


@pytest.mark.anyio
async def test_send_reaction_rejects_unsupported_emoji_before_raw_api():
    session = _session()

    with pytest.raises(TelegramReactionInvalidError):
        await session.send_reaction("chat", 42, "✅")

    session.raw_client.invoke.assert_not_awaited()


@pytest.mark.anyio
async def test_fetch_message_reaction_users_raw_invokes_request():
    client = FakeCliTelethonClient()

    await fetch_message_reaction_users_raw(client, "chat", 42, limit=10)

    request = client.invoke.await_args.args[0]
    assert request.peer == "chat"
    assert request.id == 42
    assert request.limit == 10


# --- Batch 2: Media (#187) ---


@pytest.mark.anyio
async def test_download_media():
    session = _session(download_media_side_effect=lambda *a, **kw: "/tmp/photo.jpg")
    result = await session.download_media(SimpleNamespace(id=1), file="/tmp/photo.jpg")
    assert result == "/tmp/photo.jpg"
    session.raw_client.download_media.assert_awaited_once()


@pytest.mark.anyio
async def test_transport_session_reports_flood_from_awaitable():
    """Read-family flood (resolve): scoped report, account-wide column untouched (#1832)."""
    err = FloodWaitError(request=None, capture=0)
    err.seconds = 5
    pool = AsyncMock()
    pool._rate_limit_gate = TelegramRateLimitGate()
    session = TelegramTransportSession(
        FakeCliTelethonClient(entity_resolver=lambda _arg: err),
        phone="+7000",
        pool=pool,
    )

    with pytest.raises(HandledFloodWaitError) as exc_info:
        await session.get_entity("@channel")

    assert exc_info.value.info.wait_seconds == 5
    pool.report_family_flood.assert_awaited_once_with(
        "+7000", 5, "resolve", operation="telegram_resolve_entity"
    )
    pool.report_flood.assert_not_awaited()


@pytest.mark.anyio
async def test_transport_session_reports_flood_from_stream():
    """Read-family flood (history stream): scoped report (#1832)."""
    err = FloodWaitError(request=None, capture=0)
    err.seconds = 7

    class FloodingIterator:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise err

    pool = AsyncMock()
    pool._rate_limit_gate = TelegramRateLimitGate()
    session = TelegramTransportSession(
        FakeCliTelethonClient(iter_messages_factory=lambda *a, **kw: FloodingIterator()),
        phone="+7001",
        pool=pool,
    )

    with pytest.raises(HandledFloodWaitError) as exc_info:
        async for _msg in session.iter_messages("entity"):
            pass

    assert exc_info.value.info.wait_seconds == 7
    pool.report_family_flood.assert_awaited_once_with(
        "+7001", 7, "history", operation="telegram_stream_messages"
    )
    pool.report_flood.assert_not_awaited()


@pytest.mark.anyio
async def test_transport_session_closes_stream_iterator_on_flood():
    err = FloodWaitError(request=None, capture=0)
    err.seconds = 7

    class FloodingIterator:
        closed = False

        def __aiter__(self):
            return self

        async def __anext__(self):
            raise err

        async def aclose(self):
            self.closed = True

    iterator = FloodingIterator()
    pool = AsyncMock()
    session = TelegramTransportSession(
        FakeCliTelethonClient(iter_messages_factory=lambda *a, **kw: iterator),
        phone="+7001",
        pool=pool,
    )

    with pytest.raises(HandledFloodWaitError):
        async for _msg in session.iter_messages("entity"):
            pass

    assert iterator.closed is True
    pool.report_family_flood.assert_awaited_once()
    pool.report_flood.assert_not_awaited()


@pytest.mark.anyio
async def test_transport_session_reports_flood_from_raw_request():
    err = FloodWaitError(request=None, capture=0)
    err.seconds = 9
    pool = AsyncMock()
    session = TelegramTransportSession(
        FakeCliTelethonClient(invoke_side_effect=err),
        phone="+7002",
        pool=pool,
    )

    with pytest.raises(HandledFloodWaitError) as exc_info:
        await session(object())

    assert exc_info.value.info.wait_seconds == 9
    pool.report_flood.assert_awaited_once_with("+7002", 9)


# --- Batch 3: Participants (#188, #189) ---


@pytest.mark.anyio
async def test_get_participants():
    users = [SimpleNamespace(id=1, username="alice"), SimpleNamespace(id=2, username="bob")]
    session = _session(get_participants_side_effect=lambda *a, **kw: users)
    result = await session.get_participants("chat", limit=10, search="a")
    assert len(result) == 2
    session.raw_client.get_participants.assert_awaited_once()


@pytest.mark.anyio
async def test_stream_participants():
    users = [SimpleNamespace(id=1), SimpleNamespace(id=2)]
    session = _session(iter_participants_factory=lambda *a, **kw: AsyncIterMessages(users))
    collected = []
    async for u in session.stream_participants("chat"):
        collected.append(u)
    assert len(collected) == 2


@pytest.mark.anyio
async def test_edit_admin():
    session = _session(edit_admin_side_effect=lambda *a, **kw: None)
    await session.edit_admin("chat", "user", is_admin=True)
    session.raw_client.edit_admin.assert_awaited_once()


@pytest.mark.anyio
async def test_edit_permissions():
    session = _session(edit_permissions_side_effect=lambda *a, **kw: None)
    await session.edit_permissions("chat", "user", view_messages=False)
    session.raw_client.edit_permissions.assert_awaited_once()


@pytest.mark.anyio
async def test_kick_participant():
    session = _session(kick_participant_side_effect=lambda *a: None)
    await session.kick_participant("chat", "user")
    session.raw_client.kick_participant.assert_awaited_once_with("chat", "user")


# --- Batch 4: Stats, Folder, Read Acknowledge (#191, #192, #193) ---


@pytest.mark.anyio
async def test_edit_folder():
    session = _session(edit_folder_side_effect=lambda *a: None)
    await session.edit_folder("chat", 1)
    session.raw_client.edit_folder.assert_awaited_once_with("chat", 1)


@pytest.mark.anyio
async def test_send_read_acknowledge():
    session = _session(send_read_acknowledge_side_effect=lambda *a, **kw: True)
    result = await session.send_read_acknowledge("chat", max_id=100)
    assert result is True
    session.raw_client.send_read_acknowledge.assert_awaited_once()


@pytest.mark.anyio
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


@pytest.mark.anyio
async def test_upload_file():
    result = SimpleNamespace(id=42)
    session = _session(upload_file_side_effect=lambda *a, **kw: result)
    got = await session.upload_file("/tmp/photo.jpg")
    assert got is result
    session.raw_client.upload_file.assert_awaited_once()


# --- downloads ---


@pytest.mark.anyio
async def test_download_file():
    session = _session(download_file_side_effect=lambda *a, **kw: b"data")
    result = await session.download_file(SimpleNamespace(id=1), "/tmp/out.bin")
    assert result == b"data"


@pytest.mark.anyio
async def test_stream_download():
    chunks = [b"chunk1", b"chunk2"]
    session = _session(iter_download_factory=lambda *a, **kw: AsyncIterMessages(chunks))
    collected = []
    async for chunk in session.stream_download(SimpleNamespace(id=1)):
        collected.append(chunk)
    assert len(collected) == 2


# --- dialogs ---


@pytest.mark.anyio
async def test_stream_drafts():
    drafts = [SimpleNamespace(text="draft1")]
    session = _session(iter_drafts_factory=lambda: AsyncIterMessages(drafts))
    collected = []
    async for d in session.stream_drafts():
        collected.append(d)
    assert len(collected) == 1


@pytest.mark.anyio
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


@pytest.mark.anyio
async def test_is_bot():
    session = _session(is_bot_result=False)
    result = await session.is_bot()
    assert result is False


@pytest.mark.anyio
async def test_is_user_authorized():
    session = _session(authorized=True)
    result = await session.is_user_authorized()
    assert result is True


@pytest.mark.anyio
async def test_get_peer_id():
    session = _session(get_peer_id_side_effect=lambda p: 123456)
    result = await session.get_peer_id("@username")
    assert result == 123456


# --- chats ---


@pytest.mark.anyio
async def test_stream_admin_log():
    events = [SimpleNamespace(id=1), SimpleNamespace(id=2)]
    session = _session(iter_admin_log_factory=lambda *a, **kw: AsyncIterMessages(events))
    collected = []
    async for e in session.stream_admin_log("chat"):
        collected.append(e)
    assert len(collected) == 2


@pytest.mark.anyio
async def test_get_admin_log():
    events = [SimpleNamespace(id=1)]
    session = _session(get_admin_log_side_effect=lambda *a, **kw: events)
    result = await session.get_admin_log("chat")
    assert len(result) == 1


@pytest.mark.anyio
async def test_stream_profile_photos():
    photos = [SimpleNamespace(id=1)]
    session = _session(iter_profile_photos_factory=lambda *a, **kw: AsyncIterMessages(photos))
    collected = []
    async for p in session.stream_profile_photos("user"):
        collected.append(p)
    assert len(collected) == 1


@pytest.mark.anyio
async def test_get_profile_photos():
    photos = [SimpleNamespace(id=1), SimpleNamespace(id=2)]
    session = _session(get_profile_photos_side_effect=lambda *a, **kw: photos)
    result = await session.get_profile_photos("user")
    assert len(result) == 2


def test_action():
    session = _session(action_side_effect=lambda *a, **kw: MagicMock())
    result = session.action("chat", "typing")
    assert result is not None


@pytest.mark.anyio
async def test_get_permissions():
    perms = SimpleNamespace(send_messages=True, send_media=False)
    session = _session(get_permissions_side_effect=lambda *a: perms)
    result = await session.get_permissions("chat", "user")
    assert result.send_messages is True


# --- updates ---


@pytest.mark.anyio
async def test_set_receive_updates():
    session = _session(set_receive_updates_side_effect=lambda *a: None)
    await session.set_receive_updates(True)
    session.raw_client.set_receive_updates.assert_awaited_once()


@pytest.mark.anyio
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


@pytest.mark.anyio
async def test_catch_up():
    session = _session(catch_up_side_effect=lambda: None)
    await session.catch_up()
    session.raw_client.catch_up.assert_awaited_once()


# --- bots ---


@pytest.mark.anyio
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


@pytest.mark.anyio
async def test_end_takeout():
    session = _session(end_takeout_side_effect=lambda s: None)
    await session.end_takeout(True)
    session.raw_client.end_takeout.assert_awaited_once_with(True)


@pytest.mark.anyio
async def test_delete_chat_uses_legacy_chat_request():
    session = _session(invoke_side_effect=lambda request: request)

    await session.delete_chat(333)

    from telethon.tl.functions.messages import DeleteChatRequest

    request = session.raw_client.invoke.await_args.args[0]
    assert isinstance(request, DeleteChatRequest)
    assert request.chat_id == 333


# --- stats (invoke_request) ---


@pytest.mark.anyio
async def test_get_broadcast_stats():
    stats = SimpleNamespace(period=SimpleNamespace(min_date=0, max_date=0))
    session = _session(invoke_side_effect=lambda req: stats)
    result = await session.get_broadcast_stats("channel")
    assert result is stats
    session.raw_client.invoke.assert_awaited_once()


# --- flood_sleep_threshold (#495) ---


@pytest.mark.anyio
async def test_native_backend_disables_flood_auto_sleep():
    """NativeTelethonBackend.acquire_client must set flood_sleep_threshold=0
    so Telethon raises FloodWaitError instead of sleeping silently (#495)."""
    fake_client = SimpleNamespace(flood_sleep_threshold=60)
    auth = SimpleNamespace(create_client_from_session=AsyncMock(return_value=fake_client))
    backend = NativeTelethonBackend(auth)

    lease = await backend.acquire_client(
        Account(phone="+70001112233", session_string="session-xyz")
    )

    assert fake_client.flood_sleep_threshold == 0
    assert lease.phone == "+70001112233"
    assert lease.backend_name == "native"


@pytest.mark.anyio
async def test_telethon_cli_backend_disables_flood_auto_sleep(monkeypatch, tmp_path):
    """TelethonCliBackend.acquire_client must also set flood_sleep_threshold=0 (#495)."""
    fake_client = FakeCliTelethonClient()
    monkeypatch.setattr(
        "tg_agent.telegram.backends.telethon_cli_runtime.create_client",
        lambda namespace: fake_client,
    )

    materializer = SimpleNamespace(
        materialize=lambda phone, session_string: str(tmp_path / "session"),
        ensure_empty_env_file=lambda: str(tmp_path / ".env"),
    )
    auth = SimpleNamespace(api_id=1, api_hash="hash", proxy=None)
    backend = TelethonCliBackend(auth, materializer, transport="hybrid")

    lease = await backend.acquire_client(
        Account(phone="+70001112233", session_string="session-xyz")
    )

    assert fake_client.flood_sleep_threshold == 0
    assert lease.phone == "+70001112233"
    assert lease.backend_name == "telethon_cli"


@pytest.mark.anyio
async def test_telethon_cli_backend_applies_proxy_before_connect(monkeypatch, tmp_path):
    """TG_PROXY travels through TelethonCliBackend as a pre-connect set_proxy call."""
    fake_client = FakeCliTelethonClient()
    fake_client.set_proxy = MagicMock()
    monkeypatch.setattr(
        "tg_agent.telegram.backends.telethon_cli_runtime.create_client",
        lambda namespace: fake_client,
    )

    materializer = SimpleNamespace(
        materialize=lambda phone, session_string: str(tmp_path / "session"),
        ensure_empty_env_file=lambda: str(tmp_path / ".env"),
    )
    proxy = {"proxy_type": "socks5", "addr": "h", "port": 1080, "rdns": True}
    auth = SimpleNamespace(api_id=1, api_hash="hash", proxy=proxy)
    backend = TelethonCliBackend(auth, materializer, transport="hybrid")

    lease = await backend.acquire_client(
        Account(phone="+70001112233", session_string="session-xyz")
    )

    fake_client.set_proxy.assert_called_once_with(proxy)
    assert lease.backend_name == "telethon_cli"


@pytest.mark.anyio
async def test_telethon_cli_backend_disconnects_partial_client_on_acquire_error(monkeypatch, tmp_path):
    fake_client = FakeCliTelethonClient()
    fake_client.is_user_authorized = AsyncMock(side_effect=sqlite3.OperationalError("database is locked"))
    monkeypatch.setattr(
        "tg_agent.telegram.backends.telethon_cli_runtime.create_client",
        lambda namespace: fake_client,
    )

    materializer = SimpleNamespace(
        materialize=lambda phone, session_string: str(tmp_path / "session"),
        ensure_empty_env_file=lambda: str(tmp_path / ".env"),
    )
    auth = SimpleNamespace(api_id=1, api_hash="hash", proxy=None)
    backend = TelethonCliBackend(auth, materializer, transport="hybrid")

    with pytest.raises(sqlite3.OperationalError, match="database is locked"):
        await backend.acquire_client(
            Account(phone="+70001112233", session_string="session-xyz")
        )

    fake_client.disconnect.assert_awaited_once()


# ------------------------------------------------------------------
# Mutation slice #6 killer tests: proactive gate reservation contract
# ------------------------------------------------------------------


def _gated_session(gate):
    session = TelegramTransportSession(FakeCliTelethonClient())
    return session.with_flood_context(
        phone="+7000", pool=SimpleNamespace(_rate_limit_gate=gate)
    )


def test_reserve_gate_slot_passes_phone_category_and_slots():
    """The gate sees the session phone, the resolved category and the slot
    count: slots=1 reserves bare, slots>1 carries the count, per-peer always
    carries it (#mutation slice 6)."""
    from telethon_floodgate import TelegramRateLimitGate

    gate = TelegramRateLimitGate()
    gate.category_for = MagicMock(return_value="send")
    gate.try_acquire = MagicMock(return_value=0)
    session = _gated_session(gate)

    session._reserve_gate_slot("telegram_op")
    gate.try_acquire.assert_called_once_with("+7000", "send")

    gate.try_acquire.reset_mock()
    session._reserve_gate_slot("telegram_op", slots=3)
    gate.try_acquire.assert_called_once_with("+7000", "send", slots=3)

    gate.try_acquire.reset_mock()
    session._reserve_gate_slot("telegram_op", category="send_peer", peer="@chan")
    gate.try_acquire.assert_called_once_with("+7000", "send_peer", slots=1, peer="@chan")


def test_reserve_gate_slot_boundary_and_raised_contracts():
    """Any positive retry_after defers — including exactly 1 — and the raised
    errors carry phone + peer/category + retry seconds (#mutation slice 6)."""
    from telethon_floodgate import TelegramRateLimitGate

    from tg_agent.telegram.backends import TelegramPeerRateLimitedError, TelegramRateLimitedError

    gate = TelegramRateLimitGate()
    gate.category_for = MagicMock(return_value="send")
    gate.try_acquire = MagicMock(return_value=1)
    session = _gated_session(gate)

    with pytest.raises(TelegramPeerRateLimitedError) as peer_exc:
        session._reserve_gate_slot("op", category="send_peer", peer="@chan")
    assert peer_exc.value.phone == "+7000"
    assert peer_exc.value.peer == "@chan"
    assert peer_exc.value.retry_after_sec == 1

    gate.try_acquire.return_value = 7
    with pytest.raises(TelegramRateLimitedError) as cat_exc:
        session._reserve_gate_slot("op")
    assert cat_exc.value.phone == "+7000"
    assert cat_exc.value.category == "send"
    assert cat_exc.value.retry_after_sec == 7

    # retry_after == 0 → reserved, nothing raised.
    gate.try_acquire.return_value = 0
    session._reserve_gate_slot("op")


# --- Schema drift: TypeNotFoundError ≠ «нет такого чата» (#1549) ---


@pytest.mark.anyio
async def test_transport_session_translates_type_not_found_error_to_schema_drift():
    err = TypeNotFoundError(0x1C32B11C, b"\x1c\xb12\x1c")
    pool = AsyncMock()
    session = TelegramTransportSession(
        FakeCliTelethonClient(entity_resolver=lambda _arg: err),
        phone="+7003",
        pool=pool,
    )

    with pytest.raises(SchemaDriftError) as exc_info:
        await session.get_entity("@channel")

    text = str(exc_info.value)
    # Самоописывающее сообщение вместо телетоновского «Could not find …».
    assert "schema mismatch" in text
    assert "NOT a missing user or chat" in text
    assert "during telegram_resolve_entity" in text
    assert "Could not find a matching Constructor ID" not in text
    assert exc_info.value.invalid_constructor_id == 0x1C32B11C
    # Регресс: isinstance-классификаторы (unknown_tl_type_note в коллекторе) не сломаны.
    assert isinstance(exc_info.value, TypeNotFoundError)
    assert unknown_tl_type_note(exc_info.value) is not None


@pytest.mark.anyio
async def test_transport_session_translates_schema_drift_from_stream():
    err = TypeNotFoundError(0x1C32B11C, b"\x1c\xb12\x1c")

    class DriftIterator:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise err

    pool = AsyncMock()
    session = TelegramTransportSession(
        FakeCliTelethonClient(iter_messages_factory=lambda *a, **kw: DriftIterator()),
        phone="+7004",
        pool=pool,
    )

    with pytest.raises(SchemaDriftError) as exc_info:
        async for _msg in session.iter_messages("entity"):
            pass

    assert "schema mismatch during telegram_stream_messages" in str(exc_info.value)
    pool.report_flood.assert_not_awaited()


@pytest.mark.anyio
async def test_transport_session_leaves_entity_not_found_errors_untouched():
    # Настоящее «нет такой сущности» (ValueError от Telethon) схемным дрифтом
    # не маскируется — проходит через _run как было.
    err = ValueError("Could not find the input entity for PeerChannel(123)")
    pool = AsyncMock()
    session = TelegramTransportSession(
        FakeCliTelethonClient(entity_resolver=lambda _arg: err),
        phone="+7005",
        pool=pool,
    )

    with pytest.raises(ValueError) as exc_info:
        await session.get_entity("@channel")

    assert "input entity" in str(exc_info.value)
    assert not isinstance(exc_info.value, SchemaDriftError)
