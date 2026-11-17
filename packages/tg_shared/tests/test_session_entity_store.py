"""The session store of pool clients keeps one current row per peer (#1843).

Telethon's ``MemorySession`` — the base of ``StringSession`` — keeps entities
as a set of ``(id, hash, username, phone, name)`` tuples and only ever adds to
it. When a username moves from chat A to chat B both rows stay, and the lookup
by username returns whichever the set yields first. Since writes trust the
store for cached usernames (#1839), a write could reach the previous owner.

The tests go through ``TelegramAuth.create_client_from_session`` — the factory
every pool client comes from — so they pin the store real clients get. No
network: the connect step is replaced, everything else is real Telethon.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from telethon import TelegramClient, utils
from telethon.crypto import AuthKey
from telethon.sessions import MemorySession, StringSession
from telethon.tl.types import Channel, InputPeerChannel, PeerChannel, User

from tg_shared.auth import TelegramAuth

pytestmark = pytest.mark.native_backend_allowed

CHAT_A = 8000000001
CHAT_B = 8000000002
USERNAME = "moved_name"


def _session_string() -> str:
    session = MemorySession()
    session.set_dc(2, "149.154.167.51", 443)
    session.auth_key = AuthKey(data=bytes([7]) * 256)
    return StringSession.save(session)


def _channel(channel_id: int, username: str | None, title: str = "chat") -> Channel:
    return Channel(
        id=channel_id, title=title, username=username, access_hash=channel_id + 1, photo=None, date=None
    )


@pytest.fixture
async def store(monkeypatch):
    """The session object of a client built by the production factory."""
    monkeypatch.setattr("tg_shared.auth.connect_with_auth_key_retry", AsyncMock())
    monkeypatch.setattr(TelegramClient, "is_user_authorized", AsyncMock(return_value=True))
    client = await TelegramAuth(1, "x").create_client_from_session(_session_string())
    return client.session


def _rows_with_username(store, username: str = USERNAME) -> list[int]:
    return sorted(utils.resolve_id(row[0])[0] for row in store._entities if row[2] == username)


def _rows_for_peer(store, channel_id: int) -> list[tuple]:
    marked = utils.get_peer_id(PeerChannel(channel_id))
    return [row for row in store._entities if row[0] == marked]


@pytest.mark.anyio
async def test_moved_username_belongs_to_the_current_owner_only(store):
    """A refresh that saw the move leaves exactly one row carrying the username.

    Red before the fix: two rows (A's old one and B's), and the lookup by
    username returned either, depending on set order.
    """
    store.process_entities([_channel(CHAT_A, USERNAME)])
    # What a later warm delivers after the move: A without the name, B with it.
    store.process_entities([_channel(CHAT_A, None), _channel(CHAT_B, USERNAME)])

    assert _rows_with_username(store) == [CHAT_B]
    resolved = store.get_input_entity(f"@{USERNAME}")
    assert isinstance(resolved, InputPeerChannel) and resolved.channel_id == CHAT_B


@pytest.mark.anyio
async def test_moved_username_is_cleared_even_if_the_previous_owner_is_not_seen_again(store):
    """Seeing only the new owner is enough to take the username away from the old one."""
    store.process_entities([_channel(CHAT_A, USERNAME)])
    store.process_entities([_channel(CHAT_B, USERNAME)])

    assert _rows_with_username(store) == [CHAT_B]


@pytest.mark.anyio
async def test_previous_owner_stays_resolvable_by_id(store):
    """Losing a username must not make the chat unknown: id and access hash stay."""
    store.process_entities([_channel(CHAT_A, USERNAME)])
    store.process_entities([_channel(CHAT_B, USERNAME)])

    resolved = store.get_input_entity(PeerChannel(CHAT_A))
    assert isinstance(resolved, InputPeerChannel)
    assert (resolved.channel_id, resolved.access_hash) == (CHAT_A, CHAT_A + 1)


@pytest.mark.anyio
async def test_newer_row_replaces_the_older_row_of_the_same_peer(store):
    """Rows of one peer do not accumulate across refreshes."""
    store.process_entities([_channel(CHAT_A, USERNAME, title="old title")])
    store.process_entities([_channel(CHAT_A, USERNAME, title="new title")])

    rows = _rows_for_peer(store, CHAT_A)
    assert len(rows) == 1
    assert rows[0][4] == "new title"


@pytest.mark.anyio
async def test_unchanged_entities_leave_the_store_untouched(store):
    """The common case — the same entities again — changes nothing."""
    entities = [_channel(CHAT_A, USERNAME), _channel(CHAT_B, None), User(id=5, access_hash=6, first_name="u")]
    store.process_entities(entities)
    before = set(store._entities)

    store.process_entities(entities)

    assert store._entities == before
    assert len(before) == 3


@pytest.mark.anyio
async def test_session_string_round_trip_is_unchanged(store):
    """The entity store is in-memory only; the exported session string is the same."""
    assert StringSession.save(store) == _session_string()
