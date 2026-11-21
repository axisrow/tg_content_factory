"""Unit tests for src/telegram/dm_history.py — the tg_messenger integration seam.

The key invariant under test: read_dialog_history must never open a second
Telethon connection on top of the pool's live one (see the module docstring
and the plan's "Главное архитектурное решение" — opening a second connection
on the same session risks desyncing the account's MTProto state). We assert
this directly by making the fake client's connect/disconnect raise.
"""
from __future__ import annotations

import datetime as dt
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from tg_agent.telegram.dm_history import read_dialog_history, read_dialog_history_since


def _fail(*_args, **_kwargs):
    raise AssertionError("read_dialog_history must not connect/disconnect the pool's client")


class _FakePoolClient:
    """Stands in for a live Telethon client leased from ClientPool.

    connect()/disconnect() would tear down (or hijack) the pool's own live
    connection, so both are wired to fail the test if ever called.
    Contract for tg-messenger 0.1.1 + floodgate 0.1.2: ``get_input_entity``
    (warm-entity path in history()), iterator exposes callable ``.client``
    (floodgate's gate_message_iterator requirement), raw ``__call__`` must
    never fire in these tests.
    """

    connect = _fail
    disconnect = _fail

    def __init__(self, messages):
        self._messages = messages

    async def get_input_entity(self, pid):
        return SimpleNamespace(user_id=None, channel_id=None)

    def __call__(self, request):
        raise AssertionError("raw RPC via __call__ is not expected in these tests")

    def iter_messages(self, peer, limit=50, offset_id=0):
        async def _gen():
            for m in self._messages:
                yield m

        return _FakeGatedIterator(_gen(), self)


class _FakeGatedIterator:
    """Telethon-RequestIter stand-in: floodgate 0.1.2 requires ``.client`` callable."""

    def __init__(self, agen, client):
        self._agen = agen
        self.client = client

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self._agen.__anext__()


@pytest.mark.anyio
async def test_read_dialog_history_never_connects_or_disconnects():
    msg = SimpleNamespace(
        id=1, sender_id=10, out=False, date=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
        text="hi", media=None, reply_to=None, forward=None, sender=None,
    )
    client = _FakePoolClient([msg])
    # Would raise via _fail if StandaloneTelegramClient's own connect/disconnect
    # were ever invoked on our client_factory-provided object.
    result = await read_dialog_history(client, api_id=123, api_hash="hash", peer=-100, limit=50)
    assert len(result) == 1
    assert result[0].text == "hi"


@pytest.mark.anyio
async def test_read_dialog_history_maps_fields():
    date = dt.datetime(2026, 6, 1, 12, 0, tzinfo=dt.timezone.utc)
    reply = SimpleNamespace(reply_to_msg_id=99)
    msg = SimpleNamespace(
        id=7, sender_id=55, out=True, date=date, text="reply text",
        media=None, reply_to=reply, forward=object(), sender=None,
    )
    client = _FakePoolClient([msg])
    result = await read_dialog_history(client, api_id=123, api_hash="hash", peer=-100, limit=10)
    assert len(result) == 1
    dm = result[0]
    assert dm.id == 7
    assert dm.sender_id == 55
    assert dm.out is True
    assert dm.date == date
    assert dm.text == "reply text"
    assert dm.reply_to_id == 99
    assert dm.is_forward is True
    assert dm.media_type is None


@pytest.mark.anyio
async def test_read_dialog_history_maps_sender_name():
    date = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    sender = SimpleNamespace(id=1, first_name="Alice", last_name="B", username="alice")
    msg = SimpleNamespace(
        id=1, sender_id=1, out=False, date=date, text="x",
        media=None, reply_to=None, forward=None, sender=sender,
    )
    client = _FakePoolClient([msg])
    result = await read_dialog_history(client, api_id=123, api_hash="hash", peer=-100, limit=10)
    assert result[0].sender_name == "Alice B"


@pytest.mark.anyio
async def test_read_dialog_history_empty():
    client = _FakePoolClient([])
    result = await read_dialog_history(client, api_id=123, api_hash="hash", peer=-100, limit=10)
    assert result == []


class _FakeForwardClient:
    """A pool client for the catch-up read: pages oldest-first above ``min_id``."""

    connect = _fail
    disconnect = _fail

    def __init__(self, messages):
        self._messages = messages

    def iter_messages(self, peer, limit=200, min_id=0, reverse=False):
        assert reverse is True

        async def _gen():
            for m in self._messages:
                if m.id > min_id:
                    yield m

        return _gen()


def _message(msg_id=1, text="hi"):
    return SimpleNamespace(
        id=msg_id, sender_id=10, out=False, date=dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc),
        text=text, media=None, reply_to=None, forward=None, sender=None,
    )


# A gateway project has no API credentials of its own (the gateway holds them),
# and the pool client is injected, so nothing here is ever built from them.
_NO_CREDENTIALS = [(0, ""), (0, "hash"), (123, " ")]
_NO_CREDENTIALS_IDS = ["neither", "no-api-id", "blank-api-hash"]


@pytest.mark.anyio
@pytest.mark.parametrize(("api_id", "api_hash"), _NO_CREDENTIALS, ids=_NO_CREDENTIALS_IDS)
async def test_read_dialog_history_needs_no_credentials_for_an_injected_client(api_id, api_hash):
    client = _FakePoolClient([_message()])

    result = await read_dialog_history(client, api_id=api_id, api_hash=api_hash, peer=-100)

    assert [m.text for m in result] == ["hi"]


@pytest.mark.anyio
@pytest.mark.parametrize(("api_id", "api_hash"), _NO_CREDENTIALS, ids=_NO_CREDENTIALS_IDS)
async def test_read_dialog_history_since_needs_no_credentials_for_an_injected_client(api_id, api_hash):
    client = _FakeForwardClient([_message(1, "old"), _message(2, "new")])

    result = await read_dialog_history_since(client, api_id=api_id, api_hash=api_hash, peer=5, min_id=1)

    assert [(m.id, m.text) for m in result] == [(2, "new")]


def test_client_factory_ignores_args_returns_fixed_client():
    from tg_agent.telegram.dm_history import _client_factory

    sentinel = MagicMock()
    factory = _client_factory(sentinel)
    assert factory("anything", 1, "hash") is sentinel
    assert factory() is sentinel
