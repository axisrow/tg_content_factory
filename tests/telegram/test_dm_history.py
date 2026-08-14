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

from src.telegram.dm_history import read_dialog_history


def _fail(*_args, **_kwargs):
    raise AssertionError("read_dialog_history must not connect/disconnect the pool's client")


class _FakePoolClient:
    """Stands in for a live Telethon client leased from ClientPool.

    connect()/disconnect() would tear down (or hijack) the pool's own live
    connection, so both are wired to fail the test if ever called.
    """

    connect = _fail
    disconnect = _fail

    def __init__(self, messages):
        self._messages = messages

    def iter_messages(self, peer, limit=50, offset_id=0):
        async def _gen():
            for m in self._messages:
                yield m

        return _gen()


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


def test_client_factory_ignores_args_returns_fixed_client():
    from src.telegram.dm_history import _client_factory

    sentinel = MagicMock()
    factory = _client_factory(sentinel)
    assert factory("anything", 1, "hash") is sentinel
    assert factory() is sentinel
