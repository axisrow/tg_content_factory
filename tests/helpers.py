"""Shared test helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock


class AsyncIterEmpty:
    """Async iterator that yields nothing."""

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


class AsyncIterMessages:
    """Async iterator over a list of messages."""

    def __init__(self, messages):
        self._iter = iter(messages)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._iter)
        except StopIteration:
            raise StopAsyncIteration


class FakeTelethonClient:
    """Controllable Telethon-like client for collector tests."""

    def __init__(
        self,
        *,
        entity_resolver=None,
        dialogs=None,
        iter_messages_factory=None,
    ):
        self._entity_resolver = entity_resolver or (lambda arg: SimpleNamespace())
        self._dialogs = [] if dialogs is None else dialogs
        self._iter_messages_factory = iter_messages_factory or (lambda *a, **kw: AsyncIterEmpty())
        self.get_entity = AsyncMock(side_effect=self._get_entity)
        self.get_dialogs = AsyncMock(side_effect=self._get_dialogs)
        self.iter_messages = MagicMock(side_effect=self._iter_messages)

    async def _get_entity(self, arg):
        result = self._entity_resolver(arg)
        if isinstance(result, Exception):
            raise result
        return result

    async def _get_dialogs(self):
        if isinstance(self._dialogs, Exception):
            raise self._dialogs
        return self._dialogs

    def _iter_messages(self, *args, **kwargs):
        return self._iter_messages_factory(*args, **kwargs)


class FakeClientPool(MagicMock):
    """Pool double with controllable async methods and dialog cache state."""

    def __init__(self, **kwargs):
        super().__init__()
        self.clients = kwargs.pop("clients", {})
        self.release_client = kwargs.pop("release_client", AsyncMock())
        self.report_flood = kwargs.pop("report_flood", AsyncMock())
        self.get_client_by_phone = kwargs.pop("get_client_by_phone", AsyncMock(return_value=None))
        self.get_available_client = kwargs.pop("get_available_client", AsyncMock(return_value=None))
        self.get_stats_availability = kwargs.pop("get_stats_availability", AsyncMock())
        self._dialogs_fetched: set[str] = set()
        self.is_dialogs_fetched = lambda phone: phone in self._dialogs_fetched
        self.mark_dialogs_fetched = lambda phone: self._dialogs_fetched.add(phone)
        for key, value in kwargs.items():
            setattr(self, key, value)


def make_mock_message(msg_id, text=None, media=None, sender_id=None, *, date=None):
    return SimpleNamespace(
        id=msg_id,
        text=text,
        media=media,
        sender_id=sender_id,
        sender=None,
        date=date or datetime(2025, 1, 1, tzinfo=timezone.utc),
    )


def make_stats_availability(state: str, *, next_available_at_utc=None):
    return SimpleNamespace(state=state, next_available_at_utc=next_available_at_utc)


def make_mock_pool(**kwargs) -> MagicMock:
    """Create a MagicMock pool with async methods properly mocked."""
    return FakeClientPool(**kwargs)
