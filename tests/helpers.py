"""Shared test helpers."""

from __future__ import annotations

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


def make_mock_pool(**kwargs) -> MagicMock:
    """Create a MagicMock pool with async methods properly mocked."""
    pool = MagicMock()
    pool.clients = {}
    pool.release_client = AsyncMock()
    pool.report_flood = AsyncMock()
    pool.get_client_by_phone = AsyncMock(return_value=None)
    # Simulate the per-phone dialogs-fetched tracking used by _collect_channel
    _dialogs_fetched: set[str] = set()
    pool.is_dialogs_fetched = lambda phone: phone in _dialogs_fetched
    pool.mark_dialogs_fetched = lambda phone: _dialogs_fetched.add(phone)
    for key, value in kwargs.items():
        setattr(pool, key, value)
    return pool
