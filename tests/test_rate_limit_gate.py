from __future__ import annotations

import pytest

from src.telegram.backends import TelegramTransportSession
from src.telegram.rate_limit_gate import (
    RateLimitSpec,
    TelegramRateLimitedError,
    TelegramRateLimitGate,
)


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_dialogs_gate_is_per_phone_and_conservative() -> None:
    clock = _Clock()
    gate = TelegramRateLimitGate(time_func=clock)

    assert gate.try_acquire("+1", "dialogs") == 0.0
    assert gate.try_acquire("+2", "dialogs") == 0.0
    assert gate.try_acquire("+1", "dialogs") == 60.0


def test_categories_have_independent_buckets() -> None:
    clock = _Clock()
    gate = TelegramRateLimitGate(
        category_limits={"history": RateLimitSpec(max_calls=1, window_sec=60)},
        time_func=clock,
    )
    assert gate.try_acquire("+1", "dialogs") == 0.0
    assert gate.try_acquire("+1", "history") == 0.0
    assert gate.try_acquire("+1", "history") == 60.0


@pytest.mark.asyncio
async def test_issue_1330_repeated_get_dialogs_is_stopped_before_telegram() -> None:
    calls = 0

    class Client:
        def get_dialogs(self):
            async def result():
                nonlocal calls
                calls += 1
                return []

            return result()

    class Pool:
        _rate_limit_gate = TelegramRateLimitGate()

    session = TelegramTransportSession(Client(), phone="+66...2247", pool=Pool())
    assert await session.get_dialogs() == []
    for _ in range(5):
        with pytest.raises(TelegramRateLimitedError):
            await session.get_dialogs()
    assert calls == 1


@pytest.mark.asyncio
async def test_unbound_session_is_noop_safe() -> None:
    class Client:
        async def get_dialogs(self):
            return []

    assert await TelegramTransportSession(Client()).get_dialogs() == []
