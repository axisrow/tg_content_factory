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


def test_resolve_and_reaction_keep_their_existing_dedicated_gates() -> None:
    gate = TelegramRateLimitGate(
        category_limits={"default": RateLimitSpec(max_calls=1, window_sec=60)},
    )

    for operation, expected_category in (
        ("telegram_resolve_entity", "resolve"),
        ("telegram_resolve_input_entity", "resolve"),
        ("telegram_send_reaction", "reaction"),
    ):
        category = gate.category_for(operation)
        assert category == expected_category
        assert gate.try_acquire("+1", category) == 0.0
        assert gate.try_acquire("+1", category) == 0.0


@pytest.mark.parametrize(
    "operation",
    (
        "resolve_channel_warm_dialog_cache",
        "fetch_channel_meta_warm_dialog_cache",
        "get_forum_topics_warm_dialog_cache",
        "search_warm_dialog_cache",
        "leave_channels:123_warm_dialog_cache",
        "delete_dialogs:123_warm_dialog_cache",
        "telegram_stream_dialogs",
    ),
)
def test_decorated_dialog_operations_use_the_dialogs_bucket(operation: str) -> None:
    assert TelegramRateLimitGate.category_for(operation) == "dialogs"


def test_phase_two_categories_are_separately_calibrated() -> None:
    gate = TelegramRateLimitGate()
    assert gate.category_for("telegram_stream_messages") == "history"
    assert gate.category_for("telegram_edit_admin") == "admin_action"
    assert gate.category_for("telegram_send_message") == "send"
    assert gate.category_for("telegram_create_channel") == "channel_lifecycle"

    # A history stream must not consume a write-operation slot.
    assert gate.try_acquire("+1", "history") == 0.0
    assert gate.try_acquire("+1", "send") == 0.0


def test_compound_slot_reservation_is_atomic() -> None:
    clock = _Clock()
    gate = TelegramRateLimitGate(time_func=clock)

    assert gate.try_acquire("+1", "channel_lifecycle", slots=2) == 0.0
    assert gate.try_acquire("+1", "channel_lifecycle", slots=2) == 300.0
    # The rejected two-slot reservation must not consume the one remaining slot.
    assert gate.try_acquire("+1", "channel_lifecycle") == 0.0


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
