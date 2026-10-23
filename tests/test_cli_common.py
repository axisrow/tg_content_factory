"""Tests for CLI common utilities."""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import typer

from tg_agent.cli.commands.common import resolve_channel, resolve_date_bounds


def test_resolve_channel_by_pk():
    """Test resolve channel by primary key."""
    channels = [
        MagicMock(id=1, channel_id=100, username="channel1"),
        MagicMock(id=2, channel_id=200, username="channel2"),
    ]
    result = resolve_channel(channels, "1")
    assert result.id == 1


def test_resolve_channel_by_channel_id():
    """Test resolve channel by channel_id."""
    channels = [
        MagicMock(id=1, channel_id=100, username="channel1"),
        MagicMock(id=2, channel_id=200, username="channel2"),
    ]
    result = resolve_channel(channels, "200")
    assert result.channel_id == 200


def test_resolve_channel_by_username():
    """Test resolve channel by username."""
    channels = [
        MagicMock(id=1, channel_id=100, username="channel1"),
        MagicMock(id=2, channel_id=200, username="channel2"),
    ]
    result = resolve_channel(channels, "@channel1")
    assert result.username == "channel1"


def test_resolve_channel_by_username_without_at():
    """Test resolve channel by username without @."""
    channels = [
        MagicMock(id=1, channel_id=100, username="channel1"),
        MagicMock(id=2, channel_id=200, username="channel2"),
    ]
    result = resolve_channel(channels, "channel2")
    assert result.username == "channel2"


def test_resolve_channel_not_found():
    """Test resolve channel not found."""
    channels = [
        MagicMock(id=1, channel_id=100, username="channel1"),
    ]
    result = resolve_channel(channels, "999")
    assert result is None


def test_resolve_channel_username_not_found():
    """Test resolve channel by username not found."""
    channels = [
        MagicMock(id=1, channel_id=100, username="channel1"),
    ]
    result = resolve_channel(channels, "@nonexistent")
    assert result is None


def test_resolve_channel_case_insensitive():
    """Test resolve channel is case insensitive."""
    channels = [
        MagicMock(id=1, channel_id=100, username="TestChannel"),
    ]
    result = resolve_channel(channels, "@testchannel")
    assert result is not None


def test_resolve_channel_no_username():
    """Test resolve channel with no username."""
    channels = [
        MagicMock(id=1, channel_id=100, username=None),
    ]
    result = resolve_channel(channels, "@nonexistent")
    assert result is None


# resolve_date_bounds (#1593)


def test_resolve_date_bounds_normalizes_relaxed_dates():
    """2026-10-5 ≡ 2026-10-05: relaxed day/month get canonicalized (#1593)."""
    assert resolve_date_bounds("2026-10-5", "2026-10-9") == ("2026-10-05", "2026-10-09")


def test_resolve_date_bounds_passes_canonical_and_none_through():
    assert resolve_date_bounds("2026-10-05", None) == ("2026-10-05", None)


def test_resolve_date_bounds_rejects_garbage():
    """Non-date input becomes a usage error, never a silent wrong window (#1593)."""
    with pytest.raises(typer.BadParameter):
        resolve_date_bounds("yesterday", None)
    with pytest.raises(typer.BadParameter):
        resolve_date_bounds(None, "2026-13-45")


def test_resolve_date_bounds_equivalent_tz_offsets_match_utc():
    """Equivalent instants with different offsets normalize to UTC (#1666).

    Bounds are compared as ISO TEXT against stored ``m.date`` values, so
    ``13:00+03:00`` must render as ``10:00+00:00`` — the raw offset breaks the
    lexicographic SQL comparison and silently drops the boundary message.
    """
    assert resolve_date_bounds("2026-10-20T13:00:00+03:00", "2026-10-20T18:00:00+05:00") == (
        "2026-10-20T10:00:00+00:00",
        "2026-10-20T13:00:00+00:00",
    )


# sync_prompt — Ctrl-C-safe stdin reads for confirmation gates (#1590)


def test_sync_prompt_returns_the_typed_answer(monkeypatch):
    monkeypatch.setattr("builtins.input", lambda _p: "y")
    from tg_agent.cli.commands.common import sync_prompt

    assert sync_prompt("Continue? [y/N] ") == "y"


def test_sync_prompt_installs_default_sigint_handler_while_reading(monkeypatch):
    """While blocked in input(), the first Ctrl-C must raise KeyboardInterrupt
    immediately — asyncio.Runner's task-cancelling handler cannot unblock a
    sync read, which left the process hanging with open aiosqlite threads."""
    import signal

    from tg_agent.cli.commands.common import sync_prompt

    seen = {}

    def fake_input(_p):
        seen["handler"] = signal.getsignal(signal.SIGINT)
        return "n"

    monkeypatch.setattr("builtins.input", fake_input)
    original = signal.getsignal(signal.SIGINT)
    try:
        sync_prompt("Continue? [y/N] ")
        assert seen["handler"] is signal.default_int_handler
    finally:
        assert signal.getsignal(signal.SIGINT) is original


def test_sync_prompt_restores_previous_handler(monkeypatch):
    import signal

    from tg_agent.cli.commands.common import sync_prompt

    def sentinel_handler(signum, frame):  # pragma: no cover - never fired in-test
        raise AssertionError

    monkeypatch.setattr("builtins.input", lambda _p: "y")
    previous = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, sentinel_handler)
    try:
        sync_prompt("Continue? [y/N] ")
        assert signal.getsignal(signal.SIGINT) is sentinel_handler
    finally:
        signal.signal(signal.SIGINT, previous)
