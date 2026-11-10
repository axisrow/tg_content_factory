"""Static SQL-convention guards.

Cheap regex-level checks that run in CI and fail early when someone
reintroduces a known class of bug. Keep the patterns narrow and the
messages actionable — these tests should never need debugging.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parent.parent / "tg_agent"
# The web panel lives in packages/tg_webui since #1720 wave 3; the guard keeps
# covering it.
WEB_SRC_DIR = SRC_DIR.parent / "packages" / "tg_webui" / "src" / "tg_webui"


def _iter_py_sources() -> list[Path]:
    return sorted(
        p for root in (SRC_DIR, WEB_SRC_DIR) for p in root.rglob("*.py") if "__pycache__" not in p.parts
    )


# Matches "JOIN channels <alias> ON <anything>.id" where the RHS is the
# plain primary key of the channels row, which is almost always a bug:
# messages / channel_stats / embeddings / pipelines / forward_from_channel_id
# all store the Telegram channel_id, not the DB pk.
#
# Allowed joins (positive examples) always have "channel_id" on the channels
# side — e.g. `LEFT JOIN channels c ON m.channel_id = c.channel_id`.
_CHANNELS_JOIN_RE = re.compile(
    r"""JOIN \s+ channels \s+ (?P<alias>\w+) \s+ ON \s+
        (?P<expr> [^\n]*? ) \b
        (?: (?P=alias)\.id \b )
        (?! _ )                  # do not match c.id_something
    """,
    re.IGNORECASE | re.VERBOSE,
)


def test_no_channels_join_on_pk():
    """Regression guard for #... — always JOIN on channels.channel_id, not channels.id.

    channels.id is the DB pk; every sidecar table (messages, channel_stats,
    forward_from_channel_id, etc.) stores the Telegram channel_id. Joining on
    c.id silently matches zero rows for any channel whose pk differs from
    its Telegram id — which is the normal case.
    """
    offenders: list[str] = []
    for path in _iter_py_sources():
        text = path.read_text()
        for match in _CHANNELS_JOIN_RE.finditer(text):
            line_no = text.count("\n", 0, match.start()) + 1
            offenders.append(f"{path.relative_to(SRC_DIR.parent)}:{line_no}: {match.group(0).strip()}")
    assert not offenders, (
        "JOIN on channels.id found — use channels.channel_id instead.\n"
        "channels.id is the DB pk; Telegram id lives in channels.channel_id.\n"
        "Offenders:\n  - " + "\n  - ".join(offenders)
    )


@pytest.mark.parametrize(
    "good",
    [
        "LEFT JOIN channels c ON m.channel_id = c.channel_id",
        "JOIN channels c ON m.channel_id = c.channel_id",
        "LEFT JOIN channels c ON c.channel_id = m.forward_from_channel_id",
    ],
)
def test_regex_does_not_match_good_joins(good: str):
    assert _CHANNELS_JOIN_RE.search(good) is None


@pytest.mark.parametrize(
    "bad",
    [
        "JOIN channels c ON m.channel_id = c.id",
        "LEFT JOIN channels chn ON m.channel_id = chn.id",
    ],
)
def test_regex_matches_bad_joins(bad: str):
    assert _CHANNELS_JOIN_RE.search(bad) is not None


# Matches `async with <conn>.execute(...)` without await — the raw-aiosqlite
# idiom that breaks on ProfilingConnection (#1801): its execute() is an async
# function, so the un-awaited call yields a coroutine, not an async-CM cursor.
# Exempted files own the gateway store's raw aiosqlite.connect() connection
# (store.py:122 transaction), where the idiom is correct.
_ASYNC_WITH_EXECUTE_RE = re.compile(r"async with\s+(?!\s*await\b)[A-Za-z_][\w.]*\.execute\(")
_ASYNC_WITH_EXECUTE_EXEMPT = {
    SRC_DIR / "gateway" / "store.py",
    SRC_DIR / "gateway" / "notifications.py",
    SRC_DIR / "gateway" / "pool_storage.py",
}


def test_no_async_with_execute_without_await():
    """Regression guard for #1801 — `cur = await conn.execute(...)`, never `async with conn.execute(...)`.

    Under ENV=DEV the facade write connection is a ProfilingConnection whose
    execute() is a coroutine function; the un-awaited `async with` form raises
    TypeError ('coroutine' object does not support the asynchronous context
    manager protocol). `cur = await conn.execute(...)` is valid on both raw
    aiosqlite and ProfilingConnection.
    """
    offenders: list[str] = []
    for path in _iter_py_sources():
        if path in _ASYNC_WITH_EXECUTE_EXEMPT:
            continue
        text = path.read_text()
        for match in _ASYNC_WITH_EXECUTE_RE.finditer(text):
            line_no = text.count("\n", 0, match.start()) + 1
            offenders.append(f"{path.relative_to(SRC_DIR.parent)}:{line_no}: {match.group(0).strip()}")
    assert not offenders, (
        "`async with <conn>.execute(...)` without await found — use "
        "`cur = await conn.execute(...)` (valid on raw aiosqlite and "
        "ProfilingConnection alike, #1801).\n"
        "Offenders:\n  - " + "\n  - ".join(offenders)
    )
