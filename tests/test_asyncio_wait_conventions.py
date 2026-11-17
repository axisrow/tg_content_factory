"""Static asyncio-convention guards.

Companion to test_sql_conventions.py: cheap regex-level checks that run
in CI and fail early when someone reintroduces a known class of bug.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parent.parent / "tg_agent"
TESTS_DIR = SRC_DIR.parent / "tests"
SHARED_SRC_DIR = SRC_DIR.parent / "packages" / "tg_shared" / "src" / "tg_shared"


def _iter_py_sources() -> list[Path]:
    roots = (SRC_DIR, TESTS_DIR, SHARED_SRC_DIR)
    return sorted(
        p
        for root in roots
        for p in root.rglob("*.py")
        if "__pycache__" not in p.parts and p != Path(__file__)
    )


# Matches the 3.11 hang class from #1840/#1850: asyncio.wait_for wrapping
# an ``.wait()`` call. On CPython 3.11 wait_for can leak an unreleased
# waiter when the awaited event fires exactly as the timeout lands
# (rewritten in 3.12; 3.13 is immune, which is why local green lanes hid
# it) — the caller then sleeps forever in an idle loop with its timeout
# handle gone. The replacement is the asyncio.timeout() context form,
# whose cancellation goes through the await, so the waiter is always
# released. ``[^,()]*`` keeps the match to a direct argument: a nested
# call wrapping the ``.wait()`` — e.g. ``wait_for(bar(x).wait(), t)`` —
# is out of scope for this regex (regex-level convention gate, same as
# test_sql_conventions.py).
_WAIT_FOR_EVENT_WAIT_RE = re.compile(
    r"""\b (?:asyncio\.)? wait_for \s* \( \s*
        [^,()]* \. wait \s* \(""",
    re.VERBOSE,
)


def test_no_wait_for_around_event_wait():
    """Regression guard for #1850 — never wrap ``.wait()`` in asyncio.wait_for.

    On CPython 3.11 ``asyncio.wait_for(<event>.wait(), t)`` can leak an
    unreleased waiter when the event fires exactly as the timeout lands
    (#1840 CI serial hang); asyncio.timeout() is the always-releasing
    form. Any new site must use::

        async with asyncio.timeout(t):
            await <event>.wait()
    """
    offenders: list[str] = []
    for path in _iter_py_sources():
        text = path.read_text()
        for match in _WAIT_FOR_EVENT_WAIT_RE.finditer(text):
            line_no = text.count("\n", 0, match.start()) + 1
            offenders.append(f"{path.relative_to(SRC_DIR.parent)}:{line_no}: {match.group(0).strip()}")
    assert not offenders, (
        "asyncio.wait_for(<x>.wait(), t) found — the 3.11 unreleased-waiter "
        "hang class from #1840/#1850. Use the asyncio.timeout() context form "
        "instead (always releases the waiter).\n"
        "Offenders:\n  - " + "\n  - ".join(offenders)
    )


@pytest.mark.parametrize(
    "good",
    [
        "async with asyncio.timeout(1.0):\n    await self._resume_gate.wait()",
        "await asyncio.wait_for(self._queue.get(), timeout=1.0)",
        "await asyncio.wait_for(fetch(), timeout=5.0)",
        "asyncio.wait_for(self._channels.tasks.fetch(), timeout=t)",
        "await asyncio.wait_for(client.get_me(), timeout=15.0)",
        "def wait_for_heartbeat() -> None: ...",
        "await pool.get_client_by_phone(phone, wait_for_flood=True)",
    ],
)
def test_regex_does_not_match_allowed_calls(good: str):
    assert _WAIT_FOR_EVENT_WAIT_RE.search(good) is None


@pytest.mark.parametrize(
    "bad",
    [
        "await asyncio.wait_for(self._resume_gate.wait(), timeout=1.0)",
        "await asyncio.wait_for(stop_event.wait(), HEARTBEAT_INTERVAL_SEC)",
        "from asyncio import wait_for\nawait wait_for(ev.wait(), 1)",
        "await asyncio.wait_for(collector.started.wait(), timeout=1.0)",
    ],
)
def test_regex_matches_banned_pattern(bad: str):
    assert _WAIT_FOR_EVENT_WAIT_RE.search(bad) is not None
