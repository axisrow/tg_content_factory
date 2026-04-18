"""Static SQL-convention guards.

Cheap regex-level checks that run in CI and fail early when someone
reintroduces a known class of bug. Keep the patterns narrow and the
messages actionable — these tests should never need debugging.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parent.parent / "src"


def _iter_py_sources() -> list[Path]:
    return sorted(p for p in SRC_DIR.rglob("*.py") if "__pycache__" not in p.parts)


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
