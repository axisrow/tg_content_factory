"""Regression guards for the decision-provenance principle (#1316, epic #1307).

Automation decides only where automation decided before; operator decisions
are never silently overridden. Two layers live here:

1. A static convention scan (in the spirit of test_sql_conventions.py): raw
   SQL ``UPDATE`` of a provenance-guarded column must live in a pinned file —
   inside an origin-aware repository method, an origin-guarded analyzer
   statement, or a one-shot migration backfill. A raw write added anywhere
   else fails with instructions, so unguarded writes cannot creep back in.
2. A behavioral guard: the collector auto-purge never deletes messages of a
   human channel, even when the human provenance arrived without the legacy
   ``manual`` flag (the private-channel provenance backfill #1410 sets
   ``filtered_origin='human'`` without touching ``filter_flags``).
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import pytest

from tests.helpers import make_mock_pool
from tg_agent.config import SchedulerConfig
from tg_agent.models import Channel, Message
from tg_agent.telegram.collector import Collector

SRC_DIR = Path(__file__).resolve().parent.parent / "tg_agent"


def _iter_py_sources() -> list[Path]:
    return sorted(p for p in SRC_DIR.rglob("*.py") if "__pycache__" not in p.parts)


# Provenance-guarded columns: a value here encodes WHO decided (auto vs
# human), so a raw write that flips the value without its origin column (or
# without the origin guard in its WHERE) can override a human decision.
_GUARDED_FIELDS: dict[str, set[str]] = {
    "channels": {"is_active", "active_origin", "is_filtered", "filtered_origin"},
    "accounts": {"is_active", "active_origin"},
    "generation_runs": {"moderation_status", "moderation_origin"},
}

# Where guarded writes may live: origin-aware repository methods, the
# analyzer's human-guarded auto-verdict statements, one-shot migration
# backfills. Extending this set is a reviewed decision, not a formality.
_ALLOWED_WRITE_SITES: dict[str, set[str]] = {
    "channels": {
        "tg_agent/database/repositories/channels.py",
        "tg_agent/filters/analyzer.py",
        "tg_agent/database/migrations.py",
    },
    "accounts": {
        "tg_agent/database/repositories/accounts.py",
        "tg_agent/database/migrations.py",
    },
    "generation_runs": {
        "tg_agent/database/repositories/generation_runs.py",
        "tg_agent/database/migrations.py",
    },
}

_PROVENANCE_UPDATE_RE = re.compile(
    r"\bUPDATE\s+(?P<table>channels|accounts|generation_runs)\s+SET\s+"
    r"(?P<set_clause>[^;\"]{0,400})",
    re.IGNORECASE | re.DOTALL,
)


def _guarded_fields(table: str, set_clause: str) -> set[str]:
    return {field for field in _GUARDED_FIELDS[table] if re.search(rf"\b{field}\b", set_clause)}


# mutmut_skip: source-text scan — mutant variant copies mutate pinned SQL
# literals (e.g. accounts -> ACCOUNTS) and trip the scanner inside the sandbox.
@pytest.mark.mutmut_skip
def test_no_raw_updates_of_provenance_guarded_columns():
    offenders: list[str] = []
    for path in _iter_py_sources():
        rel = path.relative_to(SRC_DIR.parent).as_posix()
        text = path.read_text()
        for match in _PROVENANCE_UPDATE_RE.finditer(text):
            fields = _guarded_fields(match.group("table"), match.group("set_clause"))
            if not fields or rel in _ALLOWED_WRITE_SITES[match.group("table")]:
                continue
            line_no = text.count("\n", 0, match.start()) + 1
            offenders.append(f"{rel}:{line_no}: sets {', '.join(sorted(fields))}")
    assert not offenders, (
        "Raw UPDATE of a provenance-guarded column outside a pinned site —\n"
        "automation must never override operator decisions (#1307).\n"
        "Write through the origin-aware repository methods instead: "
        "db.repos.channels.set_channel_active / set_channel_filtered / set_filtered_bulk / "
        "reset_all_channel_filters, db.repos.accounts.set_account_active, "
        "db.repos.generation_runs.set_moderation_status — they take origin= and journal "
        "suppressions to the decisions table. If a new pinned site is genuinely required, "
        "extend _ALLOWED_WRITE_SITES consciously and keep the origin guard "
        "(? = 'human' OR <origin> != 'human') in the statement.\n"
        "Offenders:\n  - " + "\n  - ".join(offenders)
    )


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE channels SET is_active = 0 WHERE id = 1",
        "UPDATE channels SET is_filtered = 1, filter_flags = 'x' \nWHERE channel_id = ?",
        "update accounts set is_active = 0 where phone = ?",
        "UPDATE generation_runs SET moderation_status = 'approved' WHERE id = ?",
    ],
)
def test_regex_matches_provenance_updates(sql: str):
    match = _PROVENANCE_UPDATE_RE.search(sql)
    assert match is not None
    assert _guarded_fields(match.group("table"), match.group("set_clause"))


@pytest.mark.parametrize(
    "sql",
    [
        "UPDATE channels SET title = ?, username = ? WHERE channel_id = ?",
        "UPDATE channels SET channel_type=? WHERE channel_id=?",
        "UPDATE messages SET text = ? WHERE id = ?",
        "UPDATE accounts SET is_premium = ? WHERE phone = ?",
        "UPDATE generation_runs SET image_url = ? WHERE id = ?",
        "SELECT * FROM channels WHERE is_active = 1",
    ],
)
def test_regex_ignores_non_provenance_updates(sql: str):
    match = _PROVENANCE_UPDATE_RE.search(sql)
    assert match is None or not _guarded_fields(match.group("table"), match.group("set_clause"))


# --- behavioral guard: auto-purge never touches human channels --------------


async def test_auto_delete_never_runs_on_human_channel(db):
    """filtered_origin='human' keeps messages even without the 'manual' flag.

    Reproduces the state the private-channel provenance backfill (#1410)
    leaves behind: an auto-filtered channel whose origin was flipped to
    'human' without touching filter_flags.
    """
    pk = await db.add_channel(Channel(channel_id=-100316, title="Human Private"))
    await db.insert_messages_batch(
        [
            Message(
                channel_id=-100316,
                message_id=i,
                text="x",
                date=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
            for i in range(1, 4)
        ]
    )
    # Auto verdict first (is_filtered=1, origin stays 'auto'), then the
    # backfill-style human provenance flip that adds no flag.
    await db.repos.channels.set_channel_filter_flags(pk, "low_subscriber_ratio")
    await db.execute_write(
        "UPDATE channels SET filtered_origin = 'human' WHERE id = ?", (pk,)
    )
    await db.set_setting("auto_delete_on_collect", "1")

    stored = await db.get_channel_by_channel_id(-100316)
    assert stored is not None
    assert (stored.is_filtered, stored.filtered_origin) == (True, "human")
    assert "manual" not in (stored.filter_flags or "")

    collector = Collector(make_mock_pool(), db, SchedulerConfig())
    assert await collector._maybe_auto_delete(-100316) is False

    cur = await db.execute("SELECT COUNT(*) FROM messages WHERE channel_id = ?", (-100316,))
    assert (await cur.fetchone())[0] == 3
