"""Test DB fallback (#1770): a fixture database when the real one is absent.

When the configured project database (``config.database.path``) does not
exist — a fresh clone, a scratch checkout, CI — the runtime resolves the
startup path to a fixture copy instead of a silently empty DB:

- the fixture DB lives next to the configured path under ``test/``
  (``data/test/tg_search_test.db`` by default) and is built deterministically
  from the committed seeds ``tests/fixtures/test_db_seed/*.jsonl``; the
  schema is the production one (``Database.initialize()`` runs ``schema.py``
  plus migrations), so every consumer works against a real database;
- every switch logs a loud ``test mode is on`` warning — fixture data must
  never be mistaken for a real collection;
- an existing fixture DB is reused as-is; a leftover ``.building`` file or a
  non-SQLite file triggers exactly one rebuild;
- ``TGCF_DISABLE_TEST_DB_FALLBACK=1`` (exact value) restores the previous
  behavior byte-for-byte: the configured path is used as-is (an empty DB is
  created there), nothing is built, no warning.

Seeds: sanitized exports of public channel @prog_ai and public chat
@vAIbecoding (identifier placeholders per rule #1453; data prepared in
#1794). There is deliberately no live-collection path here: refreshing seeds
is an explicit operator export, never an automatic Telegram call.

``gateway/enrollment.py`` intentionally does not resolve through this
module: enrollment legitimately creates a fresh project DB at an explicit
destination (#1684/#1724).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

from tg_agent.database import Database

if TYPE_CHECKING:
    from tg_agent.config import AppConfig

logger = logging.getLogger(__name__)

#: Set to exactly ``1`` to disable the fixture fallback (issue #1770).
FALLBACK_DISABLED_ENV = "TGCF_DISABLE_TEST_DB_FALLBACK"

#: The fixture DB lives in this subdir next to the configured DB path.
TEST_DB_SUBDIR = "test"
TEST_DB_NAME = "tg_search_test.db"

#: Channel-meta seed; every other ``*.jsonl`` in the seed dir holds messages.
CHANNELS_SEED = "channels.jsonl"

_CHANNEL_INSERT = (
    "INSERT INTO channels (channel_id, title, username, channel_type,"
    " created_at, added_at, about, last_collected_id)"
    " VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
)

_LAST_COLLECTED_UPDATE = "UPDATE channels SET last_collected_id = ? WHERE channel_id = ?"


def fallback_disabled() -> bool:
    """True when the env var is set to exactly ``1`` (issue #1770 wording).

    Anything else — unset, ``0``, ``true`` — keeps the fallback active, so a
    stray ``=false`` can never silently re-enable something the operator
    meant to disable (cf. the #1115 bool-string class of bugs).
    """
    return os.environ.get(FALLBACK_DISABLED_ENV, "").strip() == "1"


def seed_dir() -> Path:
    """Committed seed directory (repo checkout / editable install).

    A wheel install ships no ``tests/``, so the fallback degrades to the
    previous behavior with a warning instead of failing startup.
    """
    return Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "test_db_seed"


def test_db_path(database_path: str) -> Path | None:
    """Fixture path for *database_path*; ``None`` when nothing can fall back."""
    if not database_path or database_path == ":memory:":
        return None
    return Path(database_path).parent / TEST_DB_SUBDIR / TEST_DB_NAME


def _looks_like_sqlite(path: Path) -> bool:
    try:
        with path.open("rb") as fh:
            return fh.read(16) == b"SQLite format 3\x00"
    except OSError:
        return False


def _read_seeds(seeds: Path) -> tuple[list[dict], list[tuple[Path, list[dict]]]]:
    """Parse the channel meta plus every message seed file (sorted = deterministic)."""
    channels_file = seeds / CHANNELS_SEED
    if not channels_file.exists():
        raise FileNotFoundError(f"channel seed file not found: {channels_file}")
    channels = [
        json.loads(line)
        for line in channels_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    message_files = sorted(p for p in seeds.glob("*.jsonl") if p.name != CHANNELS_SEED)
    if not message_files:
        raise FileNotFoundError(f"no message seed files (*.jsonl) in {seeds}")
    parsed = [
        (
            path,
            [
                json.loads(line)
                for line in path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ],
        )
        for path in message_files
    ]
    return channels, parsed


async def build_test_db(out_path: str | Path, seeds: Path | None = None) -> int:
    """Assemble the fixture DB from the seed files; return the message count.

    The schema is the production one — ``Database.initialize()`` applies
    ``schema.py`` plus every migration — and the build is atomic: rows go
    into a ``.building`` sibling that ``os.replace`` moves into place, so a
    crash never leaves a torn DB behind and repeated runs reuse the ready
    file (the startup resolver only rebuilds when the file is absent or not
    a SQLite file).

    Message columns come from the intersection of the seed keys with the
    live ``messages`` table (``PRAGMA table_info``), so a seed column that a
    future migration drops is skipped with a warning instead of failing the
    build. ``channels.last_collected_id`` is set to the max seeded
    ``message_id`` per channel, so an incremental collection continues from
    the fixture instead of replaying its history.
    """
    out_path = Path(out_path)
    channels, message_files = _read_seeds(Path(seeds) if seeds is not None else seed_dir())

    tmp_path = out_path.with_name(out_path.name + ".building")
    # Replacing the main file strands the OLD database's -wal/-shm: SQLite
    # replays whatever -wal sits next to the main file it opens, so stale
    # frames would be poured into the freshly built fixture (torn DB). Clear
    # them up front — before the swap — and again on the failure path before
    # raise; a WAL next to a *reused* (not rebuilt) DB is normal crash
    # recovery and stays untouched on the reuse path.
    leftovers = (
        tmp_path,
        Path(f"{tmp_path}-wal"),
        Path(f"{tmp_path}-shm"),
        Path(f"{out_path}-wal"),
        Path(f"{out_path}-shm"),
    )
    for leftover in leftovers:
        leftover.unlink(missing_ok=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    db = Database(str(tmp_path))
    try:
        await db.initialize()
        async with db.transaction() as conn:
            pragma = await conn.execute("PRAGMA table_info(messages)")
            table_columns = {row[1] for row in await pragma.fetchall()}

            await conn.executemany(
                _CHANNEL_INSERT,
                [
                    (
                        row["channel_id"],
                        row["title"],
                        row["username"],
                        row["channel_type"],
                        row["created_at"],
                        # added_at = created_at: a datetime('now') default would
                        # break the byte-determinism the tests pin (#1770).
                        row["created_at"],
                        row["about"],
                        0,
                    )
                    for row in channels
                ],
            )

            insert_columns: list[str] | None = None
            max_ids: dict[int, int] = {}
            total = 0
            for path, rows in message_files:
                if not rows:
                    continue
                if insert_columns is None:
                    insert_columns = [c for c in rows[0] if c in table_columns]
                    skipped = [c for c in rows[0] if c not in table_columns]
                    if skipped:
                        logger.warning(
                            "seed %s: columns absent from the current schema are skipped: %s",
                            path.name,
                            skipped,
                        )
                await conn.executemany(
                    f"INSERT INTO messages ({', '.join(insert_columns)})"
                    f" VALUES ({', '.join('?' * len(insert_columns))})",
                    [tuple(row.get(c) for c in insert_columns) for row in rows],
                )
                total += len(rows)
                for row in rows:
                    cid, mid = row["channel_id"], row["message_id"]
                    if mid > max_ids.get(cid, 0):
                        max_ids[cid] = mid
            await conn.executemany(
                _LAST_COLLECTED_UPDATE,
                [(mid, cid) for cid, mid in sorted(max_ids.items())],
            )
    finally:
        await db.close()

    try:
        os.replace(tmp_path, out_path)
    except BaseException:
        for leftover in leftovers:
            leftover.unlink(missing_ok=True)
        raise
    return total


async def resolve_startup_db_path(config: AppConfig) -> str:
    """Return the DB path the runtime should open, applying the #1770 fallback.

    Callers assign the result back to ``config.database.path`` so every
    downstream consumer (the relay endpoint path, daemon provenance, the
    wake channel) stays consistent with the file actually opened.
    """
    raw = config.database.path
    if fallback_disabled() or not raw or raw == ":memory:":
        return raw
    if await asyncio.to_thread(os.path.exists, raw):
        return raw
    seeds = seed_dir()
    if not seeds.is_dir():
        logger.warning(
            "test DB fallback skipped: seed directory %s not found (wheel"
            " install?); a fresh empty database will be created at %s",
            seeds,
            raw,
        )
        return raw
    test = test_db_path(raw)
    if test is None:  # pragma: no cover - raw is non-empty and not ":memory:"
        return raw
    if not _looks_like_sqlite(test):
        logger.warning("test DB fallback: assembling fixture database from %s into %s", seeds, test)
        try:
            await build_test_db(test, seeds)
        except Exception as exc:  # noqa: BLE001 - degrade loudly, never block startup
            logger.error(
                "test DB fallback build failed (%s: %s); continuing with a fresh"
                " empty database at %s",
                type(exc).__name__,
                exc,
                raw,
            )
            return raw
    logger.warning(
        "test mode is on: database %s not found — running on the fixture test"
        " database %s built from committed seeds (@prog_ai, @vAIbecoding);"
        " the data is fixture data, set %s=1 to disable the fallback",
        raw,
        test,
        FALLBACK_DISABLED_ENV,
    )
    return str(test)
