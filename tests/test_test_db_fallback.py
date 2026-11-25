"""#1770: test DB fallback — builder, auto-switch, warning, env opt-out.

Everything runs on the committed seed files and tmp paths: no Telegram, no
production database. The fallback tests double as acceptance for the real
seed set (>=1000 sanitized messages across @prog_ai and @vAIbecoding).
"""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from pathlib import Path

import pytest
import typer

from tg_agent import test_db as test_db_mod
from tg_agent.cli import runtime as cli_runtime
from tg_agent.cli.commands.test_db import build_impl
from tg_agent.config import AppConfig
from tg_agent.test_db import (
    _FIXTURE_STAMP,
    CHANNELS_SEED,
    FALLBACK_DISABLED_ENV,
    build_test_db,
    fallback_disabled,
    resolve_startup_db_path,
    seed_dir,
)
from tg_agent.test_db import (
    test_db_path as fixture_db_path,
)

MESSAGE_SEEDS = ("prog_ai.jsonl", "vAIbecoding.jsonl")
PUBLIC_IDENTITIES = {"prog_ai", "vAIbecoding"}
# Forwarded channel posts keep the source channel's *public* identity in the
# sender fields (sanitizer decision in #1794) — not personal data.
PUBLIC_SENDER_NAMES = PUBLIC_IDENTITIES | {
    "Vibe Coding: OpenCode, Claude Code, Codex, Cursor, Kilo",
}
PUBLIC_SENDER_IDS = {1997034059, 2110095907}
_PLACEHOLDER = re.compile(r"user_\d+")


def _config(db_path: Path) -> AppConfig:
    config = AppConfig()
    config.database.path = str(db_path)
    return config


def _config_file(tmp_path: Path, db_path: Path) -> str:
    cfg = tmp_path / "config.yaml"
    cfg.write_text(f"database:\n  path: {db_path}\n", encoding="utf-8")
    return str(cfg)


def _sha256(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


def _enable_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(FALLBACK_DISABLED_ENV, raising=False)


# --------------------------------------------------------------------------- #
# env contract
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("raw", [None, "", "0", "true", "yes", " false"])
def test_fallback_active_by_default(raw, monkeypatch):
    if raw is None:
        monkeypatch.delenv(FALLBACK_DISABLED_ENV, raising=False)
    else:
        monkeypatch.setenv(FALLBACK_DISABLED_ENV, raw)
    assert fallback_disabled() is False


@pytest.mark.parametrize("raw", ["1", " 1 "])
def test_fallback_disabled_exactly_on_1(raw, monkeypatch):
    monkeypatch.setenv(FALLBACK_DISABLED_ENV, raw)
    assert fallback_disabled() is True


# --------------------------------------------------------------------------- #
# builder over the committed seeds
# --------------------------------------------------------------------------- #


async def test_build_from_committed_seeds(tmp_path):
    out = tmp_path / "test" / "tg_search_test.db"
    total = await build_test_db(out)

    assert total >= 1000  # issue #1770 acceptance: >=1000 messages overall
    assert not list(out.parent.glob("*.building*"))  # atomic build, no leftovers
    con = sqlite3.connect(out)
    try:
        n, n_channels = con.execute(
            "SELECT COUNT(*), COUNT(DISTINCT channel_id) FROM messages"
        ).fetchone()
        assert n == total
        assert n_channels == 2

        channels = con.execute(
            "SELECT channel_id, username, last_collected_id, is_active, is_filtered"
            " FROM channels ORDER BY channel_id"
        ).fetchall()
        assert [row[1] for row in channels] == ["vAIbecoding", "prog_ai"]
        assert all(row[3] == 1 and row[4] == 0 for row in channels)

        # incremental collection continues from the fixture, not replaying it
        for channel_id, max_id in con.execute(
            "SELECT channel_id, MAX(message_id) FROM messages GROUP BY channel_id"
        ):
            stored = con.execute(
                "SELECT last_collected_id FROM channels WHERE channel_id = ?", (channel_id,)
            ).fetchone()[0]
            assert stored == max_id

        # FTS triggers fired: the fixture is searchable like a real collection
        assert con.execute("SELECT COUNT(*) FROM messages_fts").fetchone()[0] == n
    finally:
        con.close()


async def test_build_is_deterministic(tmp_path):
    first, second = tmp_path / "a.db", tmp_path / "b.db"
    await build_test_db(first)
    await build_test_db(second)
    assert _sha256(first) == _sha256(second)


async def test_build_pins_seeded_timestamps(tmp_path):
    """The stamp contract, asserted deterministically (#1922 review).

    test_build_is_deterministic is probabilistic: back-to-back builds landing
    in the same wall-clock second hash equal even without the pin, which is
    exactly why the bug shipped as a CI flake instead of a red test. The
    build must overwrite every initialize()-seeded wall-clock timestamp with
    the fixed fixture stamp.
    """
    built = tmp_path / "pinned.db"
    await build_test_db(built)
    con = sqlite3.connect(built)
    try:
        assert con.execute("SELECT COUNT(*) FROM channel_ratings").fetchone()[0] > 0
        assert con.execute(
            "SELECT DISTINCT updated_at FROM channel_ratings"
        ).fetchall() == [(_FIXTURE_STAMP,)]
        assert con.execute("SELECT COUNT(*) FROM pipeline_templates").fetchone()[0] > 0
        assert con.execute(
            "SELECT DISTINCT created_at FROM pipeline_templates"
        ).fetchall() == [(_FIXTURE_STAMP,)]
    finally:
        con.close()


async def test_build_missing_seeds_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        await build_test_db(tmp_path / "x.db", seeds=tmp_path / "nope")


async def test_build_skips_columns_absent_from_schema(tmp_path):
    """A seed column the schema no longer has is skipped with a warning, not fatal."""
    seeds = tmp_path / "seeds"
    seeds.mkdir()
    (seeds / CHANNELS_SEED).write_text(
        json.dumps(
            {
                "channel_id": 1,
                "title": "t",
                "username": "u",
                "channel_type": None,
                "created_at": "2026-01-01T00:00:00+00:00",
                "about": None,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (seeds / "only.jsonl").write_text(
        json.dumps(
            {
                "id": 1,
                "channel_id": 1,
                "message_id": 5,
                "text": "hello",
                "date": "2026-01-01T00:00:00+00:00",
                "definitely_not_a_column": "x",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    out = tmp_path / "out.db"
    total = await build_test_db(out, seeds=seeds)
    assert total == 1
    con = sqlite3.connect(out)
    try:
        row = con.execute("SELECT channel_id, message_id, text FROM messages").fetchone()
        assert row == (1, 5, "hello")
    finally:
        con.close()


# --------------------------------------------------------------------------- #
# startup resolution: auto-switch, reuse, opt-out
# --------------------------------------------------------------------------- #


async def test_resolve_switches_to_fixture_and_warns(tmp_path, monkeypatch, caplog):
    _enable_fallback(monkeypatch)
    real = tmp_path / "data" / "tg_search.db"
    with caplog.at_level(logging.WARNING, logger="tg_agent.test_db"):
        resolved = await resolve_startup_db_path(_config(real))
    expected = tmp_path / "data" / "test" / "tg_search_test.db"
    assert resolved == str(expected)
    assert expected.exists()
    assert not real.exists()  # the real DB must not be created behind the fallback
    assert "test mode is on" in caplog.text
    assert str(real) in caplog.text
    assert FALLBACK_DISABLED_ENV in caplog.text


async def test_resolve_reuses_existing_test_db(tmp_path, monkeypatch, caplog):
    _enable_fallback(monkeypatch)
    real = tmp_path / "tg_search.db"
    test = tmp_path / "test" / "tg_search_test.db"
    await build_test_db(test)
    before = _sha256(test)
    with caplog.at_level(logging.WARNING, logger="tg_agent.test_db"):
        resolved = await resolve_startup_db_path(_config(real))
    assert resolved == str(test)
    assert _sha256(test) == before  # repeated runs reuse the ready file
    assert "assembling fixture database" not in caplog.text
    assert "test mode is on" in caplog.text


async def test_resolve_rebuilds_corrupt_test_db(tmp_path, monkeypatch, caplog):
    _enable_fallback(monkeypatch)
    real = tmp_path / "tg_search.db"
    test = fixture_db_path(str(real))
    test.parent.mkdir(parents=True)
    test.write_bytes(b"garbage, not sqlite" * 16)
    with caplog.at_level(logging.WARNING, logger="tg_agent.test_db"):
        resolved = await resolve_startup_db_path(_config(real))
    assert resolved == str(test)
    assert test.read_bytes()[:16] == b"SQLite format 3\x00"
    assert "assembling fixture database" in caplog.text


def _sibling(path: Path, suffix: str) -> Path:
    """WAL/SHM-style sibling of *path* (sync helper: ASYNC240-clean in async tests)."""
    return Path(f"{path}{suffix}")


async def test_rebuild_drops_stale_target_wal(tmp_path):
    """A rebuild must not leave the OLD database's -wal/-shm next to the new file.

    SQLite replays a found -wal into the main file it sits next to, so stale
    frames would pour into the freshly built fixture (review finding on #1770).
    """
    test = tmp_path / "test" / "tg_search_test.db"
    test.parent.mkdir(parents=True)
    test.write_bytes(b"garbage, not sqlite" * 16)  # forces a rebuild
    _sibling(test, "-wal").write_bytes(b"stale wal frames")
    _sibling(test, "-shm").write_bytes(b"stale shm")
    await build_test_db(test)
    assert test.read_bytes()[:16] == b"SQLite format 3\x00"
    assert not _sibling(test, "-wal").exists()
    assert not _sibling(test, "-shm").exists()
    assert not list(test.parent.glob("*.building*"))


async def test_resolve_passthrough_when_real_db_exists(tmp_path, monkeypatch, caplog):
    _enable_fallback(monkeypatch)
    real = tmp_path / "tg_search.db"
    real.write_bytes(b"SQLite format 3\x00 not really, but exists()")
    with caplog.at_level(logging.WARNING, logger="tg_agent.test_db"):
        resolved = await resolve_startup_db_path(_config(real))
    assert resolved == str(real)
    assert not fixture_db_path(str(real)).exists()
    assert caplog.text == ""


async def test_resolve_env_opt_out(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv(FALLBACK_DISABLED_ENV, "1")
    real = tmp_path / "tg_search.db"
    with caplog.at_level(logging.WARNING, logger="tg_agent.test_db"):
        resolved = await resolve_startup_db_path(_config(real))
    assert resolved == str(real)
    assert not fixture_db_path(str(real)).exists()  # nothing built behind the opt-out
    assert caplog.text == ""


async def test_resolve_memory_path_passthrough(monkeypatch, caplog):
    _enable_fallback(monkeypatch)
    with caplog.at_level(logging.WARNING, logger="tg_agent.test_db"):
        resolved = await resolve_startup_db_path(_config(Path(":memory:")))
    assert resolved == ":memory:"
    assert caplog.text == ""


async def test_resolve_without_seeds_degrades_loudly(tmp_path, monkeypatch, caplog):
    _enable_fallback(monkeypatch)
    monkeypatch.setattr(test_db_mod, "seed_dir", lambda: tmp_path / "absent")
    real = tmp_path / "tg_search.db"
    with caplog.at_level(logging.WARNING, logger="tg_agent.test_db"):
        resolved = await resolve_startup_db_path(_config(real))
    assert resolved == str(real)
    assert "seed directory" in caplog.text


# --------------------------------------------------------------------------- #
# choke points: CLI init_db
# --------------------------------------------------------------------------- #


async def test_init_db_applies_fallback(tmp_path, monkeypatch, caplog):
    _enable_fallback(monkeypatch)
    real = tmp_path / "data" / "tg_search.db"
    expected = tmp_path / "data" / "test" / "tg_search_test.db"
    with caplog.at_level(logging.WARNING, logger="tg_agent.test_db"):
        config, db = await cli_runtime.init_db(_config_file(tmp_path, real))
    try:
        assert config.database.path == str(expected)
        assert expected.exists()
        assert not real.exists()
        assert "test mode is on" in caplog.text
    finally:
        await db.close()
    con = sqlite3.connect(expected)
    try:
        assert con.execute("SELECT COUNT(*) FROM messages").fetchone()[0] >= 1000
    finally:
        con.close()


async def test_init_db_env_opt_out(tmp_path, monkeypatch, caplog):
    monkeypatch.setenv(FALLBACK_DISABLED_ENV, "1")
    real = tmp_path / "data" / "tg_search.db"
    with caplog.at_level(logging.WARNING, logger="tg_agent.test_db"):
        config, db = await cli_runtime.init_db(_config_file(tmp_path, real))
    try:
        assert config.database.path == str(real)
        assert real.exists()  # previous behavior: Database creates the empty DB
        assert not fixture_db_path(str(real)).exists()
        assert "test mode" not in caplog.text
    finally:
        await db.close()


# --------------------------------------------------------------------------- #
# CLI command
# --------------------------------------------------------------------------- #


async def test_cli_build_impl(tmp_path, capsys):
    real = tmp_path / "tg_search.db"
    await build_impl(_config_file(tmp_path, real))
    out = capsys.readouterr().out
    assert "Test DB ready" in out
    assert str(fixture_db_path(str(real))) in out
    assert fixture_db_path(str(real)).exists()


async def test_cli_build_impl_memory_refused(tmp_path, monkeypatch, capsys):
    cfg = tmp_path / "config.yaml"
    cfg.write_text("database:\n  path: ':memory:'\n", encoding="utf-8")
    with pytest.raises(typer.Exit):
        await build_impl(str(cfg))
    assert "nothing to build" in capsys.readouterr().err


async def test_cli_build_impl_missing_seeds_exits_1(tmp_path, monkeypatch):
    monkeypatch.setattr(test_db_mod, "seed_dir", lambda: tmp_path / "absent")
    with pytest.raises(typer.Exit):
        await build_impl(_config_file(tmp_path, tmp_path / "tg_search.db"))


# --------------------------------------------------------------------------- #
# seed hygiene pins (rule #1453; data prepared in #1794)
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("name", MESSAGE_SEEDS)
def test_seed_sender_fields_are_placeholders(name):
    for line in (seed_dir() / name).read_text(encoding="utf-8").splitlines():
        rec = json.loads(line)
        username = rec.get("sender_username")
        assert username is None or username in PUBLIC_IDENTITIES or _PLACEHOLDER.fullmatch(username), (
            f"{name}: unsanitized sender_username: {line[:120]}"
        )
        if rec.get("sender_kind") == "user":
            for field in ("sender_name", "sender_first_name", "sender_last_name"):
                value = rec.get(field)
                assert (
                    value is None
                    or value in PUBLIC_SENDER_NAMES
                    or _PLACEHOLDER.fullmatch(value)
                ), f"{name}: unsanitized {field}: {line[:120]}"
            sender_id = rec.get("sender_id")
            assert sender_id is None or sender_id in PUBLIC_SENDER_IDS or str(sender_id).startswith(
                "90000"
            ), f"{name}: sender_id is not a synthetic placeholder: {line[:120]}"


@pytest.mark.parametrize("name", MESSAGE_SEEDS)
def test_seed_texts_have_no_phone_numbers(name):
    for line in (seed_dir() / name).read_text(encoding="utf-8").splitlines():
        text = json.loads(line).get("text") or ""
        assert not re.search(r"\+\d{7,}", text), f"{name}: phone-like number in text: {line[:120]}"


def test_channels_seed_covers_message_seeds():
    channels = {
        json.loads(line)["channel_id"]
        for line in (seed_dir() / CHANNELS_SEED).read_text(encoding="utf-8").splitlines()
    }
    from_messages = {
        json.loads(line)["channel_id"]
        for name in MESSAGE_SEEDS
        for line in (seed_dir() / name).read_text(encoding="utf-8").splitlines()
    }
    assert from_messages == channels == {1997034059, 2110095907}
