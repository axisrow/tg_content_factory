"""Tests for src/cli/commands/debug.py — CLI debug subcommands."""
from __future__ import annotations

import argparse
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.commands.debug import run


def _fake_asyncio_run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _args(**overrides):
    defaults = {"config": "config.yaml"}
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def _make_db(**overrides):
    db = MagicMock()
    db.close = AsyncMock()
    db.get_stats = AsyncMock(return_value={"channels": 5, "messages": 100})
    db._path = ":memory:"
    for k, v in overrides.items():
        setattr(db, k, v)
    return db


def _make_config():
    return MagicMock()


# ---------------------------------------------------------------------------
# logs
# ---------------------------------------------------------------------------


def test_logs_file_exists(capsys, tmp_path):
    log_file = tmp_path / "app.log"
    log_file.write_text("line1\nline2\nline3\n", encoding="utf-8")
    db = _make_db()
    config = _make_config()
    with patch("src.cli.commands.debug.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.debug.APP_LOG_PATH", log_file), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(debug_action="logs", limit=2))
    out = capsys.readouterr().out
    assert "line2" in out
    assert "line3" in out


def test_logs_file_missing(capsys, tmp_path):
    missing = tmp_path / "nonexistent.log"
    db = _make_db()
    config = _make_config()
    with patch("src.cli.commands.debug.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.debug.APP_LOG_PATH", missing), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(debug_action="logs", limit=10))
    out = capsys.readouterr().out
    assert "No log file" in out


# ---------------------------------------------------------------------------
# memory
# ---------------------------------------------------------------------------


def test_memory(capsys):
    db = _make_db()
    config = _make_config()
    with patch("src.cli.commands.debug.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(debug_action="memory"))
    out = capsys.readouterr().out
    assert "Max RSS" in out
    assert "channels" in out


def test_memory_with_db_file(capsys, tmp_path):
    db_file = tmp_path / "test.db"
    db_file.write_bytes(b"x" * (2 * 1024 * 1024))  # 2MB
    db = _make_db(_path=str(db_file))
    config = _make_config()
    with patch("src.cli.commands.debug.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(debug_action="memory"))
    out = capsys.readouterr().out
    assert "DB file size" in out


# ---------------------------------------------------------------------------
# timing
# ---------------------------------------------------------------------------


def test_timing(capsys):
    db = _make_db()
    config = _make_config()
    with patch("src.cli.commands.debug.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(debug_action="timing"))
    out = capsys.readouterr().out
    assert "timing" in out.lower() or "benchmark" in out.lower()
