"""Offline CLI tests for the `global` account-store group (#1683).

No live Telegram: every test drives the production Typer app through
``CliRunner`` (or the real module entry in a subprocess) against throwaway
project directories. ``TG_AGENT_GLOBAL_DIR`` relocates the store into
``tmp_path``, so the real ``~/.tg-agent`` is never touched. Secrets are never
asserted into output: session ciphertexts are read from the DB files and only
checked with membership asserts against captured stdout.
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest
from typer.testing import CliRunner

from tg_agent.cli.typer_app import app
from tg_agent.config import (
    GLOBAL_DB_NAME,
    apply_global_store,
    global_marker_path,
    global_store_root,
    load_config,
)
from tg_agent.database import Database
from tg_agent.models import Account

runner = CliRunner()

KEY = "test-session-encryption-key"
PHONE_A = "+10000000001"  # synthetic — never a real number
PHONE_B = "+10000000002"
PHONE_C = "+10000000003"

SOURCE_ROOT = Path(__file__).resolve().parent.parent


def _chdir_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Isolated project directory: own config.yaml, cwd, store root, clean env."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.yaml").write_text(
        "database:\n  path: local.db\n", encoding="utf-8"
    )
    monkeypatch.setenv("TG_AGENT_GLOBAL_DIR", str(tmp_path / "global"))
    monkeypatch.delenv("TG_SESSION_CACHE_DIR", raising=False)


async def _make_local_db(path: Path, sessions: list[tuple[str, bool]]) -> None:
    db = Database(str(path), session_encryption_secret=KEY)
    await db.initialize()
    try:
        for phone, is_primary in sessions:
            await db.repos.accounts.add_account(
                Account(phone=phone, session_string=f"raw-session-{phone}", is_primary=is_primary)
            )
    finally:
        await db.close()


async def _add_account_to_db(path: Path, phone: str) -> None:
    db = Database(str(path), session_encryption_secret=KEY)
    await db.initialize()
    try:
        await db.repos.accounts.add_account(
            Account(phone=phone, session_string=f"raw-session-{phone}")
        )
    finally:
        await db.close()


def _account_rows(db_path: Path) -> list[tuple[str, str, int]]:
    """(phone, stored session value, is_primary) — no decryption anywhere."""
    conn = sqlite3.connect(db_path)
    try:
        return list(
            conn.execute(
                "SELECT phone, session_string, is_primary FROM accounts "
                "ORDER BY is_primary DESC, id ASC"
            )
        )
    finally:
        conn.close()


def _no_secrets_in_output(stdout: str, ciphertexts: list[str]) -> None:
    assert "enc:v2:" not in stdout
    assert "raw-session-" not in stdout
    for ciphertext in ciphertexts:
        assert ciphertext not in stdout


# --------------------------------------------------------------------------- #
# load_config marker resolution
# --------------------------------------------------------------------------- #


def test_load_config_without_marker_keeps_local_paths(tmp_path, monkeypatch):
    _chdir_project(tmp_path, monkeypatch)
    config = load_config("config.yaml")
    assert config.database.path == "local.db"
    assert config.telegram_runtime.session_cache_dir == "data/telegram_sessions"


def test_load_config_with_legacy_marker_keeps_local_db_and_warns(tmp_path, monkeypatch, caplog):
    """#1724: global auth never implies a global database.

    The legacy plain-path marker used to silently rewrite database.path and
    session_cache_dir to the shared store; since the redirect removal it must
    leave both project-local and say so out loud (WARNING on every load).
    """
    import logging

    _chdir_project(tmp_path, monkeypatch)
    global_marker_path().write_text(f"{global_store_root()}\n", encoding="utf-8")
    with caplog.at_level(logging.WARNING, logger="tg_agent.config"):
        config = load_config("config.yaml")
    assert config.database.path == "local.db"
    assert config.telegram_runtime.session_cache_dir == "data/telegram_sessions"
    assert any("no longer redirects" in r.message for r in caplog.records)


def test_legacy_marker_leaves_explicit_session_env_alone(tmp_path, monkeypatch):
    _chdir_project(tmp_path, monkeypatch)
    global_marker_path().write_text(f"{global_store_root()}\n", encoding="utf-8")
    monkeypatch.setenv("TG_SESSION_CACHE_DIR", str(tmp_path / "env-sessions"))
    config = load_config("config.yaml")
    assert config.database.path == "local.db"
    assert config.telegram_runtime.session_cache_dir == str(tmp_path / "env-sessions")


def test_apply_global_store_is_noop_without_marker(tmp_path, monkeypatch):
    _chdir_project(tmp_path, monkeypatch)
    config = load_config("config.yaml")
    apply_global_store(config)
    assert config.database.path == "local.db"


# --------------------------------------------------------------------------- #
# global status
# --------------------------------------------------------------------------- #


def test_status_reports_disabled_by_default(tmp_path, monkeypatch):
    _chdir_project(tmp_path, monkeypatch)
    result = runner.invoke(app, ["global", "status"])
    assert result.exit_code == 0
    assert "disabled" in result.stdout
    assert "local database : local.db" in result.stdout
    assert ".tg-agent-global" in result.stdout


def test_status_reports_legacy_marker_as_inert(tmp_path, monkeypatch):
    """#1724: a legacy marker no longer means "enabled" — status must say so."""
    _chdir_project(tmp_path, monkeypatch)
    global_marker_path().write_text(f"{global_store_root()}\n", encoding="utf-8")
    result = runner.invoke(app, ["global", "status"])
    assert result.exit_code == 0
    assert "Legacy marker detected" in result.stdout
    assert "no longer redirects" in result.stdout
    assert "project database : local.db" in result.stdout
    assert "global enable --account PHONE" in result.stdout
    assert str(global_store_root() / GLOBAL_DB_NAME) not in result.stdout


# --------------------------------------------------------------------------- #
# global enable
# --------------------------------------------------------------------------- #


def test_enable_requires_explicit_account_grants(tmp_path, monkeypatch):
    _chdir_project(tmp_path, monkeypatch)
    result = runner.invoke(app, ["global", "enable"])
    assert result.exit_code == 1
    assert "--account PHONE" in result.stderr
    assert not global_marker_path().exists()
    assert not (global_store_root() / GLOBAL_DB_NAME).exists()


def test_enable_over_legacy_marker_no_longer_refuses(tmp_path, monkeypatch):
    """#1724: a legacy marker no longer blocks enable — it is an inert pointer.

    The store root is pre-configured and the gateway admin call is stubbed to
    fail with a canned error, so the test proves only the guard removal: the
    failure must be the stub's, not the old "Legacy global store requires
    `global migrate`" refusal.
    """
    _chdir_project(tmp_path, monkeypatch)
    global_marker_path().write_text(str(global_store_root()))
    from tg_agent.gateway.runtime import configure

    configure(global_store_root(), api_id=12345, api_hash="offline-test-api-hash")

    from tg_agent.gateway import enrollment

    class _DownAdministrator:
        def __init__(self, *args, **kwargs):
            pass

        @classmethod
        async def connect(cls, root):
            return cls()

        async def call(self, *args, **kwargs):
            raise RuntimeError("stub-gateway-down")

        async def close(self):
            pass

    monkeypatch.setattr(enrollment, "GatewayAdministrator", _DownAdministrator)
    result = runner.invoke(app, ["global", "enable", "--account", PHONE_A])
    assert result.exit_code != 0
    assert "Legacy global store requires" not in (result.stderr or "")
    assert isinstance(result.exception, RuntimeError)
    assert "stub-gateway-down" in str(result.exception)


# --------------------------------------------------------------------------- #
# global disable
# --------------------------------------------------------------------------- #


def test_disable_restores_local_store_and_keeps_global_files(tmp_path, monkeypatch):
    _chdir_project(tmp_path, monkeypatch)
    asyncio.run(_make_local_db(tmp_path / "local.db", [(PHONE_A, True)]))
    global_store_root().mkdir()
    asyncio.run(_make_local_db(global_store_root() / GLOBAL_DB_NAME, [(PHONE_B, True)]))
    global_marker_path().write_text(str(global_store_root()))

    result = runner.invoke(app, ["global", "disable"])
    assert result.exit_code == 0
    assert "disabled" in result.stdout
    assert not global_marker_path().exists()

    # Back on the local DB: the global-only account is gone from view, and the
    # global store itself is left intact on disk.
    listing = runner.invoke(app, ["account", "list"])
    assert listing.exit_code == 0
    assert PHONE_A in listing.stdout
    assert PHONE_B not in listing.stdout
    assert (global_store_root() / GLOBAL_DB_NAME).exists()


def test_disable_without_marker_is_a_noop(tmp_path, monkeypatch):
    _chdir_project(tmp_path, monkeypatch)
    result = runner.invoke(app, ["global", "disable"])
    assert result.exit_code == 0
    assert "not enabled" in result.stdout


# --------------------------------------------------------------------------- #
# real-process end-to-end (module entry, own interpreter)
# --------------------------------------------------------------------------- #


def _run_cli(args: list[str], tmp_path: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "tg_agent.main", "--config", "config.yaml", *args],
        cwd=tmp_path,
        env={
            "PATH": os.environ.get("PATH", ""),
            "PYTHONPATH": str(SOURCE_ROOT),
            "PYTHONSAFEPATH": "1",
            "TG_AGENT_GLOBAL_DIR": str(tmp_path / "global"),
            "HOME": os.environ.get("HOME", ""),
        },
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=45,
        check=False,
    )


def test_subprocess_refuses_implicit_shared_database_mode(tmp_path, monkeypatch):
    _chdir_project(tmp_path, monkeypatch)
    enable = _run_cli(["global", "enable"], tmp_path)
    assert enable.returncode == 1
    assert "--account PHONE" in enable.stderr
    assert not (tmp_path / ".tg-agent-global").exists()
    status = _run_cli(["global", "status"], tmp_path)
    assert status.returncode == 0 and "disabled" in status.stdout
