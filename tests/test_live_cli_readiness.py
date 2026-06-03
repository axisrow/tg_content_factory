"""Unit tests for the live-CLI readiness predicate and env-gate helper.

These are deterministic and network-free: they never touch a real Telegram
account. They cover the auto-enable logic that lets
``pytest tests/cli_real_tg_integration/...`` run without manual ``RUN_*`` env
vars when the project is genuinely live-ready, while staying skipped in CI.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.cli_real_tg_integration import _live_readiness
from tests.cli_real_tg_integration._live_readiness import (
    _gate_enabled,
    _resolve_api_credentials,
    live_cli_project_ready,
)


@pytest.fixture(autouse=True)
def _clear_readiness_cache():
    """Keep the process-wide lru_cache from leaking tmp_path state to other tests."""
    live_cli_project_ready.cache_clear()
    yield
    live_cli_project_ready.cache_clear()


def _make_config(api_id: int = 0, api_hash: str = "") -> SimpleNamespace:
    return SimpleNamespace(telegram=SimpleNamespace(api_id=api_id, api_hash=api_hash))


def _make_db(path: Path, *, accounts: bool = False, settings: dict[str, str] | None = None) -> None:
    with sqlite3.connect(path) as conn:
        conn.execute(
            """
            CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                phone TEXT,
                session_string TEXT,
                is_active INTEGER,
                is_primary INTEGER,
                flood_wait_until TEXT
            )
            """
        )
        conn.execute("CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
        if accounts:
            conn.execute(
                "INSERT INTO accounts (id, phone, session_string, is_active, is_primary, flood_wait_until)"
                " VALUES (1, '+70000000000', 'session', 1, 1, NULL)"
            )
        for key, value in (settings or {}).items():
            conn.execute("INSERT INTO settings (key, value) VALUES (?, ?)", (key, value))


# --------------------------------------------------------------------------- #
# _resolve_api_credentials
# --------------------------------------------------------------------------- #


def test_resolve_api_credentials_prefers_config(tmp_path):
    db_path = tmp_path / "live.db"
    _make_db(db_path, settings={"tg_api_id": "999", "tg_api_hash": "from-db"})
    config = _make_config(api_id=123, api_hash="from-config")

    assert _resolve_api_credentials(config, db_path) == (123, "from-config")


def test_resolve_api_credentials_falls_back_to_settings_table(tmp_path):
    db_path = tmp_path / "live.db"
    _make_db(db_path, settings={"tg_api_id": "456", "tg_api_hash": "db-hash"})
    config = _make_config(api_id=0, api_hash="")

    assert _resolve_api_credentials(config, db_path) == (456, "db-hash")


def test_resolve_api_credentials_incomplete_returns_none(tmp_path):
    db_path = tmp_path / "live.db"
    _make_db(db_path, settings={"tg_api_id": "456"})  # no hash
    config = _make_config(api_id=0, api_hash="")

    assert _resolve_api_credentials(config, db_path) == (None, None)


def test_resolve_api_credentials_broken_db_returns_none(tmp_path):
    db_path = tmp_path / "nonexistent.db"  # no settings table / no file
    config = _make_config(api_id=0, api_hash="")

    assert _resolve_api_credentials(config, db_path) == (None, None)


# --------------------------------------------------------------------------- #
# live_cli_project_ready
# --------------------------------------------------------------------------- #


def _point_readiness_at(monkeypatch, tmp_path, *, write_config: bool, db_kwargs: dict | None):
    monkeypatch.setenv("CLI_REAL_TG_ROOT", str(tmp_path))
    monkeypatch.delenv("CLI_REAL_TG_CONFIG", raising=False)
    monkeypatch.delenv("CLI_REAL_TG_PHONE", raising=False)
    monkeypatch.delenv("TG_API_ID", raising=False)
    monkeypatch.delenv("TG_API_HASH", raising=False)
    if write_config:
        (tmp_path / "config.yaml").write_text(
            "telegram:\n"
            "  api_id: 123456\n"
            "  api_hash: abcdef0123456789abcdef0123456789\n"
            "database:\n"
            "  path: data.db\n",
            encoding="utf-8",
        )
    if db_kwargs is not None:
        _make_db(tmp_path / "data.db", **db_kwargs)
    live_cli_project_ready.cache_clear()


def test_live_cli_project_ready_false_without_config(monkeypatch, tmp_path):
    _point_readiness_at(monkeypatch, tmp_path, write_config=False, db_kwargs=None)
    assert live_cli_project_ready() is False


def test_live_cli_project_ready_false_without_db(monkeypatch, tmp_path):
    _point_readiness_at(monkeypatch, tmp_path, write_config=True, db_kwargs=None)
    assert live_cli_project_ready() is False


def test_live_cli_project_ready_false_without_accounts(monkeypatch, tmp_path):
    _point_readiness_at(monkeypatch, tmp_path, write_config=True, db_kwargs={"accounts": False})
    assert live_cli_project_ready() is False


def test_live_cli_project_ready_true_when_fully_configured(monkeypatch, tmp_path):
    _point_readiness_at(monkeypatch, tmp_path, write_config=True, db_kwargs={"accounts": True})
    assert live_cli_project_ready() is True


def test_live_cli_project_ready_true_with_settings_table_credentials(monkeypatch, tmp_path):
    monkeypatch.setenv("CLI_REAL_TG_ROOT", str(tmp_path))
    monkeypatch.delenv("CLI_REAL_TG_CONFIG", raising=False)
    monkeypatch.delenv("CLI_REAL_TG_PHONE", raising=False)
    monkeypatch.delenv("TG_API_ID", raising=False)
    monkeypatch.delenv("TG_API_HASH", raising=False)
    # config.yaml has no creds; they live only in the settings table.
    (tmp_path / "config.yaml").write_text("database:\n  path: data.db\n", encoding="utf-8")
    _make_db(
        tmp_path / "data.db",
        accounts=True,
        settings={"tg_api_id": "123456", "tg_api_hash": "abcdef0123456789abcdef0123456789"},
    )
    live_cli_project_ready.cache_clear()

    assert live_cli_project_ready() is True


# --------------------------------------------------------------------------- #
# _gate_enabled
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("token", ["1", "true", "TRUE", "yes", "on"])
def test_gate_enabled_true_tokens_force_on(token, monkeypatch):
    monkeypatch.setattr(_live_readiness, "live_cli_project_ready", lambda: False)
    assert _gate_enabled("ANY_GATE", {"ANY_GATE": token}) is True


@pytest.mark.parametrize("token", ["0", "false", "no", "off"])
def test_gate_enabled_false_tokens_force_off(token, monkeypatch):
    monkeypatch.setattr(_live_readiness, "live_cli_project_ready", lambda: True)
    assert _gate_enabled("ANY_GATE", {"ANY_GATE": token}) is False


def test_gate_enabled_garbage_is_off(monkeypatch):
    monkeypatch.setattr(_live_readiness, "live_cli_project_ready", lambda: True)
    assert _gate_enabled("ANY_GATE", {"ANY_GATE": "maybe"}) is False


def test_gate_enabled_unset_follows_predicate_true(monkeypatch):
    monkeypatch.setattr(_live_readiness, "live_cli_project_ready", lambda: True)
    assert _gate_enabled("ANY_GATE", {}) is True


def test_gate_enabled_unset_follows_predicate_false(monkeypatch):
    monkeypatch.setattr(_live_readiness, "live_cli_project_ready", lambda: False)
    assert _gate_enabled("ANY_GATE", {}) is False


# --------------------------------------------------------------------------- #
# integration with _evaluate_real_tg_policy (cli-live auto branch vs sandbox)
# --------------------------------------------------------------------------- #


def test_policy_auto_enables_cli_live_when_project_ready():
    from tests.conftest import (
        CLI_REAL_TG_LIVE_FIXTURE,
        REAL_TG_SAFE_MARK,
        _evaluate_real_tg_policy,
    )

    action, message = _evaluate_real_tg_policy(
        mode=REAL_TG_SAFE_MARK,
        fixturenames=(CLI_REAL_TG_LIVE_FIXTURE,),
        environ={},
        gate_enabled=lambda name, environ: True,
    )

    assert action is None
    assert message is None


def test_policy_skips_cli_live_when_project_not_ready():
    from tests.conftest import (
        CLI_REAL_TG_LIVE_FIXTURE,
        REAL_TG_SAFE_GATE_ENV,
        REAL_TG_SAFE_MARK,
        _evaluate_real_tg_policy,
    )

    action, message = _evaluate_real_tg_policy(
        mode=REAL_TG_SAFE_MARK,
        fixturenames=(CLI_REAL_TG_LIVE_FIXTURE,),
        environ={},
        gate_enabled=lambda name, environ: False,
    )

    assert action == "skip"
    assert REAL_TG_SAFE_GATE_ENV in message


def test_policy_does_not_auto_enable_sandbox_fixture():
    """Sandbox fixture stays strictly env-gated even when the predicate is True."""
    from tests.conftest import (
        REAL_TG_LIVE_FIXTURE,
        REAL_TG_SAFE_MARK,
        _evaluate_real_tg_policy,
    )

    action, _message = _evaluate_real_tg_policy(
        mode=REAL_TG_SAFE_MARK,
        fixturenames=(REAL_TG_LIVE_FIXTURE,),
        environ={},
        gate_enabled=lambda name, environ: True,  # would open if it were honoured
    )

    assert action == "skip"
