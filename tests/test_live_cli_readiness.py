"""Unit tests for the live-CLI env-gate helper and credential resolution.

These are deterministic and network-free: they never touch a real Telegram
account. They cover the opt-in-only gate policy — live CLI tests run solely when
their ``RUN_*`` env var is explicitly truthy, and never auto-enable — plus the
``api_id``/``api_hash`` resolution used by the live CLI fixture.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from tests.cli_real_tg_integration._live_readiness import (
    _gate_enabled,
    _resolve_api_credentials,
)


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
# _gate_enabled — opt-in only, no auto-enable
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize("token", ["1", "true", "TRUE", "yes", "on"])
def test_gate_enabled_true_tokens_run(token):
    assert _gate_enabled("ANY_GATE", {"ANY_GATE": token}) is True


@pytest.mark.parametrize("token", ["0", "false", "no", "off"])
def test_gate_enabled_false_tokens_skip(token):
    assert _gate_enabled("ANY_GATE", {"ANY_GATE": token}) is False


def test_gate_enabled_garbage_is_off():
    assert _gate_enabled("ANY_GATE", {"ANY_GATE": "maybe"}) is False


def test_gate_enabled_unset_is_off():
    """The core safety guarantee: an unset gate never runs (no auto-enable)."""
    assert _gate_enabled("ANY_GATE", {}) is False


# --------------------------------------------------------------------------- #
# integration with _evaluate_real_tg_policy — opt-in only
# --------------------------------------------------------------------------- #


def test_policy_runs_cli_live_when_gate_explicitly_set():
    from tests.conftest import (
        CLI_REAL_TG_LIVE_FIXTURE,
        REAL_TG_SAFE_MARK,
        _evaluate_real_tg_policy,
    )

    action, message = _evaluate_real_tg_policy(
        mode=REAL_TG_SAFE_MARK,
        fixturenames=(CLI_REAL_TG_LIVE_FIXTURE,),
        environ={},
        gate_enabled=lambda name, environ: True,  # env explicitly truthy
    )

    assert action is None
    assert message is None


def test_policy_skips_cli_live_when_gate_unset():
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
        gate_enabled=lambda name, environ: False,  # env unset → off
    )

    assert action == "skip"
    assert REAL_TG_SAFE_GATE_ENV in message


def test_policy_cli_live_unset_env_skips_via_real_gate():
    """End-to-end with the real _gate_enabled: unset env on cli-live → skip."""
    from tests.conftest import (
        CLI_REAL_TG_LIVE_FIXTURE,
        REAL_TG_SAFE_MARK,
        _evaluate_real_tg_policy,
    )

    action, _message = _evaluate_real_tg_policy(
        mode=REAL_TG_SAFE_MARK,
        fixturenames=(CLI_REAL_TG_LIVE_FIXTURE,),
        environ={},  # no gate set → real _gate_enabled returns False
    )

    assert action == "skip"


def test_policy_does_not_auto_enable_sandbox_fixture():
    """Sandbox fixture stays strictly env-gated."""
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
