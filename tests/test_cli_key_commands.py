"""Offline CLI tests for the `key` command group (#1699).

Every backend subprocess is faked (see test_secret_store.py); the real OS
secret store is never touched — conftest keeps ``TG_AGENT_SECRET_STORE=off``
and these tests opt back in with fakes.
"""

from __future__ import annotations

import pytest
import typer
from typer.testing import CliRunner

import tg_agent.cli.commands.key as key_mod
from tests.test_secret_store import KEY, FakeBackend, _install, _proc
from tg_agent.cli.commands.global_store import _maybe_suggest_key
from tg_agent.cli.typer_app import app
from tg_agent.config import AppConfig, SecurityConfig

runner = CliRunner()


class _FakeTty:
    """Minimal stdin stand-in whose isatty() drives the getpass branch."""

    def isatty(self) -> bool:
        return True

    def read(self) -> str:
        return ""


@pytest.fixture()
def store_on(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("TG_AGENT_SECRET_STORE", "")
    monkeypatch.setattr("tg_agent.security.secret_store._backend_error", None, raising=False)
    # Pin the keychain dialect — CI runs Linux; without the pin dispatch would
    # pick the file backend and never consult the faked backend.
    monkeypatch.setattr("sys.platform", "darwin")
    monkeypatch.setattr("shutil.which", lambda _n: None)
    from tg_agent.security import secret_store

    secret_store.read_stored_key.cache_clear()
    yield
    secret_store.read_stored_key.cache_clear()


def _chdir_project(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config.yaml").write_text("database:\n  path: local.db\n", encoding="utf-8")


# --------------------------------------------------------------------------- #
# key set / clear
# --------------------------------------------------------------------------- #


def test_key_set_reads_piped_stdin_and_never_echoes_key(store_on, tmp_path, monkeypatch):
    _chdir_project(tmp_path, monkeypatch)
    fake = _install(monkeypatch, FakeBackend("darwin"))
    result = runner.invoke(app, ["key", "set"], input=KEY + "\n")
    assert result.exit_code == 0
    assert fake.item == KEY
    assert "Key stored" in result.stdout
    assert "Restart any running worker" in result.stdout
    assert KEY not in result.stdout


def test_key_set_empty_stdin_fails(store_on, tmp_path, monkeypatch):
    _chdir_project(tmp_path, monkeypatch)
    _install(monkeypatch, FakeBackend("darwin"))
    result = runner.invoke(app, ["key", "set"], input="  \n")
    assert result.exit_code == 1
    assert "Empty key" in result.output


def test_key_set_double_prompt_mismatch_aborts(store_on, monkeypatch):
    fake = _install(monkeypatch, FakeBackend("darwin"))
    monkeypatch.setattr("sys.stdin", _FakeTty())
    prompts = iter(["first-key", "second-key"])
    monkeypatch.setattr(key_mod.getpass, "getpass", lambda _prompt: next(prompts))
    with pytest.raises(typer.Exit):
        key_mod._read_key_argument()
    assert fake.item is None


def test_key_set_double_prompt_match_stores(store_on, monkeypatch):
    _install(monkeypatch, FakeBackend("darwin"))
    monkeypatch.setattr("sys.stdin", _FakeTty())
    prompts = iter([KEY, KEY])
    monkeypatch.setattr(key_mod.getpass, "getpass", lambda _prompt: next(prompts))
    assert key_mod._read_key_argument() == KEY


def test_key_set_backend_failure_is_exit_1(store_on, tmp_path, monkeypatch):
    _chdir_project(tmp_path, monkeypatch)

    def boom(cmd, *, input_text=None):
        return _proc(45, "", "keychain locked")

    monkeypatch.setattr("tg_agent.security.secret_store._run", boom)
    result = runner.invoke(app, ["key", "set"], input=KEY + "\n")
    assert result.exit_code == 1
    assert "failed to store the key" in result.output


def test_key_clear_removes_and_reports_absent(store_on, tmp_path, monkeypatch):
    _chdir_project(tmp_path, monkeypatch)
    fake = _install(monkeypatch, FakeBackend("darwin", KEY))
    result = runner.invoke(app, ["key", "clear"])
    assert result.exit_code == 0
    assert "Key removed" in result.stdout
    assert fake.item is None
    result = runner.invoke(app, ["key", "clear"])
    assert result.exit_code == 0
    assert "No key was stored" in result.stdout


# --------------------------------------------------------------------------- #
# key status
# --------------------------------------------------------------------------- #


def test_key_status_off_reports_disabled(tmp_path, monkeypatch):
    _chdir_project(tmp_path, monkeypatch)
    monkeypatch.delenv("SESSION_ENCRYPTION_KEY", raising=False)
    result = runner.invoke(app, ["key", "status"])
    assert result.exit_code == 0
    assert "disabled (TG_AGENT_SECRET_STORE=off)" in result.stdout
    assert "effective     : none" in result.stdout


def test_key_status_reports_store_as_effective(store_on, tmp_path, monkeypatch):
    _chdir_project(tmp_path, monkeypatch)
    monkeypatch.delenv("SESSION_ENCRYPTION_KEY", raising=False)
    _install(monkeypatch, FakeBackend("darwin", KEY))
    result = runner.invoke(app, ["key", "status"])
    assert result.exit_code == 0
    assert "stored        : yes" in result.stdout
    assert "effective     : secret store" in result.stdout
    assert KEY not in result.stdout


def test_key_status_reports_env_as_effective(store_on, tmp_path, monkeypatch):
    _chdir_project(tmp_path, monkeypatch)
    _install(monkeypatch, FakeBackend("darwin", KEY))
    result = runner.invoke(app, ["key", "status"])
    assert result.exit_code == 0
    assert "env/config    : set (escape hatch" in result.stdout
    assert "effective     : env/config" in result.stdout


def test_key_status_surfaces_backend_error(store_on, tmp_path, monkeypatch):
    _chdir_project(tmp_path, monkeypatch)
    monkeypatch.delenv("SESSION_ENCRYPTION_KEY", raising=False)

    def boom(cmd, *, input_text=None):
        return _proc(45, "", "keychain locked")

    monkeypatch.setattr("tg_agent.security.secret_store._run", boom)
    result = runner.invoke(app, ["key", "status"])
    assert result.exit_code == 0
    assert "backend error : security find-generic-password rc=45" in result.stdout
    assert "stored        : no" in result.stdout


# --------------------------------------------------------------------------- #
# global enable suggestion (#1699: suggest only, never store)
# --------------------------------------------------------------------------- #


def test_suggest_key_printed_when_no_explicit_key_and_store_empty(store_on, capsys, monkeypatch):
    monkeypatch.setattr("tg_agent.security.secret_store._run", FakeBackend("darwin", None))
    _maybe_suggest_key(AppConfig(security=SecurityConfig(session_encryption_key="")))
    assert "tg-agent key set" in capsys.readouterr().out


def test_suggest_key_silent_with_explicit_key(store_on, capsys, monkeypatch):
    def no_store(cmd, *, input_text=None):  # pragma: no cover - must not run
        raise AssertionError("store consulted despite an explicit key")

    monkeypatch.setattr("tg_agent.security.secret_store._run", no_store)
    _maybe_suggest_key(AppConfig(security=SecurityConfig(session_encryption_key="env-key")))
    assert capsys.readouterr().out == ""


def test_suggest_key_silent_when_already_stored(store_on, capsys, monkeypatch):
    monkeypatch.setattr("tg_agent.security.secret_store._run", FakeBackend("darwin", KEY))
    _maybe_suggest_key(AppConfig(security=SecurityConfig(session_encryption_key="")))
    assert capsys.readouterr().out == ""
