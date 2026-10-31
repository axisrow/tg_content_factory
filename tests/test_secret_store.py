"""Fake-first tests for the machine secret-store layer (#1699).

No real keychain/secret-service anywhere: every backend subprocess is faked at
``secret_store._run``. ``tests/conftest.py`` sets ``TG_AGENT_SECRET_STORE=off``
by default (hermeticity — a dev machine may hold a real key); store-touching
tests opt back in via the ``store_on`` fixture.
"""

from __future__ import annotations

import base64
import logging
import os
import shutil
import stat
import subprocess
import sys

import pytest

from tg_agent.config import AppConfig, SecurityConfig, resolve_session_encryption_secret
from tg_agent.database import Database
from tg_agent.security import secret_store
from tg_agent.security.secret_store import (
    SecretStoreError,
    backend_name,
    delete_key,
    read_stored_key,
    resolve_session_secret_from_env_or_store,
    store_key,
)

KEY = "stored-machine-key"


def _proc(rc: int, out: str = "", err: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=[], returncode=rc, stdout=out, stderr=err)


class FakeBackend:
    """In-memory store speaking every platform CLI contract.

    ``platform`` selects which command dialect ``__call__`` understands; the
    recorded ``calls`` list lets tests assert exact argv/stdin shapes.
    """

    def __init__(self, platform: str = "darwin", item: str | None = None):
        self.platform = platform
        self.item = item
        self.calls: list[tuple[list[str], str | None]] = []

    def __call__(self, cmd: list[str], *, input_text: str | None = None) -> subprocess.CompletedProcess[str]:
        self.calls.append((cmd, input_text))
        if self.platform == "darwin":
            return self._security(cmd)
        if self.platform == "win32":
            return self._powershell(cmd, input_text)
        return self._secret_tool(cmd, input_text)

    # macOS: security find/add/delete-generic-password
    def _security(self, cmd: list[str]) -> subprocess.CompletedProcess[str]:
        if "find-generic-password" in cmd:
            if self.item is None:
                return _proc(44, "", "not found")
            return _proc(0, self.item + "\n")
        if "add-generic-password" in cmd:
            self.item = cmd[cmd.index("-w") + 1]
            return _proc(0)
        if "delete-generic-password" in cmd:
            existed = self.item is not None
            self.item = None
            return _proc(0 if existed else 44)
        return _proc(1, "", "unexpected command")

    # Windows: powershell -Command - with the script on stdin. Match on the
    # full call sites — the Add-Type typedef mentions every function name.
    def _powershell(
        self, cmd: list[str], input_text: str | None
    ) -> subprocess.CompletedProcess[str]:
        if cmd[:1] != ["powershell"] or "-Command" not in cmd:
            return _proc(1, "", "unexpected command")
        script = input_text or ""
        if "::CredRead(" in script:
            if self.item is None:
                return _proc(44)
            return _proc(0, base64.b64encode(self.item.encode("utf-8")).decode("ascii"))
        if "@KEY_B64@" in script:
            return _proc(1)  # caller asserts the template was substituted
        if "::CredWrite([ref]" in script:
            marker = "FromBase64String('"
            blob = script[script.index(marker) + len(marker) : script.index("')", script.index(marker))]
            self.item = base64.b64decode(blob).decode("utf-8")
            return _proc(0)
        if "::CredDelete(" in script:
            existed = self.item is not None
            self.item = None
            return _proc(0 if existed else 44)
        return _proc(1, "", "unexpected script")

    # Linux: secret-tool lookup/store/clear
    def _secret_tool(
        self, cmd: list[str], input_text: str | None
    ) -> subprocess.CompletedProcess[str]:
        if "lookup" in cmd:
            if self.item is None:
                return _proc(1, "", "not found")
            return _proc(0, self.item + "\n")
        if "store" in cmd:
            assert input_text is not None, "secret-tool store reads the secret from stdin"
            self.item = input_text
            return _proc(0)
        if "clear" in cmd:
            self.item = None
            return _proc(0)
        return _proc(1, "", "unexpected command")


@pytest.fixture()
def store_on(monkeypatch: pytest.MonkeyPatch, tmp_path):
    """Opt back into the store layer with every backend faked; reset caches.

    Pins the keychain dialect: the macOS-targeting tests must not drift with
    the host OS (CI runs Linux, where dispatch would pick the file backend
    and never consult the fake ``_run``). TG_AGENT_GLOBAL_DIR moves into tmp
    so a regression can never touch the runner's real ~/.tg-agent.
    """
    monkeypatch.setenv("TG_AGENT_SECRET_STORE", "")
    monkeypatch.setattr(secret_store, "_backend_error", None, raising=False)
    monkeypatch.setattr(sys, "platform", "darwin")
    monkeypatch.setattr(shutil, "which", lambda _n: None)
    monkeypatch.setenv("TG_AGENT_GLOBAL_DIR", str(tmp_path / "global"))
    read_stored_key.cache_clear()
    yield
    read_stored_key.cache_clear()


def _install(monkeypatch: pytest.MonkeyPatch, fake: FakeBackend) -> FakeBackend:
    monkeypatch.setattr(secret_store, "_run", fake)
    return fake


def _clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SESSION_ENCRYPTION_KEY", raising=False)
    monkeypatch.delenv("TG_AGENT_GLOBAL_DIR", raising=False)


# --------------------------------------------------------------------------- #
# backend dispatch
# --------------------------------------------------------------------------- #


def test_backend_dispatch_by_platform(store_on, monkeypatch):
    cases = [
        ("darwin", None, "keychain"),
        ("win32", None, "credential-manager"),
        ("linux", "/usr/bin/secret-tool", "secret-tool"),
        ("linux", None, "file"),
    ]
    for platform, which, expected in cases:
        monkeypatch.setattr(sys, "platform", platform)
        monkeypatch.setattr(shutil, "which", lambda _n, _w=which: _w)
        assert backend_name() == expected


def test_backend_off_switch(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("TG_AGENT_SECRET_STORE", "off")
    assert backend_name() == "off"
    assert read_stored_key() is None


def test_store_key_refuses_when_off():
    with pytest.raises(SecretStoreError, match="disabled"):
        store_key(KEY)


def test_store_on_fixture_pins_keychain_dialect(store_on):
    """Regression (#1742 CI): on a Linux host dispatch must still pick the
    faked keychain backend, not the file fallback — the macOS-targeting tests
    depend on the fixture's platform pin beating the host OS."""
    assert backend_name() == "keychain"


# --------------------------------------------------------------------------- #
# macOS keychain contract
# --------------------------------------------------------------------------- #


def test_keychain_read_strips_newline(store_on, monkeypatch):
    fake = _install(monkeypatch, FakeBackend("darwin", KEY))
    assert read_stored_key() == KEY
    (cmd, input_text), *_ = fake.calls
    assert cmd[0] == "security" and "-w" in cmd and input_text is None


def test_keychain_read_absent_is_none(store_on, monkeypatch):
    _install(monkeypatch, FakeBackend("darwin", None))
    assert read_stored_key() is None
    assert secret_store.last_backend_error() is None


def test_keychain_read_error_degrades_to_none(store_on, monkeypatch):
    def boom(cmd, *, input_text=None):
        return _proc(45, "", "keychain locked")

    monkeypatch.setattr(secret_store, "_run", boom)
    assert read_stored_key() is None
    assert "rc=45" in (secret_store.last_backend_error() or "")


def test_keychain_write_uses_update_form(store_on, monkeypatch):
    fake = _install(monkeypatch, FakeBackend("darwin"))
    store_key(KEY)
    assert fake.item == KEY
    (cmd, input_text), *_ = fake.calls
    assert "-U" in cmd and input_text is None
    assert cmd[cmd.index("-w") + 1] == KEY


def test_keychain_roundtrip_and_delete(store_on, monkeypatch):
    fake = _install(monkeypatch, FakeBackend("darwin"))
    store_key(KEY)
    assert read_stored_key() == KEY
    assert delete_key() is True
    assert fake.item is None
    assert delete_key() is False


# --------------------------------------------------------------------------- #
# Windows Credential Manager contract (P/Invoke script on stdin)
# --------------------------------------------------------------------------- #


def test_credential_manager_write_keeps_secret_off_argv(store_on, monkeypatch):
    fake = _install(monkeypatch, FakeBackend("win32"))
    monkeypatch.setattr(sys, "platform", "win32")
    store_key(KEY)
    assert fake.item == KEY
    (cmd, input_text), *_ = fake.calls
    assert cmd[:1] == ["powershell"] and cmd[-1] == "-"
    assert KEY not in (input_text or ""), "key must ride stdin base64-encoded, not plaintext argv"


def test_credential_manager_read_roundtrip(store_on, monkeypatch):
    _install(monkeypatch, FakeBackend("win32", KEY))
    monkeypatch.setattr(sys, "platform", "win32")
    assert read_stored_key() == KEY


def test_credential_manager_absent(store_on, monkeypatch):
    _install(monkeypatch, FakeBackend("win32", None))
    monkeypatch.setattr(sys, "platform", "win32")
    assert read_stored_key() is None


def test_credential_manager_write_failure_raises(store_on, monkeypatch):
    class FailingWrite(FakeBackend):
        def _powershell(self, cmd, input_text):
            self.calls.append((cmd, input_text))
            return _proc(1, "", "access denied")

    _install(monkeypatch, FailingWrite("win32"))
    monkeypatch.setattr(sys, "platform", "win32")
    with pytest.raises(SecretStoreError, match="CredWrite"):
        store_key(KEY)


def test_credential_manager_blob_is_utf8_end_to_end(store_on, monkeypatch):
    """The write script must encode the blob UTF-8 — the reader decodes UTF-8.

    A Unicode(UTF-16LE) write with a UTF-8 read silently returns NUL-padded
    garbage, and the FakeBackend roundtrip masks it (it re-encodes UTF-8), so
    the encoding marker is asserted against the real script text (#1742).
    """
    fake = _install(monkeypatch, FakeBackend("win32"))
    monkeypatch.setattr(sys, "platform", "win32")
    store_key(KEY)
    (cmd, input_text), *_ = fake.calls
    script = input_text or ""
    assert "]::Unicode.GetBytes" not in script
    assert "]::UTF8.GetBytes($key)" in script
    assert read_stored_key() == KEY


def test_run_converts_timeout_to_rc124(store_on, monkeypatch):
    def hang(cmd, **_kwargs):
        raise subprocess.TimeoutExpired(cmd, 10)

    monkeypatch.setattr(secret_store.subprocess, "run", hang)
    proc = secret_store._run(["security", "find-generic-password"])
    assert proc.returncode == 124
    assert "timed out" in proc.stderr


def test_read_timeout_degrades_to_none_with_error(store_on, monkeypatch):
    def hang(cmd, **_kwargs):
        raise subprocess.TimeoutExpired(cmd, 10)

    monkeypatch.setattr(secret_store.subprocess, "run", hang)
    assert read_stored_key() is None
    assert "timed out" in (secret_store.last_backend_error() or "")


def test_write_timeout_raises_secret_store_error(store_on, monkeypatch):
    def hang(cmd, **_kwargs):
        raise subprocess.TimeoutExpired(cmd, 10)

    monkeypatch.setattr(secret_store.subprocess, "run", hang)
    with pytest.raises(SecretStoreError, match="timed out"):
        store_key(KEY)


# --------------------------------------------------------------------------- #
# Linux secret-tool + file fallback
# --------------------------------------------------------------------------- #


def test_secret_tool_read_and_write(store_on, monkeypatch):
    fake = _install(monkeypatch, FakeBackend("linux"))
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(shutil, "which", lambda _n: "/usr/bin/secret-tool")
    store_key(KEY)
    assert fake.item == KEY
    (cmd, input_text), *_ = fake.calls
    assert "service" in cmd and "account" in cmd and input_text == KEY
    assert read_stored_key() == KEY


def test_secret_tool_absent_is_none(store_on, monkeypatch):
    _install(monkeypatch, FakeBackend("linux", None))
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(shutil, "which", lambda _n: "/usr/bin/secret-tool")
    assert read_stored_key() is None


def test_file_fallback_roundtrip_0600(store_on, tmp_path, monkeypatch):
    monkeypatch.setenv("TG_AGENT_GLOBAL_DIR", str(tmp_path / "global"))
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(shutil, "which", lambda _n: None)
    store_key(KEY)
    path = secret_store.fallback_file_path()
    assert path.read_text(encoding="utf-8").strip() == KEY
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert read_stored_key() == KEY
    assert delete_key() is True
    assert not path.exists()
    assert delete_key() is False


# --------------------------------------------------------------------------- #
# Resolution order (config resolver + Database default chain)
# --------------------------------------------------------------------------- #


def test_resolver_config_value_wins_without_store_call(monkeypatch):
    def no_store(cmd, *, input_text=None):  # pragma: no cover - must not run
        raise AssertionError("store backend consulted despite an explicit config key")

    monkeypatch.setattr(secret_store, "_run", no_store)
    config = AppConfig(security=SecurityConfig(session_encryption_key="explicit-key"))
    assert resolve_session_encryption_secret(config) == "explicit-key"


def test_resolver_env_in_config_wins_over_store(store_on, monkeypatch):
    _install(monkeypatch, FakeBackend("darwin", KEY))
    config = AppConfig(security=SecurityConfig(session_encryption_key="env-key"))
    assert resolve_session_encryption_secret(config) == "env-key"


def test_resolver_falls_back_to_store(store_on, monkeypatch, caplog):
    _clear_env(monkeypatch)
    _install(monkeypatch, FakeBackend("darwin", KEY))
    with caplog.at_level(logging.INFO, logger="tg_agent.config"):
        assert resolve_session_encryption_secret(AppConfig()) == KEY
    assert not any("No SESSION_ENCRYPTION_KEY" in r.message for r in caplog.records)


def test_resolver_without_store_keeps_warning_and_none(store_on, monkeypatch, caplog):
    _clear_env(monkeypatch)
    _install(monkeypatch, FakeBackend("darwin", None))
    with caplog.at_level(logging.WARNING, logger="tg_agent.config"):
        assert resolve_session_encryption_secret(AppConfig()) is None
    assert any("No SESSION_ENCRYPTION_KEY configured." in r.message for r in caplog.records)


def test_resolver_off_switch_ignores_stored_key(store_on, monkeypatch, caplog):
    _clear_env(monkeypatch)
    _install(monkeypatch, FakeBackend("darwin", KEY))
    monkeypatch.setenv("TG_AGENT_SECRET_STORE", "off")
    with caplog.at_level(logging.WARNING, logger="tg_agent.config"):
        assert resolve_session_encryption_secret(AppConfig()) is None
    assert any("No SESSION_ENCRYPTION_KEY configured." in r.message for r in caplog.records)


def test_env_or_store_chain_env_wins(store_on, monkeypatch):
    def no_store(cmd, *, input_text=None):  # pragma: no cover - must not run
        raise AssertionError("store consulted despite env set")

    monkeypatch.setattr(secret_store, "_run", no_store)
    monkeypatch.setenv("SESSION_ENCRYPTION_KEY", "env-key")
    assert resolve_session_secret_from_env_or_store() == "env-key"


def test_env_or_store_chain_store_fallback(store_on, monkeypatch):
    _clear_env(monkeypatch)
    _install(monkeypatch, FakeBackend("darwin", KEY))
    assert resolve_session_secret_from_env_or_store() == KEY
    assert os.environ.get("SESSION_ENCRYPTION_KEY") is None


# --------------------------------------------------------------------------- #
# Database default chain (facade unification, #1699)
# --------------------------------------------------------------------------- #


def test_database_default_falls_back_to_store(store_on, monkeypatch):
    _clear_env(monkeypatch)
    _install(monkeypatch, FakeBackend("darwin", KEY))
    assert Database(":memory:")._session_encryption_secret == KEY


def test_database_default_env_still_wins(store_on, monkeypatch):
    _install(monkeypatch, FakeBackend("darwin", KEY))
    monkeypatch.setenv("SESSION_ENCRYPTION_KEY", "env-key")
    assert Database(":memory:")._session_encryption_secret == "env-key"


def test_database_explicit_none_disables_store_fallback(store_on, monkeypatch):
    _clear_env(monkeypatch)
    _install(monkeypatch, FakeBackend("darwin", KEY))
    assert Database(":memory:", session_encryption_secret=None)._session_encryption_secret is None
