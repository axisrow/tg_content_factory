"""Offline onboarding: env-only config, encrypted import, and auth preflight."""

import json
import os
import sqlite3
from contextlib import closing
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
import typer
from dotenv import load_dotenv
from telethon.crypto import AuthKey
from telethon.sessions import StringSession

from tg_agent.cli.commands import account
from tg_agent.config import AppConfig, load_config
from tg_agent.security import SessionCipher


@pytest.mark.parametrize("yaml_text", [None, "web:\n  port: 9090\n", "security:\n  session_encryption_key: \"\"\n"])
def test_session_key_env_fallback(tmp_path, monkeypatch, yaml_text):
    monkeypatch.setenv("SESSION_ENCRYPTION_KEY", "env-only-secret")
    path = tmp_path / "config.yaml"
    if yaml_text is not None:
        path.write_text(yaml_text)
    assert load_config(path).security.session_encryption_key == "env-only-secret"


def test_session_key_explicit_yaml_wins(tmp_path, monkeypatch):
    monkeypatch.setenv("SESSION_ENCRYPTION_KEY", "different-env-secret")
    path = tmp_path / "config.yaml"
    path.write_text("security:\n  session_encryption_key: yaml-secret\n")
    assert load_config(path).security.session_encryption_key == "yaml-secret"


@pytest.mark.asyncio
@pytest.mark.parametrize("command", ["send_code_impl", "verify_code_impl", "import_impl"])
async def test_missing_key_blocks_auth_and_import(monkeypatch, capsys, command):
    require_key = Mock(
        side_effect=RuntimeError("SESSION_ENCRYPTION_KEY is required to add or import Telegram accounts.")
    )
    db = SimpleNamespace(
        repos=SimpleNamespace(accounts=SimpleNamespace(require_session_encryption_key=require_key)),
        get_setting=AsyncMock(return_value=json.dumps({"phone_code_hash": "hash"})),
        close=AsyncMock(),
        add_account=AsyncMock(),
    )
    monkeypatch.setattr(account.runtime, "init_db", AsyncMock(return_value=(AppConfig(), db)))
    auth = Mock()
    monkeypatch.setattr(account, "TelegramAuth", auth)
    read_session = Mock(side_effect=AssertionError("must not consume stdin without a key"))
    monkeypatch.setattr(account, "_read_session_arg", read_session)
    kwargs = {"phone": "+79990000009"}
    if command == "verify_code_impl":
        kwargs["code"] = "12345"
    with pytest.raises(typer.Exit) as exc:
        await getattr(account, command)("missing.yaml", **kwargs)
    assert exc.value.exit_code == 1
    assert "Set SESSION_ENCRYPTION_KEY" in capsys.readouterr().err
    auth.assert_not_called()
    read_session.assert_not_called()
    db.add_account.assert_not_awaited()
    db.close.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.aiosqlite_serial
async def test_dotenv_only_import_encrypts_session(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("SESSION_ENCRYPTION_KEY", raising=False)
    monkeypatch.delenv("TG_API_ID", raising=False)
    monkeypatch.delenv("TG_API_HASH", raising=False)
    # This test observes the account row landing in the default data/tg_search.db;
    # opt out of the #1770 fixture fallback, which would redirect the absent DB
    # to data/test/tg_search_test.db and break that observable for the wrong reason.
    monkeypatch.setenv("TGCF_DISABLE_TEST_DB_FALLBACK", "1")
    key = "onboarding-regression-secret"
    (tmp_path / ".env").write_text(f"TG_API_ID=12345\nTG_API_HASH=test-hash\nSESSION_ENCRYPTION_KEY={key}\n")
    # Startup loads .env before runtime.init_db; no YAML or Telegram connection.
    load_dotenv(tmp_path / ".env", override=True)
    session = StringSession()
    session.set_dc(2, "149.154.167.51", 443)
    session.auth_key = AuthKey(os.urandom(256))
    plaintext = session.save()
    await account.import_impl("config.yaml", phone="+79990000009", session_string=plaintext)
    with closing(sqlite3.connect("data/tg_search.db")) as conn:
        stored = conn.execute("SELECT session_string FROM accounts").fetchone()[0]
    assert stored.startswith("enc:v2:")
    assert stored != plaintext
    assert SessionCipher(key).decrypt(stored) == plaintext
