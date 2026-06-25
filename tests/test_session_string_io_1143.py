"""Tests for SSO session-string export/import (#1143, epic #828).

Covers:
- telegram-layer ``validate_session_string`` helper (CLI/web/agent must not
  import telethon directly — import-linter forbids it, so the validator lives
  in the telegram layer).
- CLI ``account export-session`` (decrypted plaintext round-trip).
- CLI ``account import`` (StringSession validation → add_account, encrypted at rest).
"""
from __future__ import annotations

import argparse

import pytest
from telethon.crypto import AuthKey
from telethon.sessions import StringSession

from src.cli.commands import account as account_cmd
from src.database import Database
from src.models import Account
from src.telegram.auth import validate_session_string


def _make_valid_session_string() -> str:
    """Build a structurally-valid StringSession (dc/address/port/auth_key set)."""
    s = StringSession()
    s.set_dc(2, "149.154.167.51", 443)
    s.auth_key = AuthKey(b"\x01" * 256)
    return s.save()


# ---------------------------------------------------------------------------
# telegram-layer helper
# ---------------------------------------------------------------------------


@pytest.mark.telegram_unit
def test_validate_session_string_accepts_valid():
    assert validate_session_string(_make_valid_session_string()) is True


@pytest.mark.telegram_unit
def test_validate_session_string_rejects_garbage():
    assert validate_session_string("not-a-session") is False


@pytest.mark.telegram_unit
def test_validate_session_string_rejects_empty():
    assert validate_session_string("") is False


@pytest.mark.telegram_unit
def test_validate_session_string_rejects_structurally_incomplete():
    # An empty StringSession is parseable but has no auth_key/dc — not usable.
    assert validate_session_string(StringSession().save()) is False


# ---------------------------------------------------------------------------
# CLI import
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_cli_import_adds_account(tmp_path, capsys):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        session = _make_valid_session_string()
        args = argparse.Namespace(
            config=None,
            account_action="import",
            phone="+15551230000",
            session_string=session,
            json=False,
        )
        # run() builds its own db via runtime.init_db — patch it to our db.
        await account_cmd._run_import(args, db)

        accounts = await db.get_accounts(active_only=False)
        assert any(a.phone == "+15551230000" for a in accounts)
        out = capsys.readouterr().out
        # The raw session string must never be echoed back on import.
        assert session not in out
    finally:
        await db.close()


@pytest.mark.anyio
async def test_cli_import_rejects_invalid_session(tmp_path, capsys):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        args = argparse.Namespace(
            config=None,
            account_action="import",
            phone="+15551230001",
            session_string="garbage",
            json=False,
        )
        await account_cmd._run_import(args, db)

        accounts = await db.get_accounts(active_only=False)
        assert not any(a.phone == "+15551230001" for a in accounts)
        out = capsys.readouterr().out.lower()
        assert "invalid" in out or "недейств" in out or "не валид" in out
    finally:
        await db.close()


# ---------------------------------------------------------------------------
# CLI export-session
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_cli_export_session_roundtrip(tmp_path, capsys):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        session = _make_valid_session_string()
        acc_id = await db.add_account(Account(phone="+15551230002", session_string=session))

        args = argparse.Namespace(
            config=None,
            account_action="export-session",
            id=acc_id,
            phone=None,
            json=False,
        )
        await account_cmd._run_export_session(args, db)

        out = capsys.readouterr().out
        # export returns the *decrypted* plaintext session — must equal what we put in.
        assert session in out
    finally:
        await db.close()


@pytest.mark.anyio
async def test_cli_export_session_unknown_id(tmp_path, capsys):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        args = argparse.Namespace(
            config=None,
            account_action="export-session",
            id=99999,
            phone=None,
            json=False,
        )
        await account_cmd._run_export_session(args, db)
        out = capsys.readouterr().out.lower()
        assert "not found" in out
    finally:
        await db.close()
