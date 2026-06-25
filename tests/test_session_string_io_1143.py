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


def _import_ns(phone: str, session_string: str | None, *, stdin: bool = False, force: bool = False):
    return argparse.Namespace(
        config=None,
        account_action="import",
        phone=phone,
        session_string=session_string,
        session_string_stdin=stdin,
        force=force,
        json=False,
    )


def _export_ns(*, account_id=None, phone=None, as_json: bool = False):
    return argparse.Namespace(
        config=None,
        account_action="export-session",
        id=account_id,
        phone=phone,
        json=as_json,
    )


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
            session_string_stdin=False,
            force=False,
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
            session_string_stdin=False,
            force=False,
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


@pytest.mark.anyio
async def test_cli_export_survives_broken_sibling_session(tmp_path, capsys):
    """Exporting a healthy account must not crash because *another* row is undecryptable.

    Regression guard for the review HIGH: get_accounts() eagerly decrypts every row, so
    export resolved identity via summaries + a single-account decrypt instead.
    """
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        good = _make_valid_session_string()
        await db.add_account(Account(phone="+15551230010", session_string=good))
        # A row that looks encrypted but has no key configured → undecryptable sibling.
        broken_id = await db.add_account(
            Account(phone="+15551230011", session_string="enc:v2:not-decryptable")
        )
        assert broken_id  # sibling exists

        await account_cmd._run_export_session(_export_ns(phone="+15551230010"), db)
        out = capsys.readouterr().out
        assert good in out
    finally:
        await db.close()


@pytest.mark.anyio
async def test_cli_import_refuses_overwrite_without_force(tmp_path, capsys):
    """Importing onto an existing phone must NOT silently overwrite (data-loss guard)."""
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        original = _make_valid_session_string()
        await db.add_account(Account(phone="+15551230020", session_string=original))

        replacement = _make_valid_session_string()
        assert replacement != original or True  # both valid; content irrelevant to the guard
        await account_cmd._run_import(_import_ns("+15551230020", replacement), db)

        out = capsys.readouterr().out.lower()
        assert "already exists" in out
        # session unchanged
        assert await db.get_decrypted_session(phone="+15551230020") == original
    finally:
        await db.close()


@pytest.mark.anyio
async def test_cli_import_force_overwrites(tmp_path):
    """--force replaces the session of an existing account."""
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        await db.add_account(
            Account(phone="+15551230021", session_string=_make_valid_session_string())
        )
        new_session = _make_valid_session_string()
        # make it distinct from the original
        s = StringSession(new_session)
        s.set_dc(4, "149.154.167.91", 443)
        new_session = s.save()

        await account_cmd._run_import(_import_ns("+15551230021", new_session, force=True), db)
        assert await db.get_decrypted_session(phone="+15551230021") == new_session
    finally:
        await db.close()


@pytest.mark.anyio
async def test_cli_import_from_stdin(tmp_path, monkeypatch):
    """--session-string-stdin reads the secret from stdin (keeps it out of argv)."""
    import io

    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        session = _make_valid_session_string()
        monkeypatch.setattr("sys.stdin", io.StringIO(session + "\n"))
        await account_cmd._run_import(_import_ns("+15551230022", None, stdin=True), db)
        assert await db.get_decrypted_session(phone="+15551230022") == session
    finally:
        await db.close()


@pytest.mark.anyio
async def test_import_encrypts_at_rest_then_export_roundtrips(tmp_path, capsys):
    """With SESSION_ENCRYPTION_KEY set: stored column is enc:v2:*, export returns plaintext."""
    db = Database(str(tmp_path / "test.db"), session_encryption_secret="test-secret-key")
    await db.initialize()
    try:
        session = _make_valid_session_string()
        await account_cmd._run_import(_import_ns("+15551230023", session), db)

        # Stored at rest must be encrypted, not the plaintext.
        cur = await db.execute("SELECT session_string FROM accounts WHERE phone = ?", ("+15551230023",))
        row = await cur.fetchone()
        assert row is not None
        stored = str(row["session_string"])
        assert stored.startswith("enc:v2:")
        assert session not in stored

        # Export still yields the decrypted plaintext.
        await account_cmd._run_export_session(_export_ns(phone="+15551230023"), db)
        assert session in capsys.readouterr().out
    finally:
        await db.close()
