"""Route tests for the web session-import form (#1146, epic #828).

`POST /auth/import-session` adds an account from a ready StringSession instead of
the interactive send-code/verify-code login. Pure DB op (validate + add_account,
encrypted at rest) — no worker/live Telegram. Refuses to overwrite an existing
phone; the session value is never logged.
"""
from __future__ import annotations

import logging

import pytest
from httpx import ASGITransport, AsyncClient
from telethon.crypto import AuthKey
from telethon.sessions import StringSession

from src.database import Database
from src.models import Account


def _valid_session() -> str:
    s = StringSession()
    s.set_dc(2, "149.154.167.51", 443)
    s.auth_key = AuthKey(b"\x02" * 256)
    return s.save()


@pytest.mark.anyio
async def test_import_session_creates_account(route_client, base_app):
    app, db, pool = base_app
    session = _valid_session()
    resp = await route_client.post(
        "/auth/import-session",
        data={"phone": "+15551112233", "session_string": session},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "account_connected" in resp.headers.get("location", "")

    accounts = await db.get_account_summaries(active_only=False)
    assert any(a.phone == "+15551112233" for a in accounts)


@pytest.mark.anyio
async def test_import_session_invalid_rejected(route_client, base_app):
    app, db, pool = base_app
    resp = await route_client.post(
        "/auth/import-session",
        data={"phone": "+15551112244", "session_string": "garbage"},
        follow_redirects=False,
    )
    # Re-renders the form with an error (200), account not created.
    assert resp.status_code == 200
    accounts = await db.get_account_summaries(active_only=False)
    assert not any(a.phone == "+15551112244" for a in accounts)


@pytest.mark.anyio
async def test_import_session_existing_phone_not_overwritten(route_client, base_app):
    app, db, pool = base_app
    # base_app seeds +1234567890 with session_string="test_session".
    resp = await route_client.post(
        "/auth/import-session",
        data={"phone": "+1234567890", "session_string": _valid_session()},
        follow_redirects=False,
    )
    assert resp.status_code == 200  # form with error, not a redirect
    # original session preserved
    assert await db.repos.accounts.get_decrypted_session(phone="+1234567890") == "test_session"


@pytest.mark.anyio
async def test_import_session_not_logged(route_client, base_app, caplog):
    """Application loggers must not record the session value.

    Scoped to project loggers (``src.*``): the third-party ``aiosqlite`` driver
    emits every SQL statement (with bound params) at DEBUG regardless of the
    column — that's driver diagnostics, off in production, and it logs all
    INSERTs identically, not a session-specific leak. We assert OUR code path
    (the route + app layers) never logs the secret.
    """
    app, db, pool = base_app
    session = _valid_session()
    with caplog.at_level(logging.DEBUG):
        await route_client.post(
            "/auth/import-session",
            data={"phone": "+15551112266", "session_string": session},
            follow_redirects=False,
        )
    app_records = [r for r in caplog.records if r.name.startswith("src.")]
    assert all(session not in r.getMessage() for r in app_records)


@pytest.mark.anyio
async def test_import_session_requires_auth(base_app):
    app, db, pool = base_app
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=False,
        headers={"Accept": "application/json", "Origin": "http://test"},
    ) as c:
        resp = await c.post(
            "/auth/import-session",
            data={"phone": "+15550000001", "session_string": _valid_session()},
        )
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_import_session_encrypts_at_rest(tmp_path):
    db = Database(str(tmp_path / "enc.db"), session_encryption_secret="form-secret")
    await db.initialize()
    try:
        # Direct repo path the route uses — verifies encryption at rest.
        await db.add_account(Account(phone="+15559998877", session_string=_valid_session()))
        cur = await db.execute(
            "SELECT session_string FROM accounts WHERE phone = ?", ("+15559998877",)
        )
        row = await cur.fetchone()
        assert row is not None
        assert str(row["session_string"]).startswith("enc:v2:")
    finally:
        await db.close()


@pytest.mark.anyio
async def test_add_account_if_absent_never_overwrites(tmp_path):
    """The atomic insert-only path the route uses: a second import for the same
    phone returns None (→ duplicate error) and CANNOT clobber the first session.

    Regression for the #1146 review HIGH: the old check-then-act guard (read
    summaries, then add_account UPSERT) let two concurrent imports both pass the
    read and the second overwrite the first. `add_account_if_absent` makes the DB
    the source of truth (ON CONFLICT DO NOTHING), so the window is closed.
    """
    db = Database(str(tmp_path / "atomic.db"))
    await db.initialize()
    try:
        first_id = await db.repos.accounts.add_account_if_absent(
            Account(phone="+15551230099", session_string="winner_session")
        )
        assert first_id is not None

        # Second import for the same phone with a DIFFERENT session must be refused.
        second_id = await db.repos.accounts.add_account_if_absent(
            Account(phone="+15551230099", session_string="loser_session")
        )
        assert second_id is None

        # The first (winning) session survives — never clobbered.
        assert await db.repos.accounts.get_decrypted_session(phone="+15551230099") == "winner_session"
    finally:
        await db.close()


@pytest.mark.anyio
async def test_import_session_normalizes_phone(route_client, base_app):
    """A phone without '+' is normalized so it can't bypass the duplicate guard."""
    app, db, pool = base_app
    # base_app seeds +1234567890; importing the same number without '+' must be refused.
    resp = await route_client.post(
        "/auth/import-session",
        data={"phone": "1234567890", "session_string": _valid_session()},
        follow_redirects=False,
    )
    assert resp.status_code == 200  # duplicate → form with error, not created twice
    accounts = await db.get_account_summaries(active_only=False)
    assert sum(1 for a in accounts if a.phone == "+1234567890") == 1
    # original session preserved
    assert await db.repos.accounts.get_decrypted_session(phone="+1234567890") == "test_session"
