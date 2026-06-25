"""Route tests for SSO session export (#1145, epic #828).

`POST /settings/{account_id}/export-session` returns the decrypted plaintext
StringSession behind the panel's existing WEB_PASS auth. POST (not GET) keeps the
secret out of URLs/logs; the value is never logged.
"""
from __future__ import annotations

import logging

import pytest
from httpx import ASGITransport, AsyncClient

from src.database import Database
from src.models import Account


@pytest.mark.anyio
async def test_export_session_returns_decrypted_string(route_client, base_app):
    app, db, pool = base_app
    accounts = await db.get_account_summaries(active_only=False)
    acc = accounts[0]  # +1234567890 seeded with session_string="test_session"

    resp = await route_client.post(f"/settings/{acc.id}/export-session")
    assert resp.status_code == 200
    body = resp.json()
    assert body["phone"] == acc.phone
    assert body["session_string"] == "test_session"


@pytest.mark.anyio
async def test_export_session_unknown_id_returns_404(route_client, base_app):
    resp = await route_client.post("/settings/99999/export-session")
    assert resp.status_code == 404
    assert resp.json()["error"] == "account_not_found"


@pytest.mark.anyio
async def test_export_session_is_post_not_get(route_client, base_app):
    # GET must not expose the secret (would land in URLs/logs).
    app, db, pool = base_app
    acc = (await db.get_account_summaries(active_only=False))[0]
    resp = await route_client.get(f"/settings/{acc.id}/export-session")
    assert resp.status_code == 405  # method not allowed


@pytest.mark.anyio
async def test_export_session_requires_auth(base_app):
    """Without WEB_PASS auth the endpoint returns 401 (never the secret)."""
    app, db, pool = base_app
    acc = (await db.get_account_summaries(active_only=False))[0]
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=False,
        headers={"Accept": "application/json", "Origin": "http://test"},
    ) as c:
        resp = await c.post(f"/settings/{acc.id}/export-session")
    assert resp.status_code == 401
    assert "test_session" not in resp.text


@pytest.mark.anyio
async def test_export_session_not_logged(route_client, base_app, caplog):
    app, db, pool = base_app
    acc = (await db.get_account_summaries(active_only=False))[0]
    with caplog.at_level(logging.DEBUG):
        resp = await route_client.post(f"/settings/{acc.id}/export-session")
    assert resp.status_code == 200
    # The secret must never appear in any log record.
    assert "test_session" not in caplog.text


@pytest.mark.anyio
async def test_export_session_roundtrip_with_encryption(tmp_path):
    """With SESSION_ENCRYPTION_KEY: stored enc:v2:*, the accessor the endpoint
    calls returns the decrypted plaintext (the route is exercised over HTTP by the
    plaintext tests above; here we verify the encryption layer the endpoint relies on)."""
    db = Database(str(tmp_path / "enc.db"), session_encryption_secret="route-secret")
    await db.initialize()
    try:
        await db.add_account(Account(phone="+15550009999", session_string="plain_sess_xyz"))
        acc = (await db.get_account_summaries(active_only=False))[0]

        # stored encrypted at rest
        cur = await db.execute("SELECT session_string FROM accounts WHERE id = ?", (acc.id,))
        row = await cur.fetchone()
        assert row is not None
        assert str(row["session_string"]).startswith("enc:v2:")

        # the exact accessor the endpoint uses returns (phone, plaintext)
        export = await db.repos.accounts.get_session_export(account_id=acc.id)
        assert export == ("+15550009999", "plain_sess_xyz")
    finally:
        await db.close()


@pytest.mark.anyio
async def test_old_two_read_pattern_would_mix_phone_and_session(tmp_path):
    """Proves the #1145 HIGH was real: the OLD two-await pattern (identity from one
    read, session from a second) pairs a STALE phone with a FRESH session when a
    delete+reinsert lands between the two reads on a reused rowid.

    This reconstructs the pre-fix route logic locally and forces the race BETWEEN
    the two reads. It must observe the mix — establishing the hazard the fixed
    code (next test) prevents. If this ever stops mixing, the race model is wrong
    and the guard below would prove nothing.
    """
    db = Database(str(tmp_path / "old.db"))
    await db.initialize()
    try:
        # Step 1 of the OLD pattern: read identity (summaries) for the FIRST account.
        first_id = await db.add_account(Account(phone="+15551110001", session_string="sess_one"))
        summaries = await db.get_account_summaries(active_only=False)
        stale_phone = next(s.phone for s in summaries if s.id == first_id)

        # The race lands BETWEEN the two reads: row deleted, a DIFFERENT account
        # reinserted onto the reused id.
        await db.delete_account(first_id)
        reused_id = await db.add_account(Account(phone="+15552220002", session_string="sess_two"))
        assert reused_id == first_id  # rowid genuinely reused

        # Step 2 of the OLD pattern: decrypt by id only → gets the NEW session.
        fresh_session = await db.repos.accounts.get_decrypted_session(account_id=first_id)

        # The old route returned stale_phone + fresh_session → a poisoned pairing.
        assert (stale_phone, fresh_session) == ("+15551110001", "sess_two")
        assert stale_phone != "+15552220002"  # phone does NOT own the live session
    finally:
        await db.close()


@pytest.mark.anyio
async def test_get_session_export_never_mixes_across_reused_rowid(tmp_path):
    """The fix: `get_session_export` reads phone AND session from ONE row, so under
    the same delete+reinsert race it returns a CONSISTENT pair (or None) — never the
    stale_phone+fresh_session mix the sibling test above demonstrates for the old code.
    """
    db = Database(str(tmp_path / "fixed.db"))
    await db.initialize()
    try:
        first_id = await db.add_account(Account(phone="+15551110001", session_string="sess_one"))
        await db.delete_account(first_id)
        reused_id = await db.add_account(Account(phone="+15552220002", session_string="sess_two"))
        assert reused_id == first_id  # same rowid the export targets

        export = await db.repos.accounts.get_session_export(account_id=reused_id)
        assert export is not None
        phone, session = export
        # Both fields come from the SAME current row — the stale phone is impossible.
        assert (phone, session) == ("+15552220002", "sess_two")

        # A genuinely absent id yields None (→ 404 at the route), never a stale pairing.
        assert await db.repos.accounts.get_session_export(account_id=999999) is None
    finally:
        await db.close()
