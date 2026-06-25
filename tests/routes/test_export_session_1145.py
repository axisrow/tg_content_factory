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
async def test_get_session_export_binds_phone_and_session_to_one_row(tmp_path):
    """Regression for the #1145 review HIGH: phone and session must come from the
    SAME row, never a stale phone paired with a fresh session after delete+reinsert.

    The endpoint previously read identity (summaries) and the session in two
    separate awaits; if a row is deleted and a different account reinserted onto
    the reused id between them, the response would mix the old phone with the new
    session. `get_session_export` reads both from one row, so the pairing is
    always consistent — or None if the row is gone.
    """
    db = Database(str(tmp_path / "consistency.db"))
    await db.initialize()
    try:
        first_id = await db.add_account(Account(phone="+15551110001", session_string="sess_one"))

        # Simulate the hazard: delete the row, reinsert a DIFFERENT account.
        await db.delete_account(first_id)
        reused_id = await db.add_account(Account(phone="+15552220002", session_string="sess_two"))

        export = await db.repos.accounts.get_session_export(account_id=reused_id)
        assert export is not None
        phone, session = export
        # phone and session are from the SAME (current) row — never mixed.
        assert (phone, session) == ("+15552220002", "sess_two")

        # A genuinely absent id yields None (→ 404 at the route), not a stale pairing.
        assert await db.repos.accounts.get_session_export(account_id=999999) is None
    finally:
        await db.close()
