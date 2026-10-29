"""Shared sessions must not merge project identities, grants or receipts."""

import json
import secrets
import sqlite3
from uuid import uuid4

import pytest

from tg_agent.gateway.store import GatewayConflictError, GatewayStore
from tg_agent.security import SessionCipher


@pytest.fixture
async def shared(tmp_path):
    store = GatewayStore(tmp_path / "gateway")
    await store.open()
    try:
        yield store
    finally:
        await store.close()


async def test_reencrypt_grants_and_independent_primary(shared, tmp_path):
    a, b = str(uuid4()), str(uuid4())
    token_a, token_b = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    await shared.register_project(a, tmp_path / "a", token_a)
    await shared.register_project(b, tmp_path / "b", token_b)
    first = await shared.import_account("+100", SessionCipher("old-key").encrypt("session-one"),
                                        source_secret="old-key")
    second = await shared.import_account("+200", "session-two")
    assert await shared.import_account("+100", "session-one") == first
    await shared.grant(a, first, primary=True)
    await shared.grant(a, first)
    await shared.grant(a, second)
    await shared.grant(b, second, primary=True)
    assert (await shared.accounts(a))[0]["id"] == first
    assert (await shared.accounts(b))[0]["id"] == second
    with pytest.raises(PermissionError):
        await shared.session(b, first)
    await shared.grant(b, first)
    await shared.revoke(a, first)
    assert await shared.session(b, first) == "session-one"
    assert "session" not in json.dumps(await shared.accounts(b))
    with sqlite3.connect(shared.root / "gateway.db") as db:
        stored = db.execute("SELECT session FROM accounts WHERE id=?", (first,)).fetchone()[0]
    assert stored.startswith("enc:v2:")
    assert SessionCipher("old-key").encrypt("session-one") != stored
    assert await shared.authenticate(token_a) == a
    assert await shared.authenticate("invalid") is None


async def test_session_conflict_does_not_replace_account(shared):
    account = await shared.import_account("+100", "original")
    with pytest.raises(GatewayConflictError):
        await shared.import_account("+100", "replacement")
    assert await shared.import_account("+100", "original") == account
    with pytest.raises(GatewayConflictError, match="another account"):
        await shared.import_account("+200", "original")


async def test_receipts_survive_restart_and_are_project_scoped(shared, tmp_path):
    a, b = str(uuid4()), str(uuid4())
    await shared.register_project(a, tmp_path / "a", secrets.token_urlsafe(32))
    await shared.register_project(b, tmp_path / "b", secrets.token_urlsafe(32))
    request = str(uuid4())
    await shared.submit(a, request, {"text": "hello"})
    await shared.transition(a, request, "pending", "running")
    await shared.recover()
    assert (await shared.submit(a, request, {"text": "hello"}))["state"] == "uncertain"
    with pytest.raises(GatewayConflictError):
        await shared.submit(a, request, {"text": "changed"})
    with pytest.raises(KeyError):
        await shared.operation(b, request)
    assert (await shared.submit(b, request, {"text": "changed"}))["state"] == "pending"


async def test_reopened_store_uses_its_own_key(shared, tmp_path):
    project = str(uuid4())
    token = secrets.token_urlsafe(32)
    await shared.register_project(project, tmp_path / "a", token)
    account = await shared.import_account("+100", "secret")
    await shared.grant(project, account)
    root = shared.root
    await shared.close()
    reopened = GatewayStore(root)
    await reopened.open()
    try:
        assert await reopened.authenticate(token) == project
        assert await reopened.session(project, account) == "secret"
    finally:
        await reopened.close()
