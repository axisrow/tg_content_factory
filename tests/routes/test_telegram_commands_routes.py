"""Tests for /telegram-commands/{id} route — sensitive data redaction."""

from __future__ import annotations

import base64

import pytest
from httpx import ASGITransport, AsyncClient

from src.models import TelegramCommand, TelegramCommandStatus


@pytest.fixture
async def client(base_app):
    app, _, _ = base_app
    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=False,
        headers={"Authorization": f"Basic {auth_header}"},
    ) as c:
        yield c


@pytest.mark.asyncio
async def test_get_command_status_redacts_sensitive_payload(client, base_app):
    app, db, _ = base_app
    cmd = TelegramCommand(
        command_type="accounts.connect",
        payload={"phone": "+123", "session_string": "SECRET_SESSION"},
        status=TelegramCommandStatus.SUCCEEDED,
        requested_by="test",
        result_payload={"phone": "+123", "token": "xxx", "is_premium": True},
    )
    cid = await db.repos.telegram_commands.create_command(cmd)

    resp = await client.get(f"/telegram-commands/{cid}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["payload"]["phone"] == "+123"
    assert body["payload"]["session_string"] == "[REDACTED]"
    assert body["result_payload"]["token"] == "[REDACTED]"
    assert body["result_payload"]["is_premium"] is True


@pytest.mark.asyncio
async def test_get_command_status_404(client):
    resp = await client.get("/telegram-commands/999999")
    assert resp.status_code == 404
