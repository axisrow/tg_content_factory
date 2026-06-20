"""Web route tests for Telegram-Desktop export (issue #834)."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.models import Message


async def _seed_messages(db, channel_id=100, count=3):
    for mid in range(1, count + 1):
        await db.repos.messages.insert_message(
            Message(
                channel_id=channel_id,
                message_id=mid,
                text=f"hello {mid}",
                date=datetime(2026, 6, 12, tzinfo=timezone.utc),
            )
        )


@pytest.mark.anyio
async def test_export_channel_writes_tree(route_client, base_app, tmp_path, monkeypatch):
    _, db, _ = base_app
    monkeypatch.setattr("src.services.export_service.EXPORT_ROOT", tmp_path)
    await _seed_messages(db)  # base_app already seeded channel 100

    resp = await route_client.post("/channels/100/export", data={"format": "both"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["message_count"] == 3
    assert "result.json" in body["files"]
    assert "messages.html" in body["files"]
    # The tree was actually written under the patched root.
    out_files = {p.name for p in (tmp_path).rglob("*") if p.is_file()}
    assert "result.json" in out_files


@pytest.mark.anyio
async def test_export_channel_no_messages_returns_404(route_client, base_app, tmp_path, monkeypatch):
    monkeypatch.setattr("src.services.export_service.EXPORT_ROOT", tmp_path)
    # channel 100 exists but has no messages
    resp = await route_client.post("/channels/100/export", data={"format": "json"})
    assert resp.status_code == 404
    assert resp.json()["error"] == "no_messages"


@pytest.mark.anyio
async def test_export_channel_with_media_note(route_client, base_app, tmp_path, monkeypatch):
    _, db, _ = base_app
    monkeypatch.setattr("src.services.export_service.EXPORT_ROOT", tmp_path)
    await _seed_messages(db)

    resp = await route_client.post("/channels/100/export", data={"format": "json", "with_media": "true"})
    assert resp.status_code == 200
    assert resp.json()["note"] is not None
