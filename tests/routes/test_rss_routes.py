"""Tests for RSS/Atom feed routes."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.models import Message

NOW = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


# --- _rfc822 / _iso8601 unit tests ---


@pytest.mark.anyio
async def test_rfc822_none():
    from src.web.routes.rss import _rfc822

    result = _rfc822(None)
    assert isinstance(result, str)
    assert len(result) > 10  # e.g. "Fri, 01 Jan 2026 00:00:00 +0000"


@pytest.mark.anyio
async def test_rfc822_naive_datetime():
    from src.web.routes.rss import _rfc822

    naive = datetime(2025, 6, 15, 10, 30, 0)
    result = _rfc822(naive)
    assert "+0000" in result or "GMT" in result or "UTC" in result


@pytest.mark.anyio
async def test_rfc822_aware_datetime():
    from src.web.routes.rss import _rfc822

    aware = datetime(2025, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
    result = _rfc822(aware)
    assert "Jun" in result or "15" in result


@pytest.mark.anyio
async def test_iso8601_none():
    from src.web.routes.rss import _iso8601

    result = _iso8601(None)
    assert isinstance(result, str)
    assert "T" in result  # ISO format


# --- RSS feed route tests ---


@pytest.mark.anyio
async def test_rss_feed_empty(route_client):
    resp = await route_client.get("/rss.xml")
    assert resp.status_code == 200
    assert "application/rss+xml" in resp.headers["content-type"]
    assert "<rss" in resp.text
    assert "<channel>" in resp.text
    assert "<title>TG Content Factory</title>" in resp.text
    assert "<item>" not in resp.text


@pytest.mark.anyio
async def test_rss_feed_with_messages(route_client, base_app):
    _, db, _ = base_app

    msg = Message(channel_id=100, message_id=1, text="Hello world", date=NOW)
    await db.insert_message(msg)

    # db.get_messages() doesn't exist on Database facade; monkeypatch it
    async def _get_messages(channel_id=None, limit=50):
        rows, _ = await db.search_messages("", channel_id=channel_id, limit=limit)
        return rows

    db.get_messages = _get_messages

    resp = await route_client.get("/rss.xml")
    assert resp.status_code == 200
    assert "Hello world" in resp.text
    assert "<item>" in resp.text


# --- Atom feed route tests ---


@pytest.mark.anyio
async def test_atom_feed_empty(route_client):
    resp = await route_client.get("/atom.xml")
    assert resp.status_code == 200
    assert "application/atom+xml" in resp.headers["content-type"]
    assert "<feed" in resp.text
    assert "<title>TG Content Factory</title>" in resp.text
    assert "<entry>" not in resp.text


@pytest.mark.anyio
async def test_atom_feed_with_messages(route_client, base_app):
    _, db, _ = base_app

    msg = Message(channel_id=100, message_id=1, text="Atom test message", date=NOW)
    await db.insert_message(msg)

    async def _get_messages(channel_id=None, limit=50):
        rows, _ = await db.search_messages("", channel_id=channel_id, limit=limit)
        return rows

    db.get_messages = _get_messages

    resp = await route_client.get("/atom.xml")
    assert resp.status_code == 200
    assert "Atom test message" in resp.text
    assert "<entry>" in resp.text
