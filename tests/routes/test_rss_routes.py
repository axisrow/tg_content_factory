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

    resp = await route_client.get("/rss.xml")
    assert resp.status_code == 200
    assert "Hello world" in resp.text
    assert "<item>" in resp.text


@pytest.mark.anyio
async def test_rss_feed_channel_filter(route_client, base_app):
    """A channel_id query must scope the feed to that channel only (#676)."""
    _, db, _ = base_app

    await db.insert_message(Message(channel_id=100, message_id=1, text="From channel 100", date=NOW))
    await db.insert_message(Message(channel_id=200, message_id=2, text="From channel 200", date=NOW))

    resp = await route_client.get("/rss.xml?channel_id=100")
    assert resp.status_code == 200
    assert "From channel 100" in resp.text
    assert "From channel 200" not in resp.text


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

    resp = await route_client.get("/atom.xml")
    assert resp.status_code == 200
    assert "Atom test message" in resp.text
    assert "<entry>" in resp.text


# --- XML well-formedness with hostile message text (#837/8 regression) ---


@pytest.mark.anyio
async def test_rss_feed_well_formed_with_control_byte_in_message(route_client, base_app):
    """A C0 control byte in message text must not break /rss.xml — it reaches the <link>
    query too, where html.escape (unlike escape_xml_text/quote_plus) left it raw and made
    the whole feed not well-formed. The full document must parse."""
    import xml.etree.ElementTree as ET

    _, db, _ = base_app
    await db.insert_message(
        Message(channel_id=100, message_id=1, text="bad\x01title with control & <stuff>", date=NOW)
    )

    resp = await route_client.get("/rss.xml")
    assert resp.status_code == 200
    # The load-bearing assertion: the document parses. A raw \x01 anywhere (incl. <link>) raises.
    ET.fromstring(resp.text)
    assert "\x01" not in resp.text


@pytest.mark.anyio
async def test_atom_feed_well_formed_with_control_byte_in_message(route_client, base_app):
    import xml.etree.ElementTree as ET

    _, db, _ = base_app
    await db.insert_message(
        Message(channel_id=100, message_id=1, text="atom\x01bad & <x>", date=NOW)
    )

    resp = await route_client.get("/atom.xml")
    assert resp.status_code == 200
    ET.fromstring(resp.text)
    assert "\x01" not in resp.text
