"""Tests for the Telegram-Desktop export builder (issue #834)."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from src.models import Channel, Message
from src.services.telegram_export_builder import (
    MANIFEST_JSON_NAME,
    MEDIA_NOT_INCLUDED,
    MEDIA_TOO_BIG,
    REASON_EXCEEDS_MAX_SIZE,
    RESULT_JSON_NAME,
    MediaArtifact,
    TelegramExportBuilder,
    html_page_name,
    offline_media_resolver,
    telegram_chat_type,
)


def _channel(**kw) -> Channel:
    base = dict(channel_id=100, title="Test Channel", username="testchan", channel_type="channel")
    base.update(kw)
    return Channel(**base)


def _msg(message_id: int, **kw) -> Message:
    base = dict(
        channel_id=100,
        message_id=message_id,
        date=datetime(2026, 6, 12, 15, 4, 5, tzinfo=timezone.utc),
        text=f"message {message_id}",
    )
    base.update(kw)
    return Message(**base)


# ── pure helpers ──────────────────────────────────────────────────────────


def test_html_page_name_convention():
    assert html_page_name(0) == "messages.html"
    assert html_page_name(1) == "messages2.html"
    assert html_page_name(4) == "messages5.html"


def test_telegram_chat_type_public_vs_private():
    assert telegram_chat_type(_channel(channel_type="channel", username="x")) == "public_channel"
    assert telegram_chat_type(_channel(channel_type="channel", username=None)) == "private_channel"
    assert telegram_chat_type(_channel(channel_type="supergroup", username="x")) == "public_supergroup"
    assert telegram_chat_type(_channel(channel_type="group", username=None)) == "private_group"


def test_offline_resolver_marks_media_not_included():
    assert offline_media_resolver(_msg(1, text="x")) is None  # no media
    art = offline_media_resolver(_msg(2, media_type="photo"))
    assert art is not None and art.skipped and art.kind == "photo"


# ── result.json structure ─────────────────────────────────────────────────


def test_result_json_top_level_and_message_shape():
    ch = _channel()
    msgs = [_msg(10, sender_id=42, sender_kind="user", sender_name="Alice")]
    result = TelegramExportBuilder().build_result_json(ch, msgs, {10: None})

    assert result["name"] == "Test Channel"
    assert result["type"] == "public_channel"
    assert result["id"] == 100
    assert len(result["messages"]) == 1

    m = result["messages"][0]
    assert m["id"] == 10
    assert m["type"] == "message"
    assert m["from"] == "Alice"
    assert m["from_id"] == "user42"
    assert m["date"] == "2026-06-12T15:04:05"
    assert m["date_unixtime"] == str(int(datetime(2026, 6, 12, 15, 4, 5, tzinfo=timezone.utc).timestamp()))
    assert m["text"] == "message 10"
    assert m["text_entities"] == [{"type": "plain", "text": "message 10"}]


def test_channel_broadcast_post_attributed_to_channel():
    ch = _channel()
    # No sender_id → channel broadcast post.
    result = TelegramExportBuilder().build_result_json(ch, [_msg(11)], {11: None})
    m = result["messages"][0]
    assert m["from_id"] == "channel100"
    assert m["from"] == "Test Channel"


def test_reactions_mapped_to_telegram_shape():
    ch = _channel()
    msg = _msg(12, reactions_json=json.dumps([{"emoji": "👍", "count": 5}]))
    result = TelegramExportBuilder().build_result_json(ch, [msg], {12: None})
    assert result["messages"][0]["reactions"] == [{"type": "emoji", "count": 5, "emoji": "👍"}]


def test_service_message_shape():
    ch = _channel()
    msg = _msg(
        13, text=None, service_action_semantic="pin_message",
        sender_id=42, sender_kind="user", sender_name="Alice",
    )
    result = TelegramExportBuilder().build_result_json(ch, [msg], {13: None})
    m = result["messages"][0]
    assert m["type"] == "service"
    assert m["action"] == "pin_message"
    assert m["actor"] == "Alice"
    assert m["text"] == ""


def test_forwarded_from_marker():
    ch = _channel()
    msg = _msg(14, forward_from_channel_id=999)
    result = TelegramExportBuilder().build_result_json(ch, [msg], {14: None})
    assert result["messages"][0]["forwarded_from"] == "channel999"


# ── media placeholders ─────────────────────────────────────────────────────


def test_media_skipped_uses_exact_placeholder_strings():
    ch = _channel()
    msgs = [_msg(20, media_type="photo"), _msg(21, media_type="document")]
    artifacts = {
        20: MediaArtifact(kind="photo", skipped=True, reason="not_included"),
        21: MediaArtifact(kind="file", skipped=True, reason=REASON_EXCEEDS_MAX_SIZE, size_bytes=9_000_000),
    }
    result = TelegramExportBuilder().build_result_json(ch, msgs, artifacts)
    assert result["messages"][0]["photo"] == MEDIA_NOT_INCLUDED
    assert result["messages"][1]["file"] == MEDIA_TOO_BIG
    assert result["messages"][1]["media_type"] == "file"
    assert result["messages"][1]["file_size"] == 9_000_000


def test_media_downloaded_links_relative_path():
    ch = _channel()
    art = {30: MediaArtifact(kind="photo", rel_path="photos/photo_30.jpg", size_bytes=1234)}
    result = TelegramExportBuilder().build_result_json(ch, [_msg(30, media_type="photo")], art)
    assert result["messages"][0]["photo"] == "photos/photo_30.jpg"


# ── HTML rendering / pagination ────────────────────────────────────────────


def test_html_single_page_no_nav():
    ch = _channel()
    pages = TelegramExportBuilder().build_html_pages(ch, [_msg(1), _msg(2)], {1: None, 2: None}, page_size=10)
    assert len(pages) == 1
    name, html_doc = pages[0]
    assert name == "messages.html"
    assert "Test Channel" in html_doc
    assert 'class="pagination"' not in html_doc  # no nav div for a single page


def test_html_pagination_splits_and_links():
    ch = _channel()
    msgs = [_msg(i) for i in range(1, 6)]
    pages = TelegramExportBuilder().build_html_pages(ch, msgs, {m.message_id: None for m in msgs}, page_size=2)
    assert [p[0] for p in pages] == ["messages.html", "messages2.html", "messages3.html"]
    assert 'href="messages2.html"' in pages[0][1]  # first links Next
    assert "Previous" in pages[1][1] and "Next" in pages[1][1]  # middle has both
    assert "Next" not in pages[2][1].split("history")[-1]  # last has no Next link


def test_html_escapes_text():
    ch = _channel()
    msg = _msg(40, text="<script>alert(1)</script>", sender_name="Bob")
    pages = TelegramExportBuilder().build_html_pages(ch, [msg], {40: None}, page_size=10)
    assert "&lt;script&gt;" in pages[0][1]
    assert "<script>alert(1)</script>" not in pages[0][1]


# ── write_export end-to-end ────────────────────────────────────────────────


def test_write_export_default_json_only(tmp_path):
    ch = _channel()
    summary = TelegramExportBuilder().write_export(tmp_path, ch, [_msg(1)], fmt="json")
    files = {p.name for p in tmp_path.iterdir()}
    assert files == {RESULT_JSON_NAME, MANIFEST_JSON_NAME}
    assert summary.message_count == 1
    # result.json parses and is well-formed
    data = json.loads((tmp_path / RESULT_JSON_NAME).read_text(encoding="utf-8"))
    assert data["messages"][0]["id"] == 1


def test_write_export_both_writes_html(tmp_path):
    ch = _channel()
    TelegramExportBuilder().write_export(tmp_path, ch, [_msg(1), _msg(2)], fmt="both", page_size=1)
    names = {p.name for p in tmp_path.iterdir()}
    assert RESULT_JSON_NAME in names
    assert "messages.html" in names and "messages2.html" in names
    assert MANIFEST_JSON_NAME in names


def test_write_export_manifest_records_skipped(tmp_path):
    ch = _channel()
    msgs = [_msg(1, media_type="document"), _msg(2, text="plain")]

    def resolver(m):
        if m.media_type:
            return MediaArtifact(kind="file", skipped=True, reason=REASON_EXCEEDS_MAX_SIZE, size_bytes=5_000_000)
        return None

    summary = TelegramExportBuilder().write_export(tmp_path, ch, msgs, fmt="json", media_resolver=resolver)
    manifest = json.loads((tmp_path / MANIFEST_JSON_NAME).read_text(encoding="utf-8"))
    assert manifest["media_skipped"] == 1
    assert summary.media_skipped == 1
    entry = manifest["skipped_files"][0]
    assert entry["message_id"] == 1
    assert entry["reason"] == REASON_EXCEEDS_MAX_SIZE
    assert entry["original_size_bytes"] == 5_000_000
    assert entry["media_type"] == "document"


def test_write_export_offline_does_not_break_on_media(tmp_path):
    ch = _channel()
    msgs = [_msg(1, media_type="photo")]
    TelegramExportBuilder().write_export(tmp_path, ch, msgs, fmt="both")
    data = json.loads((tmp_path / RESULT_JSON_NAME).read_text(encoding="utf-8"))
    assert data["messages"][0]["photo"] == MEDIA_NOT_INCLUDED
    # HTML still renders the placeholder, no broken link
    html_doc = (tmp_path / "messages.html").read_text(encoding="utf-8")
    assert MEDIA_NOT_INCLUDED in html_doc


@pytest.mark.parametrize("fmt", ["json", "html", "both"])
def test_write_export_format_selection(tmp_path, fmt):
    ch = _channel()
    TelegramExportBuilder().write_export(tmp_path, ch, [_msg(1)], fmt=fmt)
    names = {p.name for p in tmp_path.iterdir()}
    assert (RESULT_JSON_NAME in names) == (fmt in ("json", "both"))
    assert ("messages.html" in names) == (fmt in ("html", "both"))
    assert MANIFEST_JSON_NAME in names  # manifest always written
