"""live_message_lines: media marker must render alongside caption text (issue #1776)."""

from tg_agent.gateway.formatters import live_message_lines


def _data(row):
    return {"messages": [row], "reaction_users": {}}


def test_media_with_caption_renders_marker_and_text():
    # Issue #1776: captioned document rendered as plain text, media marker lost.
    row = {
        "message_id": 101,
        "date": "2026-11-03T09:18:29",
        "sender_name": "user_001",
        "text": "@user_002 registration database starts on January 1",
        "media_type": "document",
    }
    lines = live_message_lines(_data(row))
    assert "  [media: document]" in lines
    assert "  @user_002 registration database starts on January 1" in lines


def test_media_without_caption_keeps_marker():
    row = {
        "message_id": 102,
        "date": "2026-11-03T09:19:00",
        "sender_name": "user_001",
        "media_type": "photo",
    }
    lines = live_message_lines(_data(row))
    assert "  [media: photo]" in lines


def test_web_page_preview_gets_no_marker():
    # Link previews are not downloadable media — no marker line (review on #1813).
    row = {
        "message_id": 104,
        "date": "2026-11-03T09:21:00",
        "sender_name": "user_001",
        "text": "look at this https://example.com/post",
        "media_type": "web_page",
    }
    lines = live_message_lines(_data(row))
    assert "  look at this https://example.com/post" in lines
    assert not any("[media:" in line for line in lines)


def test_text_only_message_has_no_marker():
    row = {
        "message_id": 103,
        "date": "2026-11-03T09:20:00",
        "sender_name": "user_001",
        "text": "plain text",
    }
    lines = live_message_lines(_data(row))
    assert "  plain text" in lines
    assert not any("[media:" in line for line in lines)
