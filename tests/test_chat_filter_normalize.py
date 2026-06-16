"""Tests for chat-filter token normalization (audit #835/7)."""

from __future__ import annotations

from src.utils.search_query_chat_filter import _normalize_token


def test_private_chat_link_normalizes_to_bare_positive_id():
    # channel_id is stored bare-positive in the DB; a -100 prefix never matched,
    # so the private-chat filter silently found nothing.
    assert _normalize_token("t.me/c/1234567890") == "1234567890"
    assert _normalize_token("https://t.me/c/1234567890/55") == "1234567890"


def test_public_username_link_still_normalizes_to_username():
    assert _normalize_token("t.me/somechannel") == "somechannel"


def test_at_username_normalizes():
    assert _normalize_token("@somechannel") == "somechannel"
