"""Tests for src.telegram.identity — sender-identity extraction (mutation slice #6).

The module had zero direct test coverage; every mutant survived. These pins
cover the core pure-function contract — field extraction, fallback ids, name
precedence, and username normalization (the Mock-instance guards in
normalize_username/_text_attr/_int_value stay unpinned).
"""

from __future__ import annotations

from types import SimpleNamespace

from tg_shared.identity import (
    SenderIdentity,
    extract_message_sender_identity,
    extract_sender_identity,
    normalize_username,
)


def test_extract_sender_identity_user_full():
    sender = SimpleNamespace(
        id=42, first_name=" Ann ", last_name="Lee", username="@ann_l", title="ignored"
    )
    assert extract_sender_identity(sender) == SenderIdentity(
        sender_id=42,
        sender_name="Ann Lee",
        sender_first_name="Ann",
        sender_last_name="Lee",
        sender_username="ann_l",
    )


def test_extract_sender_identity_channel_falls_back_to_title():
    sender = SimpleNamespace(id=-100100, first_name=None, last_name=None, username=None, title="My Channel")
    assert extract_sender_identity(sender) == SenderIdentity(
        sender_id=-100100,
        sender_name="My Channel",
        sender_first_name=None,
        sender_last_name=None,
        sender_username=None,
    )


def test_extract_sender_identity_none_sender_uses_fallback_id():
    assert extract_sender_identity(None, fallback_sender_id=7) == SenderIdentity(sender_id=7)


def test_extract_sender_identity_sender_id_wins_over_fallback():
    sender = SimpleNamespace(id=42)
    identity = extract_sender_identity(sender, fallback_sender_id=7)
    assert identity.sender_id == 42


def test_extract_sender_identity_empty_names_yield_none_name():
    sender = SimpleNamespace(id=1, first_name="   ", last_name="", username="@@@")
    identity = extract_sender_identity(sender)
    assert identity.sender_first_name is None
    assert identity.sender_last_name is None
    assert identity.sender_username is None
    assert identity.sender_name is None


def test_extract_message_sender_identity_prefers_sender_over_msg_fallback():
    msg = SimpleNamespace(sender=SimpleNamespace(id=42), sender_id=7)
    assert extract_message_sender_identity(msg).sender_id == 42


def test_extract_message_sender_identity_missing_sender_uses_msg_fallback():
    msg = SimpleNamespace(sender=None, sender_id=7)
    assert extract_message_sender_identity(msg) == SenderIdentity(sender_id=7)


def test_extract_message_sender_identity_explicit_sender_overrides_msg():
    msg = SimpleNamespace(sender=None, sender_id=7)
    explicit = SimpleNamespace(id=99)
    assert extract_message_sender_identity(msg, sender=explicit).sender_id == 99


def test_normalize_username_strips_at_and_whitespace():
    assert normalize_username("  @ann ") == "ann"
    assert normalize_username("@@@") is None
    assert normalize_username(None) is None
