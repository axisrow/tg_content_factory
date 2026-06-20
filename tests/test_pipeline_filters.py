"""Behavior-locking tests for ``match_message_filter`` (#922).

``match_message_filter`` evaluates nine independent filter criteria against a
message. Before refactoring it from a single rank-F function into
predicate-per-criterion helpers, these tests pin the exact current behavior of
each criterion (including the legacy text-fallback paths) so the refactor is
provably non-behavioral.
"""
from __future__ import annotations

from types import SimpleNamespace

from src.services.pipeline_filters import filter_messages, match_message_filter


def _msg(**kwargs):
    """Build a message stub with the attributes match_message_filter reads."""
    defaults = {
        "text": "",
        "message_kind": None,
        "service_action_semantic": None,
        "sender_kind": None,
        "sender_id": None,
        "sender_name": None,
        "media_type": None,
        "forward_from_channel_id": None,
    }
    defaults.update(kwargs)
    return SimpleNamespace(**defaults)


def _mf(**criteria) -> dict:
    """Build a raw message_filter config."""
    return {"type": "message_filter", **criteria}


# -- No-op / passthrough ----------------------------------------------------


def test_empty_message_filter_passes_everything():
    assert match_message_filter(_msg(text="anything"), _mf()) is True


def test_none_config_normalizes_to_empty_keywords_and_rejects():
    # A None config normalizes to the default "keywords" filter type with no
    # keywords, which rejects everything. (filter_messages avoids this by
    # short-circuiting on a falsy config — see the passthrough test below.)
    assert match_message_filter(_msg(text="hi"), None) is False


def test_filter_messages_passthrough_on_empty_config():
    msgs = [_msg(text="a"), _msg(text="b")]
    assert filter_messages(msgs, None) == msgs


# -- message_kinds ----------------------------------------------------------


def test_message_kinds_match():
    assert match_message_filter(_msg(message_kind="photo"), _mf(message_kinds=["photo"])) is True


def test_message_kinds_mismatch_rejected():
    assert match_message_filter(_msg(message_kind="text"), _mf(message_kinds=["photo"])) is False


def test_message_kinds_service_legacy_text_fallback_passes():
    # message_kind != "service" but config wants ["service"], no semantic action,
    # and the text carries a service keyword → legacy fallback lets it through.
    msg = _msg(message_kind="text", service_action_semantic=None, text="User joined the group")
    assert match_message_filter(msg, _mf(message_kinds=["service"])) is True


def test_message_kinds_service_legacy_text_fallback_no_keyword_rejected():
    msg = _msg(message_kind="text", service_action_semantic=None, text="hello world")
    assert match_message_filter(msg, _mf(message_kinds=["service"])) is False


# -- service_actions --------------------------------------------------------


def test_service_actions_match():
    msg = _msg(service_action_semantic="join")
    assert match_message_filter(msg, _mf(service_actions=["join"])) is True


def test_service_actions_semantic_mismatch_rejected():
    msg = _msg(service_action_semantic="leave")
    assert match_message_filter(msg, _mf(service_actions=["join"])) is False


def test_service_actions_legacy_alias_in_text_passes():
    msg = _msg(service_action_semantic=None, text="Someone joined")
    assert match_message_filter(msg, _mf(service_actions=["join"])) is True


def test_service_actions_legacy_no_alias_rejected():
    msg = _msg(service_action_semantic=None, text="nothing here")
    assert match_message_filter(msg, _mf(service_actions=["join"])) is False


# -- media_types ------------------------------------------------------------


def test_media_types_match():
    assert match_message_filter(_msg(media_type="photo"), _mf(media_types=["photo"])) is True


def test_media_types_mismatch_rejected():
    assert match_message_filter(_msg(media_type="video"), _mf(media_types=["photo"])) is False


# -- sender_kinds -----------------------------------------------------------


def test_sender_kinds_match():
    assert match_message_filter(_msg(sender_kind="user"), _mf(sender_kinds=["user"])) is True


def test_sender_kinds_mismatch_rejected():
    assert match_message_filter(_msg(sender_kind="bot"), _mf(sender_kinds=["user"])) is False


def test_sender_kinds_anonymous_admin_inferred_passes():
    msg = _msg(sender_kind=None, sender_id=None, sender_name=None)
    assert match_message_filter(msg, _mf(sender_kinds=["anonymous_admin"])) is True


def test_sender_kinds_anonymous_admin_with_sender_id_rejected():
    msg = _msg(sender_kind=None, sender_id=123, sender_name=None)
    assert match_message_filter(msg, _mf(sender_kinds=["anonymous_admin"])) is False


# -- forwarded --------------------------------------------------------------


def test_forwarded_true_match():
    assert match_message_filter(_msg(forward_from_channel_id=42), _mf(forwarded=True)) is True


def test_forwarded_true_but_not_forwarded_rejected():
    assert match_message_filter(_msg(forward_from_channel_id=None), _mf(forwarded=True)) is False


def test_forwarded_false_match():
    assert match_message_filter(_msg(forward_from_channel_id=None), _mf(forwarded=False)) is True


def test_forwarded_false_but_forwarded_rejected():
    assert match_message_filter(_msg(forward_from_channel_id=7), _mf(forwarded=False)) is False


# -- has_text ---------------------------------------------------------------


def test_has_text_true_match():
    assert match_message_filter(_msg(text="hello"), _mf(has_text=True)) is True


def test_has_text_true_but_blank_rejected():
    assert match_message_filter(_msg(text="   "), _mf(has_text=True)) is False


def test_has_text_false_match():
    assert match_message_filter(_msg(text=""), _mf(has_text=False)) is True


# -- keywords ---------------------------------------------------------------


def test_keywords_match_case_insensitive():
    assert match_message_filter(_msg(text="Hello World"), _mf(keywords=["hello"])) is True


def test_keywords_no_match_rejected():
    assert match_message_filter(_msg(text="bye"), _mf(keywords=["hello"])) is False


def test_keywords_filter_type_with_no_keywords_rejected():
    # The legacy "keywords" filter type with neither keywords nor link matching
    # rejects everything.
    assert match_message_filter(_msg(text="anything"), {"type": "keywords", "keywords": []}) is False


def test_keywords_filter_type_matches():
    assert match_message_filter(_msg(text="buy now"), {"type": "keywords", "keywords": ["buy"]}) is True


# -- match_links ------------------------------------------------------------


def test_match_links_https_passes():
    assert match_message_filter(_msg(text="see https://example.com"), _mf(match_links=True)) is True


def test_match_links_tme_passes():
    assert match_message_filter(_msg(text="join t.me/channel"), _mf(match_links=True)) is True


def test_match_links_no_link_rejected():
    assert match_message_filter(_msg(text="no link here"), _mf(match_links=True)) is False


# -- regex ------------------------------------------------------------------


def test_regex_match():
    assert match_message_filter(_msg(text="order 123"), _mf(regex=r"\d+")) is True


def test_regex_no_match_rejected():
    assert match_message_filter(_msg(text="no digits"), _mf(regex=r"\d+")) is False


def test_regex_case_insensitive():
    assert match_message_filter(_msg(text="HELLO"), _mf(regex=r"hello")) is True


def test_regex_invalid_pattern_rejected():
    assert match_message_filter(_msg(text="anything"), _mf(regex="[")) is False


def test_regex_filter_type_empty_pattern_rejected():
    assert match_message_filter(_msg(text="anything"), {"type": "regex", "pattern": ""}) is False


def test_regex_filter_type_matches():
    assert match_message_filter(_msg(text="order 99"), {"type": "regex", "pattern": r"\d+"}) is True


# -- combined criteria (short-circuit / AND semantics) ----------------------


def test_combined_all_pass():
    msg = _msg(message_kind="text", media_type=None, text="buy https://x.io", forward_from_channel_id=None)
    config = _mf(message_kinds=["text"], keywords=["buy"], match_links=True, forwarded=False)
    assert match_message_filter(msg, config) is True


def test_combined_one_fails_rejected():
    # keyword present but message_kind mismatches → overall reject.
    msg = _msg(message_kind="photo", text="buy now")
    config = _mf(message_kinds=["text"], keywords=["buy"])
    assert match_message_filter(msg, config) is False


def test_anonymous_sender_filter_type_passes_for_anon():
    msg = _msg(sender_kind=None, sender_id=None, sender_name=None)
    assert match_message_filter(msg, {"type": "anonymous_sender"}) is True
