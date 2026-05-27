from __future__ import annotations

from types import SimpleNamespace

from telethon.tl.types import ReactionPaid

from src.telegram.reactions import (
    extract_message_reactions,
    extract_message_reactions_json,
    format_message_reactions,
    format_reaction_counts,
    format_reactions_json,
    parse_reactions_json,
)
from tests.helpers import make_mock_reactions


def test_extract_message_reactions_none():
    msg = SimpleNamespace(reactions=None)

    assert extract_message_reactions(msg) == []
    assert extract_message_reactions_json(msg) is None
    assert format_message_reactions(msg) == ""


def test_extract_message_reactions_multiple_emoji():
    msg = SimpleNamespace(reactions=make_mock_reactions([("👍", 5), ("❤️", 2)]))

    assert extract_message_reactions(msg) == [
        {"emoji": "👍", "count": 5},
        {"emoji": "❤️", "count": 2},
    ]
    assert format_message_reactions(msg) == "👍 5 ❤️ 2"


def test_extract_message_reactions_custom_emoji():
    msg = SimpleNamespace(reactions=make_mock_reactions([(12345678, 2)]))

    assert extract_message_reactions(msg) == [{"emoji": "custom:12345678", "count": 2}]
    assert format_message_reactions(msg) == "custom:12345678 2"


def test_extract_message_reactions_paid_and_unknown():
    msg = SimpleNamespace(
        reactions=SimpleNamespace(
            results=[
                SimpleNamespace(reaction=ReactionPaid(), count=4),
                SimpleNamespace(reaction=SimpleNamespace(), count=9),
            ]
        )
    )

    assert extract_message_reactions(msg) == [{"emoji": "paid", "count": 4}]
    assert format_message_reactions(msg) == "paid 4"


def test_parse_reactions_json_valid_and_format():
    items = parse_reactions_json('[{"emoji": "🔥", "count": 7}, {"emoji": "custom:42", "count": "3"}]')

    assert items == [{"emoji": "🔥", "count": 7}, {"emoji": "custom:42", "count": 3}]
    assert format_reaction_counts(items) == "🔥 7 custom:42 3"
    assert format_reactions_json('[{"emoji": "🔥", "count": 7}]') == "🔥 7"


def test_parse_reactions_json_invalid():
    assert parse_reactions_json("not json") == []
    assert parse_reactions_json(None) == []
    assert parse_reactions_json('{"emoji": "👍", "count": 1}') == []
