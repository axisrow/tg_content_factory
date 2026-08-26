"""Property-based regression tests for parser input boundaries."""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from src.parsers import (
    bare_channel_id,
    deduplicate_identifiers,
    extract_identifiers,
    normalize_identifier,
    parse_identifiers,
)

_USERNAME = st.from_regex(r"[A-Za-z][A-Za-z0-9_]{3,31}", fullmatch=True)


@given(st.lists(_USERNAME, min_size=0, max_size=40), st.sampled_from(["\n", ",", ";", "\t"]))
def test_parse_identifiers_round_trips_supported_separators(usernames: list[str], separator: str):
    text = separator.join(f"@{username}" for username in usernames)
    assert parse_identifiers(text) == [f"@{username}" for username in usernames]


@given(st.lists(_USERNAME, min_size=0, max_size=40))
def test_extract_identifiers_finds_generated_usernames(usernames: list[str]):
    text = " surrounding ".join(f"@{username}" for username in usernames)
    assert extract_identifiers(text) == [f"@{username}" for username in usernames]


@given(_USERNAME, st.sampled_from(["", " ", "\t", "\n"]))
def test_normalize_identifier_is_case_insensitive_and_trims(username: str, padding: str):
    value, kind = normalize_identifier(f"{padding}@{username}{padding}")
    assert (value, kind) == (username.lower(), "username")


@given(st.lists(st.one_of(_USERNAME.map(lambda value: f"@{value}"), st.just("")), max_size=40))
def test_deduplicate_identifiers_preserves_first_occurrence(identifiers: list[str]):
    result = deduplicate_identifiers(identifiers)
    expected: list[str] = []
    seen: set[str] = set()
    for value in identifiers:
        key = value.lower().strip()
        if key and key not in seen:
            expected.append(value)
            seen.add(key)
    assert result == expected
    assert len({value.lower().strip() for value in result}) == len(result)


@given(st.integers(min_value=0, max_value=10**15))
def test_bare_channel_id_leaves_positive_storage_ids_unchanged(channel_id: int):
    assert bare_channel_id(channel_id) == channel_id
