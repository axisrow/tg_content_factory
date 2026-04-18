"""Tests for CLI filter command helpers and core logic paths."""
from __future__ import annotations

from unittest.mock import MagicMock

from src.cli.commands.filter import _parse_pks, _print_result


def test_parse_pks_single():
    assert _parse_pks("1") == [1]


def test_parse_pks_multiple():
    assert _parse_pks("1,2,3") == [1, 2, 3]


def test_parse_pks_with_spaces():
    assert _parse_pks(" 1 , 2 , 3 ") == [1, 2, 3]


def test_parse_pks_empty():
    assert _parse_pks("") == []


def test_parse_pks_invalid():
    assert _parse_pks("abc,1,xyz,2") == [1, 2]


def test_parse_pks_commas_only():
    assert _parse_pks(",,,") == []


def test_print_result_nothing():
    result = MagicMock()
    result.purged_count = 0
    result.purged_titles = []
    result.skipped_count = 0
    _print_result(result)


def test_print_result_with_purged(capsys):
    result = MagicMock()
    result.purged_count = 3
    result.purged_titles = ["Ch1", "Ch2", "Ch3"]
    result.skipped_count = 1
    _print_result(result, "Deleted")
    captured = capsys.readouterr()
    assert "Deleted 3 channels" in captured.out
    assert "Ch1" in captured.out
    assert "Skipped: 1" in captured.out
