"""Argparse wiring for `export telegram` (issue #834)."""

from __future__ import annotations

from src.cli.parser import build_parser


def test_export_telegram_parses_all_flags():
    parser = build_parser()
    args = parser.parse_args(
        [
            "export", "telegram",
            "--channel-id", "555",
            "--format", "both",
            "--with-media",
            "--max-file-size", "7",
            "--date-from", "2026-01-01",
            "--date-to", "2026-06-01",
            "--limit", "1234",
            "--output", "/tmp/exp",
        ]
    )
    assert args.command == "export"
    assert args.export_action == "telegram"
    assert args.channel_id == 555
    assert args.export_format == "both"
    assert args.with_media is True
    assert args.max_file_size == 7
    assert args.date_from == "2026-01-01"
    assert args.date_to == "2026-06-01"
    assert args.limit == 1234
    assert args.output == "/tmp/exp"


def test_export_telegram_defaults():
    parser = build_parser()
    args = parser.parse_args(["export", "telegram", "--channel-id", "1"])
    assert args.export_format == "json"
    assert args.with_media is False
    assert args.max_file_size is None
    assert args.limit == 5000
    assert args.output is None


def test_export_telegram_rejects_bad_format():
    parser = build_parser()
    import pytest

    with pytest.raises(SystemExit):
        parser.parse_args(["export", "telegram", "--channel-id", "1", "--format", "pdf"])
