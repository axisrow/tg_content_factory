"""Typer wiring for `export telegram` (issue #834)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from src.cli.typer_app import app

runner = CliRunner()


def test_export_telegram_parses_all_flags():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.export_cmd.telegram_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app,
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
            ],
        )
    assert result.exit_code == 0
    kwargs = mock_impl.call_args.kwargs
    assert kwargs["channel_id"] == 555
    assert kwargs["export_format"] == "both"
    assert kwargs["with_media"] is True
    assert kwargs["max_file_size"] == 7
    assert kwargs["date_from"] == "2026-01-01"
    assert kwargs["date_to"] == "2026-06-01"
    assert kwargs["limit"] == 1234
    assert kwargs["output"] == "/tmp/exp"


def test_export_telegram_defaults():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.export_cmd.telegram_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["export", "telegram", "--channel-id", "1"])
    assert result.exit_code == 0
    kwargs = mock_impl.call_args.kwargs
    assert kwargs["export_format"] == "json"
    assert kwargs["with_media"] is False
    assert kwargs["max_file_size"] is None
    assert kwargs["limit"] == 5000
    assert kwargs["output"] is None


def test_export_telegram_rejects_bad_format():
    result = runner.invoke(app, ["export", "telegram", "--channel-id", "1", "--format", "pdf"])
    assert result.exit_code != 0
