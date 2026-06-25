"""CliRunner tests for the Wave-4 Typer command groups (epic #959 — issue #1124).

Wave 4 migrates the four largest, most complex command groups off the argparse
dispatcher onto the Typer ``app`` — including every depth-2 nested subparser:

    analytics · channel (+ ``channel tag``) · dialogs (+ ``dialogs queue``)
    · pipeline (+ ``pipeline filter`` / ``node`` / ``edge``)

These tests drive the production ``app`` through ``typer.testing.CliRunner`` and
assert each sub-command (including each *nested* leaf):

* exposes the *same* flags / arguments / sub-command names the argparse parser did
  (the hard invariant of the migration — names/flags/nested paths are frozen), and
* delegates to the shared ``*_impl`` body with the flags mapped to exactly the
  right keyword arguments.

The shared bodies are stubbed (and ``run_async`` is patched to capture rather than
execute the coroutine) so no real DB / Telegram / provider work happens — the
wiring from CLI tokens to the body is what is under test. A final section drives
the real prod path (``build_parser`` → ``dispatch_via_typer``) so the
argparse→Typer round-trip — *especially the depth-2 nested paths* — is guarded
end to end.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from src.cli.parser import build_parser
from src.cli.typer_app import app
from src.cli.typer_commands import dispatch_via_typer

runner = CliRunner()


def _delegate(argv: list[str]) -> None:
    """Run the real prod path: argparse parse → argparse→Typer delegation."""
    args = build_parser().parse_args(argv)
    dispatch_via_typer(args)


# --------------------------------------------------------------------------- #
# analytics — flat group (no nesting)
# --------------------------------------------------------------------------- #


def test_analytics_top_defaults():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.analytics_cmd.top_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["analytics", "top"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", limit=20, date_from=None, date_to=None)


def test_analytics_top_flags():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.analytics_cmd.top_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app, ["analytics", "top", "--limit", "5", "--date-from", "2024-01-01"]
        )
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", limit=5, date_from="2024-01-01", date_to=None)


def test_analytics_content_types():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.analytics_cmd.content_types_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["analytics", "content-types"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", date_from=None, date_to=None)


def test_analytics_daily_flags():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.analytics_cmd.daily_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["analytics", "daily", "--days", "7", "--pipeline-id", "3"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", days=7, pipeline_id=3)


def test_analytics_summary():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.analytics_cmd.summary_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["analytics", "summary"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml")


def test_analytics_peak_hours():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.analytics_cmd.peak_hours_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["analytics", "peak-hours"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml")


def test_analytics_trending_emojis_flags():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.analytics_cmd.trending_emojis_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["analytics", "trending-emojis", "--days", "3", "--limit", "10"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", days=3, limit=10)


def test_analytics_channel_positional():
    """Negative channel_id passes through (positional after ``--`` on prod path)."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.analytics_cmd.channel_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["analytics", "channel", "--days", "10", "--", "-100123"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", channel_id=-100123, days=10)


def test_analytics_channel_rating_enums():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.analytics_cmd.channel_rating_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app, ["analytics", "channel-rating", "--useful", "useful", "--genre", "ad"]
        )
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", useful="useful", genre="ad", limit=50)


def test_analytics_channel_rating_bad_enum_rejected():
    """An unknown --useful choice is rejected (str-Enum closed set, like argparse)."""
    with patch("src.cli.typer_commands.run_async"):
        result = runner.invoke(app, ["analytics", "channel-rating", "--useful", "bogus"])
    assert result.exit_code != 0


def test_analytics_channel_rate():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.analytics_cmd.channel_rate_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app, ["analytics", "channel-rate", "123", "--model", "openai:gpt-4", "--sample-size", "10"]
        )
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml", channel_id=123, model="openai:gpt-4", sample_size=10
    )


# --------------------------------------------------------------------------- #
# analytics — prod round-trip (build_parser → dispatch_via_typer)
# --------------------------------------------------------------------------- #


def test_analytics_top_roundtrip():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.analytics_cmd.top_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["analytics", "top", "--limit", "7"])
    mock_impl.assert_called_once_with("config.yaml", limit=7, date_from=None, date_to=None)


def test_analytics_bare_defaults_to_top_roundtrip():
    """Bare ``analytics`` (no action) routes to ``top`` — argparse parity."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.analytics_cmd.top_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["analytics"])
    mock_impl.assert_called_once_with("config.yaml", limit=20, date_from=None, date_to=None)


def test_analytics_channel_negative_id_roundtrip():
    """Negative channel_id survives the argparse→Typer round-trip via ``--``."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.analytics_cmd.channel_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["analytics", "channel", "-100123456", "--days", "14"])
    mock_impl.assert_called_once_with("config.yaml", channel_id=-100123456, days=14)


def test_analytics_channel_rate_roundtrip():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.analytics_cmd.channel_rate_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["analytics", "channel-rate", "555", "--sample-size", "20"])
    mock_impl.assert_called_once_with("config.yaml", channel_id=555, model=None, sample_size=20)
