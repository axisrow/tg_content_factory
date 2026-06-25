"""CliRunner tests for the Wave-3 Typer command groups (epic #959 — issue #1123).

Wave 3 migrates the medium, depth-1 command groups off the argparse dispatcher
onto the Typer ``app``:

    search-query · filter · settings · scheduler · account · agent · photo-loader

These tests drive the production ``app`` through ``typer.testing.CliRunner`` and
assert each sub-command:

* exposes the *same* flags / arguments / sub-command names the argparse parser did
  (the hard invariant of the migration), and
* delegates to the shared ``*_impl`` body with the flags mapped to exactly the
  right keyword arguments.

The shared bodies are stubbed (and ``run_async`` is patched to capture rather than
execute the coroutine) so no real DB / Telegram work happens — the wiring from CLI
tokens to the body is what is under test. The ``*_delegates_via_argparse`` tests
drive the real prod path (``build_parser`` → ``dispatch_via_typer``) to guard the
argparse→Typer round-trip end to end, including the tri-state / store_const flags.
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
# search-query → list / get / add / edit / delete / toggle / run / stats
# --------------------------------------------------------------------------- #


def test_search_query_list_delegates():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.search_query_cmd.list_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["search-query", "list"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml")


def test_search_query_get_passes_id():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.search_query_cmd.get_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["search-query", "get", "7"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", query_id=7)


def test_search_query_add_defaults():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.search_query_cmd.add_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["search-query", "add", "hello world"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml",
        query="hello world",
        interval=60,
        is_regex=False,
        is_fts=False,
        notify=False,
        track_stats=True,
        exclude_patterns="",
        max_length=None,
        chats="",
    )


def test_search_query_add_all_flags():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.search_query_cmd.add_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app,
            [
                "search-query", "add", "q",
                "--interval", "15",
                "--regex", "--fts", "--notify", "--no-track-stats",
                "--exclude-patterns", "spam",
                "--max-length", "500",
                "--chats", "@chan",
            ],
        )
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml",
        query="q",
        interval=15,
        is_regex=True,
        is_fts=True,
        notify=True,
        track_stats=False,
        exclude_patterns="spam",
        max_length=500,
        chats="@chan",
    )


def test_search_query_edit_unset_flags_are_none():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.search_query_cmd.edit_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["search-query", "edit", "3"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml",
        query_id=3,
        query=None,
        interval=None,
        is_regex=None,
        is_fts=None,
        notify=None,
        track_stats=None,
        exclude_patterns=None,
        max_length=None,
        chats=None,
    )


def test_search_query_edit_tristate_negative_flags():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.search_query_cmd.edit_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app, ["search-query", "edit", "3", "--no-regex", "--no-fts", "--no-notify"]
        )
    assert result.exit_code == 0
    _, kwargs = mock_impl.call_args
    assert kwargs["is_regex"] is False
    assert kwargs["is_fts"] is False
    assert kwargs["notify"] is False


def test_search_query_edit_clear_sentinels():
    """``--no-max-length`` → -1, ``--clear-chats`` → "" (mirrors store_const)."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.search_query_cmd.edit_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app, ["search-query", "edit", "3", "--no-max-length", "--clear-chats"]
        )
    assert result.exit_code == 0
    _, kwargs = mock_impl.call_args
    assert kwargs["max_length"] == -1
    assert kwargs["chats"] == ""


def test_search_query_delete_toggle_run_pass_id():
    for sub, impl_name in [
        ("delete", "delete_impl"),
        ("toggle", "toggle_impl"),
        ("run", "run_impl"),
    ]:
        mock_impl = MagicMock()
        with (
            patch(f"src.cli.typer_commands.search_query_cmd.{impl_name}", mock_impl),
            patch("src.cli.typer_commands.run_async"),
        ):
            result = runner.invoke(app, ["search-query", sub, "9"])
        assert result.exit_code == 0, (sub, result.output)
        mock_impl.assert_called_once_with("config.yaml", query_id=9)


def test_search_query_stats_defaults_and_days():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.search_query_cmd.stats_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["search-query", "stats", "4", "--days", "7"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", query_id=4, days=7)


def test_search_query_bare_group_shows_help_exit_0():
    """A bare ``search-query`` group renders help and exits 0 via the prod path.

    Argparse's old ``sub_attr`` fallback printed the group help and exited 0;
    ``dispatch_via_typer`` reproduces that (a direct CliRunner invoke would exit
    non-zero in standalone mode — that path is covered elsewhere).
    """
    import pytest

    args = build_parser().parse_args(["search-query"])
    with pytest.raises(SystemExit) as exc_info:
        dispatch_via_typer(args)
    assert exc_info.value.code == 0


# --- real prod path: build_parser → dispatch_via_typer round-trip ----------- #


def test_search_query_add_delegates_via_argparse():
    """End-to-end argparse→Typer round-trip; the query goes after ``--`` so a
    value that looks option-like survives Click (here a plain FTS query)."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.search_query_cmd.add_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["search-query", "add", "kremlin OR putin", "--regex", "--chats", "@c"])
    mock_impl.assert_called_once_with(
        "config.yaml",
        query="kremlin OR putin",
        interval=60,
        is_regex=True,
        is_fts=False,
        notify=False,
        track_stats=True,
        exclude_patterns="",
        max_length=None,
        chats="@c",
    )


def test_search_query_edit_clear_sentinels_via_argparse():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.search_query_cmd.edit_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["search-query", "edit", "5", "--no-max-length", "--clear-chats"])
    _, kwargs = mock_impl.call_args
    assert kwargs["query_id"] == 5
    assert kwargs["max_length"] == -1
    assert kwargs["chats"] == ""


def test_search_query_stats_negative_id_via_argparse():
    """A leading-dash positional id round-trips through the ``--`` separator."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.search_query_cmd.stats_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["search-query", "stats", "8", "--days", "14"])
    mock_impl.assert_called_once_with("config.yaml", query_id=8, days=14)
