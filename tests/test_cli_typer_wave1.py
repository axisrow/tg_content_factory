"""CliRunner tests for the Wave-1 Typer commands (epic #959 — issue #1121).

Wave 1 migrates the super-simple commands off the argparse dispatcher onto the
Typer ``app``: serve · worker · stop · restart · mcp-server · collect
(+ ``collect sample``) · search · messages read.

These tests drive the production ``app`` through ``typer.testing.CliRunner`` and
assert each command:

* exposes the *same* flags / arguments the argparse parser did (identical names,
  defaults, behaviour — the hard invariant of the migration), and
* delegates to the shared command body (``serve_web`` / ``*_impl``) with the
  flags mapped to exactly the right keyword arguments.

The shared bodies are stubbed (and ``run_async`` is patched to capture rather
than execute the coroutine) so no real DB / Telegram / uvicorn work happens —
the wiring from CLI tokens to the body is what is under test.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from src.cli.parser import build_parser
from src.cli.typer_app import app
from src.cli.typer_commands import dispatch_via_typer

runner = CliRunner()


def _delegate(argv: list[str]) -> None:
    """Run the real prod path: argparse parse → argparse→Typer delegation.

    Mirrors ``src/cli/main.py``: the process argv is parsed by ``build_parser``,
    then a migrated command is executed by ``dispatch_via_typer`` (which rebuilds
    the Typer argv from the Namespace). This is the round-trip that must preserve
    every flag / positional, including the awkward ones (negative ids, queries
    that start with ``-``).
    """
    args = build_parser().parse_args(argv)
    dispatch_via_typer(args)


# --------------------------------------------------------------------------- #
# serve / worker (synchronous bodies — no async-bridge)
# --------------------------------------------------------------------------- #


def test_serve_delegates_with_defaults():
    with patch("src.cli.typer_commands.serve_cmd.serve_web") as mock_serve:
        result = runner.invoke(app, ["serve"])
    assert result.exit_code == 0
    mock_serve.assert_called_once_with("config.yaml", web_pass=None, no_worker=False)


def test_serve_threads_web_pass_and_no_worker():
    with patch("src.cli.typer_commands.serve_cmd.serve_web") as mock_serve:
        result = runner.invoke(app, ["serve", "--web-pass", "s3cret", "--no-worker"])
    assert result.exit_code == 0
    mock_serve.assert_called_once_with("config.yaml", web_pass="s3cret", no_worker=True)


def test_serve_honours_global_config_option():
    with patch("src.cli.typer_commands.serve_cmd.serve_web") as mock_serve:
        result = runner.invoke(app, ["--config", "prod.yaml", "serve"])
    assert result.exit_code == 0
    mock_serve.assert_called_once_with("prod.yaml", web_pass=None, no_worker=False)


def test_worker_delegates():
    with patch("src.cli.typer_commands.worker_cmd.serve_worker") as mock_worker:
        result = runner.invoke(app, ["worker"])
    assert result.exit_code == 0
    mock_worker.assert_called_once_with("config.yaml")


# --------------------------------------------------------------------------- #
# stop / restart
# --------------------------------------------------------------------------- #


def test_stop_delegates():
    with patch("src.cli.typer_commands.server_control_cmd.stop_web") as mock_stop:
        result = runner.invoke(app, ["stop"])
    assert result.exit_code == 0
    mock_stop.assert_called_once_with("config.yaml")


def test_restart_threads_web_pass():
    with patch("src.cli.typer_commands.server_control_cmd.restart_web") as mock_restart:
        result = runner.invoke(app, ["restart", "--web-pass", "pw"])
    assert result.exit_code == 0
    mock_restart.assert_called_once_with("config.yaml", web_pass="pw")


def test_restart_defaults_web_pass_to_none():
    with patch("src.cli.typer_commands.server_control_cmd.restart_web") as mock_restart:
        result = runner.invoke(app, ["restart"])
    assert result.exit_code == 0
    mock_restart.assert_called_once_with("config.yaml", web_pass=None)


# --------------------------------------------------------------------------- #
# mcp-server
# --------------------------------------------------------------------------- #


def test_mcp_server_defaults_pool_on():
    with patch("src.cli.typer_commands.mcp_server_cmd.serve_mcp") as mock_mcp:
        result = runner.invoke(app, ["mcp-server"])
    assert result.exit_code == 0
    mock_mcp.assert_called_once_with("config.yaml", no_pool=False)


def test_mcp_server_no_pool_flag():
    with patch("src.cli.typer_commands.mcp_server_cmd.serve_mcp") as mock_mcp:
        result = runner.invoke(app, ["mcp-server", "--no-pool"])
    assert result.exit_code == 0
    mock_mcp.assert_called_once_with("config.yaml", no_pool=True)


# --------------------------------------------------------------------------- #
# collect (+ collect sample) — async bodies via run_async
# --------------------------------------------------------------------------- #


def test_collect_all_channels_defaults():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.collect_cmd.collect_impl", mock_impl),
        patch("src.cli.typer_commands.run_async") as mock_run_async,
    ):
        result = runner.invoke(app, ["collect"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", channel_id=None, full=False)
    mock_run_async.assert_called_once()


def test_collect_single_channel_full():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.collect_cmd.collect_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["collect", "--channel-id", "12345", "--full"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", channel_id=12345, full=True)


def test_collect_sample_positional_and_limit():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.collect_cmd.collect_sample_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        # Negative channel ids must be passed after ``--`` (Click, unlike
        # argparse, treats a leading ``-`` as an option). The argparse→Typer
        # delegation emits exactly this form; see test_collect_sample_negative_id.
        result = runner.invoke(app, ["collect", "sample", "--limit", "5", "--", "-100123"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", channel_id=-100123, limit=5)


def test_collect_sample_defaults_limit_to_ten():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.collect_cmd.collect_sample_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["collect", "sample", "777"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", channel_id=777, limit=10)


# --------------------------------------------------------------------------- #
# search
# --------------------------------------------------------------------------- #


def test_search_defaults():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.search_cmd.search_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["search", "hello world"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml",
        query="hello world",
        limit=20,
        mode="local",
        channel_id=None,
        min_length=None,
        max_length=None,
        fts=False,
        include_filtered=False,
        index_now=False,
        reset_index=False,
        purge_cache=False,
    )


def test_search_limit_and_mode():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.search_cmd.search_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["search", "q", "--limit", "5", "--mode", "semantic"])
    assert result.exit_code == 0
    kwargs = mock_impl.call_args.kwargs
    assert kwargs["query"] == "q"
    assert kwargs["limit"] == 5
    assert kwargs["mode"] == "semantic"


def test_search_all_maps_to_include_filtered():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.search_cmd.search_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["search", "q", "--all", "--fts"])
    assert result.exit_code == 0
    kwargs = mock_impl.call_args.kwargs
    assert kwargs["include_filtered"] is True
    assert kwargs["fts"] is True


def test_search_channel_mode_threads_channel_id():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.search_cmd.search_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["search", "q", "--mode", "channel", "--channel-id", "42"])
    assert result.exit_code == 0
    kwargs = mock_impl.call_args.kwargs
    assert kwargs["mode"] == "channel"
    assert kwargs["channel_id"] == 42


def test_search_rejects_unknown_mode():
    """--mode keeps argparse's closed choice set: an unknown value is rejected.

    The Typer signature uses a str-Enum mirroring argparse ``choices``, so a bad
    mode errors (exit 2) before the body runs — it is not silently treated as
    ``local``.
    """
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.search_cmd.search_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["search", "q", "--mode", "bogus"])
    assert result.exit_code == 2
    mock_impl.assert_not_called()


def test_search_mode_value_is_plain_str():
    """The Enum subclasses ``str`` so the body still receives a plain string."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.search_cmd.search_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["search", "q", "--mode", "semantic"])
    assert result.exit_code == 0
    assert mock_impl.call_args.kwargs["mode"] == "semantic"
    assert type(mock_impl.call_args.kwargs["mode"]) is str


def test_messages_read_rejects_unknown_format():
    """--format keeps argparse's text/json/csv choice set (rejects others)."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.messages_cmd.messages_read_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["messages", "read", "100", "--format", "bogus"])
    assert result.exit_code == 2
    mock_impl.assert_not_called()


# --------------------------------------------------------------------------- #
# messages read
# --------------------------------------------------------------------------- #


def test_messages_read_defaults():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.messages_cmd.messages_read_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["messages", "read", "100"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml",
        identifier="100",
        limit=50,
        live=False,
        phone=None,
        query="",
        date_from=None,
        date_to=None,
        topic_id=None,
        offset_id=None,
        include_reaction_users=False,
        reaction_users_limit=20,
        output_format="text",
    )


def test_messages_read_format_json():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.messages_cmd.messages_read_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["messages", "read", "@chan", "--format", "json"])
    assert result.exit_code == 0
    assert mock_impl.call_args.kwargs["output_format"] == "json"


def test_messages_read_live_flags():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.messages_cmd.messages_read_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app,
            [
                "messages", "read", "100",
                "--live", "--phone", "+15550001111",
                "--offset-id", "900", "--topic-id", "7",
            ],
        )
    assert result.exit_code == 0
    kwargs = mock_impl.call_args.kwargs
    assert kwargs["live"] is True
    assert kwargs["phone"] == "+15550001111"
    assert kwargs["offset_id"] == 900
    assert kwargs["topic_id"] == 7


def test_messages_read_date_filters_db_mode():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.messages_cmd.messages_read_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app,
            [
                "messages", "read", "100",
                "--query", "hello",
                "--date-from", "2025-01-01", "--date-to", "2025-12-31",
            ],
        )
    assert result.exit_code == 0
    kwargs = mock_impl.call_args.kwargs
    assert kwargs["query"] == "hello"
    assert kwargs["date_from"] == "2025-01-01"
    assert kwargs["date_to"] == "2025-12-31"


# --------------------------------------------------------------------------- #
# argparse → Typer delegation round-trip (the real prod path)
#
# These guard the invariant "names / flags / behaviour are unchanged" end to
# end: `main()` parses with argparse, then `dispatch_via_typer` rebuilds the
# Typer argv. Awkward positionals (negative ids, dash-leading queries) that
# argparse accepts but Click would mis-read as options must survive the trip.
# --------------------------------------------------------------------------- #


def test_delegation_serve_flags_roundtrip():
    with patch("src.cli.typer_commands.serve_cmd.serve_web") as mock_serve:
        _delegate(["serve", "--web-pass", "pw", "--no-worker"])
    mock_serve.assert_called_once_with("config.yaml", web_pass="pw", no_worker=True)


def test_delegation_collect_single_channel_roundtrip():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.collect_cmd.collect_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["collect", "--channel-id", "12345", "--full"])
    mock_impl.assert_called_once_with("config.yaml", channel_id=12345, full=True)


def test_delegation_collect_sample_negative_id():
    """Regression: a negative channel id must survive argparse→Typer delegation."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.collect_cmd.collect_sample_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["collect", "sample", "-100123", "--limit", "5"])
    mock_impl.assert_called_once_with("config.yaml", channel_id=-100123, limit=5)


def test_delegation_search_dash_query():
    """Regression: a query starting with '-' must survive delegation as the query."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.search_cmd.search_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["search", "-100500", "--mode", "semantic"])
    kwargs = mock_impl.call_args.kwargs
    assert kwargs["query"] == "-100500"
    assert kwargs["mode"] == "semantic"


def test_delegation_search_channel_mode_negative_channel_id():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.search_cmd.search_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["search", "q", "--mode", "channel", "--channel-id", "-100500"])
    kwargs = mock_impl.call_args.kwargs
    assert kwargs["channel_id"] == -100500


def test_delegation_messages_read_negative_identifier():
    """Regression: a negative-id identifier must survive argparse→Typer delegation."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.messages_cmd.messages_read_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["messages", "read", "-100500", "--format", "json", "--limit", "10"])
    kwargs = mock_impl.call_args.kwargs
    assert kwargs["identifier"] == "-100500"
    assert kwargs["output_format"] == "json"
    assert kwargs["limit"] == 10


def test_delegation_mcp_server_no_pool():
    with patch("src.cli.typer_commands.mcp_server_cmd.serve_mcp") as mock_mcp:
        _delegate(["mcp-server", "--no-pool"])
    mock_mcp.assert_called_once_with("config.yaml", no_pool=True)


def test_delegation_honours_global_config():
    with patch("src.cli.typer_commands.worker_cmd.serve_worker") as mock_worker:
        _delegate(["--config", "prod.yaml", "worker"])
    mock_worker.assert_called_once_with("prod.yaml")


# --------------------------------------------------------------------------- #
# Bare-group help parity (regression for the messages-no-subcommand blocker)
#
# argparse's old `sub_attr` fallback printed `messages --help` and exited 0 for
# a bare `messages` (no `read`). The Typer path must match: render help, exit 0,
# and crucially NOT leak a `NoArgsIsHelpError` traceback. Because Typer vendors
# its own Click, the exception is `typer._click.exceptions.NoArgsIsHelpError`
# (not the stdlib `click` one), so `dispatch_via_typer` must catch both.
# --------------------------------------------------------------------------- #


def test_messages_without_subcommand_shows_help_and_exits_zero():
    import pytest

    args = build_parser().parse_args(["messages"])
    # No body should run — only help. dispatch_via_typer raises SystemExit(0).
    with (
        patch("src.cli.typer_commands.messages_cmd.messages_read_impl") as mock_impl,
        patch("src.cli.typer_commands.run_async"),
    ):
        with pytest.raises(SystemExit) as exc_info:
            dispatch_via_typer(args)
    assert exc_info.value.code == 0
    mock_impl.assert_not_called()


def test_messages_without_subcommand_clean_via_full_cli(capsys):
    """End-to-end: `python -m src.main messages` prints help, exits 0, no traceback."""
    import pytest

    from src.cli.main import main

    with patch("sys.argv", ["main.py", "messages"]):
        with pytest.raises(SystemExit) as exc_info:
            main()
    # Exit 0 (argparse parity) and the help — not a NoArgsIsHelpError traceback.
    assert exc_info.value.code == 0
    out = capsys.readouterr()
    combined = out.out + out.err
    assert "NoArgsIsHelpError" not in combined
    assert "Traceback" not in combined
    assert "read" in combined  # the help lists the `read` sub-command
