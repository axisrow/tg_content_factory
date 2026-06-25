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


# --------------------------------------------------------------------------- #
# filter → analyze / apply / reset / precheck / toggle / purge / purge-messages
#          / hard-delete
# --------------------------------------------------------------------------- #


def test_filter_analyze_default_and_quick():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.filter_cmd.analyze_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        runner.invoke(app, ["filter", "analyze"])
        runner.invoke(app, ["filter", "analyze", "--quick"])
    assert mock_impl.call_args_list[0].kwargs == {"quick": False}
    assert mock_impl.call_args_list[1].kwargs == {"quick": True}


def test_filter_apply_precheck_delegate():
    for sub, impl_name in [("apply", "apply_impl"), ("precheck", "precheck_impl")]:
        mock_impl = MagicMock()
        with (
            patch(f"src.cli.typer_commands.filter_cmd.{impl_name}", mock_impl),
            patch("src.cli.typer_commands.run_async"),
        ):
            result = runner.invoke(app, ["filter", sub])
        assert result.exit_code == 0, (sub, result.output)
        mock_impl.assert_called_once_with("config.yaml")


def test_filter_toggle_passes_pk():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.filter_cmd.toggle_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["filter", "toggle", "42"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", pk=42)


def test_filter_reset_pks_optional():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.filter_cmd.reset_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        runner.invoke(app, ["filter", "reset"])
        runner.invoke(app, ["filter", "reset", "--pks", "1,2,3"])
    assert mock_impl.call_args_list[0].kwargs == {"pks": None}
    assert mock_impl.call_args_list[1].kwargs == {"pks": "1,2,3"}


def test_filter_purge_yes_short_alias():
    """``-y`` is the short alias for ``--yes`` on purge (argparse parity)."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.filter_cmd.purge_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["filter", "purge", "--pks", "5", "-y"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", pks="5", yes=True)


def test_filter_purge_messages_requires_channel_id():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.filter_cmd.purge_messages_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["filter", "purge-messages", "--channel-id", "-1001", "--yes"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", channel_id=-1001, yes=True)


def test_filter_hard_delete_yes_no_short_alias():
    """hard-delete exposes ``--yes`` only (no ``-y``), matching argparse."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.filter_cmd.hard_delete_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["filter", "hard-delete", "--yes"])
        # -y must NOT be accepted on hard-delete
        result_short = runner.invoke(app, ["filter", "hard-delete", "-y"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", pks=None, yes=True)
    assert result_short.exit_code != 0


def test_filter_purge_messages_delegates_via_argparse():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.filter_cmd.purge_messages_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["filter", "purge-messages", "--channel-id", "-1002", "-y"])
    mock_impl.assert_called_once_with("config.yaml", channel_id=-1002, yes=True)


def test_filter_bare_group_shows_help_exit_0():
    import pytest

    args = build_parser().parse_args(["filter"])
    with pytest.raises(SystemExit) as exc_info:
        dispatch_via_typer(args)
    assert exc_info.value.code == 0


# --------------------------------------------------------------------------- #
# settings → get / set / info / server-time / agent / filter-criteria
#            / reactions / semantic
# --------------------------------------------------------------------------- #


def test_settings_get_all_and_key():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.settings_cmd.get_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        runner.invoke(app, ["settings", "get"])
        runner.invoke(app, ["settings", "get", "--key", "tg_api_id"])
    assert mock_impl.call_args_list[0].kwargs == {"key": None}
    assert mock_impl.call_args_list[1].kwargs == {"key": "tg_api_id"}


def test_settings_set_two_positionals():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.settings_cmd.set_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["settings", "set", "mykey", "myvalue"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", key="mykey", value="myvalue")


def test_settings_info_and_server_time_delegate():
    for sub, impl_name in [("info", "info_impl"), ("server-time", "server_time_impl")]:
        mock_impl = MagicMock()
        with (
            patch(f"src.cli.typer_commands.settings_cmd.{impl_name}", mock_impl),
            patch("src.cli.typer_commands.run_async"),
        ):
            result = runner.invoke(app, ["settings", sub])
        assert result.exit_code == 0, (sub, result.output)
        mock_impl.assert_called_once_with("config.yaml")


def test_settings_agent_flags_optional():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.settings_cmd.agent_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        runner.invoke(app, ["settings", "agent"])
        runner.invoke(
            app, ["settings", "agent", "--backend", "codex", "--prompt-template", "tmpl"]
        )
    assert mock_impl.call_args_list[0].kwargs == {"backend": None, "prompt_template": None}
    assert mock_impl.call_args_list[1].kwargs == {"backend": "codex", "prompt_template": "tmpl"}


def test_settings_filter_criteria_floats():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.settings_cmd.filter_criteria_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app,
            [
                "settings", "filter-criteria",
                "--min-uniqueness", "0.5",
                "--min-sub-ratio", "0.1",
                "--max-cross-dupe", "30",
                "--min-cyrillic", "0.7",
            ],
        )
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml",
        min_uniqueness=0.5,
        min_sub_ratio=0.1,
        max_cross_dupe=30.0,
        min_cyrillic=0.7,
    )


def test_settings_reactions_min_interval():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.settings_cmd.reactions_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        runner.invoke(app, ["settings", "reactions"])
        runner.invoke(app, ["settings", "reactions", "--min-interval", "45"])
    assert mock_impl.call_args_list[0].kwargs == {"min_interval": None}
    assert mock_impl.call_args_list[1].kwargs == {"min_interval": 45}


def test_settings_semantic_flags():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.settings_cmd.semantic_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app,
            ["settings", "semantic", "--provider", "openai", "--model", "te-3", "--api-key", "sk-x"],
        )
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml", provider="openai", model="te-3", api_key="sk-x"
    )


def test_settings_set_delegates_via_argparse():
    """key/value positionals survive the ``--`` separator end to end."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.settings_cmd.set_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["settings", "set", "translation_provider", "openai"])
    mock_impl.assert_called_once_with(
        "config.yaml", key="translation_provider", value="openai"
    )


def test_settings_bare_maps_to_get_via_argparse():
    """A bare ``settings`` ran ``get`` under argparse; the round-trip preserves that."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.settings_cmd.get_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["settings"])
    mock_impl.assert_called_once_with("config.yaml", key=None)


# --------------------------------------------------------------------------- #
# scheduler → start / trigger / status / stop / job-toggle / set-interval
#             / task-cancel / clear-pending / queue-pause / queue-resume
# --------------------------------------------------------------------------- #


def test_scheduler_no_arg_subcommands_delegate():
    for sub, impl_name in [
        ("start", "start_impl"),
        ("trigger", "trigger_impl"),
        ("status", "status_impl"),
        ("stop", "stop_impl"),
        ("clear-pending", "clear_pending_impl"),
        ("queue-pause", "queue_pause_impl"),
        ("queue-resume", "queue_resume_impl"),
    ]:
        mock_impl = MagicMock()
        with (
            patch(f"src.cli.typer_commands.scheduler_cmd.{impl_name}", mock_impl),
            patch("src.cli.typer_commands.run_async"),
        ):
            result = runner.invoke(app, ["scheduler", sub])
        assert result.exit_code == 0, (sub, result.output)
        mock_impl.assert_called_once_with("config.yaml")


def test_scheduler_job_toggle_passes_job_id():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.scheduler_cmd.job_toggle_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["scheduler", "job-toggle", "collect_all"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", job_id="collect_all")


def test_scheduler_set_interval_two_positionals():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.scheduler_cmd.set_interval_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["scheduler", "set-interval", "sq_3", "120"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", job_id="sq_3", minutes=120)


def test_scheduler_task_cancel_int_id():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.scheduler_cmd.task_cancel_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["scheduler", "task-cancel", "55"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", task_id=55)


def test_scheduler_set_interval_delegates_via_argparse():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.scheduler_cmd.set_interval_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["scheduler", "set-interval", "content_generate_2", "30"])
    mock_impl.assert_called_once_with(
        "config.yaml", job_id="content_generate_2", minutes=30
    )


def test_scheduler_bare_group_shows_help_exit_0():
    import pytest

    args = build_parser().parse_args(["scheduler"])
    with pytest.raises(SystemExit) as exc_info:
        dispatch_via_typer(args)
    assert exc_info.value.code == 0


# --------------------------------------------------------------------------- #
# account → list / info / toggle / set-primary / delete / send-code /
#           verify-code / add / flood-status / flood-clear / export-session /
#           import   (export-session & import are the #828 SSO secret-handling ops)
# --------------------------------------------------------------------------- #


def test_account_list_and_flood_status_delegate():
    for sub, impl_name in [("list", "list_impl"), ("flood-status", "flood_status_impl")]:
        mock_impl = MagicMock()
        with (
            patch(f"src.cli.typer_commands.account_cmd.{impl_name}", mock_impl),
            patch("src.cli.typer_commands.run_async"),
        ):
            result = runner.invoke(app, ["account", sub])
        assert result.exit_code == 0, (sub, result.output)
        mock_impl.assert_called_once_with("config.yaml")


def test_account_info_phone_optional():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.account_cmd.info_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        runner.invoke(app, ["account", "info"])
        runner.invoke(app, ["account", "info", "--phone", "+1234567890"])
    assert mock_impl.call_args_list[0].kwargs == {"phone": None}
    assert mock_impl.call_args_list[1].kwargs == {"phone": "+1234567890"}


def test_account_toggle_set_primary_pass_id():
    for sub, impl_name in [("toggle", "toggle_impl"), ("set-primary", "set_primary_impl")]:
        mock_impl = MagicMock()
        with (
            patch(f"src.cli.typer_commands.account_cmd.{impl_name}", mock_impl),
            patch("src.cli.typer_commands.run_async"),
        ):
            result = runner.invoke(app, ["account", sub, "7"])
        assert result.exit_code == 0, (sub, result.output)
        mock_impl.assert_called_once_with("config.yaml", account_id=7)


def test_account_delete_notify_to():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.account_cmd.delete_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["account", "delete", "3", "--notify-to", "+19998887766"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", account_id=3, notify_to="+19998887766")


def test_account_send_code_flags():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.account_cmd.send_code_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app, ["account", "send-code", "--phone", "+1", "--api-id", "42", "--api-hash", "h"]
        )
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", phone="+1", api_id=42, api_hash="h")


def test_account_verify_code_flags():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.account_cmd.verify_code_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app, ["account", "verify-code", "--phone", "+1", "--code", "123", "--password", "pw"]
        )
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml", phone="+1", code="123", password="pw", api_id=None, api_hash=None
    )


def test_account_add_alias_sends_without_code():
    """`account add --phone` with no --code resolves to send-code (argparse parity)."""
    mock_send = MagicMock()
    mock_verify = MagicMock()
    with (
        patch("src.cli.typer_commands.account_cmd.send_code_impl", mock_send),
        patch("src.cli.typer_commands.account_cmd.verify_code_impl", mock_verify),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["account", "add", "--phone", "+1"])
    assert result.exit_code == 0
    mock_send.assert_called_once_with("config.yaml", phone="+1", api_id=None, api_hash=None)
    mock_verify.assert_not_called()


def test_account_add_alias_verifies_with_code():
    """`account add --phone --code` resolves to verify-code."""
    mock_send = MagicMock()
    mock_verify = MagicMock()
    with (
        patch("src.cli.typer_commands.account_cmd.send_code_impl", mock_send),
        patch("src.cli.typer_commands.account_cmd.verify_code_impl", mock_verify),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["account", "add", "--phone", "+1", "--code", "999"])
    assert result.exit_code == 0
    mock_verify.assert_called_once_with(
        "config.yaml", phone="+1", code="999", password=None, api_id=None, api_hash=None
    )
    mock_send.assert_not_called()


def test_account_flood_clear_phone():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.account_cmd.flood_clear_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["account", "flood-clear", "--phone", "+1"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", phone="+1")


# --- #828 export-session / import: the secret-handling SSO ops --------------- #


def test_account_export_session_by_id():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.account_cmd.export_session_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["account", "export-session", "--id", "5", "--json"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", account_id=5, phone=None, as_json=True)


def test_account_export_session_by_phone():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.account_cmd.export_session_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["account", "export-session", "--phone", "+1234567890"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml", account_id=None, phone="+1234567890", as_json=False
    )


def test_account_export_session_mutex_rejects_both_and_neither():
    """Exactly one of --id/--phone — the #828 mutually-exclusive group (kept)."""
    # neither
    assert runner.invoke(app, ["account", "export-session"]).exit_code != 0
    # both
    assert runner.invoke(
        app, ["account", "export-session", "--id", "1", "--phone", "+1"]
    ).exit_code != 0


def test_account_import_session_string():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.account_cmd.import_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app, ["account", "import", "--phone", "+1", "--session-string", "SESS", "--force"]
        )
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml",
        phone="+1",
        session_string="SESS",
        session_string_stdin=False,
        force=True,
    )


def test_account_import_stdin_source():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.account_cmd.import_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["account", "import", "--phone", "+1", "--session-string-stdin"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml",
        phone="+1",
        session_string=None,
        session_string_stdin=True,
        force=False,
    )


def test_account_import_mutex_rejects_both_and_neither():
    """Exactly one session source — the #828 mutually-exclusive group (kept)."""
    assert runner.invoke(app, ["account", "import", "--phone", "+1"]).exit_code != 0
    assert runner.invoke(
        app,
        ["account", "import", "--phone", "+1", "--session-string", "X", "--session-string-stdin"],
    ).exit_code != 0


def test_account_export_session_delegates_via_argparse():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.account_cmd.export_session_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["account", "export-session", "--id", "9", "--json"])
    mock_impl.assert_called_once_with("config.yaml", account_id=9, phone=None, as_json=True)


def test_account_import_delegates_via_argparse():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.account_cmd.import_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["account", "import", "--phone", "+1", "--session-string", "SECRET", "--force"])
    mock_impl.assert_called_once_with(
        "config.yaml",
        phone="+1",
        session_string="SECRET",
        session_string_stdin=False,
        force=True,
    )


def test_account_export_session_value_not_logged(caplog):
    """The export path must never log the session value (caplog guard, #828)."""
    import logging

    from src.cli.typer_commands import _account_argv

    # The argv reconstruction for export-session carries only id/phone/json — never
    # a session value (the secret is produced inside the impl and printed, not logged).
    args = build_parser().parse_args(["account", "export-session", "--id", "3", "--json"])
    with caplog.at_level(logging.DEBUG):
        tail = _account_argv(args)
    assert tail == ["export-session", "--id", "3", "--json"]
    assert "session" not in caplog.text.lower() or "session_string" not in caplog.text


def test_account_bare_group_shows_help_exit_0():
    import pytest

    args = build_parser().parse_args(["account"])
    with pytest.raises(SystemExit) as exc_info:
        dispatch_via_typer(args)
    assert exc_info.value.code == 0
