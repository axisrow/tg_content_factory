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
tokens to the body is what is under test.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from src.cli.typer_app import app

runner = CliRunner()


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


def test_search_query_add_rejects_track_stats_flag():
    """argparse ``add`` declares ONLY ``--no-track-stats`` — ``--track-stats`` must
    be rejected so the Typer surface is not one flag wider than argparse (#1123
    review). ``edit`` legitimately has both (argparse declares both there)."""
    # add: --track-stats is NOT a valid flag
    assert runner.invoke(app, ["search-query", "add", "q", "--track-stats"]).exit_code != 0
    # edit: --track-stats IS valid (argparse parser_domains declares both)
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.search_query_cmd.edit_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["search-query", "edit", "1", "--track-stats"])
    assert result.exit_code == 0
    assert mock_impl.call_args.kwargs["track_stats"] is True


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


def test_account_export_session_value_is_printed_not_logged(capsys, caplog):
    """The SSO export prints the session string to stdout but NEVER logs it (#828).

    Regression guard for the secret-handling invariant: ``_run_export_session``
    (the real business logic, not the removed argv bridge) emits the decrypted
    session string on stdout and a warning on stderr, but the secret must not
    leak into the logging subsystem at any level. Exercises the impl with a
    stubbed DB so a known sentinel session string flows through.
    """
    import argparse
    import asyncio
    import logging
    from unittest.mock import AsyncMock

    from src.cli.commands.account import _run_export_session
    from src.models import AccountSummary

    secret = "enc-sentinel-SESSION-STRING-do-not-log"
    summary = AccountSummary(id=3, phone="+1234567890")

    db = MagicMock()
    db.get_account_summaries = AsyncMock(return_value=[summary])
    db.repos.accounts.get_decrypted_session = AsyncMock(return_value=secret)

    args = argparse.Namespace(id=3, phone=None, json=False)
    with caplog.at_level(logging.DEBUG, logger="src.cli.commands.account"):
        asyncio.run(_run_export_session(args, db))

    out = capsys.readouterr()
    # Printed to stdout (the operator asked for it), warning to stderr.
    assert secret in out.out
    assert "full access" in out.err
    # …but never written to the logging subsystem.
    assert secret not in caplog.text


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


def test_account_export_session_invalid_mutex_skips_startup():
    """#1162 drift §3: an invalid --id/--phone mutex is rejected at parse time.

    The check now runs *before* ``apply_startup`` (the env / logging / data-dir
    side effects), the way argparse rejected the mutex during ``parse_args()``.
    So neither the startup side effects nor the impl ever fire on a bad mutex —
    important in a read-only runtime where ``apply_startup`` could raise first.
    """
    for argv in (
        ["account", "export-session"],  # neither
        ["account", "export-session", "--id", "1", "--phone", "+1"],  # both
    ):
        with (
            patch("src.cli.typer_commands.apply_startup") as mock_startup,
            patch("src.cli.typer_commands.run_async") as mock_run,
            patch("src.cli.typer_commands.account_cmd.export_session_impl") as mock_impl,
        ):
            result = runner.invoke(app, argv)
        assert result.exit_code != 0
        mock_startup.assert_not_called()
        mock_run.assert_not_called()
        mock_impl.assert_not_called()


def test_account_import_invalid_mutex_skips_startup():
    """#1162 drift §3: a bad session-source mutex is rejected before apply_startup."""
    for argv in (
        ["account", "import", "--phone", "+1"],  # neither source
        ["account", "import", "--phone", "+1", "--session-string", "X", "--session-string-stdin"],
    ):
        with (
            patch("src.cli.typer_commands.apply_startup") as mock_startup,
            patch("src.cli.typer_commands.run_async") as mock_run,
            patch("src.cli.typer_commands.account_cmd.import_impl") as mock_impl,
        ):
            result = runner.invoke(app, argv)
        assert result.exit_code != 0
        mock_startup.assert_not_called()
        mock_run.assert_not_called()
        mock_impl.assert_not_called()


# --------------------------------------------------------------------------- #
# agent → threads / thread-create / thread-delete / chat / thread-rename /
#         thread-stop / messages / context / test-escaping / test-tools
# --------------------------------------------------------------------------- #


def test_agent_threads_and_test_subcommands_delegate():
    for sub, impl_name in [
        ("threads", "threads_impl"),
        ("test-escaping", "test_escaping_impl"),
        ("test-tools", "test_tools_impl"),
    ]:
        mock_impl = MagicMock()
        with (
            patch(f"src.cli.typer_commands.agent_cmd.{impl_name}", mock_impl),
            patch("src.cli.typer_commands.run_async"),
        ):
            result = runner.invoke(app, ["agent", sub])
        assert result.exit_code == 0, (sub, result.output)
        mock_impl.assert_called_once_with("config.yaml")


def test_agent_thread_create_title_optional():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.agent_cmd.thread_create_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        runner.invoke(app, ["agent", "thread-create"])
        runner.invoke(app, ["agent", "thread-create", "--title", "My Thread"])
    assert mock_impl.call_args_list[0].kwargs == {"title": None}
    assert mock_impl.call_args_list[1].kwargs == {"title": "My Thread"}


def test_agent_thread_delete_stop_pass_id():
    for sub, impl_name in [
        ("thread-delete", "thread_delete_impl"),
        ("thread-stop", "thread_stop_impl"),
    ]:
        mock_impl = MagicMock()
        with (
            patch(f"src.cli.typer_commands.agent_cmd.{impl_name}", mock_impl),
            patch("src.cli.typer_commands.run_async"),
        ):
            result = runner.invoke(app, ["agent", sub, "12"])
        assert result.exit_code == 0, (sub, result.output)
        mock_impl.assert_called_once_with("config.yaml", thread_id=12)


def test_agent_chat_prompt_short_alias():
    """``-p`` is the short alias for ``--prompt`` (argparse parity)."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.agent_cmd.chat_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app, ["agent", "chat", "-p", "hi", "--thread-id", "4", "--model", "opus"]
        )
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", prompt="hi", thread_id=4, model="opus")


def test_agent_chat_interactive_no_prompt():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.agent_cmd.chat_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["agent", "chat"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", prompt=None, thread_id=None, model=None)


def test_agent_thread_rename_two_positionals():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.agent_cmd.thread_rename_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["agent", "thread-rename", "5", "New Title"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", thread_id=5, title="New Title")


def test_agent_messages_limit_optional():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.agent_cmd.messages_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        runner.invoke(app, ["agent", "messages", "8"])
        runner.invoke(app, ["agent", "messages", "8", "--limit", "20"])
    assert mock_impl.call_args_list[0].kwargs == {"thread_id": 8, "limit": None}
    assert mock_impl.call_args_list[1].kwargs == {"thread_id": 8, "limit": 20}


def test_agent_context_required_channel_id():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.agent_cmd.context_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app,
            ["agent", "context", "3", "--channel-id", "-1001", "--limit", "50", "--topic-id", "7"],
        )
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml", thread_id=3, channel_id=-1001, limit=50, topic_id=7
    )


# --------------------------------------------------------------------------- #
# photo-loader → dialogs / refresh / send / schedule-send / batch-create /
#                batch-list / items / batch-cancel / auto-create / auto-list /
#                auto-update / auto-toggle / auto-delete / run-due
# --------------------------------------------------------------------------- #


def test_photo_loader_dialogs_refresh_require_phone():
    for sub, impl_name in [("dialogs", "dialogs_impl"), ("refresh", "refresh_impl")]:
        mock_impl = MagicMock()
        with (
            patch(f"src.cli.typer_commands.photo_loader_cmd.{impl_name}", mock_impl),
            patch("src.cli.typer_commands.run_async"),
        ):
            result = runner.invoke(app, ["photo-loader", sub, "--phone", "+1"])
        assert result.exit_code == 0, (sub, result.output)
        mock_impl.assert_called_once_with("config.yaml", phone="+1")


def test_photo_loader_send_files_list_and_mode():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.photo_loader_cmd.send_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app,
            [
                "photo-loader", "send",
                "--phone", "+1", "--target", "me",
                "--files", "a.jpg", "--files", "b.jpg",
                "--mode", "separate", "--caption", "hi",
            ],
        )
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml", phone="+1", target="me", files=["a.jpg", "b.jpg"], mode="separate", caption="hi"
    )


def test_photo_loader_send_mode_defaults_album():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.photo_loader_cmd.send_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app, ["photo-loader", "send", "--phone", "+1", "--target", "me", "--files", "a.jpg"]
        )
    assert result.exit_code == 0
    _, kwargs = mock_impl.call_args
    assert kwargs["mode"] == "album"
    assert kwargs["caption"] is None


def test_photo_loader_send_rejects_bad_mode():
    """``--mode`` is constrained to album/separate (the argparse choices, as an Enum)."""
    result = runner.invoke(
        app, ["photo-loader", "send", "--phone", "+1", "--target", "me", "--files", "a", "--mode", "bogus"]
    )
    assert result.exit_code != 0


def test_photo_loader_schedule_send_requires_at():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.photo_loader_cmd.schedule_send_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app,
            [
                "photo-loader", "schedule-send",
                "--phone", "+1", "--target", "me", "--files", "a.jpg",
                "--at", "2026-07-01T10:00:00",
            ],
        )
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml",
        phone="+1",
        target="me",
        files=["a.jpg"],
        at="2026-07-01T10:00:00",
        mode="album",
        caption=None,
    )


def test_photo_loader_batch_list_auto_list_no_args():
    for sub, impl_name in [("batch-list", "batch_list_impl"), ("auto-list", "auto_list_impl")]:
        mock_impl = MagicMock()
        with (
            patch(f"src.cli.typer_commands.photo_loader_cmd.{impl_name}", mock_impl),
            patch("src.cli.typer_commands.run_async"),
        ):
            result = runner.invoke(app, ["photo-loader", sub])
        assert result.exit_code == 0, (sub, result.output)
        mock_impl.assert_called_once_with("config.yaml")


def test_photo_loader_items_optional_flags():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.photo_loader_cmd.items_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        runner.invoke(app, ["photo-loader", "items"])
        runner.invoke(app, ["photo-loader", "items", "--batch-id", "5", "--limit", "20"])
    assert mock_impl.call_args_list[0].kwargs == {"batch_id": None, "limit": 100}
    assert mock_impl.call_args_list[1].kwargs == {"batch_id": 5, "limit": 20}


def test_photo_loader_id_positional_subcommands():
    for sub, impl_name in [
        ("batch-cancel", "batch_cancel_impl"),
        ("auto-toggle", "auto_toggle_impl"),
        ("auto-delete", "auto_delete_impl"),
    ]:
        mock_impl = MagicMock()
        with (
            patch(f"src.cli.typer_commands.photo_loader_cmd.{impl_name}", mock_impl),
            patch("src.cli.typer_commands.run_async"),
        ):
            result = runner.invoke(app, ["photo-loader", sub, "9"])
        assert result.exit_code == 0, (sub, result.output)
        kwargs = mock_impl.call_args.kwargs
        # batch-cancel uses item_id; auto-* use job_id
        assert kwargs in ({"item_id": 9}, {"job_id": 9})


def test_photo_loader_auto_create_required_flags():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.photo_loader_cmd.auto_create_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app,
            [
                "photo-loader", "auto-create",
                "--phone", "+1", "--target", "me",
                "--folder", "/pics", "--interval", "60",
            ],
        )
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml",
        phone="+1",
        target="me",
        folder="/pics",
        interval=60,
        mode="album",
        caption=None,
    )


def test_photo_loader_auto_update_active_paused():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.photo_loader_cmd.auto_update_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app, ["photo-loader", "auto-update", "4", "--interval", "30", "--active"]
        )
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml",
        job_id=4,
        folder=None,
        interval=30,
        mode=None,
        caption=None,
        active=True,
        paused=False,
    )


def test_photo_loader_run_due_flags():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.photo_loader_cmd.run_due_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        runner.invoke(app, ["photo-loader", "run-due"])
        runner.invoke(app, ["photo-loader", "run-due", "--item-id", "7", "--dry-run"])
    assert mock_impl.call_args_list[0].kwargs == {"item_id": None, "dry_run": False}
    assert mock_impl.call_args_list[1].kwargs == {"item_id": 7, "dry_run": True}


def test_photo_loader_auto_update_empty_caption_clears():
    """``auto-update --caption ""`` is a deliberate CLEAR (repo writes when caption
    is not None); the empty string must be forwarded, not dropped as "unset"
    (#1123 review). caption=None (omitted) stays a no-op."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.photo_loader_cmd.auto_update_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        runner.invoke(app, ["photo-loader", "auto-update", "5", "--caption", ""])
    assert mock_impl.call_args.kwargs["caption"] == ""
    # omitting --caption keeps it None (no write)
    mock_impl.reset_mock()
    with (
        patch("src.cli.typer_commands.photo_loader_cmd.auto_update_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        runner.invoke(app, ["photo-loader", "auto-update", "5", "--folder", "/x"])
    assert mock_impl.call_args.kwargs["caption"] is None


def test_settings_bare_runs_get_on_direct_typer_surface():
    """Bare ``settings`` (no sub-command) runs ``get`` on the DIRECT Typer surface,
    not just the argparse bridge — argparse defaulted settings_action to get (#1123
    review)."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.settings_cmd.get_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["settings"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", key=None)
