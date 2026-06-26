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
wiring from CLI tokens to the body is what is under test, *especially the depth-2
nested paths*.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from src.cli.main import main
from src.cli.typer_app import app

runner = CliRunner()


def _bare_group_exit_code(argv: list[str]) -> object:
    """Exit code of a bare command group through the production ``main()`` entry.

    A bare group (``no_args_is_help``) raises ``NoArgsIsHelpError``; the argparse
    parity (render help, exit 0) is reproduced by ``main()``'s exception handling,
    not by a raw ``CliRunner`` invoke (which would exit 2). So bare-group help
    parity is asserted end to end through ``main()``. Returns the raw
    ``SystemExit.code`` (``int`` in practice; typed ``object`` since the stdlib
    annotates it ``str | int | None``).
    """
    with patch("sys.argv", ["main.py", *argv]):
        with pytest.raises(SystemExit) as exc:
            main()
    return exc.value.code


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
# analytics — bare group + negative-id positional (direct Typer surface)
# --------------------------------------------------------------------------- #


def test_analytics_bare_shows_help_exit0():
    """Bare ``analytics`` (no action) shows help and exits 0 — argparse parity.

    The bare group must render help and exit 0, NOT fall through to ``top`` and
    open the DB. ``top_impl`` is patched so any accidental invocation of the body
    would be caught by ``assert_not_called``.
    """
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.analytics_cmd.top_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        assert _bare_group_exit_code(["analytics"]) == 0
    mock_impl.assert_not_called()


def test_analytics_channel_negative_id_positional():
    """Negative channel_id is accepted as the positional via the ``--`` separator."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.analytics_cmd.channel_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["analytics", "channel", "--days", "14", "--", "-100123456"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", channel_id=-100123456, days=14)


# --------------------------------------------------------------------------- #
# channel — flat leaves + nested depth-2 ``channel tag`` group
# --------------------------------------------------------------------------- #


def test_channel_list():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.channel_cmd.list_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["channel", "list"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml")


def test_channel_add():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.channel_cmd.add_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["channel", "add", "@somechan"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", identifier="@somechan")


def test_channel_collect_full():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.channel_cmd.collect_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["channel", "collect", "5", "--full"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", identifier="5", full=True)


def test_channel_stats_all():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.channel_cmd.stats_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["channel", "stats", "--all", "--max-channels", "10"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with(
        "config.yaml", all_channels=True, identifier=None, max_channels=10
    )


def test_channel_refresh_meta_single():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.channel_cmd.refresh_meta_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["channel", "refresh-meta", "@chan"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", all_channels=False, identifier="@chan")


def test_channel_add_bulk():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.channel_cmd.add_bulk_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app, ["channel", "add-bulk", "--phone", "+123", "--dialog-ids", "1,2,3"]
        )
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", phone="+123", dialog_ids="1,2,3")


def test_channel_list_for_import_json():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.channel_cmd.list_for_import_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["channel", "list-for-import", "--json"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", as_json=True)


# --- nested: channel tag <action> (depth-2) -------------------------------- #


def test_channel_tag_list_nested():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.channel_cmd._tag_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["channel", "tag", "list"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", "list")


def test_channel_tag_add_nested():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.channel_cmd._tag_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["channel", "tag", "add", "sports"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", "add", name="sports")


def test_channel_tag_set_two_positionals_nested():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.channel_cmd._tag_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["channel", "tag", "set", "5", "a,b,c"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", "set", pk=5, tags="a,b,c")


def test_channel_tag_get_nested():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.channel_cmd._tag_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["channel", "tag", "get", "10"])
    assert result.exit_code == 0
    mock_impl.assert_called_once_with("config.yaml", "get", pk=10)


# --- channel bare-group help parity (flat + nested depth-2) ---------------- #


def test_channel_bare_shows_help_exit0():
    """Bare ``channel`` (no action) shows help and exits 0 (argparse parity)."""
    assert _bare_group_exit_code(["channel"]) == 0


def test_channel_tag_bare_shows_help_exit0():
    """Bare ``channel tag`` (no nested action) shows help and exits 0."""
    assert _bare_group_exit_code(["channel", "tag"]) == 0


# --------------------------------------------------------------------------- #
# dialogs — leaves + nested depth-2 ``dialogs queue`` group
#
# Every dialogs leaf builds an argparse Namespace and runs the shared
# ``_dispatch`` body, so the tests patch ``_dispatch`` and assert the Namespace
# attributes it received (the CLI-token → Namespace wiring is what is verified).
# --------------------------------------------------------------------------- #


def _dialogs_ns_of(mock_dispatch):
    """Return the argparse Namespace passed to the patched ``_dispatch``."""
    return mock_dispatch.call_args[0][0]


def test_dialogs_list():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.dialogs_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["dialogs", "list", "--phone", "+1"])
    assert result.exit_code == 0
    ns = _dialogs_ns_of(mock)
    assert ns.dialogs_action == "list"
    assert ns.phone == "+1"
    assert ns.config == "config.yaml"


def test_dialogs_send_with_yes():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.dialogs_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["dialogs", "send", "@user", "hello", "--yes"])
    assert result.exit_code == 0
    ns = _dialogs_ns_of(mock)
    assert (ns.dialogs_action, ns.recipient, ns.text, ns.yes) == ("send", "@user", "hello", True)


def test_dialogs_leave_variadic():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.dialogs_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["dialogs", "leave", "100", "200", "300"])
    assert result.exit_code == 0
    ns = _dialogs_ns_of(mock)
    assert ns.dialogs_action == "leave"
    assert ns.dialog_ids == ["100", "200", "300"]


def test_dialogs_topics_required_channel_id():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.dialogs_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["dialogs", "topics", "--channel-id", "555"])
    assert result.exit_code == 0
    assert _dialogs_ns_of(mock).channel_id == 555


def test_dialogs_topics_missing_channel_id_rejected():
    """--channel-id is required (argparse required=True parity)."""
    with patch("src.cli.typer_commands.run_async"):
        result = runner.invoke(app, ["dialogs", "topics"])
    assert result.exit_code != 0


def test_dialogs_react_optional_emoji():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.dialogs_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["dialogs", "react", "123", "99", "👍"])
    assert result.exit_code == 0
    ns = _dialogs_ns_of(mock)
    assert (ns.chat_id, ns.message_id, ns.emoji, ns.clear) == ("123", 99, "👍", False)


def test_dialogs_react_clear_no_emoji():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.dialogs_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["dialogs", "react", "123", "99", "--clear"])
    assert result.exit_code == 0
    ns = _dialogs_ns_of(mock)
    assert ns.emoji is None and ns.clear is True


def test_dialogs_edit_admin_no_admin_flag():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.dialogs_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["dialogs", "edit-admin", "chat", "user", "--no-admin"])
    assert result.exit_code == 0
    assert _dialogs_ns_of(mock).is_admin is False


def test_dialogs_create_channel_required_title():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.dialogs_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["dialogs", "create-channel", "--title", "My Chan"])
    assert result.exit_code == 0
    ns = _dialogs_ns_of(mock)
    assert (ns.dialogs_action, ns.title) == ("create-channel", "My Chan")


# --- nested: dialogs queue <action> (depth-2) ------------------------------ #


def test_dialogs_queue_status_nested():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.dialogs_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["dialogs", "queue", "status", "--limit", "5"])
    assert result.exit_code == 0
    ns = _dialogs_ns_of(mock)
    assert (ns.dialogs_action, ns.queue_action, ns.limit) == ("queue", "status", 5)


def test_dialogs_queue_cancel_nested():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.dialogs_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["dialogs", "queue", "cancel", "42", "--yes"])
    assert result.exit_code == 0
    ns = _dialogs_ns_of(mock)
    assert (ns.dialogs_action, ns.queue_action, ns.command_id, ns.yes) == ("queue", "cancel", 42, True)


def test_dialogs_queue_clear_pending_nested():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.dialogs_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app, ["dialogs", "queue", "clear-pending", "--command-type", "dialogs.react"]
        )
    assert result.exit_code == 0
    ns = _dialogs_ns_of(mock)
    assert (ns.dialogs_action, ns.queue_action, ns.command_type) == (
        "queue", "clear-pending", "dialogs.react",
    )


# --- dialogs bare-group help parity ---------------------------------------- #


def test_dialogs_bare_shows_help_exit0():
    """Bare ``dialogs`` (no action) shows help and exits 0 (argparse parity)."""
    assert _bare_group_exit_code(["dialogs"]) == 0


# --------------------------------------------------------------------------- #
# pipeline — leaves + three depth-2 nested groups (filter / node / edge)
#
# Each pipeline leaf builds an argparse Namespace and runs the shared
# ``_dispatch`` body, so the tests patch ``_dispatch`` and assert the Namespace
# it received (the CLI-token → Namespace wiring under test).
# --------------------------------------------------------------------------- #


def _pipeline_ns_of(mock_dispatch):
    return mock_dispatch.call_args[0][0]


def test_pipeline_list():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.pipeline_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["pipeline", "list"])
    assert result.exit_code == 0
    assert _pipeline_ns_of(mock).pipeline_action == "list"


def test_pipeline_show():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.pipeline_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["pipeline", "show", "5"])
    assert result.exit_code == 0
    ns = _pipeline_ns_of(mock)
    assert (ns.pipeline_action, ns.id) == ("show", 5)


def test_pipeline_add_variadic_source():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.pipeline_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app,
            ["pipeline", "add", "MyPipe", "--prompt-template", "tpl",
             "--source", "100", "--source", "200", "--target", "+1|99"],
        )
    assert result.exit_code == 0
    ns = _pipeline_ns_of(mock)
    assert ns.name == "MyPipe" and ns.source == [100, 200] and ns.target == ["+1|99"]
    assert ns.publish_mode == "moderated"  # enum → plain str


def test_pipeline_add_bad_publish_mode_rejected():
    """Unknown --publish-mode choice rejected (str-Enum closed set)."""
    with patch("src.cli.typer_commands.run_async"):
        result = runner.invoke(app, ["pipeline", "add", "P", "--publish-mode", "bogus"])
    assert result.exit_code != 0


def test_pipeline_edit_active_tristate():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.pipeline_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["pipeline", "edit", "5", "--inactive"])
    assert result.exit_code == 0
    assert _pipeline_ns_of(mock).active is False


def test_pipeline_generate_stream():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.pipeline_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["pipeline", "generate-stream", "7", "--model", "gpt-4"])
    assert result.exit_code == 0
    ns = _pipeline_ns_of(mock)
    assert (ns.pipeline_action, ns.id, ns.model) == ("generate-stream", 7, "gpt-4")


def test_pipeline_bulk_approve_variadic_positional():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.pipeline_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["pipeline", "bulk-approve", "1", "2", "3"])
    assert result.exit_code == 0
    assert _pipeline_ns_of(mock).run_ids == [1, 2, 3]


def test_pipeline_export_force():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.pipeline_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["pipeline", "export", "5", "--force", "-o", "out.json"])
    assert result.exit_code == 0
    ns = _pipeline_ns_of(mock)
    assert ns.force is True and ns.output == "out.json"


# --- nested: pipeline filter / node / edge (depth-2) ----------------------- #


def test_pipeline_filter_set_nested():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.pipeline_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(
            app, ["pipeline", "filter", "set", "3", "--keyword", "a", "--keyword", "b", "--forwarded", "true"]
        )
    assert result.exit_code == 0
    ns = _pipeline_ns_of(mock)
    assert (ns.pipeline_action, ns.filter_action, ns.id) == ("filter", "set", 3)
    assert ns.keywords == ["a", "b"] and ns.forwarded == "true"


def test_pipeline_filter_show_nested():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.pipeline_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["pipeline", "filter", "show", "3"])
    assert result.exit_code == 0
    ns = _pipeline_ns_of(mock)
    assert (ns.filter_action, ns.id) == ("show", 3)


def test_pipeline_node_add_nested():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.pipeline_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["pipeline", "node", "add", "5", "fetch:limit=10"])
    assert result.exit_code == 0
    ns = _pipeline_ns_of(mock)
    assert (ns.pipeline_action, ns.node_action, ns.pipeline_id, ns.node_spec) == (
        "node", "add", 5, "fetch:limit=10",
    )


def test_pipeline_node_replace_three_positionals_nested():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.pipeline_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["pipeline", "node", "replace", "5", "n1", "llm:model=gpt"])
    assert result.exit_code == 0
    ns = _pipeline_ns_of(mock)
    assert (ns.node_action, ns.pipeline_id, ns.node_id, ns.node_spec) == (
        "replace", 5, "n1", "llm:model=gpt",
    )


def test_pipeline_edge_add_nested():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.pipeline_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["pipeline", "edge", "add", "5", "n1", "n2"])
    assert result.exit_code == 0
    ns = _pipeline_ns_of(mock)
    assert (ns.pipeline_action, ns.edge_action, ns.pipeline_id, ns.from_node, ns.to_node) == (
        "edge", "add", 5, "n1", "n2",
    )


def test_pipeline_graph_flat():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.pipeline_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["pipeline", "graph", "5"])
    assert result.exit_code == 0
    ns = _pipeline_ns_of(mock)
    assert (ns.pipeline_action, ns.id) == ("graph", 5)



# --- pipeline bare-group help parity --------------------------------------- #


def test_pipeline_bare_shows_help_exit0():
    """Bare ``pipeline`` (no action) shows help and exits 0 (argparse parity)."""
    assert _bare_group_exit_code(["pipeline"]) == 0
