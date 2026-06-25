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

import pytest
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


def test_analytics_bare_shows_help_exit0():
    """Bare ``analytics`` (no action) shows help and exits 0 — argparse parity.

    Argparse never ran ``top`` for a bare ``analytics``: ``main.py`` reparses
    ``analytics --help`` when the sub-action is missing. The Typer round-trip
    must do the same (``NoArgsIsHelpError`` → help, exit 0), NOT fall through to
    ``top`` and open the DB. ``top_impl`` is patched so any accidental
    invocation of the body would be caught by ``assert_not_called``.
    """
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.analytics_cmd.top_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
        pytest.raises(SystemExit) as exc,
    ):
        _delegate(["analytics"])  # NoArgsIsHelpError → help, SystemExit(0)
    assert exc.value.code == 0
    mock_impl.assert_not_called()


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


# --- channel prod round-trip (incl. nested depth-2 paths) ------------------ #


def test_channel_add_roundtrip():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.channel_cmd.add_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["channel", "add", "@chan"])
    mock_impl.assert_called_once_with("config.yaml", identifier="@chan")


def test_channel_stats_single_roundtrip():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.channel_cmd.stats_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["channel", "stats", "@chan"])
    mock_impl.assert_called_once_with(
        "config.yaml", all_channels=False, identifier="@chan", max_channels=None
    )


def test_channel_tag_add_roundtrip_nested():
    """The depth-2 ``channel tag add`` path survives argparse→Typer round-trip."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.channel_cmd._tag_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["channel", "tag", "add", "news"])
    mock_impl.assert_called_once_with("config.yaml", "add", name="news")


def test_channel_tag_set_roundtrip_nested():
    """The depth-2 ``channel tag set`` (two positionals) round-trips correctly."""
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.channel_cmd._tag_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["channel", "tag", "set", "7", "x,y,z"])
    mock_impl.assert_called_once_with("config.yaml", "set", pk=7, tags="x,y,z")


def test_channel_tag_get_roundtrip_nested():
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.channel_cmd._tag_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["channel", "tag", "get", "3"])
    mock_impl.assert_called_once_with("config.yaml", "get", pk=3)


def test_channel_bare_shows_help_exit0():
    """Bare ``channel`` (no action) shows help and exits 0 (argparse parity)."""
    with pytest.raises(SystemExit) as exc:
        _delegate(["channel"])  # NoArgsIsHelpError → help, SystemExit(0)
    assert exc.value.code == 0


def test_channel_tag_bare_shows_help_exit0():
    """Bare ``channel tag`` (no nested action) shows help and exits 0."""
    with pytest.raises(SystemExit) as exc:
        _delegate(["channel", "tag"])
    assert exc.value.code == 0


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


# --- dialogs prod round-trip (incl. nested + negative chat id) ------------- #


def test_dialogs_send_negative_chatid_roundtrip():
    """Negative chat id + dashy text survive the argparse→Typer round-trip."""
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.dialogs_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["dialogs", "send", "-100500", "hi", "--phone", "+1", "--yes"])
    ns = _dialogs_ns_of(mock)
    assert (ns.dialogs_action, ns.recipient, ns.text, ns.phone, ns.yes) == (
        "send", "-100500", "hi", "+1", True,
    )


def test_dialogs_forward_variadic_roundtrip():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.dialogs_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["dialogs", "forward", "src", "dst", "1", "2", "3"])
    ns = _dialogs_ns_of(mock)
    assert ns.from_chat == "src" and ns.to_chat == "dst" and ns.message_ids == ["1", "2", "3"]


def test_dialogs_queue_cancel_roundtrip_nested():
    """The depth-2 ``dialogs queue cancel`` path survives the round-trip."""
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.dialogs_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["dialogs", "queue", "cancel", "7", "--yes"])
    ns = _dialogs_ns_of(mock)
    assert (ns.dialogs_action, ns.queue_action, ns.command_id, ns.yes) == ("queue", "cancel", 7, True)


def test_dialogs_queue_status_roundtrip_nested():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.dialogs_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["dialogs", "queue", "status", "--phone", "+1", "--limit", "3"])
    ns = _dialogs_ns_of(mock)
    assert (ns.queue_action, ns.phone, ns.limit) == ("status", "+1", 3)


def test_dialogs_react_clear_roundtrip():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.dialogs_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["dialogs", "react", "123", "99", "--clear"])
    ns = _dialogs_ns_of(mock)
    assert ns.clear is True and ns.emoji is None


def test_dialogs_bare_shows_help_exit0():
    """Bare ``dialogs`` (no action) shows help and exits 0 (argparse parity)."""
    with pytest.raises(SystemExit) as exc:
        _delegate(["dialogs"])
    assert exc.value.code == 0


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


# --- pipeline prod round-trip (incl. all three nested depth-2 paths) ------- #


def test_pipeline_node_add_roundtrip_nested():
    """The depth-2 ``pipeline node add`` path survives the argparse→Typer round-trip."""
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.pipeline_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["pipeline", "node", "add", "5", "fetch:limit=10"])
    ns = _pipeline_ns_of(mock)
    assert (ns.node_action, ns.pipeline_id, ns.node_spec) == ("add", 5, "fetch:limit=10")


def test_pipeline_node_replace_roundtrip_nested():
    """The depth-2 ``pipeline node replace`` (three positionals) round-trips in order."""
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.pipeline_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["pipeline", "node", "replace", "5", "n1", "llm:model=gpt"])
    ns = _pipeline_ns_of(mock)
    assert (ns.node_action, ns.pipeline_id, ns.node_id, ns.node_spec) == (
        "replace", 5, "n1", "llm:model=gpt",
    )


def test_pipeline_edge_remove_roundtrip_nested():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.pipeline_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["pipeline", "edge", "remove", "5", "n1", "n2"])
    ns = _pipeline_ns_of(mock)
    assert (ns.edge_action, ns.pipeline_id, ns.from_node, ns.to_node) == ("remove", 5, "n1", "n2")


def test_pipeline_filter_set_roundtrip_nested():
    """Variadic --keyword re-emits as repeated flags across the round-trip."""
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.pipeline_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["pipeline", "filter", "set", "3", "--keyword", "x", "--keyword", "y"])
    ns = _pipeline_ns_of(mock)
    assert (ns.filter_action, ns.id, ns.keywords) == ("set", 3, ["x", "y"])


def test_pipeline_add_variadic_source_roundtrip():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.pipeline_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(
            ["pipeline", "add", "P", "--prompt-template", "t",
             "--source", "1", "--source", "2", "--target", "+1|9"]
        )
    ns = _pipeline_ns_of(mock)
    assert ns.name == "P" and ns.source == [1, 2] and ns.target == ["+1|9"]


def test_pipeline_generate_stream_roundtrip():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.pipeline_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["pipeline", "generate-stream", "7", "--limit", "12"])
    ns = _pipeline_ns_of(mock)
    assert (ns.pipeline_action, ns.id, ns.limit) == ("generate-stream", 7, 12)


def test_pipeline_bulk_reject_roundtrip():
    mock = MagicMock()
    with (
        patch("src.cli.typer_commands.pipeline_cmd._dispatch", mock),
        patch("src.cli.typer_commands.run_async"),
    ):
        _delegate(["pipeline", "bulk-reject", "10", "20"])
    assert _pipeline_ns_of(mock).run_ids == [10, 20]


def test_pipeline_bare_shows_help_exit0():
    """Bare ``pipeline`` (no action) shows help and exits 0 (argparse parity)."""
    with pytest.raises(SystemExit) as exc:
        _delegate(["pipeline"])
    assert exc.value.code == 0


# --------------------------------------------------------------------------- #
# argv-builder branch coverage (``_argv_from_namespace``)
#
# Every Wave-4 group routes through a per-action argv builder (``_pa_*`` /
# ``_da_*`` / ``_analytics_argv`` / ``_channel_argv`` and the depth-2 nested
# builders). Those builders re-emit each *non-default* flag / positional so the
# Typer leaf re-parses to the same Namespace. The CliRunner/round-trip tests
# above exercise the *default* paths; the cases below drive each builder with
# its full set of non-default flags so the conditional arms (``if getattr(...)``)
# are all hit. Asserting the rebuilt argv tail makes this a real regression
# guard on the argparse→Typer token mapping, not just a line-coverage filler.
#
# Driven through the production ``build_parser() → _argv_from_namespace`` path
# (the first half of ``dispatch_via_typer``) so the Namespace the builder sees
# is the real parsed one, never a hand-rolled stub.
# --------------------------------------------------------------------------- #


def _tail(argv_in: list[str]) -> list[str]:
    """Parse ``argv_in`` and return the rebuilt argv *after* the group token.

    ``_argv_from_namespace`` prefixes ``["--config", "config.yaml", <group>]``;
    the slice drops that shared head so a case asserts only on the action +
    flags its builder emits.
    """
    from src.cli.typer_commands import _argv_from_namespace

    ns = build_parser().parse_args(argv_in)
    return _argv_from_namespace(ns)[3:]


# --- pipeline builders (``_pa_*`` + nested filter/node/edge) --------------- #


@pytest.mark.parametrize(
    ("argv_in", "expected"),
    [
        (
            ["pipeline", "add", "P", "--prompt-template", "t", "--json-file", "f.json",
             "--source", "9", "--target", "+1|7", "--llm-model", "m", "--image-model", "im",
             "--publish-mode", "auto", "--generation-backend", "agent", "--interval", "30",
             "--inactive", "--ab-variants", "2", "--ab-auto-select", "--node", "fetch:x",
             "--edge", "a:b", "--node-config", "k=v", "--run-after",
             "--since-value", "5", "--since-unit", "d"],
            ["add", "--prompt-template", "t", "--json-file", "f.json", "--source", "9",
             "--target", "+1|7", "--llm-model", "m", "--image-model", "im",
             "--publish-mode", "auto", "--generation-backend", "agent", "--interval", "30",
             "--inactive", "--ab-variants", "2", "--ab-auto-select", "--node", "fetch:x",
             "--edge", "a:b", "--node-config", "k=v", "--run-after",
             "--since-value", "5", "--since-unit", "d", "--", "P"],
        ),
        (
            ["pipeline", "edit", "5", "--name", "N", "--prompt-template", "t",
             "--source", "9", "--target", "+1|7", "--llm-model", "m", "--image-model", "im",
             "--publish-mode", "auto", "--generation-backend", "agent", "--interval", "30",
             "--active", "--ab-variants", "2", "--ab-auto-select"],
            ["edit", "--name", "N", "--prompt-template", "t", "--source", "9",
             "--target", "+1|7", "--llm-model", "m", "--image-model", "im",
             "--publish-mode", "auto", "--generation-backend", "agent", "--interval", "30",
             "--active", "--ab-variants", "2", "--ab-auto-select", "--", "5"],
        ),
        # edit's other tri-state arm: --inactive / --no-ab-auto-select
        (
            ["pipeline", "edit", "5", "--inactive", "--no-ab-auto-select"],
            ["edit", "--inactive", "--no-ab-auto-select", "--", "5"],
        ),
        (
            ["pipeline", "run", "5", "--preview", "--publish", "--limit", "3",
             "--max-tokens", "100", "--temperature", "0.5"],
            ["run", "--preview", "--publish", "--limit", "3", "--max-tokens", "100",
             "--temperature", "0.5", "--", "5"],
        ),
        (
            ["pipeline", "generate", "5", "--max-tokens", "99", "--temperature", "0.3",
             "--model", "gpt", "--preview", "--ab-variants", "3", "--auto-select"],
            ["generate", "--max-tokens", "99", "--temperature", "0.3", "--model", "gpt",
             "--preview", "--ab-variants", "3", "--auto-select", "--", "5"],
        ),
        (
            ["pipeline", "generate-stream", "5", "--model", "gpt", "--max-tokens", "99",
             "--temperature", "0.3", "--limit", "3"],
            ["generate-stream", "--model", "gpt", "--max-tokens", "99",
             "--temperature", "0.3", "--limit", "3", "--", "5"],
        ),
        (
            ["pipeline", "runs", "5", "--limit", "3", "--status", "completed"],
            ["runs", "--limit", "3", "--status", "completed", "--", "5"],
        ),
        (
            ["pipeline", "select-variant", "7", "2"],
            ["select-variant", "--", "7", "2"],
        ),
        (
            ["pipeline", "queue", "5", "--limit", "3"],
            ["queue", "--limit", "3", "--", "5"],
        ),
        (
            ["pipeline", "moderation-list", "--pipeline-id", "2", "--limit", "5"],
            ["moderation-list", "--pipeline-id", "2", "--limit", "5"],
        ),
        (
            ["pipeline", "refinement-steps", "5", "--set", "[]"],
            ["refinement-steps", "--set", "[]", "--", "5"],
        ),
        (
            ["pipeline", "import", "f.json", "--name", "N"],
            ["import", "--name", "N", "--", "f.json"],
        ),
        (
            ["pipeline", "templates", "--category", "news"],
            ["templates", "--category", "news"],
        ),
        (
            ["pipeline", "from-template", "3", "N", "--source-ids", "1,2", "--target-refs", "+1|9"],
            ["from-template", "--source-ids", "1,2", "--target-refs", "+1|9", "--", "3", "N"],
        ),
        (
            ["pipeline", "ai-edit", "5", "do it", "--show"],
            ["ai-edit", "--show", "--", "5", "do it"],
        ),
        (
            ["pipeline", "dry-run-count", "--source", "9", "--since-value", "5", "--since-unit", "d"],
            ["dry-run-count", "--source", "9", "--since-value", "5", "--since-unit", "d"],
        ),
        # nested filter set — every optional arm of _pipeline_filter_argv
        (
            ["pipeline", "filter", "set", "3", "--message-kind", "text",
             "--service-action", "join", "--media-type", "photo", "--sender-kind", "user",
             "--keyword", "x", "--regex", "rx", "--forwarded", "true", "--has-text", "true"],
            ["filter", "set", "--message-kind", "text", "--service-action", "join",
             "--media-type", "photo", "--sender-kind", "user", "--keyword", "x",
             "--regex", "rx", "--forwarded", "true", "--has-text", "true", "--", "3"],
        ),
        (
            ["pipeline", "filter", "clear", "3"],
            ["filter", "clear", "--", "3"],
        ),
        # nested node — replace (3 positionals) + remove (2 positionals)
        (
            ["pipeline", "node", "replace", "5", "n1", "llm:model=gpt"],
            ["node", "replace", "--", "5", "n1", "llm:model=gpt"],
        ),
        (
            ["pipeline", "node", "remove", "5", "n1"],
            ["node", "remove", "--", "5", "n1"],
        ),
    ],
)
def test_pipeline_argv_builder_branches(argv_in, expected):
    assert _tail(argv_in) == expected


# --- dialogs builders (``_da_*`` + nested queue) -------------------------- #


@pytest.mark.parametrize(
    ("argv_in", "expected"),
    [
        (["dialogs", "resolve", "@x", "--phone", "+1"],
         ["resolve", "--phone", "+1", "--", "@x"]),
        (["dialogs", "leave", "100", "200", "--phone", "+1", "--yes"],
         ["leave", "--phone", "+1", "--yes", "--", "100", "200"]),
        (["dialogs", "join", "@x", "--phone", "+1", "--yes"],
         ["join", "--phone", "+1", "--yes", "--", "@x"]),
        (["dialogs", "topics", "--channel-id", "5", "--phone", "+1"],
         ["topics", "--channel-id", "5", "--phone", "+1"]),
        (["dialogs", "cache-clear", "--phone", "+1"],
         ["cache-clear", "--phone", "+1"]),
        (["dialogs", "send", "@u", "hi", "--phone", "+1", "--yes"],
         ["send", "--phone", "+1", "--yes", "--", "@u", "hi"]),
        (["dialogs", "forward", "a", "b", "1", "2", "--phone", "+1", "--yes"],
         ["forward", "--phone", "+1", "--yes", "--", "a", "b", "1", "2"]),
        (["dialogs", "edit-message", "c", "9", "txt", "--phone", "+1", "--yes"],
         ["edit-message", "--phone", "+1", "--yes", "--", "c", "9", "txt"]),
        (["dialogs", "delete-message", "c", "9", "10", "--phone", "+1", "--yes"],
         ["delete-message", "--phone", "+1", "--yes", "--", "c", "9", "10"]),
        (["dialogs", "create-channel", "--title", "T", "--phone", "+1",
          "--about", "A", "--username", "U"],
         ["create-channel", "--title", "T", "--phone", "+1", "--about", "A", "--username", "U"]),
        (["dialogs", "create-group", "--title", "T", "--phone", "+1", "--about", "A"],
         ["create-group", "--title", "T", "--phone", "+1", "--about", "A"]),
        (["dialogs", "pin-message", "c", "9", "--phone", "+1", "--notify", "--yes"],
         ["pin-message", "--phone", "+1", "--notify", "--yes", "--", "c", "9"]),
        # react: --clear arm + emoji omitted
        (["dialogs", "react", "c", "9", "--clear", "--phone", "+1", "--yes"],
         ["react", "--clear", "--phone", "+1", "--yes", "--", "c", "9"]),
        # react: explicit emoji positional (the trailing-append arm)
        (["dialogs", "react", "c", "9", "👍", "--phone", "+1"],
         ["react", "--phone", "+1", "--", "c", "9", "👍"]),
        (["dialogs", "unpin-message", "c", "--message-id", "9", "--phone", "+1", "--yes"],
         ["unpin-message", "--message-id", "9", "--phone", "+1", "--yes", "--", "c"]),
        (["dialogs", "download-media", "c", "9", "--phone", "+1", "--output-dir", "/tmp/x"],
         ["download-media", "--phone", "+1", "--output-dir", "/tmp/x", "--", "c", "9"]),
        (["dialogs", "participants", "c", "--phone", "+1", "--limit", "10", "--search", "s"],
         ["participants", "--phone", "+1", "--limit", "10", "--search", "s", "--", "c"]),
        (["dialogs", "edit-admin", "c", "u", "--phone", "+1", "--title", "T", "--no-admin", "--yes"],
         ["edit-admin", "--phone", "+1", "--title", "T", "--no-admin", "--yes", "--", "c", "u"]),
        (["dialogs", "edit-permissions", "c", "u", "--phone", "+1", "--until-date", "2025-01-01",
          "--send-messages", "false", "--send-media", "true", "--yes"],
         ["edit-permissions", "--phone", "+1", "--until-date", "2025-01-01",
          "--send-messages", "false", "--send-media", "true", "--yes", "--", "c", "u"]),
        (["dialogs", "kick", "c", "u", "--phone", "+1", "--yes"],
         ["kick", "--phone", "+1", "--yes", "--", "c", "u"]),
        (["dialogs", "broadcast-stats", "c", "--phone", "+1"],
         ["broadcast-stats", "--phone", "+1", "--", "c"]),
        (["dialogs", "archive", "c", "--phone", "+1"],
         ["archive", "--phone", "+1", "--", "c"]),
        (["dialogs", "mark-read", "c", "--phone", "+1", "--max-id", "99"],
         ["mark-read", "--phone", "+1", "--max-id", "99", "--", "c"]),
        # nested queue — status (all opts) + clear-pending (all opts)
        (["dialogs", "queue", "status", "--command-type", "t", "--phone", "+1", "--limit", "3"],
         ["queue", "status", "--command-type", "t", "--phone", "+1", "--limit", "3"]),
        (["dialogs", "queue", "clear-pending", "--command-type", "t", "--phone", "+1", "--yes"],
         ["queue", "clear-pending", "--command-type", "t", "--phone", "+1", "--yes"]),
    ],
)
def test_dialogs_argv_builder_branches(argv_in, expected):
    assert _tail(argv_in) == expected


# --- analytics builder (``_analytics_argv``) ------------------------------ #


@pytest.mark.parametrize(
    ("argv_in", "expected"),
    [
        (["analytics", "top", "--limit", "5", "--date-from", "2024-01-01", "--date-to", "2024-02-01"],
         ["top", "--limit", "5", "--date-from", "2024-01-01", "--date-to", "2024-02-01"]),
        (["analytics", "content-types", "--date-from", "2024-01-01", "--date-to", "2024-02-01"],
         ["content-types", "--date-from", "2024-01-01", "--date-to", "2024-02-01"]),
        (["analytics", "hourly", "--date-from", "2024-01-01", "--date-to", "2024-02-01"],
         ["hourly", "--date-from", "2024-01-01", "--date-to", "2024-02-01"]),
        (["analytics", "daily", "--days", "5", "--pipeline-id", "2"],
         ["daily", "--days", "5", "--pipeline-id", "2"]),
        (["analytics", "pipeline-stats", "--pipeline-id", "2"],
         ["pipeline-stats", "--pipeline-id", "2"]),
        (["analytics", "trending-channels", "--days", "3", "--limit", "9"],
         ["trending-channels", "--days", "3", "--limit", "9"]),
        (["analytics", "velocity", "--days", "5"],
         ["velocity", "--days", "5"]),
        (["analytics", "calendar", "--limit", "9", "--pipeline-id", "2"],
         ["calendar", "--limit", "9", "--pipeline-id", "2"]),
        (["analytics", "channel", "555", "--days", "5"],
         ["channel", "--days", "5", "--", "555"]),
        (["analytics", "channel-rating", "--useful", "useful", "--genre", "ad", "--limit", "10"],
         ["channel-rating", "--useful", "useful", "--genre", "ad", "--limit", "10"]),
        (["analytics", "channel-rate", "555", "--model", "gpt", "--sample-size", "20"],
         ["channel-rate", "--model", "gpt", "--sample-size", "20", "--", "555"]),
    ],
)
def test_analytics_argv_builder_branches(argv_in, expected):
    assert _tail(argv_in) == expected


# --- channel builder (``_channel_argv`` + nested tag) --------------------- #


@pytest.mark.parametrize(
    ("argv_in", "expected"),
    [
        (["channel", "collect", "@c", "--full"], ["collect", "--full", "--", "@c"]),
        (["channel", "stats", "--all", "--max-channels", "5"], ["stats", "--all", "--max-channels", "5"]),
        (["channel", "stats", "@c"], ["stats", "--", "@c"]),
        (["channel", "refresh-meta", "--all"], ["refresh-meta", "--all"]),
        (["channel", "refresh-meta", "@c"], ["refresh-meta", "--", "@c"]),
        (["channel", "list-for-import", "--json"], ["list-for-import", "--json"]),
        (["channel", "import", "src.txt"], ["import", "--", "src.txt"]),
        (["channel", "tag", "set", "7", "a,b"], ["tag", "set", "--", "7", "a,b"]),
        (["channel", "tag", "get", "3"], ["tag", "get", "--", "3"]),
        (["channel", "tag", "delete", "news"], ["tag", "delete", "--", "news"]),
    ],
)
def test_channel_argv_builder_branches(argv_in, expected):
    assert _tail(argv_in) == expected
