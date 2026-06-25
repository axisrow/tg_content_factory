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
