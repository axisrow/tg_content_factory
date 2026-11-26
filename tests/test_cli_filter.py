"""Tests for filter CLI commands."""
from __future__ import annotations

import argparse
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import typer

from tg_agent.database import Database
from tg_agent.models import Account, Channel

pytestmark = pytest.mark.aiosqlite_serial

_FILTER_INIT_DB_TARGET = "tg_agent.cli.commands.filter.runtime.init_db"


def _ns(**kwargs) -> argparse.Namespace:
    defaults = {"config": "config.yaml"}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_parse_pks():
    """Test _parse_pks parses comma-separated PKs."""
    from tg_agent.cli.commands.filter import _parse_pks

    assert _parse_pks("1,2,3") == [1, 2, 3]
    assert _parse_pks(" 1 , 2 , 3 ") == [1, 2, 3]
    assert _parse_pks("") == []
    assert _parse_pks("abc,1,def,2") == [1, 2]


def test_print_result_empty():
    """Test _print_result with empty result."""
    import sys
    from io import StringIO

    from tg_agent.cli.commands.filter import _print_result

    result = MagicMock()
    result.purged_count = 0
    result.purged_titles = []
    result.skipped_count = 0

    old_stdout = sys.stdout
    sys.stdout = StringIO()
    try:
        _print_result(result)
        output = sys.stdout.getvalue()
    finally:
        sys.stdout = old_stdout

    assert "No filtered channels affected" in output


def test_print_result_with_purged(capsys):
    """Test _print_result with purged channels."""
    from tg_agent.cli.commands.filter import _print_result

    result = MagicMock()
    result.purged_count = 2
    result.purged_titles = ["Channel A", "Channel B"]
    result.skipped_count = 1

    _print_result(result, "Deleted")
    out = capsys.readouterr().out
    assert "Deleted 2 channels" in out
    assert "Channel A" in out
    assert "Skipped: 1" in out


def test_filter_analyze(tmp_path, cli_init_patch, capsys):
    """Test filter analyze action."""
    db_path = str(tmp_path / "filter_analyze.db")
    db = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(db.initialize())
    asyncio.run(db.add_account(Account(phone="+100", session_string="sess")))
    asyncio.run(db.add_channel(Channel(channel_id=1001, title="Test Channel")))
    with cli_init_patch(db, _FILTER_INIT_DB_TARGET):
        from tg_agent.cli.commands.filter import run

        with patch("tg_agent.cli.commands.filter.ChannelAnalyzer") as mock_analyzer:
            mock_instance = MagicMock()
            mock_report = MagicMock()
            mock_report.results = []
            mock_report.total_channels = 0
            mock_report.filtered_count = 0
            mock_instance.analyze_all = AsyncMock(return_value=mock_report)
            mock_instance.apply_filters = AsyncMock(return_value=0)
            mock_analyzer.return_value = mock_instance

            run(_ns(filter_action="analyze"))

    out = capsys.readouterr().out
    assert "No channels found" in out
    mock_instance.analyze_all.assert_awaited_once_with(quick=False, sample_size=None)
    mock_instance.apply_filters.assert_not_awaited()


def test_analyze_quick_skips_cross_dupe(tmp_path, cli_init_patch, capsys):
    """`filter analyze --quick` requests the analyzer's quick mode (#774)."""
    db_path = str(tmp_path / "filter_analyze_quick.db")
    db = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(db.initialize())
    asyncio.run(db.add_account(Account(phone="+100", session_string="sess")))
    asyncio.run(db.add_channel(Channel(channel_id=1001, title="Test Channel")))
    with cli_init_patch(db, _FILTER_INIT_DB_TARGET):
        from tg_agent.cli.commands.filter import run

        with patch("tg_agent.cli.commands.filter.ChannelAnalyzer") as mock_analyzer:
            mock_instance = MagicMock()
            mock_report = MagicMock()
            mock_report.results = []
            mock_report.total_channels = 0
            mock_report.filtered_count = 0
            mock_instance.analyze_all = AsyncMock(return_value=mock_report)
            mock_analyzer.return_value = mock_instance

            run(_ns(filter_action="analyze", quick=True))

    out = capsys.readouterr().out
    assert "No channels found" in out
    mock_instance.analyze_all.assert_awaited_once_with(quick=True, sample_size=None)


def test_analyze_quick_sample_size_flows_through(tmp_path, cli_init_patch):
    """`filter analyze --quick --sample-size N` forwards N to the analyzer (#1138)."""
    db_path = str(tmp_path / "filter_analyze_sample.db")
    db = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(db.initialize())
    asyncio.run(db.add_account(Account(phone="+100", session_string="sess")))
    with cli_init_patch(db, _FILTER_INIT_DB_TARGET):
        from tg_agent.cli.commands.filter import run

        with patch("tg_agent.cli.commands.filter.ChannelAnalyzer") as mock_analyzer:
            mock_instance = MagicMock()
            mock_report = MagicMock()
            mock_report.results = []
            mock_report.total_channels = 0
            mock_report.filtered_count = 0
            mock_instance.analyze_all = AsyncMock(return_value=mock_report)
            mock_analyzer.return_value = mock_instance

            run(_ns(filter_action="analyze", quick=True, sample_size=50))

    mock_instance.analyze_all.assert_awaited_once_with(quick=True, sample_size=50)


def test_analyze_prints_views_and_forward_columns(tmp_path, cli_init_patch, capsys):
    """`filter analyze` exposes views_ratio_pct/forward_pct as table columns (#1550)."""
    db_path = str(tmp_path / "filter_analyze_columns.db")
    db = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(db.initialize())
    asyncio.run(db.add_account(Account(phone="+100", session_string="sess")))
    with cli_init_patch(db, _FILTER_INIT_DB_TARGET):
        from tg_agent.cli.commands.filter import run
        from tg_agent.filters.models import ChannelFilterResult

        with patch("tg_agent.cli.commands.filter.ChannelAnalyzer") as mock_analyzer:
            mock_instance = MagicMock()
            mock_report = MagicMock()
            mock_report.results = [
                ChannelFilterResult(
                    channel_id=4200,
                    title="Farm",
                    flags=["low_engagement", "forwarder"],
                    views_ratio_pct=7.4,
                    forward_pct=80.0,
                    is_filtered=True,
                )
            ]
            mock_report.total_channels = 1
            mock_report.filtered_count = 1
            mock_instance.analyze_all = AsyncMock(return_value=mock_report)
            mock_analyzer.return_value = mock_instance

            run(_ns(filter_action="analyze"))

    out = capsys.readouterr().out
    assert "Views%" in out
    assert "Fwd%" in out
    assert "7.4" in out
    assert "80.0" in out
    assert "low_engagement, forwarder" in out


def test_analyze_prints_volume_column(tmp_path, cli_init_patch, capsys):
    """`filter analyze` exposes the msgs/24h column (#1571)."""
    db_path = str(tmp_path / "filter_analyze_volume.db")
    db = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(db.initialize())
    asyncio.run(db.add_account(Account(phone="+100", session_string="sess")))
    with cli_init_patch(db, _FILTER_INIT_DB_TARGET):
        from tg_agent.cli.commands.filter import run
        from tg_agent.filters.models import ChannelFilterResult

        with patch("tg_agent.cli.commands.filter.ChannelAnalyzer") as mock_analyzer:
            mock_instance = MagicMock()
            mock_report = MagicMock()
            mock_report.results = [
                ChannelFilterResult(
                    channel_id=5200,
                    title="Feed",
                    flags=["high_volume"],
                    volume_24h=123,
                    is_filtered=False,
                )
            ]
            mock_report.total_channels = 1
            mock_report.filtered_count = 0
            mock_instance.analyze_all = AsyncMock(return_value=mock_report)
            mock_analyzer.return_value = mock_instance

            run(_ns(filter_action="analyze"))

    out = capsys.readouterr().out
    assert "msgs/24h" in out
    assert "123" in out
    assert "high_volume" in out


def test_filter_analyze_parser_accepts_quick_flag():
    """CLI exposes --quick on `filter analyze` (#774)."""
    from typer.testing import CliRunner

    from tg_agent.cli.typer_app import app

    runner = CliRunner()

    mock_impl = MagicMock()
    with (
        patch("tg_agent.cli.typer_commands.filter_cmd.analyze_impl", mock_impl),
        patch("tg_agent.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["filter", "analyze", "--quick"])
    assert result.exit_code == 0
    assert mock_impl.call_args.kwargs["quick"] is True

    mock_impl_default = MagicMock()
    with (
        patch("tg_agent.cli.typer_commands.filter_cmd.analyze_impl", mock_impl_default),
        patch("tg_agent.cli.typer_commands.run_async"),
    ):
        result_default = runner.invoke(app, ["filter", "analyze"])
    assert result_default.exit_code == 0
    assert mock_impl_default.call_args.kwargs["quick"] is False


def test_filter_analyze_parser_accepts_sample_size():
    """CLI exposes --sample-size on `filter analyze` (#1138, default 300)."""
    from typer.testing import CliRunner

    from tg_agent.cli.typer_app import app

    runner = CliRunner()

    mock_impl = MagicMock()
    with (
        patch("tg_agent.cli.typer_commands.filter_cmd.analyze_impl", mock_impl),
        patch("tg_agent.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["filter", "analyze", "--quick", "--sample-size", "100"])
    assert result.exit_code == 0
    assert mock_impl.call_args.kwargs["sample_size"] == 100

    mock_impl_default = MagicMock()
    with (
        patch("tg_agent.cli.typer_commands.filter_cmd.analyze_impl", mock_impl_default),
        patch("tg_agent.cli.typer_commands.run_async"),
    ):
        result_default = runner.invoke(app, ["filter", "analyze"])
    assert result_default.exit_code == 0
    assert mock_impl_default.call_args.kwargs["sample_size"] == 300


def test_filter_apply(tmp_path, cli_init_patch, capsys):
    """Test filter apply action."""
    db_path = str(tmp_path / "filter_apply.db")
    db = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(db.initialize())
    with cli_init_patch(db, _FILTER_INIT_DB_TARGET):
        from tg_agent.cli.commands.filter import run

        with patch("tg_agent.cli.commands.filter.ChannelAnalyzer") as mock_analyzer:
            mock_instance = MagicMock()
            mock_report = MagicMock()
            mock_instance.analyze_all = AsyncMock(return_value=mock_report)
            mock_instance.apply_filters = AsyncMock(return_value=5)
            mock_analyzer.return_value = mock_instance

            run(_ns(filter_action="apply"))

    out = capsys.readouterr().out
    assert "Applied filters: 5" in out


def test_filter_precheck(tmp_path, cli_init_patch, capsys):
    """Test filter precheck action."""
    db_path = str(tmp_path / "filter_precheck.db")
    db = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(db.initialize())
    with cli_init_patch(db, _FILTER_INIT_DB_TARGET):
        from tg_agent.cli.commands.filter import run

        with patch("tg_agent.cli.commands.filter.ChannelAnalyzer") as mock_analyzer:
            mock_instance = MagicMock()
            mock_instance.precheck_subscriber_ratio = AsyncMock(return_value=3)
            mock_analyzer.return_value = mock_instance

            run(_ns(filter_action="precheck"))

    out = capsys.readouterr().out
    assert "Pre-filter applied: 3" in out


def test_filter_reset(tmp_path, cli_init_patch, capsys):
    """Test filter reset action."""
    db_path = str(tmp_path / "filter_reset.db")
    db = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(db.initialize())
    with cli_init_patch(db, _FILTER_INIT_DB_TARGET):
        from tg_agent.cli.commands.filter import run

        with patch("tg_agent.cli.commands.filter.ChannelAnalyzer") as mock_analyzer:
            mock_instance = MagicMock()
            mock_instance.reset_filters = AsyncMock()
            mock_analyzer.return_value = mock_instance

            run(_ns(filter_action="reset"))

    out = capsys.readouterr().out
    assert "All channel filters have been reset" in out


def test_filter_purge_all(tmp_path, cli_init_patch, capsys):
    """Test filter purge all action."""
    db_path = str(tmp_path / "filter_purge.db")
    db = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(db.initialize())
    with cli_init_patch(db, _FILTER_INIT_DB_TARGET):
        from tg_agent.cli.commands.filter import run

        with patch("tg_agent.cli.commands.filter._build_deletion_service") as mock_build:
            mock_svc = MagicMock()
            mock_result = MagicMock()
            mock_result.purged_count = 2
            mock_result.purged_titles = ["Channel A", "Channel B"]
            mock_result.skipped_count = 0
            mock_svc.purge_all_filtered = AsyncMock(return_value=mock_result)
            mock_build.return_value = mock_svc

            run(_ns(filter_action="purge", pks=None, yes=True))

    out = capsys.readouterr().out
    assert "Purged messages from 2 channels" in out


def test_filter_purge_by_pks(tmp_path, cli_init_patch, capsys):
    """Test filter purge by PKs action."""
    db_path = str(tmp_path / "filter_purge_pks.db")
    db = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(db.initialize())
    # The command looks every requested pk up before it asks or purges (#1955).
    for channel_id in (101, 102, 103):
        asyncio.run(db.add_channel(Channel(channel_id=channel_id, title=f"Channel {channel_id}")))
    with cli_init_patch(db, _FILTER_INIT_DB_TARGET):
        from tg_agent.cli.commands.filter import run

        with patch("tg_agent.cli.commands.filter._build_deletion_service") as mock_build:
            mock_svc = MagicMock()
            mock_result = MagicMock()
            mock_result.purged_count = 1
            mock_result.purged_titles = ["Channel X"]
            mock_result.skipped_count = 0
            mock_svc.purge_channels_by_pks = AsyncMock(return_value=mock_result)
            mock_build.return_value = mock_svc

            run(_ns(filter_action="purge", pks="1,2,3", yes=True))

    out = capsys.readouterr().out
    assert "Purged messages from 1 channel" in out


def test_filter_purge_invalid_pks(tmp_path, cli_init_patch, capsys):
    """Test filter purge with invalid PKs."""
    db_path = str(tmp_path / "filter_purge_invalid.db")
    db = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(db.initialize())
    with cli_init_patch(db, _FILTER_INIT_DB_TARGET):
        from tg_agent.cli.commands.filter import run

        # A token that is not a pk refuses the whole list (#1955).
        with pytest.raises(typer.Exit) as excinfo:
            run(_ns(filter_action="purge", pks="abc,def"))

    assert excinfo.value.exit_code == 1
    assert "Invalid pk(s) in --pks: abc, def" in capsys.readouterr().err


def test_filter_hard_delete_requires_dev_mode(tmp_path, cli_init_patch, capsys):
    """Test filter hard-delete requires dev mode."""
    db_path = str(tmp_path / "filter_hard_delete_dev.db")
    db = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(db.initialize())
    with cli_init_patch(db, _FILTER_INIT_DB_TARGET):
        from tg_agent.cli.commands.filter import run

        # Refused without developer mode — as a failure, not a printed note (#1955).
        with pytest.raises(typer.Exit) as excinfo:
            run(_ns(filter_action="hard-delete", pks=None, yes=False))

    assert excinfo.value.exit_code == 1
    assert "Hard-delete requires developer mode" in capsys.readouterr().err


def test_filter_no_action(cli_init_patch, capsys):
    """Test filter without action."""
    from tg_agent.cli.commands.filter import run

    db = MagicMock()
    db.close = AsyncMock()

    with cli_init_patch(db, _FILTER_INIT_DB_TARGET):
        run(_ns(filter_action=None))

    out = capsys.readouterr().out
    assert "Usage:" in out


def _read_channel_is_filtered(db_path: str, pk: int) -> int:
    """Read is_filtered for a channel using a fresh sqlite3 connection.

    Needed because CLI handlers call db.close() in finally — the original
    aiosqlite Database is unusable after run().
    """
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute("SELECT is_filtered FROM channels WHERE id = ?", (pk,))
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def test_filter_toggle_flips_flag(tmp_path, cli_init_patch, capsys):
    """filter toggle: marks unfiltered channel as filtered, then back.

    filter.run() calls db.close() in its own finally — we use fresh_database=True
    so cli_init_patch opens (and closes) a separate Database for the handler each
    time, leaving the seed Database fully manageable.
    """
    db_path = str(tmp_path / "filter_toggle.db")
    seed = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(seed.initialize())
    try:
        pk = asyncio.run(
            seed.add_channel(Channel(channel_id=900_001, title="ToggleCh", is_filtered=False))
        )

        with cli_init_patch(seed, _FILTER_INIT_DB_TARGET, fresh_database=True):
            from tg_agent.cli.commands.filter import run

            run(_ns(filter_action="toggle", pk=pk))

        assert _read_channel_is_filtered(db_path, pk) == 1
        out = capsys.readouterr().out
        assert "marked as filtered" in out

        with cli_init_patch(seed, _FILTER_INIT_DB_TARGET, fresh_database=True):
            from tg_agent.cli.commands.filter import run

            run(_ns(filter_action="toggle", pk=pk))

        assert _read_channel_is_filtered(db_path, pk) == 0
        out2 = capsys.readouterr().out
        assert "marked as unfiltered" in out2
    finally:
        asyncio.run(seed.close())


def test_filter_toggle_not_found(tmp_path, cli_init_patch, capsys):
    """filter toggle: pk that doesn't exist prints not-found."""
    db_path = str(tmp_path / "filter_toggle_missing.db")
    db = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(db.initialize())
    try:
        with cli_init_patch(db, _FILTER_INIT_DB_TARGET, fresh_database=True):
            from tg_agent.cli.commands.filter import run

            with pytest.raises(typer.Exit) as excinfo:
                run(_ns(filter_action="toggle", pk=999))
            assert excinfo.value.exit_code == 1

        out = capsys.readouterr().err
        assert "not found" in out
    finally:
        asyncio.run(db.close())


# ── Confirm-gate consistency (issue #1039 p.4) ───────────────────────────────
#
# purge/purge-messages accept a case-insensitive "y"; hard-delete keeps the
# stronger "YES" word as a deliberate barrier for an irreversible op — but the
# *case handling* must be consistent. A lowercase "yes" used to be rejected
# (case-sensitive == "YES"), which is the inconsistency these tests pin down.


def _run_hard_delete_with_prompt(tmp_path, cli_init_patch, db_name, typed):
    """Run `filter hard-delete` once in dev mode with the prompt stubbed to *typed*
    and the deletion service mocked. Returns (mock_service, stdout).

    Seeds the dev-mode flag, then closes the seed DB *before* run() — run() reopens
    the same file via cli_init_patch and closes it in its own finally. Keeping two
    aiosqlite connections open on one WAL file would deadlock on the write lock.
    """
    db_path = str(tmp_path / db_name)
    seed = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(seed.initialize())
    asyncio.run(seed.set_setting("agent_dev_mode_enabled", "1"))
    # pk 1 must exist: the command looks every requested pk up first (#1955).
    asyncio.run(seed.add_channel(Channel(channel_id=100, title="HardDeleteTarget")))
    asyncio.run(seed.close())

    captured = {}
    with cli_init_patch(seed, _FILTER_INIT_DB_TARGET):
        from tg_agent.cli.commands.filter import run

        with patch("tg_agent.cli.commands.filter._build_deletion_service") as mock_build, patch(
            "builtins.input", return_value=typed
        ):
            mock_svc = MagicMock()
            mock_result = MagicMock()
            mock_result.purged_count = 1
            mock_result.purged_titles = ["DelCh"]
            mock_result.skipped_count = 0
            mock_result.errors = []
            mock_svc.hard_delete_channels_by_pks = AsyncMock(return_value=mock_result)
            mock_build.return_value = mock_svc
            captured["svc"] = mock_svc

            run(_ns(filter_action="hard-delete", pks="1", yes=False))

    return captured["svc"]


def test_hard_delete_confirm_accepts_lowercase_yes(tmp_path, cli_init_patch, capsys):
    """Regression (#1039): a lowercase 'yes' must confirm hard-delete. Before the
    fix the gate compared case-sensitively against 'YES' and aborted."""
    svc = _run_hard_delete_with_prompt(tmp_path, cli_init_patch, "hd_lower.db", "yes")
    out = capsys.readouterr().out
    assert "Aborted." not in out
    svc.hard_delete_channels_by_pks.assert_awaited_once()


def test_hard_delete_confirm_accepts_mixed_case_yes(tmp_path, cli_init_patch, capsys):
    """'Yes' (mixed case) is also accepted — the gate is case-insensitive (#1039)."""
    svc = _run_hard_delete_with_prompt(tmp_path, cli_init_patch, "hd_mixed.db", "Yes")
    out = capsys.readouterr().out
    assert "Aborted." not in out
    svc.hard_delete_channels_by_pks.assert_awaited_once()


def test_hard_delete_confirm_rejects_other_words(tmp_path, cli_init_patch, capsys):
    """A non-'yes' answer still aborts — the confirmation word stays meaningful,
    so a bare 'y' is NOT enough for the irreversible hard-delete (#1039)."""
    svc = _run_hard_delete_with_prompt(tmp_path, cli_init_patch, "hd_reject.db", "y")
    out = capsys.readouterr().out
    assert "Aborted." in out
    svc.hard_delete_channels_by_pks.assert_not_awaited()


# ── filter flag: операторские sticky-флаги (#1499) ───────────────────────────


def _read_channel_filter_row(db_path: str, pk: int) -> tuple:
    """(is_filtered, filtered_origin, filter_flags) через свежий sqlite3-коннект."""
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            "SELECT is_filtered, filtered_origin, filter_flags FROM channels WHERE id = ?",
            (pk,),
        )
        return cur.fetchone()
    finally:
        conn.close()


def test_filter_flag_set_and_unset(tmp_path, cli_init_patch, capsys):
    """`filter flag --set/--unset manual` (#1499): durable-вердикт и возврат под авто."""
    db_path = str(tmp_path / "filter_flag.db")
    seed = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(seed.initialize())
    try:
        pk = asyncio.run(
            seed.add_channel(Channel(channel_id=900_100, title="FlagCh", is_filtered=False))
        )

        with cli_init_patch(seed, _FILTER_INIT_DB_TARGET, fresh_database=True):
            from tg_agent.cli.commands.filter import run

            run(_ns(filter_action="flag", pk=pk, set_flag="manual", unset_flag=None))

        assert _read_channel_filter_row(db_path, pk) == (1, "human", "manual")
        assert "sticky flag 'manual' set" in capsys.readouterr().out

        with cli_init_patch(seed, _FILTER_INIT_DB_TARGET, fresh_database=True):
            from tg_agent.cli.commands.filter import run

            run(_ns(filter_action="flag", pk=pk, set_flag=None, unset_flag="manual"))

        assert _read_channel_filter_row(db_path, pk) == (1, "auto", "")
        assert "back under auto verdicts" in capsys.readouterr().out
    finally:
        asyncio.run(seed.close())


def test_filter_flag_unset_without_flag_is_noop(tmp_path, cli_init_patch, capsys):
    """Холостой unset (#1573): флага не было — CLI не рапортует изменение."""
    db_path = str(tmp_path / "filter_flag_noop.db")
    seed = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(seed.initialize())
    try:
        pk = asyncio.run(
            seed.add_channel(Channel(channel_id=900_102, title="FlagCh3", is_filtered=False))
        )

        with cli_init_patch(seed, _FILTER_INIT_DB_TARGET, fresh_database=True):
            from tg_agent.cli.commands.filter import run

            run(_ns(filter_action="flag", pk=pk, set_flag=None, unset_flag="manual"))

        out = capsys.readouterr().out
        assert "was not set; nothing changed" in out
        assert "back under auto verdicts" not in out
    finally:
        asyncio.run(seed.close())


def test_filter_flag_invalid_flag_rejected(tmp_path, cli_init_patch, capsys):
    """Чужие флаги анализатора руками не ставятся (#1499)."""
    db_path = str(tmp_path / "filter_flag_invalid.db")
    seed = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(seed.initialize())
    try:
        pk = asyncio.run(
            seed.add_channel(Channel(channel_id=900_101, title="FlagCh2", is_filtered=False))
        )

        with cli_init_patch(seed, _FILTER_INIT_DB_TARGET, fresh_database=True):
            from tg_agent.cli.commands.filter import run

            with pytest.raises(typer.Exit):
                run(_ns(filter_action="flag", pk=pk, set_flag="low_uniqueness", unset_flag=None))

        err = capsys.readouterr().err
        assert "Unknown operator flag 'low_uniqueness'" in err
        assert _read_channel_filter_row(db_path, pk)[0] == 0
    finally:
        asyncio.run(seed.close())


def test_filter_flag_requires_exactly_one_action(tmp_path, cli_init_patch, capsys):
    db_path = str(tmp_path / "filter_flag_actions.db")
    seed = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(seed.initialize())
    try:
        with cli_init_patch(seed, _FILTER_INIT_DB_TARGET, fresh_database=True):
            from tg_agent.cli.commands.filter import run

            with pytest.raises(typer.Exit):
                run(_ns(filter_action="flag", pk=1, set_flag=None, unset_flag=None))
            with pytest.raises(typer.Exit):
                run(_ns(filter_action="flag", pk=1, set_flag="manual", unset_flag="manual"))
    finally:
        asyncio.run(seed.close())

    err = capsys.readouterr().err
    assert err.count("Usage: filter flag") == 2


def test_filter_flag_not_found(tmp_path, cli_init_patch, capsys):
    db_path = str(tmp_path / "filter_flag_missing.db")
    seed = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(seed.initialize())
    try:
        with cli_init_patch(seed, _FILTER_INIT_DB_TARGET, fresh_database=True):
            from tg_agent.cli.commands.filter import run

            with pytest.raises(typer.Exit) as excinfo:
                run(_ns(filter_action="flag", pk=424242, set_flag="manual", unset_flag=None))
            assert excinfo.value.exit_code == 1
    finally:
        asyncio.run(seed.close())

    assert "Channel pk=424242 not found" in capsys.readouterr().err


def test_filter_apply_whitelist_mode(tmp_path, cli_init_patch, capsys):
    """`filter apply --whitelist` зовёт apply_whitelist; analyze не запускается (#1490)."""
    db_path = str(tmp_path / "filter_whitelist.db")
    db = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(db.initialize())
    try:
        with cli_init_patch(db, _FILTER_INIT_DB_TARGET, fresh_database=True):
            from tg_agent.cli.commands.filter import run

            with patch("tg_agent.cli.commands.filter.ChannelAnalyzer") as mock_analyzer:
                mock_instance = MagicMock()
                mock_instance.apply_whitelist = AsyncMock(return_value=3)
                mock_analyzer.return_value = mock_instance

                run(_ns(filter_action="apply", whitelist=True))

        out = capsys.readouterr().out
        assert "Whitelist applied: 3" in out
        mock_instance.apply_whitelist.assert_awaited_once_with()
        mock_instance.analyze_all.assert_not_called()
    finally:
        asyncio.run(db.close())
