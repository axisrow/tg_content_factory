"""Tests for filter CLI commands."""
from __future__ import annotations

import argparse
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.database import Database
from src.models import Account, Channel

pytestmark = pytest.mark.aiosqlite_serial

_FILTER_INIT_DB_TARGET = "src.cli.commands.filter.runtime.init_db"


def _ns(**kwargs) -> argparse.Namespace:
    defaults = {"config": "config.yaml"}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_parse_pks():
    """Test _parse_pks parses comma-separated PKs."""
    from src.cli.commands.filter import _parse_pks

    assert _parse_pks("1,2,3") == [1, 2, 3]
    assert _parse_pks(" 1 , 2 , 3 ") == [1, 2, 3]
    assert _parse_pks("") == []
    assert _parse_pks("abc,1,def,2") == [1, 2]


def test_print_result_empty():
    """Test _print_result with empty result."""
    import sys
    from io import StringIO

    from src.cli.commands.filter import _print_result

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
    from src.cli.commands.filter import _print_result

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
    db = Database(db_path)
    asyncio.run(db.initialize())
    asyncio.run(db.add_account(Account(phone="+100", session_string="sess")))
    asyncio.run(db.add_channel(Channel(channel_id=1001, title="Test Channel")))
    with cli_init_patch(db, _FILTER_INIT_DB_TARGET):
        from src.cli.commands.filter import run

        with patch("src.cli.commands.filter.ChannelAnalyzer") as mock_analyzer:
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
    mock_instance.analyze_all.assert_awaited_once_with(quick=False)
    mock_instance.apply_filters.assert_not_awaited()


def test_analyze_quick_skips_cross_dupe(tmp_path, cli_init_patch, capsys):
    """`filter analyze --quick` requests the analyzer's quick mode (#774)."""
    db_path = str(tmp_path / "filter_analyze_quick.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    asyncio.run(db.add_account(Account(phone="+100", session_string="sess")))
    asyncio.run(db.add_channel(Channel(channel_id=1001, title="Test Channel")))
    with cli_init_patch(db, _FILTER_INIT_DB_TARGET):
        from src.cli.commands.filter import run

        with patch("src.cli.commands.filter.ChannelAnalyzer") as mock_analyzer:
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
    mock_instance.analyze_all.assert_awaited_once_with(quick=True)


def test_filter_analyze_parser_accepts_quick_flag():
    """CLI exposes --quick on `filter analyze` (#774)."""
    from typer.testing import CliRunner

    from src.cli.typer_app import app

    runner = CliRunner()

    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.filter_cmd.analyze_impl", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = runner.invoke(app, ["filter", "analyze", "--quick"])
    assert result.exit_code == 0
    assert mock_impl.call_args.kwargs["quick"] is True

    mock_impl_default = MagicMock()
    with (
        patch("src.cli.typer_commands.filter_cmd.analyze_impl", mock_impl_default),
        patch("src.cli.typer_commands.run_async"),
    ):
        result_default = runner.invoke(app, ["filter", "analyze"])
    assert result_default.exit_code == 0
    assert mock_impl_default.call_args.kwargs["quick"] is False


def test_filter_apply(tmp_path, cli_init_patch, capsys):
    """Test filter apply action."""
    db_path = str(tmp_path / "filter_apply.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    with cli_init_patch(db, _FILTER_INIT_DB_TARGET):
        from src.cli.commands.filter import run

        with patch("src.cli.commands.filter.ChannelAnalyzer") as mock_analyzer:
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
    db = Database(db_path)
    asyncio.run(db.initialize())
    with cli_init_patch(db, _FILTER_INIT_DB_TARGET):
        from src.cli.commands.filter import run

        with patch("src.cli.commands.filter.ChannelAnalyzer") as mock_analyzer:
            mock_instance = MagicMock()
            mock_instance.precheck_subscriber_ratio = AsyncMock(return_value=3)
            mock_analyzer.return_value = mock_instance

            run(_ns(filter_action="precheck"))

    out = capsys.readouterr().out
    assert "Pre-filter applied: 3" in out


def test_filter_reset(tmp_path, cli_init_patch, capsys):
    """Test filter reset action."""
    db_path = str(tmp_path / "filter_reset.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    with cli_init_patch(db, _FILTER_INIT_DB_TARGET):
        from src.cli.commands.filter import run

        with patch("src.cli.commands.filter.ChannelAnalyzer") as mock_analyzer:
            mock_instance = MagicMock()
            mock_instance.reset_filters = AsyncMock()
            mock_analyzer.return_value = mock_instance

            run(_ns(filter_action="reset"))

    out = capsys.readouterr().out
    assert "All channel filters have been reset" in out


def test_filter_purge_all(tmp_path, cli_init_patch, capsys):
    """Test filter purge all action."""
    db_path = str(tmp_path / "filter_purge.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    with cli_init_patch(db, _FILTER_INIT_DB_TARGET):
        from src.cli.commands.filter import run

        with patch("src.cli.commands.filter._build_deletion_service") as mock_build:
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
    db = Database(db_path)
    asyncio.run(db.initialize())
    with cli_init_patch(db, _FILTER_INIT_DB_TARGET):
        from src.cli.commands.filter import run

        with patch("src.cli.commands.filter._build_deletion_service") as mock_build:
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
    db = Database(db_path)
    asyncio.run(db.initialize())
    with cli_init_patch(db, _FILTER_INIT_DB_TARGET):
        from src.cli.commands.filter import run

        run(_ns(filter_action="purge", pks="abc,def"))

    out = capsys.readouterr().out
    assert "No valid PKs provided" in out


def test_filter_hard_delete_requires_dev_mode(tmp_path, cli_init_patch, capsys):
    """Test filter hard-delete requires dev mode."""
    db_path = str(tmp_path / "filter_hard_delete_dev.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    with cli_init_patch(db, _FILTER_INIT_DB_TARGET):
        from src.cli.commands.filter import run

        run(_ns(filter_action="hard-delete", pks=None, yes=False))

    out = capsys.readouterr().out
    assert "Hard-delete requires developer mode" in out


def test_filter_no_action(cli_init_patch, capsys):
    """Test filter without action."""
    from src.cli.commands.filter import run

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
    seed = Database(db_path)
    asyncio.run(seed.initialize())
    try:
        pk = asyncio.run(
            seed.add_channel(Channel(channel_id=900_001, title="ToggleCh", is_filtered=False))
        )

        with cli_init_patch(seed, _FILTER_INIT_DB_TARGET, fresh_database=True):
            from src.cli.commands.filter import run

            run(_ns(filter_action="toggle", pk=pk))

        assert _read_channel_is_filtered(db_path, pk) == 1
        out = capsys.readouterr().out
        assert "marked as filtered" in out

        with cli_init_patch(seed, _FILTER_INIT_DB_TARGET, fresh_database=True):
            from src.cli.commands.filter import run

            run(_ns(filter_action="toggle", pk=pk))

        assert _read_channel_is_filtered(db_path, pk) == 0
        out2 = capsys.readouterr().out
        assert "marked as unfiltered" in out2
    finally:
        asyncio.run(seed.close())


def test_filter_toggle_not_found(tmp_path, cli_init_patch, capsys):
    """filter toggle: pk that doesn't exist prints not-found."""
    db_path = str(tmp_path / "filter_toggle_missing.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    try:
        with cli_init_patch(db, _FILTER_INIT_DB_TARGET, fresh_database=True):
            from src.cli.commands.filter import run

            run(_ns(filter_action="toggle", pk=999))

        out = capsys.readouterr().out
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
    seed = Database(db_path)
    asyncio.run(seed.initialize())
    asyncio.run(seed.set_setting("agent_dev_mode_enabled", "1"))
    asyncio.run(seed.close())

    captured = {}
    with cli_init_patch(seed, _FILTER_INIT_DB_TARGET):
        from src.cli.commands.filter import run

        with patch("src.cli.commands.filter._build_deletion_service") as mock_build, patch(
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
