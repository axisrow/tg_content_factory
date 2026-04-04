"""Tests for filter CLI commands."""
from __future__ import annotations

import argparse
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from src.database import Database
from src.models import Account, Channel

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
    asyncio.run(db.close())

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


def test_filter_apply(tmp_path, cli_init_patch, capsys):
    """Test filter apply action."""
    db_path = str(tmp_path / "filter_apply.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    asyncio.run(db.close())

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
    asyncio.run(db.close())

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
    asyncio.run(db.close())

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
    asyncio.run(db.close())

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

            run(_ns(filter_action="purge", pks=None))

    out = capsys.readouterr().out
    assert "Purged messages from 2 channels" in out


def test_filter_purge_by_pks(tmp_path, cli_init_patch, capsys):
    """Test filter purge by PKs action."""
    db_path = str(tmp_path / "filter_purge_pks.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    asyncio.run(db.close())

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

            run(_ns(filter_action="purge", pks="1,2,3"))

    out = capsys.readouterr().out
    assert "Purged messages from 1 channel" in out


def test_filter_purge_invalid_pks(tmp_path, cli_init_patch, capsys):
    """Test filter purge with invalid PKs."""
    db_path = str(tmp_path / "filter_purge_invalid.db")
    db = Database(db_path)
    asyncio.run(db.initialize())
    asyncio.run(db.close())

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
    asyncio.run(db.close())

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
