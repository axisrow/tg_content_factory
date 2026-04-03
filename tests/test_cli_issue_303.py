"""Regression coverage for issue #303 CLI parity and dialogs rename."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import AppConfig
from tests.helpers import cli_ns

ISSUE_303_PARSE_CASES = {
    "messages read": ["messages", "read", "demo"],
    "account add": ["account", "add", "--phone", "+10000000000"],
    "provider list": ["provider", "list"],
    "provider add": ["provider", "add", "openai", "--api-key", "secret"],
    "provider delete": ["provider", "delete", "openai"],
    "provider probe": ["provider", "probe", "openai"],
    "provider refresh": ["provider", "refresh"],
    "provider test-all": ["provider", "test-all"],
    "dialogs list": ["dialogs", "list"],
    "channel tag list": ["channel", "tag", "list"],
    "export json": ["export", "json"],
    "filter purge-messages": ["filter", "purge-messages", "--channel-id", "1"],
    "notification set-account": ["notification", "set-account", "--phone", "+10000000000"],
    "channel add-bulk": ["channel", "add-bulk", "--phone", "+10000000000", "--dialog-ids", "1,2"],
    "pipeline refinement-steps": ["pipeline", "refinement-steps", "1"],
    "analytics trending-emojis": ["analytics", "trending-emojis"],
    "translate message": ["translate", "message", "1"],
    "settings agent": ["settings", "agent"],
    "settings filter-criteria": ["settings", "filter-criteria"],
    "settings semantic": ["settings", "semantic"],
    "debug logs": ["debug", "logs"],
    "debug memory": ["debug", "memory"],
    "debug timing": ["debug", "timing"],
}


def test_issue_303_command_signatures_parse():
    from src.cli.parser import build_parser

    parser = build_parser()

    for label, argv in ISSUE_303_PARSE_CASES.items():
        parsed = parser.parse_args(argv)
        assert parsed.command == argv[0], label


def test_dialogs_and_legacy_alias_parse():
    from src.cli.parser import build_parser

    parser = build_parser()

    dialogs_args = parser.parse_args(["dialogs", "topics", "--channel-id", "123"])
    alias_args = parser.parse_args(["my-telegram", "topics", "--channel-id", "456"])

    assert dialogs_args.command == "dialogs"
    assert dialogs_args.dialogs_action == "topics"
    assert dialogs_args.channel_id == 123

    assert alias_args.command == "my-telegram"
    assert alias_args.dialogs_action == "topics"
    assert alias_args.channel_id == 456


def test_dialogs_help_lists_subcommands(capsys):
    from src.cli.parser import build_parser

    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["dialogs", "--help"])

    out = capsys.readouterr().out
    assert "usage:" in out
    assert "dialogs" in out
    assert "broadcast-stats" in out
    assert "cache-status" in out


def test_dialogs_help_still_uses_dialogs_usage(capsys):
    from src.cli.parser import build_parser

    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["my-telegram", "--help"])

    out = capsys.readouterr().out
    assert "usage:" in out
    assert "dialogs" in out
    assert "mark-read" in out


def test_dialogs_run_is_primary_entrypoint(cli_db, cli_init_patch, capsys):
    pool = MagicMock()
    pool.clients = {"+79001234567": AsyncMock()}
    pool.disconnect_all = AsyncMock()

    async def fake_init_pool(config, db):
        return MagicMock(), pool

    dialogs_payload = [
        {
            "channel_id": -100111,
            "title": "Primary Dialogs Entry",
            "username": "primary_dialogs",
            "channel_type": "channel",
            "already_added": True,
        }
    ]

    with (
        cli_init_patch(cli_db, "src.cli.commands.dialogs.runtime.init_db"),
        patch("src.cli.commands.dialogs.runtime.init_pool", side_effect=fake_init_pool),
        patch(
            "src.cli.commands.dialogs.ChannelService.get_my_dialogs",
            new_callable=AsyncMock,
            return_value=dialogs_payload,
        ),
    ):
        from src.cli.commands.dialogs import run

        run(cli_ns(dialogs_action="list", phone="+79001234567"))

    out = capsys.readouterr().out
    assert "Primary Dialogs Entry" in out
    assert "@primary_dialogs" in out


def test_dialogs_legacy_alias_remains_compatible(cli_db, cli_init_patch, capsys):
    pool = MagicMock()
    pool.clients = {"+79001234567": AsyncMock()}
    pool.disconnect_all = AsyncMock()

    async def fake_init_pool(config, db):
        return MagicMock(), pool

    with (
        cli_init_patch(cli_db, "src.cli.commands.dialogs.runtime.init_db"),
        patch("src.cli.commands.dialogs.runtime.init_pool", side_effect=fake_init_pool),
        patch(
            "src.cli.commands.dialogs.ChannelService.get_my_dialogs",
            new_callable=AsyncMock,
            return_value=[
                {
                    "channel_id": -100222,
                    "title": "Legacy Alias Dialog",
                    "username": None,
                    "channel_type": "supergroup",
                    "already_added": False,
                }
            ],
        ),
    ):
        from src.cli.commands.dialogs import run

        run(cli_ns(dialogs_action="list", phone="+79001234567"))

    out = capsys.readouterr().out
    assert "Legacy Alias Dialog" in out
