"""Regression coverage for issue #303 CLI parity and dialogs rename."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

from tests.helpers import cli_ns

ISSUE_303_PARSE_CASES = {
    "messages read": ["messages", "read", "demo"],
    "account send-code": ["account", "send-code", "--phone", "+10000000000"],
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


def _command_path_resolves(argv: list[str]) -> bool:
    """True if *argv* resolves to a real leaf command on the Typer ``app``.

    Walks the Click command tree the Typer app produces, descending one token at
    a time and stopping at the first non-group. The remaining tokens are options /
    positionals for that leaf — they don't need to resolve as sub-commands. This
    replaces the old ``build_parser().parse_args(argv)`` parse check (#1125 removed
    argparse) without invoking the command body.
    """
    import click
    import typer

    from src.cli.typer_commands import app

    node: click.BaseCommand = typer.main.get_command(app)
    for token in argv:
        if not isinstance(node, click.Group):
            return True  # reached a leaf; the rest are its args
        child = node.commands.get(token)
        if child is None:
            return False
        node = child
    return True


def test_issue_303_command_signatures_parse():
    for label, argv in ISSUE_303_PARSE_CASES.items():
        assert _command_path_resolves(argv), label


def test_dialogs_parse():
    from typer.testing import CliRunner

    from src.cli.typer_app import app

    # Drive the real Typer surface: `dialogs topics --channel-id 123` must parse
    # and reach the body with the resolved channel_id (argparse-parity flags).
    mock_impl = MagicMock()
    with (
        patch("src.cli.typer_commands.dialogs_cmd._dispatch", mock_impl),
        patch("src.cli.typer_commands.run_async"),
    ):
        result = CliRunner().invoke(app, ["dialogs", "topics", "--channel-id", "123"])
    assert result.exit_code == 0
    ns = mock_impl.call_args.args[0]
    assert ns.dialogs_action == "topics"
    assert ns.channel_id == 123


def test_dialogs_help_lists_subcommands():
    from typer.testing import CliRunner

    from src.cli.typer_app import app

    result = CliRunner().invoke(app, ["dialogs", "--help"])

    assert result.exit_code == 0
    out = result.output
    assert "dialogs" in out
    assert "broadcast-stats" in out
    assert "cache-status" in out


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
