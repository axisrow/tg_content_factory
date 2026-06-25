from __future__ import annotations

import os
import sys

from src.cli.commands import (
    account,
    channel,
    collect,
    export,
    image,
    mcp_server,
    messages,
    notification,
    photo_loader,
    provider,
    scheduler,
    search,
    search_query,
    serve,
    server_control,
    worker,
)
from src.cli.commands import agent as agent_cmd
from src.cli.commands import analytics as analytics_cmd
from src.cli.commands import debug as debug_cmd
from src.cli.commands import dialogs as dialogs_cmd
from src.cli.commands import filter as filter_cmd
from src.cli.commands import pipeline as pipeline_cmd
from src.cli.commands import settings as settings_cmd
from src.cli.commands import test as test_cmd
from src.cli.commands import translate as translate_cmd
from src.cli.dotenv import load_cli_dotenv
from src.cli.parser import build_parser
from src.cli.runtime import ensure_data_dirs, setup_logging
from src.cli.typer_commands import MIGRATED_COMMANDS, dispatch_via_typer


# Migration note (epic #959, Wave 0 — issue #1120): the Typer scaffold lives in
# ``src/cli/typer_app.py`` (``app`` + ``@app.callback()`` global options +
# ``apply_startup`` startup side effects + ``run_async`` async-bridge). The
# startup side effects below (lines exporting TG_CONFIG_PATH / dotenv / logging /
# data-dirs) are mirrored 1:1 by ``apply_startup``, which migrated commands call
# as their first line — keeping them on the command path so a ``subcommand
# --help`` stays side-effect-free, exactly as argparse short-circuits here.
# Wave 0 ships the scaffold only — no commands are migrated yet, so this argparse
# dispatcher stays the single entry point and the ``build_parser()`` leaf-coverage
# audit remains the source of truth. Waves 1–4 register their migrated commands on
# ``app`` and route them here (the dispatcher delegates not-yet-migrated commands
# to ``commands.X.run``); the Final wave (#1125) removes this argparse path.
def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    # Wave 1 (epic #959, issue #1121): the super-simple commands now live as
    # Typer commands on ``src/cli/typer_app.py::app``. Route them through the
    # Typer app *before* the argparse-path startup side effects below: the Typer
    # commands run those exact side effects themselves via ``apply_startup`` (the
    # 1:1 port), so doing them here too would double-fire logging setup / dotenv.
    # Every other command keeps the argparse ``commands.X.run`` path (and the
    # side effects below) until its own wave lands. The argparse ``register()``
    # declarations stay so ``build_parser()`` remains the leaf-coverage source of
    # truth (test_real_telegram_policy manifest sweep).
    if args.command in MIGRATED_COMMANDS:
        dispatch_via_typer(args)
        return

    # Export the resolved config path so subprocess-spawning backends inherit it.
    # CodexSdkBackend spawns `python -m src.main --config <path> mcp-server`; it
    # learns <path> only via TG_CONFIG_PATH (AppConfig doesn't carry its source).
    # Without this, a non-default `--config /srv/prod.yaml` would silently spawn
    # the MCP server against the default config.yaml / data/tg_search.db — the
    # wrong DB for write-capable tool calls. abspath so a differing subprocess
    # CWD still resolves it.
    os.environ["TG_CONFIG_PATH"] = os.path.abspath(args.config)

    load_cli_dotenv(args.config)
    setup_logging()
    ensure_data_dirs()

    commands = {
        "serve": serve.run,
        "worker": worker.run,
        "mcp-server": mcp_server.run,
        "stop": server_control.run_stop,
        "restart": server_control.run_restart,
        "collect": collect.run,
        "search": search.run,
        "messages": messages.run,
        "channel": channel.run,
        "filter": filter_cmd.run,
        "search-query": search_query.run,
        "pipeline": pipeline_cmd.run,
        "account": account.run,
        "scheduler": scheduler.run,
        "notification": notification.run,
        "photo-loader": photo_loader.run,
        "dialogs": dialogs_cmd.run,
        "test": test_cmd.run,
        "agent": agent_cmd.run,
        "analytics": analytics_cmd.run,
        "image": image.run,
        "settings": settings_cmd.run,
        "translate": translate_cmd.run,
        "provider": provider.run,
        "export": export.run,
        "debug": debug_cmd.run,
    }

    handler = commands.get(args.command)
    if handler:
        sub_attr = {
            "messages": "messages_action",
            "channel": "channel_action",
            "filter": "filter_action",
            "search-query": "search_query_action",
            "pipeline": "pipeline_action",
            "account": "account_action",
            "scheduler": "scheduler_action",
            "notification": "notification_action",
            "photo-loader": "photo_loader_action",
            "dialogs": "dialogs_action",
            "test": "test_action",
            "agent": "agent_action",
            "analytics": "analytics_action",
            "image": "image_action",
            "settings": "settings_action",
            "provider": "provider_action",
            "export": "export_action",
            "debug": "debug_action",
        }
        if args.command in sub_attr and not getattr(args, sub_attr[args.command], None):
            parser.parse_args([args.command, "--help"])
        else:
            handler(args)
    else:
        parser.print_help()
        sys.exit(1)
