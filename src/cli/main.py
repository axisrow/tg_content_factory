from __future__ import annotations

import sys

from dotenv import load_dotenv

from src.cli.commands import (
    account,
    channel,
    collect,
    export,
    image,
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
from src.cli.parser import build_parser
from src.cli.runtime import ensure_data_dirs, setup_logging


def main() -> None:
    load_dotenv()
    setup_logging()
    ensure_data_dirs()

    parser = build_parser()
    args = parser.parse_args()

    commands = {
        "serve": serve.run,
        "worker": worker.run,
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
        "my-telegram": dialogs_cmd.run,  # backward-compat alias
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
            "my-telegram": "dialogs_action",
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
