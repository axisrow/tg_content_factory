from __future__ import annotations

import sys

from dotenv import load_dotenv

from src.cli.commands import account, channel, collect, keyword, scheduler, search, serve
from src.cli.parser import build_parser
from src.cli.runtime import setup_logging


def main() -> None:
    load_dotenv()
    setup_logging()

    parser = build_parser()
    args = parser.parse_args()

    commands = {
        "serve": serve.run,
        "collect": collect.run,
        "search": search.run,
        "channel": channel.run,
        "keyword": keyword.run,
        "account": account.run,
        "scheduler": scheduler.run,
    }

    handler = commands.get(args.command)
    if handler:
        sub_attr = {
            "channel": "channel_action",
            "keyword": "keyword_action",
            "account": "account_action",
            "scheduler": "scheduler_action",
        }
        if args.command in sub_attr and not getattr(args, sub_attr[args.command], None):
            parser.parse_args([args.command, "--help"])
        else:
            handler(args)
    else:
        parser.print_help()
        sys.exit(1)
