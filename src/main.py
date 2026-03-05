from __future__ import annotations

import argparse

from src.cli import runtime
from src.cli.commands import account, channel, collect, keyword, scheduler, search, serve
from src.cli.main import main


async def _init_db(config_path: str):
    return await runtime.init_db(config_path)


async def _init_pool(config, db):
    return await runtime.init_pool(config, db)


def setup_logging() -> None:
    runtime.setup_logging()


def _run_with_legacy_runtime(handler, args: argparse.Namespace) -> None:
    old_init_db = runtime.init_db
    old_init_pool = runtime.init_pool
    runtime.init_db = _init_db
    runtime.init_pool = _init_pool
    try:
        handler(args)
    finally:
        runtime.init_db = old_init_db
        runtime.init_pool = old_init_pool


def cmd_serve(args: argparse.Namespace) -> None:
    _run_with_legacy_runtime(serve.run, args)


def cmd_collect(args: argparse.Namespace) -> None:
    _run_with_legacy_runtime(collect.run, args)


def cmd_search(args: argparse.Namespace) -> None:
    _run_with_legacy_runtime(search.run, args)


def cmd_channel(args: argparse.Namespace) -> None:
    _run_with_legacy_runtime(channel.run, args)


def cmd_keyword(args: argparse.Namespace) -> None:
    _run_with_legacy_runtime(keyword.run, args)


def cmd_account(args: argparse.Namespace) -> None:
    _run_with_legacy_runtime(account.run, args)


def cmd_scheduler(args: argparse.Namespace) -> None:
    _run_with_legacy_runtime(scheduler.run, args)


if __name__ == "__main__":
    main()
