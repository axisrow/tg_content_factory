from __future__ import annotations

import argparse

from src.cli import runtime
from src.cli.commands import (
    account,
    channel,
    collect,
    photo_loader,
    scheduler,
    search,
    search_query,
    serve,
)
from src.cli.commands import test as test_cmd
from src.cli.main import main

_init_db = runtime.init_db
_init_pool = runtime.init_pool


def setup_logging() -> None:
    runtime.setup_logging()


def _run_with_legacy_runtime(handler, args: argparse.Namespace) -> None:
    if runtime.init_db is _init_db and runtime.init_pool is _init_pool:
        handler(args)
        return

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


def cmd_search_query(args: argparse.Namespace) -> None:
    _run_with_legacy_runtime(search_query.run, args)


def cmd_account(args: argparse.Namespace) -> None:
    _run_with_legacy_runtime(account.run, args)


def cmd_scheduler(args: argparse.Namespace) -> None:
    _run_with_legacy_runtime(scheduler.run, args)


def cmd_photo_loader(args: argparse.Namespace) -> None:
    _run_with_legacy_runtime(photo_loader.run, args)


def cmd_test(args: argparse.Namespace) -> None:
    _run_with_legacy_runtime(test_cmd.run, args)


if __name__ == "__main__":
    main()
