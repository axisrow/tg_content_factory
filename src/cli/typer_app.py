"""Typer scaffold for the CLI (epic #959, Wave 0 — issue #1120).

This module is the *foundation* for migrating the argparse CLI to Typer. It
introduces nothing user-facing on its own: no leaf commands are registered yet,
so ``python -m src.main`` keeps routing through the argparse path in
``src/cli/main.py``. Waves 1–4 attach their migrated commands to ``app`` here.

Three pieces make up the scaffold:

* ``app`` — the single :class:`typer.Typer` application that replaces the
  hand-rolled ``subparsers.add_parser`` + dict-dispatcher in ``main()``.
* ``main_callback`` — the ``@app.callback()`` that owns the global options
  (``--version`` / ``--config``) and reproduces the side effects of the current
  ``src/cli/main.py::main()`` (lines 37–48) *one-to-one*: it exports
  ``TG_CONFIG_PATH`` (abspath), loads the ``.env`` next to the config, sets up
  logging and ensures the data directories exist.
* ``run_async`` — the async bridge. Migrated command functions stay plain
  ``def`` (so Typer can introspect their type-hints) and call
  ``run_async(_impl(...))`` for the async body. Exactly one ``asyncio.run`` per
  process, so Wave 1–4 commands never nest event loops.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Coroutine
from typing import Any, TypeVar

import typer

from src import __version__
from src.cli.dotenv import load_cli_dotenv
from src.cli.runtime import ensure_data_dirs, setup_logging

_T = TypeVar("_T")

#: Default config path — mirrors the argparse ``--config`` default in
#: ``src/cli/parser.py`` so the two entry points resolve identically.
DEFAULT_CONFIG = "config.yaml"

app = typer.Typer(
    no_args_is_help=True,
    add_completion=True,
    help="TG Post Search",
)


def run_async(coro: Coroutine[Any, Any, _T]) -> _T:
    """Run *coro* to completion with exactly one ``asyncio.run`` per process.

    The async bridge for the Typer CLI. Migrated commands keep a synchronous
    ``def`` signature (Typer derives the option/argument schema from the
    type-hints) and delegate their async body through this helper::

        @app.command()
        def collect(channel_id: int | None = None) -> None:
            run_async(_collect_impl(channel_id))

    Centralising the ``asyncio.run`` call here keeps the "one event loop per
    process" invariant that the argparse handlers rely on — no command nests
    a second loop, and the pattern stays uniform across Waves 1–4.
    """
    return asyncio.run(coro)


def _version_callback(value: bool) -> None:
    """Eager ``--version`` handler: print and exit before any side effects.

    Matches argparse ``action="version"`` (``%(prog)s <version>``) which exits
    *before* the dispatcher runs. Being eager means ``--version`` short-circuits
    the callback body, so logging/data-dir side effects never fire for it.
    """
    if value:
        typer.echo(f"src {__version__}")
        raise typer.Exit()


@app.callback()
def main_callback(
    ctx: typer.Context,
    config: str = typer.Option(
        DEFAULT_CONFIG,
        "--config",
        help="Path to config file",
    ),
    version: bool = typer.Option(
        False,
        "--version",
        callback=_version_callback,
        is_eager=True,
        help="Show version and exit",
    ),
) -> None:
    """Global options + startup side effects, ported 1:1 from ``main()``.

    Reproduces ``src/cli/main.py::main()`` lines 37–48: export the resolved
    config path as ``TG_CONFIG_PATH`` (abspath, so subprocess-spawning backends
    such as ``CodexSdkBackend`` inherit the right config / DB), load the ``.env``
    next to the config, set up logging and ensure the data directories exist.

    Skipped when Click is doing resilient parsing (``--help`` / shell
    completion) — there is no command to run, so the side effects would be
    spurious. ``--version`` is handled eagerly by ``_version_callback`` and
    never reaches this body.
    """
    # ``--help`` and completion run the callback with resilient parsing; there
    # is no subcommand to execute, so skip the startup side effects entirely.
    if ctx.resilient_parsing:
        return

    # Export the resolved config path so subprocess-spawning backends inherit it.
    # CodexSdkBackend spawns `python -m src.main --config <path> mcp-server`; it
    # learns <path> only via TG_CONFIG_PATH (AppConfig doesn't carry its source).
    # Without this, a non-default `--config /srv/prod.yaml` would silently spawn
    # the MCP server against the default config.yaml / data/tg_search.db — the
    # wrong DB for write-capable tool calls. abspath so a differing subprocess
    # CWD still resolves it.
    os.environ["TG_CONFIG_PATH"] = os.path.abspath(config)

    load_cli_dotenv(config)
    setup_logging()
    ensure_data_dirs()
