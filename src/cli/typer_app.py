"""Typer scaffold for the CLI (epic #959, Wave 0 — issue #1120).

This module is the *foundation* for migrating the argparse CLI to Typer. It
introduces nothing user-facing on its own: no leaf commands are registered yet,
so ``python -m src.main`` keeps routing through the argparse path in
``src/cli/main.py``. Waves 1–4 attach their migrated commands to ``app`` here.

Four pieces make up the scaffold:

* ``app`` — the single :class:`typer.Typer` application that replaces the
  hand-rolled ``subparsers.add_parser`` + dict-dispatcher in ``main()``.
* ``main_callback`` — the ``@app.callback()`` that owns the global options
  (``--version`` / ``--config``). It only *records* the resolved config on
  ``ctx.obj``; it deliberately runs **no** startup side effects (see
  ``apply_startup`` for why).
* ``apply_startup`` — performs the startup side effects of the current
  ``src/cli/main.py::main()`` (lines 37–48) *one-to-one*: export
  ``TG_CONFIG_PATH`` (abspath), load the ``.env`` next to the config, set up
  logging and ensure the data dirs exist. Migrated commands call it as their
  first line. Keeping it out of the callback is what makes the Typer path
  argparse-identical for ``--help``: in argparse those side effects run *after*
  ``parse_args()``, which short-circuits on any ``--help`` (root **or
  subcommand**); a Typer ``@app.callback()`` body, by contrast, still runs when
  a *subcommand's* ``--help`` is requested, so putting the side effects there
  would spuriously touch the env / filesystem on ``mycmd --help``.
* ``run_async`` — the async bridge. Migrated command functions stay plain
  ``def`` (so Typer can introspect their type-hints) and call
  ``run_async(_impl(...))`` for the async body. Exactly one ``asyncio.run`` per
  process, so Wave 1–4 commands never nest event loops.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Coroutine
from dataclasses import dataclass
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


@dataclass
class CliState:
    """Per-invocation CLI state carried on ``ctx.obj``.

    ``config`` is the raw ``--config`` value (not the abspath) so commands can
    pass it through to ``apply_startup`` exactly as ``main()`` does. ``started``
    makes ``apply_startup`` idempotent — a second call within the same process
    is a no-op, so chained internal command calls don't re-run logging setup.
    """

    config: str = DEFAULT_CONFIG
    started: bool = False


def run_async(coro: Coroutine[Any, Any, _T]) -> _T:
    """Run *coro* to completion with exactly one ``asyncio.run`` per process.

    The async bridge for the Typer CLI. Migrated commands keep a synchronous
    ``def`` signature (Typer derives the option/argument schema from the
    type-hints) and delegate their async body through this helper::

        @app.command()
        def collect(ctx: typer.Context, channel_id: int | None = None) -> None:
            apply_startup(ctx)
            run_async(_collect_impl(channel_id))

    Centralising the ``asyncio.run`` call here keeps the "one event loop per
    process" invariant that the argparse handlers rely on — no command nests
    a second loop, and the pattern stays uniform across Waves 1–4.
    """
    return asyncio.run(coro)


def apply_startup(ctx: typer.Context) -> None:
    """Run the global startup side effects, ported 1:1 from ``main()``.

    Reproduces ``src/cli/main.py::main()`` lines 37–48: export the resolved
    config path as ``TG_CONFIG_PATH`` (abspath, so subprocess-spawning backends
    such as ``CodexSdkBackend`` inherit the right config / DB), load the ``.env``
    next to the config, set up logging and ensure the data directories exist.

    Migrated commands call this as their first line. It lives here — on the
    command execution path — rather than in ``main_callback`` so that a
    ``subcommand --help`` invocation never triggers it: argparse short-circuits
    subcommand help during ``parse_args()`` *before* these side effects run, and
    a Typer callback body does not, so co-locating them with the command keeps
    the two entry points behaviourally identical. Idempotent via
    ``CliState.started`` so a second call in the same process is a no-op.
    """
    state = ctx.ensure_object(CliState)
    if state.started:
        return
    state.started = True

    # Export the resolved config path so subprocess-spawning backends inherit it.
    # CodexSdkBackend spawns `python -m src.main --config <path> mcp-server`; it
    # learns <path> only via TG_CONFIG_PATH (AppConfig doesn't carry its source).
    # Without this, a non-default `--config /srv/prod.yaml` would silently spawn
    # the MCP server against the default config.yaml / data/tg_search.db — the
    # wrong DB for write-capable tool calls. abspath so a differing subprocess
    # CWD still resolves it.
    os.environ["TG_CONFIG_PATH"] = os.path.abspath(state.config)

    load_cli_dotenv(state.config)
    setup_logging()
    ensure_data_dirs()


def _version_callback(value: bool) -> None:
    """Eager ``--version`` handler: print and exit before any command runs.

    Mirrors argparse ``action="version"``, which exits *before* the dispatcher
    runs. The version string itself (``src.__version__``) is the hard parity
    invariant from #1120; being eager means ``--version`` short-circuits the
    whole CLI, so no command body / startup side effect ever fires for it.
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
    """Record global options on ``ctx.obj``; run no side effects here.

    The callback only resolves ``--config`` (and lets the eager
    ``--version`` callback handle versioning). The actual startup work is
    deferred to ``apply_startup``, invoked by each command — see that function
    and the module docstring for why the side effects must not live here (a
    ``subcommand --help`` request still executes this callback body, but must
    not touch the env / filesystem, to stay argparse-identical).
    """
    state = ctx.ensure_object(CliState)
    state.config = config
