"""Typer leaf commands migrated from argparse (epic #959, Wave 1 — issue #1121).

Wave 1 moves the *super-simple* commands (no nested groups, or a single trivial
sub-command) off the hand-rolled argparse dispatcher and onto the Typer ``app``
from the Wave-0 scaffold (``src/cli/typer_app.py``):

    serve · worker · stop · restart · mcp-server · collect (+ ``collect sample``)
    · search · messages read

Design (the hybrid chosen for Wave 1):

* **Type-hints are the schema.** Each command declares the *same* flags /
  arguments the argparse parser did (identical names, defaults and behaviour),
  expressed as Typer ``Option`` / ``Argument`` parameters. No ``add_argument``.
* **One async bridge.** Async command bodies funnel through ``run_async`` (one
  ``asyncio.run`` per process); the local ``asyncio.run(_run())`` blocks that
  used to live in ``commands/*.py`` are gone — the shared bodies in those
  modules are now plain ``async def`` ``*_impl`` functions called from here.
* **argparse stays as the leaf-coverage source of truth.** The
  ``register()`` declarations in ``parser_domains/*.py`` are intentionally kept:
  ``test_real_telegram_policy.py`` derives the live-CLI manifest from
  ``build_parser()``, so removing them would drop migrated commands from the
  manifest sweep. ``src/cli/main.py`` routes these commands through the Typer
  ``app`` (``dispatch_via_typer``) while every other command keeps the argparse
  path, so the production CLI executes the migrated Typer bodies.

The Final wave (#1125) removes the argparse path entirely.
"""

from __future__ import annotations

import argparse
from enum import Enum
from typing import cast

import click
import typer

# Typer vendors its *own* copy of Click under ``typer._click``, so the exception
# a Typer sub-group raises (``NoArgsIsHelpError`` / ``ClickException``) is NOT the
# same class as the one in the top-level ``click`` package — an ``except
# click.exceptions.ClickException`` would silently miss it. ``dispatch_via_typer``
# must catch both. The vendored module is private; fall back gracefully to the
# public ``click`` types if a future Typer drops it, so the import can never
# crash the CLI.
try:  # pragma: no cover - exercised indirectly via dispatch_via_typer tests
    from typer._click import exceptions as _typer_click_exc

    _CLICK_EXCEPTIONS: tuple[type[BaseException], ...] = (
        click.exceptions.ClickException,
        _typer_click_exc.ClickException,
    )
    _NO_ARGS_HELP_EXCEPTIONS: tuple[type[BaseException], ...] = (
        click.exceptions.NoArgsIsHelpError,
        _typer_click_exc.NoArgsIsHelpError,
    )
except ImportError:  # pragma: no cover - defensive fallback
    _CLICK_EXCEPTIONS = (click.exceptions.ClickException,)
    _NO_ARGS_HELP_EXCEPTIONS = (click.exceptions.NoArgsIsHelpError,)

from src.cli.commands import collect as collect_cmd
from src.cli.commands import mcp_server as mcp_server_cmd
from src.cli.commands import messages as messages_cmd
from src.cli.commands import search as search_cmd
from src.cli.commands import serve as serve_cmd
from src.cli.commands import server_control as server_control_cmd
from src.cli.commands import worker as worker_cmd
from src.cli.typer_app import app, apply_startup, run_async


class SearchMode(str, Enum):
    """``search --mode`` choices — mirrors the argparse ``choices=[…]`` set.

    Subclassing ``str`` keeps the value a plain string for the command bodies
    (which compare against ``"local"`` / ``"telegram"`` / … literals) while
    giving Typer the closed choice set argparse enforced, so an unknown ``--mode``
    is rejected on the Typer path too (not silently treated as ``local``).
    """

    local = "local"
    semantic = "semantic"
    hybrid = "hybrid"
    telegram = "telegram"
    my_chats = "my_chats"
    channel = "channel"


class OutputFormat(str, Enum):
    """``messages read --format`` choices — mirrors the argparse ``choices=[…]``."""

    text = "text"
    json = "json"
    csv = "csv"


#: Argparse ``args.command`` names (dash form for ``mcp-server``) that Wave 1
#: migrated to Typer. ``src/cli/main.py`` routes these through the Typer ``app``
#: via :func:`dispatch_via_typer` instead of the argparse ``commands.X.run``
#: handler; :func:`_argv_from_namespace` rebuilds the Typer argv from the parsed
#: Namespace so the two entry points stay equivalent on the resolved flags.
MIGRATED_COMMANDS: frozenset[str] = frozenset(
    {"serve", "worker", "stop", "restart", "mcp-server", "collect", "search", "messages"}
)


# --------------------------------------------------------------------------- #
# serve / worker
# --------------------------------------------------------------------------- #


@app.command()
def serve(
    ctx: typer.Context,
    web_pass: str | None = typer.Option(None, "--web-pass", help="Web panel password (overrides config)"),
    no_worker: bool = typer.Option(
        False,
        "--no-worker",
        help=(
            "Do not spawn the embedded Telegram worker inside this process. "
            "Use this when you run `python -m src.main worker` in a separate "
            "process / container (Docker, k8s). Without this flag the serve "
            "command runs both the web app and the worker in one process — "
            "clicking 'Collect' in the UI immediately triggers collection."
        ),
    ),
) -> None:
    """Start web server."""
    apply_startup(ctx)
    serve_cmd.serve_web(ctx.obj.config, web_pass=web_pass, no_worker=no_worker)


@app.command()
def worker(ctx: typer.Context) -> None:
    """Start Telegram worker runtime."""
    apply_startup(ctx)
    worker_cmd.serve_worker(ctx.obj.config)


# --------------------------------------------------------------------------- #
# stop / restart
# --------------------------------------------------------------------------- #


@app.command()
def stop(ctx: typer.Context) -> None:
    """Stop web server started by this app."""
    apply_startup(ctx)
    server_control_cmd.stop_web(ctx.obj.config)


@app.command()
def restart(
    ctx: typer.Context,
    web_pass: str | None = typer.Option(None, "--web-pass", help="Web panel password (overrides config)"),
) -> None:
    """Restart web server."""
    apply_startup(ctx)
    server_control_cmd.restart_web(ctx.obj.config, web_pass=web_pass)


# --------------------------------------------------------------------------- #
# mcp-server
# --------------------------------------------------------------------------- #


@app.command("mcp-server")
def mcp_server(
    ctx: typer.Context,
    no_pool: bool = typer.Option(
        False,
        "--no-pool",
        help="Skip Telegram client pool init; pool-dependent tools return an error message",
    ),
) -> None:
    """Expose the agent tool registry as a stdio MCP server (for external agents like Codex)."""
    apply_startup(ctx)
    mcp_server_cmd.serve_mcp(ctx.obj.config, no_pool=no_pool)


# --------------------------------------------------------------------------- #
# collect (+ collect sample)
# --------------------------------------------------------------------------- #

collect_app = typer.Typer(no_args_is_help=False, help="Run one-shot collection")
app.add_typer(collect_app, name="collect")


@collect_app.callback(invoke_without_command=True)
def collect(
    ctx: typer.Context,
    channel_id: int | None = typer.Option(
        None,
        "--channel-id",
        help="Collect single channel by channel_id (incremental by default)",
    ),
    full: bool = typer.Option(
        False,
        "--full",
        help="For --channel-id, explicitly backfill the full channel history",
    ),
) -> None:
    """Run one-shot collection (no sub-command = collect all / single channel)."""
    # The ``sample`` sub-command has its own body; only run the top-level
    # collection when no sub-command was invoked.
    if ctx.invoked_subcommand is not None:
        return
    apply_startup(ctx)
    run_async(collect_cmd.collect_impl(ctx.obj.config, channel_id=channel_id, full=full))


@collect_app.command("sample")
def collect_sample(
    ctx: typer.Context,
    channel_id: int = typer.Argument(..., help="Channel ID (numeric)"),
    limit: int = typer.Option(10, "--limit", help="Number of messages to preview (default: 10)"),
) -> None:
    """Preview last N messages without saving to DB."""
    apply_startup(ctx)
    run_async(collect_cmd.collect_sample_impl(ctx.obj.config, channel_id=channel_id, limit=limit))


# --------------------------------------------------------------------------- #
# search
# --------------------------------------------------------------------------- #


@app.command()
def search(
    ctx: typer.Context,
    query: str = typer.Argument("", help="Search query"),
    limit: int = typer.Option(20, "--limit", help="Max results"),
    mode: SearchMode = typer.Option(
        SearchMode.local,
        "--mode",
        help="Search mode: local, semantic, hybrid, telegram, my_chats, channel",
    ),
    channel_id: int | None = typer.Option(None, "--channel-id", help="Channel ID for --mode=channel"),
    min_length: int | None = typer.Option(None, "--min-length", help="Min message length"),
    max_length: int | None = typer.Option(None, "--max-length", help="Max message length"),
    fts: bool = typer.Option(False, "--fts", help="Use FTS5 boolean syntax"),
    all_channels: bool = typer.Option(
        False, "--all", help="Search all channels including filtered ones"
    ),
    index_now: bool = typer.Option(
        False, "--index-now", help="Run semantic embeddings indexing/backfill before exiting"
    ),
    reset_index: bool = typer.Option(
        False, "--reset-index", help="Drop semantic vector index before --index-now"
    ),
    purge_cache: bool = typer.Option(
        False,
        "--purge-cache",
        help="Delete messages cached by a previous Premium global search for <query> and exit",
    ),
) -> None:
    """Search messages."""
    apply_startup(ctx)
    run_async(
        search_cmd.search_impl(
            ctx.obj.config,
            query=query,
            limit=limit,
            mode=mode.value,
            channel_id=channel_id,
            min_length=min_length,
            max_length=max_length,
            fts=fts,
            include_filtered=all_channels,
            index_now=index_now,
            reset_index=reset_index,
            purge_cache=purge_cache,
        )
    )


# --------------------------------------------------------------------------- #
# messages read
# --------------------------------------------------------------------------- #

messages_app = typer.Typer(no_args_is_help=True, help="Read messages from DB or live Telegram")
app.add_typer(messages_app, name="messages")


@messages_app.command("read")
def messages_read(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Channel pk, channel_id, @username, or dialog ID"),
    limit: int = typer.Option(50, "--limit", help="Max messages (default: 50)"),
    live: bool = typer.Option(False, "--live", help="Read from Telegram instead of DB"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (for --live)"),
    query: str = typer.Option("", "--query", help="Text filter (DB only)"),
    date_from: str | None = typer.Option(None, "--date-from", help="Start date YYYY-MM-DD (DB only)"),
    date_to: str | None = typer.Option(None, "--date-to", help="End date YYYY-MM-DD (DB only)"),
    topic_id: int | None = typer.Option(None, "--topic-id", help="Forum topic ID"),
    offset_id: int | None = typer.Option(
        None, "--offset-id", help="Read messages before this message ID (--live)"
    ),
    include_reaction_users: bool = typer.Option(
        False, "--include-reaction-users", help="Show users who reacted (live mode only)"
    ),
    reaction_users_limit: int = typer.Option(
        20,
        "--reaction-users-limit",
        help="Max reaction users per message for --include-reaction-users (default: 20)",
    ),
    output_format: OutputFormat = typer.Option(
        OutputFormat.text, "--format", help="Output format (default: text)"
    ),
) -> None:
    """Read messages from a channel/dialog."""
    apply_startup(ctx)
    run_async(
        messages_cmd.messages_read_impl(
            ctx.obj.config,
            identifier=identifier,
            limit=limit,
            live=live,
            phone=phone,
            query=query,
            date_from=date_from,
            date_to=date_to,
            topic_id=topic_id,
            offset_id=offset_id,
            include_reaction_users=include_reaction_users,
            reaction_users_limit=reaction_users_limit,
            output_format=output_format.value,
        )
    )


# --------------------------------------------------------------------------- #
# argparse → Typer delegation
# --------------------------------------------------------------------------- #


def _argv_from_namespace(args: argparse.Namespace) -> list[str]:
    """Reconstruct the Typer argv for a migrated command from parsed argparse args.

    ``src/cli/main.py`` has already parsed the process argv with the argparse
    parser; we rebuild the minimal token list Typer needs so the Typer command
    body runs with exactly the resolved flags. The global ``--config`` is passed
    as a root option so ``main_callback`` records it on ``ctx.obj`` (matching the
    argparse global). Only flags that differ from their defaults are emitted, so
    store_true flags stay absent unless set.
    """
    command = args.command
    argv: list[str] = ["--config", args.config]

    if command == "serve":
        argv.append("serve")
        if getattr(args, "web_pass", None):
            argv += ["--web-pass", args.web_pass]
        if getattr(args, "no_worker", False):
            argv.append("--no-worker")
    elif command == "worker":
        argv.append("worker")
    elif command == "stop":
        argv.append("stop")
    elif command == "restart":
        argv.append("restart")
        if getattr(args, "web_pass", None):
            argv += ["--web-pass", args.web_pass]
    elif command == "mcp-server":
        argv.append("mcp-server")
        if getattr(args, "no_pool", False):
            argv.append("--no-pool")
    elif command == "collect":
        argv.append("collect")
        if getattr(args, "collect_action", None) == "sample":
            argv.append("sample")
            # Options before ``--``; the positional channel_id after it, so a
            # negative id (e.g. ``-100123``) is never mistaken for an option —
            # argparse accepts negative-number positionals, Click does not.
            if getattr(args, "limit", 10) != 10:
                argv += ["--limit", str(args.limit)]
            argv += ["--", str(args.channel_id)]
        else:
            if getattr(args, "channel_id", None) is not None:
                argv += ["--channel-id", str(args.channel_id)]
            if getattr(args, "full", False):
                argv.append("--full")
    elif command == "search":
        argv.append("search")
        argv += _search_argv(args)
    elif command == "messages":
        argv.append("messages")
        if getattr(args, "messages_action", None) == "read":
            argv += _messages_read_argv(args)
    return argv


def _search_argv(args: argparse.Namespace) -> list[str]:
    """argv tail for ``search`` — non-default flags, then ``--`` + positional query.

    The query is emitted after a ``--`` separator so a query starting with ``-``
    (which argparse accepts as the positional) is not parsed as an option by
    Click. Always emit ``--`` so an empty query stays an explicit empty argument.
    """
    tail: list[str] = []
    if getattr(args, "limit", 20) != 20:
        tail += ["--limit", str(args.limit)]
    if getattr(args, "mode", "local") != "local":
        tail += ["--mode", args.mode]
    if getattr(args, "channel_id", None) is not None:
        tail += ["--channel-id", str(args.channel_id)]
    if getattr(args, "min_length", None) is not None:
        tail += ["--min-length", str(args.min_length)]
    if getattr(args, "max_length", None) is not None:
        tail += ["--max-length", str(args.max_length)]
    if getattr(args, "fts", False):
        tail.append("--fts")
    if getattr(args, "all", False):
        tail.append("--all")
    if getattr(args, "index_now", False):
        tail.append("--index-now")
    if getattr(args, "reset_index", False):
        tail.append("--reset-index")
    if getattr(args, "purge_cache", False):
        tail.append("--purge-cache")
    tail += ["--", getattr(args, "query", "") or ""]
    return tail


def _messages_read_argv(args: argparse.Namespace) -> list[str]:
    """argv tail for ``messages read`` — flags, then ``--`` + positional identifier.

    The identifier follows a ``--`` separator so a negative channel id passed as
    the identifier survives Click's option parsing (see the collect-sample note).
    """
    tail: list[str] = ["read"]
    if getattr(args, "limit", 50) != 50:
        tail += ["--limit", str(args.limit)]
    if getattr(args, "live", False):
        tail.append("--live")
    if getattr(args, "phone", None):
        tail += ["--phone", args.phone]
    if getattr(args, "query", ""):
        tail += ["--query", args.query]
    if getattr(args, "date_from", None):
        tail += ["--date-from", args.date_from]
    if getattr(args, "date_to", None):
        tail += ["--date-to", args.date_to]
    if getattr(args, "topic_id", None) is not None:
        tail += ["--topic-id", str(args.topic_id)]
    if getattr(args, "offset_id", None) is not None:
        tail += ["--offset-id", str(args.offset_id)]
    if getattr(args, "include_reaction_users", False):
        tail.append("--include-reaction-users")
    if getattr(args, "reaction_users_limit", 20) != 20:
        tail += ["--reaction-users-limit", str(args.reaction_users_limit)]
    if getattr(args, "output_format", "text") != "text":
        tail += ["--format", args.output_format]
    tail += ["--", args.identifier]
    return tail


def dispatch_via_typer(args: argparse.Namespace) -> None:
    """Execute a migrated command through the Typer ``app``.

    Called by ``src/cli/main.py`` for commands in :data:`MIGRATED_COMMANDS`.
    Runs the Typer app in non-standalone mode so a command's own ``SystemExit`` /
    a Typer ``Exit`` propagate exactly as they did under argparse (the argparse
    dispatcher never swallowed them either).

    Non-standalone mode also makes Click *re-raise* ``ClickException`` instead of
    rendering it, so we handle those here to preserve the argparse behaviour:

    * A bare command group with ``no_args_is_help`` (``messages`` with no ``read``
      sub-command) raises :class:`click.exceptions.NoArgsIsHelpError`. Argparse's
      old ``sub_attr`` fallback printed that group's ``--help`` and exited **0**;
      we reproduce that — render the help, exit 0 — so the user sees usage, not a
      ``NoArgsIsHelpError`` traceback with exit 1.
    * Any other usage error (unknown option, bad value) renders normally and
      exits with its own (non-zero) code, matching argparse's exit-2 on misuse.
    """
    argv = _argv_from_namespace(args)
    try:
        app(args=argv, standalone_mode=False)
    except _NO_ARGS_HELP_EXCEPTIONS as exc:
        # Bare group → show help and exit cleanly (argparse parity: exit 0).
        # Must precede the generic ClickException arm — NoArgsIsHelpError is a
        # subclass of it but argparse exited 0 here, not 2.
        # ``exc`` is a (vendored or stdlib) ClickException; both share the
        # ``.show()`` / ``.exit_code`` interface — cast for the type checker.
        cast("click.ClickException", exc).show()
        raise SystemExit(0) from None
    except _CLICK_EXCEPTIONS as exc:
        click_exc = cast("click.ClickException", exc)
        click_exc.show()
        raise SystemExit(click_exc.exit_code) from None
