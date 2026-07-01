"""Typer leaf commands for the CLI (epic #959 — the argparse→Typer migration).

This module declares every CLI command on the Typer ``app`` from the Wave-0
scaffold (``src/cli/typer_app.py``). Since the Final wave (#1125) removed the
argparse framework, ``app`` is the single source of truth for the CLI surface —
``src/cli/main.py`` simply runs it.

Design:

* **Type-hints are the schema.** Each command declares its flags / arguments as
  Typer ``Option`` / ``Argument`` parameters (identical names, defaults and
  behaviour to the original argparse parser). No ``add_argument``.
* **One async bridge.** Async command bodies funnel through ``run_async`` (one
  ``asyncio.run`` per process); the shared bodies live in ``commands/*.py`` as
  plain ``async def`` ``*_impl`` functions called from here.
* **Startup side effects are per-command.** Each command calls ``apply_startup``
  as its first line (export ``TG_CONFIG_PATH`` / dotenv / logging / data dirs),
  keeping a ``subcommand --help`` side-effect-free — see ``typer_app`` for why
  those must not live in the ``@app.callback()``.
"""

from __future__ import annotations

import typer

from src.cli.commands import account as account_cmd
from src.cli.commands import agent as agent_cmd
from src.cli.commands import analytics as analytics_cmd
from src.cli.commands import channel as channel_cmd
from src.cli.commands import collect as collect_cmd
from src.cli.commands import debug as debug_cmd
from src.cli.commands import dialogs as dialogs_cmd
from src.cli.commands import export as export_cmd
from src.cli.commands import filter as filter_cmd
from src.cli.commands import image as image_cmd
from src.cli.commands import mcp_server as mcp_server_cmd
from src.cli.commands import messages as messages_cmd
from src.cli.commands import notification as notification_cmd
from src.cli.commands import photo_loader as photo_loader_cmd
from src.cli.commands import pipeline as pipeline_cmd
from src.cli.commands import provider as provider_cmd
from src.cli.commands import scheduler as scheduler_cmd
from src.cli.commands import search as search_cmd
from src.cli.commands import search_query as search_query_cmd
from src.cli.commands import serve as serve_cmd
from src.cli.commands import server_control as server_control_cmd
from src.cli.commands import settings as settings_cmd
from src.cli.commands import test as test_cmd
from src.cli.commands import translate as translate_cmd
from src.cli.commands import worker as worker_cmd
from src.cli.commands.account import account_app
from src.cli.commands.agent import agent_app
from src.cli.commands.analytics import analytics_app
from src.cli.commands.channel import channel_app
from src.cli.commands.collect import collect_app
from src.cli.commands.common import (
    _NEG_ID_POSITIONAL,
    AnalyticsGenre,
    AnalyticsUseful,
    ExportFormat,
    GenerationBackend,
    OutputFormat,
    PhotoMode,
    PublishMode,
    SearchMode,
    SinceUnit,
    TriBool,
)
from src.cli.commands.debug import debug_app
from src.cli.commands.dialogs import dialogs_app
from src.cli.commands.export import export_app
from src.cli.commands.filter import filter_app
from src.cli.commands.image import image_app
from src.cli.commands.messages import messages_app
from src.cli.commands.notification import notification_app
from src.cli.commands.photo_loader import photo_loader_app
from src.cli.commands.pipeline import pipeline_app
from src.cli.commands.provider import provider_app
from src.cli.commands.scheduler import scheduler_app
from src.cli.commands.search_query import search_query_app
from src.cli.commands.settings import settings_app
from src.cli.commands.test import test_app
from src.cli.commands.translate import translate_app
from src.cli.typer_app import app, apply_startup, run_async

_COMMAND_MODULE_ALIASES_FOR_TEST_PATCHING = (
    account_cmd,
    agent_cmd,
    analytics_cmd,
    channel_cmd,
    collect_cmd,
    debug_cmd,
    dialogs_cmd,
    export_cmd,
    filter_cmd,
    image_cmd,
    messages_cmd,
    notification_cmd,
    photo_loader_cmd,
    pipeline_cmd,
    provider_cmd,
    scheduler_cmd,
    search_query_cmd,
    settings_cmd,
    test_cmd,
    translate_cmd,
)

_COMMON_TYPER_NAMES_FOR_COMPAT = (
    _NEG_ID_POSITIONAL,
    AnalyticsGenre,
    AnalyticsUseful,
    ExportFormat,
    GenerationBackend,
    OutputFormat,
    PhotoMode,
    PublishMode,
    SearchMode,
    SinceUnit,
    TriBool,
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

app.add_typer(collect_app, name="collect")


# --------------------------------------------------------------------------- #
# search
# --------------------------------------------------------------------------- #


@app.command(context_settings=_NEG_ID_POSITIONAL)
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

app.add_typer(messages_app, name="messages")


# --------------------------------------------------------------------------- #
# debug → logs / memory / timing
# --------------------------------------------------------------------------- #

app.add_typer(debug_app, name="debug")


# --------------------------------------------------------------------------- #
# export → json / csv / rss / telegram
# --------------------------------------------------------------------------- #

app.add_typer(export_app, name="export")


# --------------------------------------------------------------------------- #
# translate → stats / detect / run / message
# --------------------------------------------------------------------------- #

app.add_typer(translate_app, name="translate")


# --------------------------------------------------------------------------- #
# image → generate / models / providers / generated
# --------------------------------------------------------------------------- #

app.add_typer(image_app, name="image")


# --------------------------------------------------------------------------- #
# provider → list / add / delete / probe / refresh / test-all
# --------------------------------------------------------------------------- #

app.add_typer(provider_app, name="provider")


# --------------------------------------------------------------------------- #
# notification → setup / status / delete / test / dry-run / set-account
# --------------------------------------------------------------------------- #

app.add_typer(notification_app, name="notification")


# --------------------------------------------------------------------------- #
# analytics → top / content-types / hourly / summary / daily / pipeline-stats /
#   trending-topics / trending-channels / velocity / peak-hours / calendar /

app.add_typer(analytics_app, name="analytics")


# --------------------------------------------------------------------------- #
# channel → list / add / delete / toggle / collect / stats / refresh-types /
#   refresh-meta / review-list / review-confirm / review-keep / import /

app.add_typer(channel_app, name="channel")


# --------------------------------------------------------------------------- #
# dialogs → list / refresh / resolve / leave / join / topics / cache-clear /
#   cache-status / send / forward / edit-message / delete-message /

app.add_typer(dialogs_app, name="dialogs")


# --------------------------------------------------------------------------- #
# pipeline → list / show / add / dry-run-count / edit / delete / toggle / run /
#   generate / generate-stream / runs / run-show / variants / select-variant /

app.add_typer(pipeline_app, name="pipeline")


# --------------------------------------------------------------------------- #
# search-query → list / get / add / edit / delete / toggle / run / stats
# --------------------------------------------------------------------------- #

app.add_typer(search_query_app, name="search-query")


# --------------------------------------------------------------------------- #
# filter → analyze / apply / reset / precheck / toggle / purge / purge-messages
#          / hard-delete

app.add_typer(filter_app, name="filter")


# --------------------------------------------------------------------------- #
# settings → get / set / info / server-time / agent / filter-criteria
#            / reactions / semantic

app.add_typer(settings_app, name="settings")


# --------------------------------------------------------------------------- #
# scheduler → start / trigger / status / stop / job-toggle / set-interval
#             / task-cancel / clear-pending / queue-pause / queue-resume

app.add_typer(scheduler_app, name="scheduler")


# --------------------------------------------------------------------------- #
# account → list / info / toggle / set-primary / delete / send-code /
#           verify-code / add / flood-status / flood-clear / export-session /

app.add_typer(account_app, name="account")


# --------------------------------------------------------------------------- #
# agent → threads / thread-create / thread-delete / chat / thread-rename /
#         thread-stop / messages / context / test-escaping / test-tools

app.add_typer(agent_app, name="agent")


# --------------------------------------------------------------------------- #
# photo-loader → dialogs / refresh / send / schedule-send / batch-create /
#                publish / batch-list / items / batch-cancel / auto-create / auto-list /

app.add_typer(photo_loader_app, name="photo-loader")


# --------------------------------------------------------------------------- #
# test → all / read / write / telegram / benchmark
# --------------------------------------------------------------------------- #

app.add_typer(test_app, name="test")
