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
from src.cli.commands.image import image_app
from src.cli.commands.messages import messages_app
from src.cli.commands.notification import notification_app
from src.cli.commands.pipeline import pipeline_app
from src.cli.commands.provider import provider_app
from src.cli.commands.translate import translate_app
from src.cli.typer_app import app, apply_startup, run_async
from src.filters.criteria import DEFAULT_QUICK_SAMPLE_SIZE

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

search_query_app = typer.Typer(no_args_is_help=True, help="Search query management")
app.add_typer(search_query_app, name="search-query")


@search_query_app.command("list")
def search_query_list(ctx: typer.Context) -> None:
    """List search queries."""
    apply_startup(ctx)
    run_async(search_query_cmd.list_impl(ctx.obj.config))


@search_query_app.command("get")
def search_query_get(
    ctx: typer.Context,
    query_id: int = typer.Argument(..., metavar="id", help="Search query id"),
) -> None:
    """Show search query details."""
    apply_startup(ctx)
    run_async(search_query_cmd.get_impl(ctx.obj.config, query_id=query_id))


@search_query_app.command("add", context_settings=_NEG_ID_POSITIONAL)
def search_query_add(
    ctx: typer.Context,
    query: str = typer.Argument(..., help="FTS5 search query text"),
    interval: int = typer.Option(60, "--interval", help="Run interval in minutes"),
    regex: bool = typer.Option(False, "--regex", help="Use regex matching"),
    fts: bool = typer.Option(False, "--fts", help="Use FTS5 boolean syntax (no quoting)"),
    notify: bool = typer.Option(False, "--notify", help="Notify on collect"),
    no_track_stats: bool = typer.Option(
        False, "--no-track-stats", help="Disable stat tracking (default: tracking on)"
    ),
    exclude_patterns: str = typer.Option(
        "", "--exclude-patterns", help="Exclude patterns, one per line (use \\n)"
    ),
    max_length: int | None = typer.Option(None, "--max-length", help="Max message text length"),
    chats: str = typer.Option("", "--chats", help="Chat filter: IDs, usernames or t.me links"),
) -> None:
    """Add search query."""
    apply_startup(ctx)
    # argparse declares ONLY ``--no-track-stats`` (store_false, default True) on
    # ``add`` — no ``--track-stats`` flag. Mirror that exactly so the Typer surface
    # is not one flag wider than argparse (#1123 review).
    run_async(
        search_query_cmd.add_impl(
            ctx.obj.config,
            query=query,
            interval=interval,
            is_regex=regex,
            is_fts=fts,
            notify=notify,
            track_stats=not no_track_stats,
            exclude_patterns=exclude_patterns,
            max_length=max_length,
            chats=chats,
        )
    )


@search_query_app.command("edit")
def search_query_edit(
    ctx: typer.Context,
    query_id: int = typer.Argument(..., metavar="id", help="Search query id"),
    query: str | None = typer.Option(None, "--query", help="New query text"),
    interval: int | None = typer.Option(None, "--interval", help="New interval in minutes"),
    regex: bool | None = typer.Option(None, "--regex/--no-regex", help="Toggle regex matching"),
    fts: bool | None = typer.Option(None, "--fts/--no-fts", help="Toggle FTS5 syntax"),
    notify: bool | None = typer.Option(None, "--notify/--no-notify", help="Toggle notify on collect"),
    track_stats: bool | None = typer.Option(
        None, "--track-stats/--no-track-stats", help="Toggle stat tracking"
    ),
    exclude_patterns: str | None = typer.Option(
        None, "--exclude-patterns", help="Exclude patterns (use \\n)"
    ),
    max_length: int | None = typer.Option(None, "--max-length", help="Max message text length"),
    clear_max_length: bool = typer.Option(
        False, "--no-max-length", help="Clear the max-length limit"
    ),
    chats: str | None = typer.Option(
        None, "--chats", help="Chat filter: IDs, usernames or t.me links"
    ),
    clear_chats: bool = typer.Option(False, "--clear-chats", help="Clear the chat filter"),
) -> None:
    """Edit search query; unset flags keep their current value."""
    apply_startup(ctx)
    # ``--no-max-length`` maps to the sentinel -1 the impl treats as "clear";
    # ``--clear-chats`` maps to "" — mirrors the argparse store_const declarations.
    resolved_max_length = -1 if clear_max_length else max_length
    resolved_chats = "" if clear_chats else chats
    run_async(
        search_query_cmd.edit_impl(
            ctx.obj.config,
            query_id=query_id,
            query=query,
            interval=interval,
            is_regex=regex,
            is_fts=fts,
            notify=notify,
            track_stats=track_stats,
            exclude_patterns=exclude_patterns,
            max_length=resolved_max_length,
            chats=resolved_chats,
        )
    )


@search_query_app.command("delete")
def search_query_delete(
    ctx: typer.Context,
    query_id: int = typer.Argument(..., metavar="id", help="Search query id"),
) -> None:
    """Delete search query."""
    apply_startup(ctx)
    run_async(search_query_cmd.delete_impl(ctx.obj.config, query_id=query_id))


@search_query_app.command("toggle")
def search_query_toggle(
    ctx: typer.Context,
    query_id: int = typer.Argument(..., metavar="id", help="Search query id"),
) -> None:
    """Toggle search query active state."""
    apply_startup(ctx)
    run_async(search_query_cmd.toggle_impl(ctx.obj.config, query_id=query_id))


@search_query_app.command("run")
def search_query_run(
    ctx: typer.Context,
    query_id: int = typer.Argument(..., metavar="id", help="Search query id"),
) -> None:
    """Run a search query once and show matches."""
    apply_startup(ctx)
    run_async(search_query_cmd.run_impl(ctx.obj.config, query_id=query_id))


@search_query_app.command("stats")
def search_query_stats(
    ctx: typer.Context,
    query_id: int = typer.Argument(..., metavar="id", help="Search query id"),
    days: int = typer.Option(30, "--days", help="Number of days"),
) -> None:
    """Show daily stats for a search query."""
    apply_startup(ctx)
    run_async(search_query_cmd.stats_impl(ctx.obj.config, query_id=query_id, days=days))


# --------------------------------------------------------------------------- #
# filter → analyze / apply / reset / precheck / toggle / purge / purge-messages
#          / hard-delete
# --------------------------------------------------------------------------- #

filter_app = typer.Typer(no_args_is_help=True, help="Channel content filter")
app.add_typer(filter_app, name="filter")


@filter_app.command("analyze")
def filter_analyze(
    ctx: typer.Context,
    quick: bool = typer.Option(
        False,
        "--quick",
        help="Sample the last N messages/channel + skip cross-dupe analysis (seconds on large DBs)",
    ),
    sample_size: int = typer.Option(
        DEFAULT_QUICK_SAMPLE_SIZE,
        "--sample-size",
        help=(
            f"Messages/channel to sample in --quick mode "
            f"(default: {DEFAULT_QUICK_SAMPLE_SIZE}; ignored without --quick)"
        ),
    ),
) -> None:
    """Analyze channels and show report."""
    apply_startup(ctx)
    run_async(filter_cmd.analyze_impl(ctx.obj.config, quick=quick, sample_size=sample_size))


@filter_app.command("apply")
def filter_apply(ctx: typer.Context) -> None:
    """Analyze and mark filtered channels."""
    apply_startup(ctx)
    run_async(filter_cmd.apply_impl(ctx.obj.config))


@filter_app.command("reset")
def filter_reset(
    ctx: typer.Context,
    pks: str | None = typer.Option(None, "--pks", help="Comma-separated PKs (default: all)"),
) -> None:
    """Reset channel filter flag."""
    apply_startup(ctx)
    run_async(filter_cmd.reset_impl(ctx.obj.config, pks=pks))


@filter_app.command("precheck")
def filter_precheck(ctx: typer.Context) -> None:
    """Apply pre-filter by subscriber ratio (no Telegram needed)."""
    apply_startup(ctx)
    run_async(filter_cmd.precheck_impl(ctx.obj.config))


@filter_app.command("toggle")
def filter_toggle(
    ctx: typer.Context,
    pk: int = typer.Argument(..., help="Channel primary key"),
) -> None:
    """Toggle filter for a single channel."""
    apply_startup(ctx)
    run_async(filter_cmd.toggle_impl(ctx.obj.config, pk=pk))


@filter_app.command("purge")
def filter_purge(
    ctx: typer.Context,
    pks: str | None = typer.Option(None, "--pks", help="Comma-separated PKs (default: all)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Purge messages from filtered channels."""
    apply_startup(ctx)
    run_async(filter_cmd.purge_impl(ctx.obj.config, pks=pks, yes=yes))


@filter_app.command("purge-messages")
def filter_purge_messages(
    ctx: typer.Context,
    channel_id: int = typer.Option(..., "--channel-id", help="Channel ID whose messages to delete"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Delete messages for a specific channel from DB."""
    apply_startup(ctx)
    run_async(filter_cmd.purge_messages_impl(ctx.obj.config, channel_id=channel_id, yes=yes))


@filter_app.command("hard-delete")
def filter_hard_delete(
    ctx: typer.Context,
    pks: str | None = typer.Option(None, "--pks", help="Comma-separated PKs (default: all)"),
    yes: bool = typer.Option(False, "--yes", help="Skip confirmation prompt"),
) -> None:
    """Hard-delete filtered channels from DB (dev/testing)."""
    apply_startup(ctx)
    run_async(filter_cmd.hard_delete_impl(ctx.obj.config, pks=pks, yes=yes))


# --------------------------------------------------------------------------- #
# settings → get / set / info / server-time / agent / filter-criteria
#            / reactions / semantic
# --------------------------------------------------------------------------- #

settings_app = typer.Typer(
    invoke_without_command=True, help="System settings management"
)
app.add_typer(settings_app, name="settings")


@settings_app.callback()
def settings_main(ctx: typer.Context) -> None:
    """Bare ``settings`` (no sub-command) runs ``get`` — argparse parity (#1123 review).

    The legacy dispatcher defaulted ``settings_action`` to ``get`` and listed all
    settings; preserve that on the direct Typer surface, not just the argparse
    bridge. With a sub-command this is a no-op and the sub-command runs normally.
    """
    if ctx.invoked_subcommand is None:
        apply_startup(ctx)
        run_async(settings_cmd.get_impl(ctx.obj.config, key=None))


@settings_app.command("get")
def settings_get(
    ctx: typer.Context,
    key: str | None = typer.Option(None, "--key", help="Specific setting key (default: show all)"),
) -> None:
    """Show settings."""
    apply_startup(ctx)
    run_async(settings_cmd.get_impl(ctx.obj.config, key=key))


@settings_app.command("set")
def settings_set(
    ctx: typer.Context,
    key: str = typer.Argument(..., help="Setting key"),
    value: str = typer.Argument(..., help="Setting value"),
) -> None:
    """Set a setting value."""
    apply_startup(ctx)
    run_async(settings_cmd.set_impl(ctx.obj.config, key=key, value=value))


@settings_app.command("info")
def settings_info(ctx: typer.Context) -> None:
    """Show system diagnostics."""
    apply_startup(ctx)
    run_async(settings_cmd.info_impl(ctx.obj.config))


@settings_app.command("server-time")
def settings_server_time(ctx: typer.Context) -> None:
    """Show current server time (UTC)."""
    apply_startup(ctx)
    run_async(settings_cmd.server_time_impl(ctx.obj.config))


@settings_app.command("agent")
def settings_agent(
    ctx: typer.Context,
    backend: str | None = typer.Option(
        None, "--backend", help="Agent backend override (auto, claude, deepagents, codex, adk)"
    ),
    prompt_template: str | None = typer.Option(
        None, "--prompt-template", help="Default prompt template"
    ),
) -> None:
    """Configure agent backend and defaults."""
    apply_startup(ctx)
    run_async(
        settings_cmd.agent_impl(ctx.obj.config, backend=backend, prompt_template=prompt_template)
    )


@settings_app.command("filter-criteria")
def settings_filter_criteria(
    ctx: typer.Context,
    min_uniqueness: float | None = typer.Option(None, "--min-uniqueness"),
    min_sub_ratio: float | None = typer.Option(None, "--min-sub-ratio"),
    max_cross_dupe: float | None = typer.Option(None, "--max-cross-dupe"),
    min_cyrillic: float | None = typer.Option(None, "--min-cyrillic"),
) -> None:
    """Configure filter thresholds."""
    apply_startup(ctx)
    run_async(
        settings_cmd.filter_criteria_impl(
            ctx.obj.config,
            min_uniqueness=min_uniqueness,
            min_sub_ratio=min_sub_ratio,
            max_cross_dupe=max_cross_dupe,
            min_cyrillic=min_cyrillic,
        )
    )


@settings_app.command("reactions")
def settings_reactions(
    ctx: typer.Context,
    min_interval: int | None = typer.Option(
        None,
        "--min-interval",
        help="Minimum seconds between reactions per account (clamped to 1–300; default 30)",
    ),
) -> None:
    """Configure reaction sending cadence."""
    apply_startup(ctx)
    run_async(settings_cmd.reactions_impl(ctx.obj.config, min_interval=min_interval))


@settings_app.command("semantic")
def settings_semantic(
    ctx: typer.Context,
    provider: str | None = typer.Option(None, "--provider", help="Embedding provider"),
    model: str | None = typer.Option(None, "--model", help="Embedding model"),
    api_key: str | None = typer.Option(None, "--api-key", help="Embedding API key"),
) -> None:
    """Configure semantic search."""
    apply_startup(ctx)
    run_async(
        settings_cmd.semantic_impl(
            ctx.obj.config, provider=provider, model=model, api_key=api_key
        )
    )


# --------------------------------------------------------------------------- #
# scheduler → start / trigger / status / stop / job-toggle / set-interval
#             / task-cancel / clear-pending / queue-pause / queue-resume
# --------------------------------------------------------------------------- #

scheduler_app = typer.Typer(no_args_is_help=True, help="Scheduler control")
app.add_typer(scheduler_app, name="scheduler")


@scheduler_app.command("start")
def scheduler_start(ctx: typer.Context) -> None:
    """Start scheduler (foreground)."""
    apply_startup(ctx)
    run_async(scheduler_cmd.start_impl(ctx.obj.config))


@scheduler_app.command("trigger")
def scheduler_trigger(ctx: typer.Context) -> None:
    """Trigger one-shot collection."""
    apply_startup(ctx)
    run_async(scheduler_cmd.trigger_impl(ctx.obj.config))


@scheduler_app.command("status")
def scheduler_status(ctx: typer.Context) -> None:
    """Show scheduler configuration and status."""
    apply_startup(ctx)
    run_async(scheduler_cmd.status_impl(ctx.obj.config))


@scheduler_app.command("stop")
def scheduler_stop(ctx: typer.Context) -> None:
    """Disable scheduler autostart."""
    apply_startup(ctx)
    run_async(scheduler_cmd.stop_impl(ctx.obj.config))


@scheduler_app.command("job-toggle")
def scheduler_job_toggle(
    ctx: typer.Context,
    job_id: str = typer.Argument(..., help="Job identifier (e.g. collect_all, sq_1)"),
) -> None:
    """Toggle scheduler job enabled/disabled."""
    apply_startup(ctx)
    run_async(scheduler_cmd.job_toggle_impl(ctx.obj.config, job_id=job_id))


@scheduler_app.command("set-interval")
def scheduler_set_interval(
    ctx: typer.Context,
    job_id: str = typer.Argument(..., help="Job identifier"),
    minutes: int = typer.Argument(..., help="Interval in minutes (1-1440)"),
) -> None:
    """Set scheduler job interval."""
    apply_startup(ctx)
    run_async(scheduler_cmd.set_interval_impl(ctx.obj.config, job_id=job_id, minutes=minutes))


@scheduler_app.command("task-cancel")
def scheduler_task_cancel(
    ctx: typer.Context,
    task_id: int = typer.Argument(..., help="Task ID to cancel"),
) -> None:
    """Cancel a collection task."""
    apply_startup(ctx)
    run_async(scheduler_cmd.task_cancel_impl(ctx.obj.config, task_id=task_id))


@scheduler_app.command("clear-pending")
def scheduler_clear_pending(ctx: typer.Context) -> None:
    """Clear all pending collection tasks."""
    apply_startup(ctx)
    run_async(scheduler_cmd.clear_pending_impl(ctx.obj.config))


@scheduler_app.command("queue-pause")
def scheduler_queue_pause(ctx: typer.Context) -> None:
    """Pause the collection queue (queued tasks stay pending)."""
    apply_startup(ctx)
    run_async(scheduler_cmd.queue_pause_impl(ctx.obj.config))


@scheduler_app.command("queue-resume")
def scheduler_queue_resume(ctx: typer.Context) -> None:
    """Resume the collection queue."""
    apply_startup(ctx)
    run_async(scheduler_cmd.queue_resume_impl(ctx.obj.config))


# --------------------------------------------------------------------------- #
# account → list / info / toggle / set-primary / delete / send-code /
#           verify-code / add / flood-status / flood-clear / export-session /
#           import  (export-session & import are the SSO secret-handling ops, #828)
# --------------------------------------------------------------------------- #

account_app = typer.Typer(no_args_is_help=True, help="Account management")
app.add_typer(account_app, name="account")


@account_app.command("list")
def account_list(ctx: typer.Context) -> None:
    """List accounts."""
    apply_startup(ctx)
    run_async(account_cmd.list_impl(ctx.obj.config))


@account_app.command("info")
def account_info(
    ctx: typer.Context,
    phone: str | None = typer.Option(None, "--phone", help="Filter by phone number"),
) -> None:
    """Show profile info for connected accounts."""
    apply_startup(ctx)
    run_async(account_cmd.info_impl(ctx.obj.config, phone=phone))


@account_app.command("toggle")
def account_toggle(
    ctx: typer.Context,
    account_id: int = typer.Argument(..., metavar="id", help="Account id"),
) -> None:
    """Toggle account active state."""
    apply_startup(ctx)
    run_async(account_cmd.toggle_impl(ctx.obj.config, account_id=account_id))


@account_app.command("set-primary")
def account_set_primary(
    ctx: typer.Context,
    account_id: int = typer.Argument(..., metavar="id", help="Account id"),
) -> None:
    """Make account the primary one."""
    apply_startup(ctx)
    run_async(account_cmd.set_primary_impl(ctx.obj.config, account_id=account_id))


@account_app.command("delete")
def account_delete(
    ctx: typer.Context,
    account_id: int = typer.Argument(..., metavar="id", help="Account id"),
    notify_to: str | None = typer.Option(
        None,
        "--notify-to",
        help="Phone to reassign notifications to if deleting the notification account",
    ),
) -> None:
    """Delete account."""
    apply_startup(ctx)
    run_async(account_cmd.delete_impl(ctx.obj.config, account_id=account_id, notify_to=notify_to))


@account_app.command("send-code")
def account_send_code(
    ctx: typer.Context,
    phone: str = typer.Option(..., "--phone", help="Phone number with country code"),
    api_id: int | None = typer.Option(
        None, "--api-id", help="Telegram API ID (uses stored if omitted)"
    ),
    api_hash: str | None = typer.Option(
        None, "--api-hash", help="Telegram API hash (uses stored if omitted)"
    ),
) -> None:
    """Send Telegram auth code to phone."""
    apply_startup(ctx)
    run_async(account_cmd.send_code_impl(ctx.obj.config, phone=phone, api_id=api_id, api_hash=api_hash))


@account_app.command("verify-code")
def account_verify_code(
    ctx: typer.Context,
    phone: str = typer.Option(..., "--phone", help="Phone number with country code"),
    code: str = typer.Option(..., "--code", help="Auth code received in Telegram"),
    password: str | None = typer.Option(None, "--password", help="2FA password (if required)"),
    api_id: int | None = typer.Option(
        None, "--api-id", help="Telegram API ID (uses stored if omitted)"
    ),
    api_hash: str | None = typer.Option(
        None, "--api-hash", help="Telegram API hash (uses stored if omitted)"
    ),
) -> None:
    """Verify Telegram auth code and add account."""
    apply_startup(ctx)
    run_async(
        account_cmd.verify_code_impl(
            ctx.obj.config, phone=phone, code=code, password=password, api_id=api_id, api_hash=api_hash
        )
    )


@account_app.command("add")
def account_add(
    ctx: typer.Context,
    phone: str = typer.Option(..., "--phone", help="Phone number with country code"),
    code: str | None = typer.Option(None, "--code", help="Auth code received in Telegram"),
    password: str | None = typer.Option(None, "--password", help="2FA password (if required)"),
    api_id: int | None = typer.Option(
        None, "--api-id", help="Telegram API ID (uses stored if omitted)"
    ),
    api_hash: str | None = typer.Option(
        None, "--api-hash", help="Telegram API hash (uses stored if omitted)"
    ),
) -> None:
    """Compatibility alias for send-code / verify-code account onboarding."""
    apply_startup(ctx)
    # ``add`` resolves to verify-code when a --code is supplied, else send-code —
    # exactly as the old argparse run() adapter did.
    if code:
        run_async(
            account_cmd.verify_code_impl(
                ctx.obj.config,
                phone=phone,
                code=code,
                password=password,
                api_id=api_id,
                api_hash=api_hash,
            )
        )
    else:
        run_async(
            account_cmd.send_code_impl(ctx.obj.config, phone=phone, api_id=api_id, api_hash=api_hash)
        )


@account_app.command("flood-status")
def account_flood_status(ctx: typer.Context) -> None:
    """Show flood wait timers for all accounts."""
    apply_startup(ctx)
    run_async(account_cmd.flood_status_impl(ctx.obj.config))


@account_app.command("flood-clear")
def account_flood_clear(
    ctx: typer.Context,
    phone: str = typer.Option(..., "--phone", help="Account phone number"),
) -> None:
    """Clear flood wait for an account."""
    apply_startup(ctx)
    run_async(account_cmd.flood_clear_impl(ctx.obj.config, phone=phone))


@account_app.command("export-session")
def account_export_session(
    ctx: typer.Context,
    account_id: int | None = typer.Option(None, "--id", help="Account id"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone number"),
    as_json: bool = typer.Option(False, "--json", help="Emit {phone, session_string} JSON"),
) -> None:
    """Print the decrypted StringSession for SSO (⚠️ full account access — keep secret).

    Exactly one of --id / --phone is required (the argparse mutually-exclusive
    group is enforced here — Typer has no native mutex group). The check runs
    *before* ``apply_startup`` so an invalid mutex is rejected at parse time, the
    way argparse rejected it during ``parse_args()`` — without first touching the
    env / logging / data dirs (matters in a read-only runtime, #1162 drift §3).
    The session string is NEVER logged.
    """
    if (account_id is None) == (phone is None):
        # Mirror argparse's "exactly one required" mutually-exclusive group.
        raise typer.BadParameter("provide exactly one of --id or --phone")
    apply_startup(ctx)
    run_async(
        account_cmd.export_session_impl(
            ctx.obj.config, account_id=account_id, phone=phone, as_json=as_json
        )
    )


@account_app.command("import")
def account_import(
    ctx: typer.Context,
    phone: str = typer.Option(..., "--phone", help="Phone number with country code"),
    session_string: str | None = typer.Option(
        None,
        "--session-string",
        help="Telegram StringSession to import (⚠️ appears in shell history — prefer --session-string-stdin)",
    ),
    session_string_stdin: bool = typer.Option(
        False,
        "--session-string-stdin",
        help="Read the StringSession from stdin (keeps the secret out of argv / shell history)",
    ),
    force: bool = typer.Option(
        False, "--force", help="Overwrite the session of an account that already exists for this phone"
    ),
) -> None:
    """Add an account from a ready StringSession (SSO import, skips login).

    Exactly one of --session-string / --session-string-stdin is required (the
    argparse mutually-exclusive group is enforced here). The check runs *before*
    ``apply_startup`` so an invalid mutex is rejected at parse time — without
    first touching the env / logging / data dirs (#1162 drift §3). The raw
    session string is never echoed back or logged.
    """
    if (session_string is None) == (not session_string_stdin):
        # Mirror argparse's required mutually-exclusive group: exactly one source.
        raise typer.BadParameter(
            "provide exactly one of --session-string or --session-string-stdin"
        )
    apply_startup(ctx)
    run_async(
        account_cmd.import_impl(
            ctx.obj.config,
            phone=phone,
            session_string=session_string,
            session_string_stdin=session_string_stdin,
            force=force,
        )
    )


# --------------------------------------------------------------------------- #
# agent → threads / thread-create / thread-delete / chat / thread-rename /
#         thread-stop / messages / context / test-escaping / test-tools
# --------------------------------------------------------------------------- #

agent_app = typer.Typer(no_args_is_help=True, help="Agent chat management")
app.add_typer(agent_app, name="agent")


@agent_app.command("threads")
def agent_threads(ctx: typer.Context) -> None:
    """List agent threads."""
    apply_startup(ctx)
    run_async(agent_cmd.threads_impl(ctx.obj.config))


@agent_app.command("thread-create")
def agent_thread_create(
    ctx: typer.Context,
    title: str | None = typer.Option(None, "--title", help="Thread title"),
) -> None:
    """Create new thread."""
    apply_startup(ctx)
    run_async(agent_cmd.thread_create_impl(ctx.obj.config, title=title))


@agent_app.command("thread-delete")
def agent_thread_delete(
    ctx: typer.Context,
    thread_id: int = typer.Argument(..., help="Thread ID"),
) -> None:
    """Delete thread."""
    apply_startup(ctx)
    run_async(agent_cmd.thread_delete_impl(ctx.obj.config, thread_id=thread_id))


@agent_app.command("chat")
def agent_chat(
    ctx: typer.Context,
    prompt: str | None = typer.Option(
        None, "-p", "--prompt", help="Message text (non-interactive mode)"
    ),
    thread_id: int | None = typer.Option(None, "--thread-id"),
    model: str | None = typer.Option(None, "--model", help="Model name"),
) -> None:
    """Interactive TUI chat or one-shot message (with -p)."""
    apply_startup(ctx)
    run_async(agent_cmd.chat_impl(ctx.obj.config, prompt=prompt, thread_id=thread_id, model=model))


@agent_app.command("thread-rename")
def agent_thread_rename(
    ctx: typer.Context,
    thread_id: int = typer.Argument(..., help="Thread ID"),
    title: str = typer.Argument(..., help="New title"),
) -> None:
    """Rename thread."""
    apply_startup(ctx)
    run_async(agent_cmd.thread_rename_impl(ctx.obj.config, thread_id=thread_id, title=title))


@agent_app.command("thread-stop")
def agent_thread_stop(
    ctx: typer.Context,
    thread_id: int = typer.Argument(..., help="Thread ID"),
) -> None:
    """Stop/cancel an ongoing agent response for a thread."""
    apply_startup(ctx)
    run_async(agent_cmd.thread_stop_impl(ctx.obj.config, thread_id=thread_id))


@agent_app.command("messages")
def agent_messages(
    ctx: typer.Context,
    thread_id: int = typer.Argument(..., help="Thread ID"),
    limit: int | None = typer.Option(None, "--limit", help="Last N messages"),
) -> None:
    """Show thread messages."""
    apply_startup(ctx)
    run_async(agent_cmd.messages_impl(ctx.obj.config, thread_id=thread_id, limit=limit))


@agent_app.command("context")
def agent_context(
    ctx: typer.Context,
    thread_id: int = typer.Argument(..., help="Thread ID"),
    channel_id: int = typer.Option(..., "--channel-id"),
    limit: int = typer.Option(100000, "--limit", help="Max messages"),
    topic_id: int | None = typer.Option(None, "--topic-id"),
) -> None:
    """Inject channel context into thread."""
    apply_startup(ctx)
    run_async(
        agent_cmd.context_impl(
            ctx.obj.config,
            thread_id=thread_id,
            channel_id=channel_id,
            limit=limit,
            topic_id=topic_id,
        )
    )


@agent_app.command("test-escaping")
def agent_test_escaping(ctx: typer.Context) -> None:
    """Test agent with special characters."""
    apply_startup(ctx)
    run_async(agent_cmd.test_escaping_impl(ctx.obj.config))


@agent_app.command("test-tools")
def agent_test_tools(ctx: typer.Context) -> None:
    """Test that agent tool calls produce tool_start/tool_end events."""
    apply_startup(ctx)
    run_async(agent_cmd.test_tools_impl(ctx.obj.config))


# --------------------------------------------------------------------------- #
# photo-loader → dialogs / refresh / send / schedule-send / batch-create /
#                publish / batch-list / items / batch-cancel / auto-create / auto-list /
#                auto-update / auto-toggle / auto-delete / run-due
# --------------------------------------------------------------------------- #

photo_loader_app = typer.Typer(no_args_is_help=True, help="Photo upload automation")
app.add_typer(photo_loader_app, name="photo-loader")


@photo_loader_app.command("dialogs")
def photo_loader_dialogs(
    ctx: typer.Context,
    phone: str = typer.Option(..., "--phone", help="Account phone"),
) -> None:
    """List dialogs for an account."""
    apply_startup(ctx)
    run_async(photo_loader_cmd.dialogs_impl(ctx.obj.config, phone=phone))


@photo_loader_app.command("refresh")
def photo_loader_refresh(
    ctx: typer.Context,
    phone: str = typer.Option(..., "--phone", help="Account phone"),
) -> None:
    """Refresh dialog cache for photo loader."""
    apply_startup(ctx)
    run_async(photo_loader_cmd.refresh_impl(ctx.obj.config, phone=phone))


@photo_loader_app.command("send")
def photo_loader_send(
    ctx: typer.Context,
    phone: str = typer.Option(..., "--phone", help="Account phone"),
    target: str = typer.Option(..., "--target", help="Dialog id"),
    # ``--files`` is a repeatable option: ``--files a --files b --files c``.
    # Click options cannot be variadic (``nargs=-1`` is arguments-only), so the
    # argparse ``--files a b c`` (nargs='+') form maps to the repeated flag here.
    # Keeping the ``--files`` flag name (rather than a positional variadic) holds
    # the CLI surface / manifest tuple stable (#1162 drift §2, resolved by keeping
    # the repeated form as the single direct surface once argparse was removed).
    files: list[str] = typer.Option(..., "--files", help="Photo file paths (repeat per file)"),
    mode: PhotoMode = typer.Option(PhotoMode.album, "--mode"),
    caption: str | None = typer.Option(None, "--caption", help="Caption"),
) -> None:
    """Send photos now."""
    apply_startup(ctx)
    run_async(
        photo_loader_cmd.send_impl(
            ctx.obj.config, phone=phone, target=target, files=files, mode=mode.value, caption=caption
        )
    )


@photo_loader_app.command("schedule-send")
def photo_loader_schedule_send(
    ctx: typer.Context,
    phone: str = typer.Option(..., "--phone", help="Account phone"),
    target: str = typer.Option(..., "--target", help="Dialog id"),
    files: list[str] = typer.Option(..., "--files", help="Photo file paths (repeat per file)"),
    at: str = typer.Option(..., "--at", help="ISO datetime"),
    mode: PhotoMode = typer.Option(PhotoMode.album, "--mode"),
    caption: str | None = typer.Option(None, "--caption", help="Caption"),
) -> None:
    """Schedule photo send via Telegram."""
    apply_startup(ctx)
    run_async(
        photo_loader_cmd.schedule_send_impl(
            ctx.obj.config,
            phone=phone,
            target=target,
            files=files,
            at=at,
            mode=mode.value,
            caption=caption,
        )
    )


@photo_loader_app.command("batch-create")
def photo_loader_batch_create(
    ctx: typer.Context,
    phone: str = typer.Option(..., "--phone", help="Account phone"),
    target: str = typer.Option(..., "--target", help="Dialog id"),
    manifest: str = typer.Option(..., "--manifest", help="JSON/YAML manifest path"),
    caption: str | None = typer.Option(None, "--caption", help="Default caption"),
) -> None:
    """Create delayed batch from manifest."""
    apply_startup(ctx)
    run_async(
        photo_loader_cmd.batch_create_impl(
            ctx.obj.config, phone=phone, target=target, manifest=manifest, caption=caption
        )
    )


@photo_loader_app.command("batch-list")
def photo_loader_batch_list(ctx: typer.Context) -> None:
    """List photo batches."""
    apply_startup(ctx)
    run_async(photo_loader_cmd.batch_list_impl(ctx.obj.config))


@photo_loader_app.command("publish")
def photo_loader_publish(
    ctx: typer.Context,
    batch_id: int = typer.Argument(..., metavar="batch_id", help="Photo batch id"),
) -> None:
    """Publish a held photo batch into the due queue."""
    apply_startup(ctx)
    run_async(photo_loader_cmd.publish_impl(ctx.obj.config, batch_id=batch_id))


@photo_loader_app.command("items")
def photo_loader_items(
    ctx: typer.Context,
    batch_id: int | None = typer.Option(None, "--batch-id", help="Filter by batch id"),
    limit: int = typer.Option(100, "--limit", help="Max items to show"),
) -> None:
    """List photo batch items."""
    apply_startup(ctx)
    run_async(photo_loader_cmd.items_impl(ctx.obj.config, batch_id=batch_id, limit=limit))


@photo_loader_app.command("batch-cancel")
def photo_loader_batch_cancel(
    ctx: typer.Context,
    item_id: int = typer.Argument(..., metavar="id", help="Photo item id"),
) -> None:
    """Cancel a photo batch item."""
    apply_startup(ctx)
    run_async(photo_loader_cmd.batch_cancel_impl(ctx.obj.config, item_id=item_id))


@photo_loader_app.command("auto-create")
def photo_loader_auto_create(
    ctx: typer.Context,
    phone: str = typer.Option(..., "--phone", help="Account phone"),
    target: str = typer.Option(..., "--target", help="Dialog id"),
    folder: str = typer.Option(..., "--folder", help="Folder path"),
    interval: int = typer.Option(..., "--interval", help="Interval in minutes"),
    mode: PhotoMode = typer.Option(PhotoMode.album, "--mode"),
    caption: str | None = typer.Option(None, "--caption", help="Caption"),
) -> None:
    """Create auto-upload job."""
    apply_startup(ctx)
    run_async(
        photo_loader_cmd.auto_create_impl(
            ctx.obj.config,
            phone=phone,
            target=target,
            folder=folder,
            interval=interval,
            mode=mode.value,
            caption=caption,
        )
    )


@photo_loader_app.command("auto-list")
def photo_loader_auto_list(ctx: typer.Context) -> None:
    """List auto-upload jobs."""
    apply_startup(ctx)
    run_async(photo_loader_cmd.auto_list_impl(ctx.obj.config))


@photo_loader_app.command("auto-update")
def photo_loader_auto_update(
    ctx: typer.Context,
    job_id: int = typer.Argument(..., metavar="id", help="Job id"),
    folder: str | None = typer.Option(None, "--folder", help="Folder path"),
    interval: int | None = typer.Option(None, "--interval", help="Interval in minutes"),
    mode: PhotoMode | None = typer.Option(None, "--mode"),
    caption: str | None = typer.Option(None, "--caption", help="Caption"),
    active: bool = typer.Option(False, "--active", help="Enable job"),
    paused: bool = typer.Option(False, "--paused", help="Pause job"),
) -> None:
    """Update auto-upload job."""
    apply_startup(ctx)
    run_async(
        photo_loader_cmd.auto_update_impl(
            ctx.obj.config,
            job_id=job_id,
            folder=folder,
            interval=interval,
            mode=mode.value if mode else None,
            caption=caption,
            active=active,
            paused=paused,
        )
    )


@photo_loader_app.command("auto-toggle")
def photo_loader_auto_toggle(
    ctx: typer.Context,
    job_id: int = typer.Argument(..., metavar="id", help="Job id"),
) -> None:
    """Toggle auto-upload job."""
    apply_startup(ctx)
    run_async(photo_loader_cmd.auto_toggle_impl(ctx.obj.config, job_id=job_id))


@photo_loader_app.command("auto-delete")
def photo_loader_auto_delete(
    ctx: typer.Context,
    job_id: int = typer.Argument(..., metavar="id", help="Job id"),
) -> None:
    """Delete auto-upload job."""
    apply_startup(ctx)
    run_async(photo_loader_cmd.auto_delete_impl(ctx.obj.config, job_id=job_id))


@photo_loader_app.command("run-due")
def photo_loader_run_due(
    ctx: typer.Context,
    item_id: int | None = typer.Option(None, "--item-id", help="Run only one due photo item"),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Preview which auto-job files would be posted (where/when) without sending or marking",
    ),
) -> None:
    """Run due photo items and auto jobs now."""
    apply_startup(ctx)
    run_async(photo_loader_cmd.run_due_impl(ctx.obj.config, item_id=item_id, dry_run=dry_run))




# --------------------------------------------------------------------------- #
# test → all / read / write / telegram / benchmark
# --------------------------------------------------------------------------- #

test_app = typer.Typer(no_args_is_help=True, help="Run diagnostic tests")
app.add_typer(test_app, name="test")


@test_app.command("all")
def test_all(ctx: typer.Context) -> None:
    """Run all test sections (read + write + telegram)."""
    apply_startup(ctx)
    test_cmd.run_impl(ctx.obj.config, "all")


@test_app.command("read")
def test_read(ctx: typer.Context) -> None:
    """Read-only DB checks."""
    apply_startup(ctx)
    test_cmd.run_impl(ctx.obj.config, "read")


@test_app.command("write")
def test_write(ctx: typer.Context) -> None:
    """Write DB checks on a temporary DB copy."""
    apply_startup(ctx)
    test_cmd.run_impl(ctx.obj.config, "write")


@test_app.command("telegram")
def test_telegram(ctx: typer.Context) -> None:
    """Live Telegram API tests on a temporary DB copy."""
    apply_startup(ctx)
    test_cmd.run_impl(ctx.obj.config, "telegram")


@test_app.command("benchmark")
def test_benchmark(ctx: typer.Context) -> None:
    """Benchmark serial pytest run against the safe mixed parallel test workflow."""
    apply_startup(ctx)
    test_cmd.run_impl(ctx.obj.config, "benchmark")
