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
from src.cli.commands import debug as debug_cmd
from src.cli.commands import export as export_cmd
from src.cli.commands import filter as filter_cmd
from src.cli.commands import image as image_cmd
from src.cli.commands import mcp_server as mcp_server_cmd
from src.cli.commands import messages as messages_cmd
from src.cli.commands import notification as notification_cmd
from src.cli.commands import provider as provider_cmd
from src.cli.commands import search as search_cmd
from src.cli.commands import search_query as search_query_cmd
from src.cli.commands import serve as serve_cmd
from src.cli.commands import server_control as server_control_cmd
from src.cli.commands import translate as translate_cmd
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
    {
        # Wave 1 (#1121)
        "serve", "worker", "stop", "restart", "mcp-server", "collect", "search", "messages",
        # Wave 2 (#1122) — flat simple groups
        "debug", "export", "translate", "image", "provider", "notification",
        # Wave 3 (#1123) — medium groups
        "search-query", "filter",
    }
)


class ExportFormat(str, Enum):
    """``export telegram --format`` choices — mirrors the argparse ``choices=[…]``."""

    json = "json"
    html = "html"
    both = "both"


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
# debug → logs / memory / timing
# --------------------------------------------------------------------------- #

debug_app = typer.Typer(no_args_is_help=True, help="Diagnostic tools")
app.add_typer(debug_app, name="debug")


@debug_app.command("logs")
def debug_logs(
    ctx: typer.Context,
    limit: int = typer.Option(50, "--limit", help="Number of log lines (default: 50)"),
) -> None:
    """Show recent log entries."""
    apply_startup(ctx)
    run_async(debug_cmd.logs_impl(ctx.obj.config, limit=limit))


@debug_app.command("memory")
def debug_memory(ctx: typer.Context) -> None:
    """Show memory usage statistics."""
    apply_startup(ctx)
    run_async(debug_cmd.memory_impl(ctx.obj.config))


@debug_app.command("timing")
def debug_timing(ctx: typer.Context) -> None:
    """Show operation timing stats."""
    apply_startup(ctx)
    run_async(debug_cmd.timing_impl(ctx.obj.config))


# --------------------------------------------------------------------------- #
# export → json / csv / rss / telegram
# --------------------------------------------------------------------------- #

export_app = typer.Typer(no_args_is_help=True, help="Export collected messages")
app.add_typer(export_app, name="export")


def _export_flat(
    ctx: typer.Context,
    fmt: str,
    channel_id: int | None,
    limit: int,
    output: str | None,
) -> None:
    """Shared body for the flat json/csv/rss export sub-commands."""
    apply_startup(ctx)
    run_async(
        export_cmd.export_impl(
            ctx.obj.config,
            fmt=fmt,
            channel_id=channel_id,
            limit=limit,
            output=output,
        )
    )


@export_app.command("json")
def export_json(
    ctx: typer.Context,
    channel_id: int | None = typer.Option(None, "--channel-id", help="Filter by channel ID"),
    limit: int = typer.Option(200, "--limit", help="Max messages (default: 200)"),
    output: str | None = typer.Option(None, "--output", "-o", help="Output file (default: stdout)"),
) -> None:
    """Export as JSON."""
    _export_flat(ctx, "json", channel_id, limit, output)


@export_app.command("csv")
def export_csv(
    ctx: typer.Context,
    channel_id: int | None = typer.Option(None, "--channel-id", help="Filter by channel ID"),
    limit: int = typer.Option(200, "--limit", help="Max messages (default: 200)"),
    output: str | None = typer.Option(None, "--output", "-o", help="Output file (default: stdout)"),
) -> None:
    """Export as CSV."""
    _export_flat(ctx, "csv", channel_id, limit, output)


@export_app.command("rss")
def export_rss(
    ctx: typer.Context,
    channel_id: int | None = typer.Option(None, "--channel-id", help="Filter by channel ID"),
    limit: int = typer.Option(200, "--limit", help="Max messages (default: 200)"),
    output: str | None = typer.Option(None, "--output", "-o", help="Output file (default: stdout)"),
) -> None:
    """Export as RSS."""
    _export_flat(ctx, "rss", channel_id, limit, output)


@export_app.command("telegram")
def export_telegram(
    ctx: typer.Context,
    channel_id: int | None = typer.Option(None, "--channel-id", help="Telegram channel ID to export (required)"),
    export_format: ExportFormat = typer.Option(ExportFormat.json, "--format", help="Output format (default: json)"),
    with_media: bool = typer.Option(
        False, "--with-media", help="Download media artifacts (enqueues a worker task)"
    ),
    wait: bool = typer.Option(
        False, "--wait", help="With --with-media: poll the enqueued task until it finishes"
    ),
    max_file_size: int | None = typer.Option(
        None, "--max-file-size", help="Skip files larger than N MB (default: from settings or 3)"
    ),
    date_from: str | None = typer.Option(None, "--date-from", help="Start date YYYY-MM-DD"),
    date_to: str | None = typer.Option(None, "--date-to", help="End date YYYY-MM-DD"),
    limit: int = typer.Option(5000, "--limit", help="Max messages (default: 5000)"),
    output: str | None = typer.Option(
        None, "--output", "-o",
        help="Output directory (default: data/exports/ChatExport_<date>_<channel>)",
    ),
) -> None:
    """Export as Telegram-Desktop JSON/HTML."""
    apply_startup(ctx)
    run_async(
        export_cmd.telegram_impl(
            ctx.obj.config,
            channel_id=channel_id,
            export_format=export_format.value,
            with_media=with_media,
            wait=wait,
            max_file_size=max_file_size,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            output=output,
        )
    )


# --------------------------------------------------------------------------- #
# translate → stats / detect / run / message
# --------------------------------------------------------------------------- #

translate_app = typer.Typer(no_args_is_help=True, help="Language detection and translation")
app.add_typer(translate_app, name="translate")


@translate_app.command("stats")
def translate_stats(ctx: typer.Context) -> None:
    """Show language distribution."""
    apply_startup(ctx)
    run_async(translate_cmd.stats_impl(ctx.obj.config))


@translate_app.command("detect")
def translate_detect(
    ctx: typer.Context,
    batch_size: int = typer.Option(5000, "--batch-size"),
) -> None:
    """Backfill language detection."""
    apply_startup(ctx)
    run_async(translate_cmd.detect_impl(ctx.obj.config, batch_size=batch_size))


@translate_app.command("run")
def translate_run(
    ctx: typer.Context,
    target: str = typer.Option("en", "--target", help="Target language code"),
    source_filter: str = typer.Option("", "--source-filter", help="Comma-separated source languages"),
    limit: int = typer.Option(100, "--limit", help="Max messages to translate"),
) -> None:
    """Run translation batch."""
    apply_startup(ctx)
    run_async(
        translate_cmd.run_impl(
            ctx.obj.config,
            target=target,
            source_filter=source_filter,
            limit=limit,
        )
    )


@translate_app.command("message")
def translate_message(
    ctx: typer.Context,
    message_id: int = typer.Argument(..., help="Message DB id"),
    target: str = typer.Option("en", "--target", help="Target language code"),
) -> None:
    """Translate a single message."""
    apply_startup(ctx)
    run_async(translate_cmd.message_impl(ctx.obj.config, message_id=message_id, target=target))


# --------------------------------------------------------------------------- #
# image → generate / models / providers / generated
# --------------------------------------------------------------------------- #

image_app = typer.Typer(no_args_is_help=True, help="Image generation")
app.add_typer(image_app, name="image")


@image_app.command("generate")
def image_generate(
    ctx: typer.Context,
    prompt: str = typer.Argument(..., help="Text prompt for image generation"),
    model: str | None = typer.Option(None, "--model", help="Model string (e.g. replicate:flux-schnell)"),
) -> None:
    """Generate an image from prompt."""
    apply_startup(ctx)
    run_async(image_cmd.generate_impl(ctx.obj.config, prompt=prompt, model=model))


@image_app.command("models")
def image_models(
    ctx: typer.Context,
    provider: str = typer.Option(..., "--provider", help="Provider name (replicate, together, openai)"),
    query: str = typer.Option("", "--query", help="Search query"),
    refresh: bool = typer.Option(
        False, "--refresh", help="Fetch the live model list from the provider (OpenAI: /v1/models)"
    ),
) -> None:
    """Search available models."""
    apply_startup(ctx)
    run_async(image_cmd.models_impl(ctx.obj.config, provider=provider, query=query, refresh=refresh))


@image_app.command("providers")
def image_providers(ctx: typer.Context) -> None:
    """List configured image providers."""
    apply_startup(ctx)
    run_async(image_cmd.providers_impl(ctx.obj.config))


@image_app.command("generated")
def image_generated(
    ctx: typer.Context,
    limit: int = typer.Option(20, "--limit", help="Max images to show"),
) -> None:
    """List generated images."""
    apply_startup(ctx)
    run_async(image_cmd.generated_impl(ctx.obj.config, limit=limit))


# --------------------------------------------------------------------------- #
# provider → list / add / delete / probe / refresh / test-all
# --------------------------------------------------------------------------- #

provider_app = typer.Typer(no_args_is_help=True, help="LLM provider management")
app.add_typer(provider_app, name="provider")


@provider_app.command("list")
def provider_list(ctx: typer.Context) -> None:
    """List configured providers with models and status."""
    apply_startup(ctx)
    run_async(provider_cmd.list_impl(ctx.obj.config))


@provider_app.command("add")
def provider_add(
    ctx: typer.Context,
    name: str = typer.Argument(..., help="Provider name (e.g. openai, groq, anthropic)"),
    api_key: str = typer.Option(..., "--api-key", help="API key"),
    base_url: str | None = typer.Option(None, "--base-url", help="Custom base URL"),
) -> None:
    """Add or update a provider."""
    apply_startup(ctx)
    run_async(provider_cmd.add_impl(ctx.obj.config, name=name, api_key=api_key, base_url=base_url))


@provider_app.command("delete")
def provider_delete(
    ctx: typer.Context,
    name: str = typer.Argument(..., help="Provider name"),
) -> None:
    """Delete a provider."""
    apply_startup(ctx)
    run_async(provider_cmd.delete_impl(ctx.obj.config, name=name))


@provider_app.command("probe")
def provider_probe(
    ctx: typer.Context,
    name: str = typer.Argument(..., help="Provider name"),
) -> None:
    """Test provider connection."""
    apply_startup(ctx)
    run_async(provider_cmd.probe_impl(ctx.obj.config, name=name))


@provider_app.command("refresh")
def provider_refresh(
    ctx: typer.Context,
    name: str | None = typer.Argument(None, help="Provider name (default: all)"),
) -> None:
    """Refresh provider models."""
    apply_startup(ctx)
    run_async(provider_cmd.refresh_impl(ctx.obj.config, name=name))


@provider_app.command("test-all")
def provider_test_all(ctx: typer.Context) -> None:
    """Test all configured providers."""
    apply_startup(ctx)
    run_async(provider_cmd.test_all_impl(ctx.obj.config))


# --------------------------------------------------------------------------- #
# notification → setup / status / delete / test / dry-run / set-account
# --------------------------------------------------------------------------- #

notification_app = typer.Typer(no_args_is_help=True, help="Personal notification bot management")
app.add_typer(notification_app, name="notification")


@notification_app.command("setup")
def notification_setup(ctx: typer.Context) -> None:
    """Create personal notification bot via BotFather."""
    apply_startup(ctx)
    run_async(notification_cmd.setup_impl(ctx.obj.config))


@notification_app.command("status")
def notification_status(ctx: typer.Context) -> None:
    """Show notification bot status."""
    apply_startup(ctx)
    run_async(notification_cmd.status_impl(ctx.obj.config))


@notification_app.command("delete")
def notification_delete(ctx: typer.Context) -> None:
    """Delete notification bot via BotFather."""
    apply_startup(ctx)
    run_async(notification_cmd.delete_impl(ctx.obj.config))


@notification_app.command("test")
def notification_test(
    ctx: typer.Context,
    message: str = typer.Option("Тестовое уведомление", "--message", help="Message text"),
) -> None:
    """Send a test notification message."""
    apply_startup(ctx)
    run_async(notification_cmd.test_impl(ctx.obj.config, message=message))


@notification_app.command("dry-run")
def notification_dry_run(ctx: typer.Context) -> None:
    """Preview notification matches without sending."""
    apply_startup(ctx)
    run_async(notification_cmd.dry_run_impl(ctx.obj.config))


@notification_app.command("set-account")
def notification_set_account(
    ctx: typer.Context,
    phone: str = typer.Option(..., "--phone", help="Account phone number"),
) -> None:
    """Set account for notification bot."""
    apply_startup(ctx)
    run_async(notification_cmd.set_account_impl(ctx.obj.config, phone=phone))


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


@search_query_app.command("add")
def search_query_add(
    ctx: typer.Context,
    query: str = typer.Argument(..., help="FTS5 search query text"),
    interval: int = typer.Option(60, "--interval", help="Run interval in minutes"),
    regex: bool = typer.Option(False, "--regex", help="Use regex matching"),
    fts: bool = typer.Option(False, "--fts", help="Use FTS5 boolean syntax (no quoting)"),
    notify: bool = typer.Option(False, "--notify", help="Notify on collect"),
    track_stats: bool = typer.Option(
        True, "--track-stats/--no-track-stats", help="Track stats (default: on)"
    ),
    exclude_patterns: str = typer.Option(
        "", "--exclude-patterns", help="Exclude patterns, one per line (use \\n)"
    ),
    max_length: int | None = typer.Option(None, "--max-length", help="Max message text length"),
    chats: str = typer.Option("", "--chats", help="Chat filter: IDs, usernames or t.me links"),
) -> None:
    """Add search query."""
    apply_startup(ctx)
    run_async(
        search_query_cmd.add_impl(
            ctx.obj.config,
            query=query,
            interval=interval,
            is_regex=regex,
            is_fts=fts,
            notify=notify,
            track_stats=track_stats,
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
        False, "--quick", help="Skip cross-channel duplicate analysis (fast on large DBs)"
    ),
) -> None:
    """Analyze channels and show report."""
    apply_startup(ctx)
    run_async(filter_cmd.analyze_impl(ctx.obj.config, quick=quick))


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
    elif command == "debug":
        argv.append("debug")
        argv += _debug_argv(args)
    elif command == "export":
        argv.append("export")
        argv += _export_argv(args)
    elif command == "translate":
        argv.append("translate")
        argv += _translate_argv(args)
    elif command == "image":
        argv.append("image")
        argv += _image_argv(args)
    elif command == "provider":
        argv.append("provider")
        argv += _provider_argv(args)
    elif command == "notification":
        argv.append("notification")
        argv += _notification_argv(args)
    elif command == "search-query":
        argv.append("search-query")
        argv += _search_query_argv(args)
    elif command == "filter":
        argv.append("filter")
        argv += _filter_argv(args)
    return argv


def _debug_argv(args: argparse.Namespace) -> list[str]:
    """argv tail for ``debug`` — the action plus its only flag (``logs --limit``)."""
    action = getattr(args, "debug_action", None)
    if action is None:
        return []
    tail = [action]
    if action == "logs" and getattr(args, "limit", 50) != 50:
        tail += ["--limit", str(args.limit)]
    return tail


def _export_argv(args: argparse.Namespace) -> list[str]:
    """argv tail for ``export`` — the action plus its flags.

    The flat json/csv/rss commands share one flag set; ``telegram`` has its own.
    """
    action = getattr(args, "export_action", None)
    if action is None:
        return []
    tail = [action]
    if action in ("json", "csv", "rss"):
        if getattr(args, "channel_id", None) is not None:
            tail += ["--channel-id", str(args.channel_id)]
        if getattr(args, "limit", 200) != 200:
            tail += ["--limit", str(args.limit)]
        if getattr(args, "output", None):
            tail += ["--output", args.output]
    elif action == "telegram":
        if getattr(args, "channel_id", None) is not None:
            tail += ["--channel-id", str(args.channel_id)]
        if getattr(args, "export_format", "json") != "json":
            tail += ["--format", args.export_format]
        if getattr(args, "with_media", False):
            tail.append("--with-media")
        if getattr(args, "wait", False):
            tail.append("--wait")
        if getattr(args, "max_file_size", None) is not None:
            tail += ["--max-file-size", str(args.max_file_size)]
        if getattr(args, "date_from", None):
            tail += ["--date-from", args.date_from]
        if getattr(args, "date_to", None):
            tail += ["--date-to", args.date_to]
        if getattr(args, "limit", 5000) != 5000:
            tail += ["--limit", str(args.limit)]
        if getattr(args, "output", None):
            tail += ["--output", args.output]
    return tail


def _translate_argv(args: argparse.Namespace) -> list[str]:
    """argv tail for ``translate`` — the action plus its flags / positional."""
    action = getattr(args, "translate_action", None)
    if action is None:
        return []
    tail = [action]
    if action == "detect":
        if getattr(args, "batch_size", 5000) != 5000:
            tail += ["--batch-size", str(args.batch_size)]
    elif action == "run":
        if getattr(args, "target", "en") != "en":
            tail += ["--target", args.target]
        if getattr(args, "source_filter", ""):
            tail += ["--source-filter", args.source_filter]
        if getattr(args, "limit", 100) != 100:
            tail += ["--limit", str(args.limit)]
    elif action == "message":
        if getattr(args, "target", "en") != "en":
            tail += ["--target", args.target]
        # Positional message_id after ``--`` (defensive, though it is always positive).
        tail += ["--", str(args.message_id)]
    return tail


def _image_argv(args: argparse.Namespace) -> list[str]:
    """argv tail for ``image`` — the action plus its flags / positional prompt."""
    action = getattr(args, "image_action", None)
    if action is None:
        return []
    tail = [action]
    if action == "generate":
        if getattr(args, "model", None):
            tail += ["--model", args.model]
        # Prompt is free text; emit after ``--`` so a leading ``-`` survives.
        tail += ["--", args.prompt]
    elif action == "models":
        tail += ["--provider", args.provider]
        if getattr(args, "query", ""):
            tail += ["--query", args.query]
        if getattr(args, "refresh", False):
            tail.append("--refresh")
    elif action == "generated":
        if getattr(args, "limit", 20) != 20:
            tail += ["--limit", str(args.limit)]
    return tail


def _provider_argv(args: argparse.Namespace) -> list[str]:
    """argv tail for ``provider`` — the action plus its flags / positional name."""
    action = getattr(args, "provider_action", None)
    if action is None:
        return []
    tail = [action]
    if action == "add":
        tail += ["--api-key", args.api_key]
        if getattr(args, "base_url", None):
            tail += ["--base-url", args.base_url]
        tail += ["--", args.name]
    elif action in ("delete", "probe"):
        tail += ["--", args.name]
    elif action == "refresh":
        if getattr(args, "name", None):
            tail += ["--", args.name]
    return tail


def _notification_argv(args: argparse.Namespace) -> list[str]:
    """argv tail for ``notification`` — the action plus its flags."""
    action = getattr(args, "notification_action", None)
    if action is None:
        return []
    tail = [action]
    if action == "test":
        if getattr(args, "message", "Тестовое уведомление") != "Тестовое уведомление":
            tail += ["--message", args.message]
    elif action == "set-account":
        tail += ["--phone", args.phone]
    return tail


def _filter_argv(args: argparse.Namespace) -> list[str]:
    """argv tail for ``filter`` — the action plus its flags / positional pk."""
    action = getattr(args, "filter_action", None)
    if action is None:
        return []
    tail = [action]
    if action == "analyze":
        if getattr(args, "quick", False):
            tail.append("--quick")
    elif action == "toggle":
        tail += ["--", str(args.pk)]
    elif action in ("reset", "purge", "hard-delete"):
        if getattr(args, "pks", None):
            tail += ["--pks", args.pks]
        if action in ("purge", "hard-delete") and getattr(args, "yes", False):
            tail.append("--yes")
    elif action == "purge-messages":
        tail += ["--channel-id", str(args.channel_id)]
        if getattr(args, "yes", False):
            tail.append("--yes")
    return tail


def _search_query_argv(args: argparse.Namespace) -> list[str]:
    """argv tail for ``search-query`` — the action plus its flags / positionals.

    ``add`` and ``edit`` share many flags but with different defaults: ``add`` has
    store_true flags (absent unless set) and a positional query; ``edit`` uses
    tri-state flags (``--x`` / ``--no-x``, default ``None`` = leave unchanged) plus
    the ``--no-max-length`` / ``--clear-chats`` sentinels. We emit each flag only
    when it diverges from its default so the Typer command sees the same resolved
    state argparse produced.
    """
    action = getattr(args, "search_query_action", None)
    if action is None:
        return []
    tail = [action]
    if action == "get":
        tail += ["--", str(args.id)]
    elif action in ("delete", "toggle", "run"):
        tail += ["--", str(args.id)]
    elif action == "stats":
        if getattr(args, "days", 30) != 30:
            tail += ["--days", str(args.days)]
        tail += ["--", str(args.id)]
    elif action == "add":
        if getattr(args, "interval", 60) != 60:
            tail += ["--interval", str(args.interval)]
        if getattr(args, "regex", False):
            tail.append("--regex")
        if getattr(args, "fts", False):
            tail.append("--fts")
        if getattr(args, "notify", False):
            tail.append("--notify")
        if getattr(args, "track_stats", True) is False:
            tail.append("--no-track-stats")
        if getattr(args, "exclude_patterns", ""):
            tail += ["--exclude-patterns", args.exclude_patterns]
        if getattr(args, "max_length", None) is not None:
            tail += ["--max-length", str(args.max_length)]
        if getattr(args, "chats", ""):
            tail += ["--chats", args.chats]
        # Positional query after ``--`` so a leading ``-`` survives Click parsing.
        tail += ["--", args.query]
    elif action == "edit":
        if getattr(args, "query", None):
            tail += ["--query", args.query]
        if getattr(args, "interval", None) is not None:
            tail += ["--interval", str(args.interval)]
        regex = getattr(args, "regex", None)
        if regex is True:
            tail.append("--regex")
        elif regex is False:
            tail.append("--no-regex")
        fts = getattr(args, "fts", None)
        if fts is True:
            tail.append("--fts")
        elif fts is False:
            tail.append("--no-fts")
        notify = getattr(args, "notify", None)
        if notify is True:
            tail.append("--notify")
        elif notify is False:
            tail.append("--no-notify")
        track_stats = getattr(args, "track_stats", None)
        if track_stats is True:
            tail.append("--track-stats")
        elif track_stats is False:
            tail.append("--no-track-stats")
        if getattr(args, "exclude_patterns", None) is not None:
            tail += ["--exclude-patterns", args.exclude_patterns]
        max_length = getattr(args, "max_length", None)
        # argparse maps --no-max-length onto the -1 store_const sentinel.
        if max_length == -1:
            tail.append("--no-max-length")
        elif max_length is not None:
            tail += ["--max-length", str(max_length)]
        chats = getattr(args, "chats", None)
        # --clear-chats is the "" store_const; --chats carries any other value.
        if chats == "":
            tail.append("--clear-chats")
        elif chats is not None:
            tail += ["--chats", chats]
        tail += ["--", str(args.id)]
    return tail


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
