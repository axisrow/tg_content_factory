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

from src.cli.commands import analytics as analytics_cmd
from src.cli.commands import channel as channel_cmd
from src.cli.commands import collect as collect_cmd
from src.cli.commands import debug as debug_cmd
from src.cli.commands import dialogs as dialogs_cmd
from src.cli.commands import export as export_cmd
from src.cli.commands import image as image_cmd
from src.cli.commands import mcp_server as mcp_server_cmd
from src.cli.commands import messages as messages_cmd
from src.cli.commands import notification as notification_cmd
from src.cli.commands import pipeline as pipeline_cmd
from src.cli.commands import provider as provider_cmd
from src.cli.commands import search as search_cmd
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
        # Wave 4 (#1124) — complex nested groups (incl. depth-2 subparsers)
        "analytics", "channel", "dialogs", "pipeline",
    }
)


class ExportFormat(str, Enum):
    """``export telegram --format`` choices — mirrors the argparse ``choices=[…]``."""

    json = "json"
    html = "html"
    both = "both"


class AnalyticsUseful(str, Enum):
    """``analytics channel-rating --useful`` choices — mirrors the argparse set."""

    useful = "useful"
    useless = "useless"


class AnalyticsGenre(str, Enum):
    """``analytics channel-rating --genre`` choices — mirrors the argparse set."""

    ad = "ad"
    infobiz = "infobiz"
    aggregator = "aggregator"
    copy = "copy"
    original = "original"


class PublishMode(str, Enum):
    """``pipeline add/edit --publish-mode`` choices."""

    auto = "auto"
    moderated = "moderated"


class GenerationBackend(str, Enum):
    """``pipeline add/edit --generation-backend`` choices."""

    chain = "chain"
    agent = "agent"
    deep_agents = "deep_agents"


class SinceUnit(str, Enum):
    """``pipeline add/dry-run-count --since-unit`` choices."""

    m = "m"
    h = "h"
    d = "d"


class TriBool(str, Enum):
    """``pipeline filter set --forwarded/--has-text`` choices (true/false)."""

    true = "true"
    false = "false"


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
# analytics → top / content-types / hourly / summary / daily / pipeline-stats /
#   trending-topics / trending-channels / velocity / peak-hours / calendar /
#   trending-emojis / channel / channel-rating / channel-rate
# --------------------------------------------------------------------------- #

analytics_app = typer.Typer(no_args_is_help=True, help="Message analytics")
app.add_typer(analytics_app, name="analytics")


@analytics_app.command("top")
def analytics_top(
    ctx: typer.Context,
    limit: int = typer.Option(20, "--limit", help="Number of results (default: 20)"),
    date_from: str | None = typer.Option(None, "--date-from", help="Start date (YYYY-MM-DD)"),
    date_to: str | None = typer.Option(None, "--date-to", help="End date (YYYY-MM-DD)"),
) -> None:
    """Top messages by reactions."""
    apply_startup(ctx)
    run_async(analytics_cmd.top_impl(ctx.obj.config, limit=limit, date_from=date_from, date_to=date_to))


@analytics_app.command("content-types")
def analytics_content_types(
    ctx: typer.Context,
    date_from: str | None = typer.Option(None, "--date-from", help="Start date (YYYY-MM-DD)"),
    date_to: str | None = typer.Option(None, "--date-to", help="End date (YYYY-MM-DD)"),
) -> None:
    """Engagement by content type."""
    apply_startup(ctx)
    run_async(analytics_cmd.content_types_impl(ctx.obj.config, date_from=date_from, date_to=date_to))


@analytics_app.command("hourly")
def analytics_hourly(
    ctx: typer.Context,
    date_from: str | None = typer.Option(None, "--date-from", help="Start date (YYYY-MM-DD)"),
    date_to: str | None = typer.Option(None, "--date-to", help="End date (YYYY-MM-DD)"),
) -> None:
    """Hourly activity patterns."""
    apply_startup(ctx)
    run_async(analytics_cmd.hourly_impl(ctx.obj.config, date_from=date_from, date_to=date_to))


@analytics_app.command("summary")
def analytics_summary(ctx: typer.Context) -> None:
    """Content generation summary."""
    apply_startup(ctx)
    run_async(analytics_cmd.summary_impl(ctx.obj.config))


@analytics_app.command("daily")
def analytics_daily(
    ctx: typer.Context,
    days: int = typer.Option(30, "--days", help="Number of days (default: 30)"),
    pipeline_id: int | None = typer.Option(None, "--pipeline-id"),
) -> None:
    """Daily generation stats."""
    apply_startup(ctx)
    run_async(analytics_cmd.daily_impl(ctx.obj.config, days=days, pipeline_id=pipeline_id))


@analytics_app.command("pipeline-stats")
def analytics_pipeline_stats(
    ctx: typer.Context,
    pipeline_id: int | None = typer.Option(None, "--pipeline-id"),
) -> None:
    """Per-pipeline statistics."""
    apply_startup(ctx)
    run_async(analytics_cmd.pipeline_stats_impl(ctx.obj.config, pipeline_id=pipeline_id))


@analytics_app.command("trending-topics")
def analytics_trending_topics(
    ctx: typer.Context,
    days: int = typer.Option(7, "--days", help="Number of days (default: 7)"),
    limit: int = typer.Option(20, "--limit"),
) -> None:
    """Trending topics/keywords."""
    apply_startup(ctx)
    run_async(analytics_cmd.trending_topics_impl(ctx.obj.config, days=days, limit=limit))


@analytics_app.command("trending-channels")
def analytics_trending_channels(
    ctx: typer.Context,
    days: int = typer.Option(7, "--days", help="Number of days (default: 7)"),
    limit: int = typer.Option(20, "--limit"),
) -> None:
    """Top channels by activity."""
    apply_startup(ctx)
    run_async(analytics_cmd.trending_channels_impl(ctx.obj.config, days=days, limit=limit))


@analytics_app.command("velocity")
def analytics_velocity(
    ctx: typer.Context,
    days: int = typer.Option(30, "--days", help="Number of days (default: 30)"),
) -> None:
    """Message volume per day."""
    apply_startup(ctx)
    run_async(analytics_cmd.velocity_impl(ctx.obj.config, days=days))


@analytics_app.command("peak-hours")
def analytics_peak_hours(ctx: typer.Context) -> None:
    """Peak activity hours."""
    apply_startup(ctx)
    run_async(analytics_cmd.peak_hours_impl(ctx.obj.config))


@analytics_app.command("calendar")
def analytics_calendar(
    ctx: typer.Context,
    limit: int = typer.Option(20, "--limit"),
    pipeline_id: int | None = typer.Option(None, "--pipeline-id"),
) -> None:
    """Upcoming scheduled publications."""
    apply_startup(ctx)
    run_async(analytics_cmd.calendar_impl(ctx.obj.config, limit=limit, pipeline_id=pipeline_id))


@analytics_app.command("trending-emojis")
def analytics_trending_emojis(
    ctx: typer.Context,
    days: int = typer.Option(7, "--days", help="Number of days (default: 7)"),
    limit: int = typer.Option(20, "--limit"),
) -> None:
    """Trending emojis in messages."""
    apply_startup(ctx)
    run_async(analytics_cmd.trending_emojis_impl(ctx.obj.config, days=days, limit=limit))


@analytics_app.command("channel")
def analytics_channel(
    ctx: typer.Context,
    channel_id: int = typer.Argument(..., help="Telegram channel_id (negative int)"),
    days: int = typer.Option(30, "--days", help="Time window in days (default: 30)"),
) -> None:
    """Per-channel statistics overview."""
    apply_startup(ctx)
    run_async(analytics_cmd.channel_impl(ctx.obj.config, channel_id=channel_id, days=days))


@analytics_app.command("channel-rating")
def analytics_channel_rating(
    ctx: typer.Context,
    useful: AnalyticsUseful | None = typer.Option(None, "--useful", help="Filter by usefulness axis"),
    genre: AnalyticsGenre | None = typer.Option(None, "--genre", help="Filter by genre axis"),
    limit: int = typer.Option(50, "--limit", help="Max rows (default: 50)"),
) -> None:
    """Channel ratings (usefulness × genre)."""
    apply_startup(ctx)
    run_async(
        analytics_cmd.channel_rating_impl(
            ctx.obj.config,
            useful=useful.value if useful else None,
            genre=genre.value if genre else None,
            limit=limit,
        )
    )


@analytics_app.command("channel-rate")
def analytics_channel_rate(
    ctx: typer.Context,
    channel_id: int = typer.Argument(..., help="Telegram channel_id (bare positive int)"),
    model: str | None = typer.Option(None, "--model", help="LLM model/provider name"),
    sample_size: int = typer.Option(
        40, "--sample-size", help="Number of recent posts to sample for the judge (default: 40)"
    ),
) -> None:
    """Run the LLM judge on a channel and upsert its rating (usefulness × genre)."""
    apply_startup(ctx)
    run_async(
        analytics_cmd.channel_rate_impl(
            ctx.obj.config, channel_id=channel_id, model=model, sample_size=sample_size
        )
    )


# --------------------------------------------------------------------------- #
# channel → list / add / delete / toggle / collect / stats / refresh-types /
#   refresh-meta / review-list / review-confirm / review-keep / import /
#   add-bulk / list-for-import / tag (NESTED depth-2: list/add/delete/set/get)
# --------------------------------------------------------------------------- #

channel_app = typer.Typer(no_args_is_help=True, help="Channel management")
app.add_typer(channel_app, name="channel")

# Nested depth-2 group: ``channel tag`` is its own Typer added onto the channel
# sub-app via ``add_typer`` — the exact path ``channel tag <action>`` is the
# fragile frozen invariant of Wave 4.
channel_tag_app = typer.Typer(no_args_is_help=True, help="Manage channel tags")
channel_app.add_typer(channel_tag_app, name="tag")


@channel_app.command("list")
def channel_list(ctx: typer.Context) -> None:
    """List channels."""
    apply_startup(ctx)
    run_async(channel_cmd.list_impl(ctx.obj.config))


@channel_app.command("add")
def channel_add(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Username, link, or numeric ID"),
) -> None:
    """Add a channel."""
    apply_startup(ctx)
    run_async(channel_cmd.add_impl(ctx.obj.config, identifier=identifier))


@channel_app.command("delete")
def channel_delete(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Channel pk, channel_id, or @username"),
) -> None:
    """Delete a channel."""
    apply_startup(ctx)
    run_async(channel_cmd.delete_impl(ctx.obj.config, identifier=identifier))


@channel_app.command("toggle")
def channel_toggle(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Channel pk, channel_id, or @username"),
) -> None:
    """Toggle channel active state."""
    apply_startup(ctx)
    run_async(channel_cmd.toggle_impl(ctx.obj.config, identifier=identifier))


@channel_app.command("collect")
def channel_collect(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Channel pk, channel_id, or @username"),
    full: bool = typer.Option(False, "--full", help="Explicitly backfill the full channel history"),
) -> None:
    """Collect a single channel one-shot."""
    apply_startup(ctx)
    run_async(channel_cmd.collect_impl(ctx.obj.config, identifier=identifier, full=full))


@channel_app.command("stats")
def channel_stats(
    ctx: typer.Context,
    identifier: str | None = typer.Argument(None, help="Channel pk, channel_id, or @username"),
    all_channels: bool = typer.Option(False, "--all", help="Collect stats for all active channels"),
    max_channels: int | None = typer.Option(
        None, "--max-channels", help="Maximum active channels to process in this bounded stats-all run"
    ),
) -> None:
    """Collect channel stats."""
    apply_startup(ctx)
    run_async(
        channel_cmd.stats_impl(
            ctx.obj.config, all_channels=all_channels, identifier=identifier, max_channels=max_channels
        )
    )


@channel_app.command("refresh-types")
def channel_refresh_types(ctx: typer.Context) -> None:
    """Re-resolve channel types for all active channels."""
    apply_startup(ctx)
    run_async(channel_cmd.refresh_types_impl(ctx.obj.config))


@channel_app.command("refresh-meta")
def channel_refresh_meta(
    ctx: typer.Context,
    identifier: str | None = typer.Argument(None, help="Channel pk, channel_id, or @username (omit for all)"),
    all_channels: bool = typer.Option(False, "--all", help="Refresh metadata for all active channels"),
) -> None:
    """Refresh channel metadata."""
    apply_startup(ctx)
    run_async(
        channel_cmd.refresh_meta_impl(ctx.obj.config, all_channels=all_channels, identifier=identifier)
    )


@channel_app.command("review-list")
def channel_review_list(ctx: typer.Context) -> None:
    """List channels quarantined for review."""
    apply_startup(ctx)
    run_async(channel_cmd.review_list_impl(ctx.obj.config))


@channel_app.command("review-confirm")
def channel_review_confirm(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Channel pk, channel_id, or @username"),
) -> None:
    """Confirm a quarantined channel is dead and deactivate it."""
    apply_startup(ctx)
    run_async(channel_cmd.review_confirm_impl(ctx.obj.config, identifier=identifier))


@channel_app.command("review-keep")
def channel_review_keep(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Channel pk, channel_id, or @username"),
) -> None:
    """Clear a channel's review flag and keep it active."""
    apply_startup(ctx)
    run_async(channel_cmd.review_keep_impl(ctx.obj.config, identifier=identifier))


@channel_app.command("import")
def channel_import(
    ctx: typer.Context,
    source: str = typer.Argument(..., help="Path to .txt/.csv file, or comma-separated identifiers"),
) -> None:
    """Bulk-import channels from a file or identifier list."""
    apply_startup(ctx)
    run_async(channel_cmd.import_impl(ctx.obj.config, source=source))


@channel_app.command("add-bulk")
def channel_add_bulk(
    ctx: typer.Context,
    phone: str = typer.Option(..., "--phone", help="Account phone"),
    dialog_ids: str = typer.Option(..., "--dialog-ids", help="Comma-separated dialog IDs to add as channels"),
) -> None:
    """Add channels from an account's dialogs by id list."""
    apply_startup(ctx)
    run_async(channel_cmd.add_bulk_impl(ctx.obj.config, phone=phone, dialog_ids=dialog_ids))


@channel_app.command("list-for-import")
def channel_list_for_import(
    ctx: typer.Context,
    as_json: bool = typer.Option(False, "--json", help="Output as JSON instead of a table"),
) -> None:
    """List dialogs with an already-added flag."""
    apply_startup(ctx)
    run_async(channel_cmd.list_for_import_impl(ctx.obj.config, as_json=as_json))


# ---- nested: channel tag <action> ---------------------------------------- #


@channel_tag_app.command("list")
def channel_tag_list(ctx: typer.Context) -> None:
    """List all channel tags."""
    apply_startup(ctx)
    run_async(channel_cmd._tag_impl(ctx.obj.config, "list"))


@channel_tag_app.command("add")
def channel_tag_add(
    ctx: typer.Context,
    name: str = typer.Argument(..., help="Tag name"),
) -> None:
    """Create a channel tag."""
    apply_startup(ctx)
    run_async(channel_cmd._tag_impl(ctx.obj.config, "add", name=name))


@channel_tag_app.command("delete")
def channel_tag_delete(
    ctx: typer.Context,
    name: str = typer.Argument(..., help="Tag name"),
) -> None:
    """Delete a channel tag."""
    apply_startup(ctx)
    run_async(channel_cmd._tag_impl(ctx.obj.config, "delete", name=name))


@channel_tag_app.command("set")
def channel_tag_set(
    ctx: typer.Context,
    pk: int = typer.Argument(..., help="Channel primary key"),
    tags: str = typer.Argument(..., help="Comma-separated tag names"),
) -> None:
    """Replace a channel's tags."""
    apply_startup(ctx)
    run_async(channel_cmd._tag_impl(ctx.obj.config, "set", pk=pk, tags=tags))


@channel_tag_app.command("get")
def channel_tag_get(
    ctx: typer.Context,
    pk: int = typer.Argument(..., help="Channel primary key"),
) -> None:
    """Show a channel's tags."""
    apply_startup(ctx)
    run_async(channel_cmd._tag_impl(ctx.obj.config, "get", pk=pk))


# --------------------------------------------------------------------------- #
# dialogs → list / refresh / resolve / leave / join / topics / cache-clear /
#   cache-status / send / forward / edit-message / delete-message /
#   create-channel / create-group / pin-message / react / unpin-message /
#   download-media / participants / edit-admin / edit-permissions / kick /
#   broadcast-stats / archive / unarchive / mark-read /
#   queue (NESTED depth-2: status/cancel/clear-pending)
#
# Every dialogs leaf reuses the shared async ``dialogs_cmd._dispatch`` body by
# building the argparse Namespace it dispatches on — so the Typer path executes
# the exact same (heavily tested) logic, including the mutating-command
# ``--yes`` confirmation flow and the single pool-disconnect/db-close finally.
# --------------------------------------------------------------------------- #

dialogs_app = typer.Typer(no_args_is_help=True, help="Telegram dialogs management")
app.add_typer(dialogs_app, name="dialogs")

# Nested depth-2 group: ``dialogs queue`` mounted via add_typer; the frozen
# ``dialogs queue <action>`` paths are the fragile Wave-4 invariant.
dialogs_queue_app = typer.Typer(
    no_args_is_help=True,
    help="Inspect and manage the Telegram command queue (reactions, sends, forwards, ...)",
)
dialogs_app.add_typer(dialogs_queue_app, name="queue")


def _run_dialogs(ctx: typer.Context, dialogs_action: str, **ns_kwargs) -> None:
    """Build the argparse Namespace a dialogs action dispatches on, then run it.

    Centralises the apply_startup → Namespace → ``_dispatch`` bridge so each leaf
    stays a thin type-hinted signature. ``ns_kwargs`` carries exactly the
    attributes the matching ``_dispatch`` branch reads off ``args``.
    """
    apply_startup(ctx)
    ns = argparse.Namespace(
        config=ctx.obj.config, dialogs_action=dialogs_action, **ns_kwargs
    )
    run_async(dialogs_cmd._dispatch(ns))


@dialogs_app.command("list")
def dialogs_list(
    ctx: typer.Context,
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
) -> None:
    """List all dialogs for an account."""
    _run_dialogs(ctx, "list", phone=phone)


@dialogs_app.command("refresh")
def dialogs_refresh(
    ctx: typer.Context,
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
) -> None:
    """Refresh dialog cache from Telegram."""
    _run_dialogs(ctx, "refresh", phone=phone)


@dialogs_app.command("resolve")
def dialogs_resolve(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Identifier to resolve"),
    phone: str | None = typer.Option(None, "--phone", help="Preferred account phone"),
) -> None:
    """Resolve @username, t.me link, or numeric ID."""
    _run_dialogs(ctx, "resolve", identifier=identifier, phone=phone)


@dialogs_app.command("leave")
def dialogs_leave(
    ctx: typer.Context,
    dialog_ids: list[str] = typer.Argument(..., help="Dialog IDs to leave (space- or comma-separated)"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Leave dialogs by ID."""
    _run_dialogs(ctx, "leave", dialog_ids=dialog_ids, phone=phone, yes=yes)


@dialogs_app.command("join")
def dialogs_join(
    ctx: typer.Context,
    target: str = typer.Argument(..., help="@username, t.me link, or invite link"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Join/subscribe to a channel or group."""
    _run_dialogs(ctx, "join", target=target, phone=phone, yes=yes)


@dialogs_app.command("topics")
def dialogs_topics(
    ctx: typer.Context,
    channel_id: int = typer.Option(..., "--channel-id", help="Channel ID to fetch forum topics for"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: any available)"),
) -> None:
    """List forum topics for a channel."""
    _run_dialogs(ctx, "topics", channel_id=channel_id, phone=phone)


@dialogs_app.command("cache-clear")
def dialogs_cache_clear(
    ctx: typer.Context,
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: all accounts)"),
) -> None:
    """Clear in-memory and DB dialog cache."""
    _run_dialogs(ctx, "cache-clear", phone=phone)


@dialogs_app.command("cache-status")
def dialogs_cache_status(ctx: typer.Context) -> None:
    """Show dialog cache status (entries, age)."""
    _run_dialogs(ctx, "cache-status")


@dialogs_app.command("send")
def dialogs_send(
    ctx: typer.Context,
    recipient: str = typer.Argument(..., help="Recipient: @username, phone number, or numeric ID"),
    text: str = typer.Argument(..., help="Message text to send"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Send a direct message to a user or chat."""
    _run_dialogs(ctx, "send", recipient=recipient, text=text, phone=phone, yes=yes)


@dialogs_app.command("forward")
def dialogs_forward(
    ctx: typer.Context,
    from_chat: str = typer.Argument(..., help="Source chat ID or @username"),
    to_chat: str = typer.Argument(..., help="Destination chat ID or @username"),
    message_ids: list[str] = typer.Argument(..., help="Message IDs to forward (space or comma-separated)"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Forward messages between chats."""
    _run_dialogs(
        ctx, "forward", from_chat=from_chat, to_chat=to_chat, message_ids=message_ids, phone=phone, yes=yes
    )


@dialogs_app.command("edit-message")
def dialogs_edit_message(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    message_id: int = typer.Argument(..., help="Message ID to edit"),
    text: str = typer.Argument(..., help="New message text"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Edit a sent message."""
    _run_dialogs(ctx, "edit-message", chat_id=chat_id, message_id=message_id, text=text, phone=phone, yes=yes)


@dialogs_app.command("delete-message")
def dialogs_delete_message(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    message_ids: list[str] = typer.Argument(..., help="Message IDs to delete (space or comma-separated)"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Delete messages from a chat."""
    _run_dialogs(ctx, "delete-message", chat_id=chat_id, message_ids=message_ids, phone=phone, yes=yes)


@dialogs_app.command("create-channel")
def dialogs_create_channel(
    ctx: typer.Context,
    title: str = typer.Option(..., "--title", help="Channel title"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    about: str = typer.Option("", "--about", help="Channel description"),
    username: str = typer.Option("", "--username", help="Public username (leave empty for private)"),
) -> None:
    """Create a new Telegram broadcast channel."""
    _run_dialogs(ctx, "create-channel", title=title, phone=phone, about=about, username=username)


@dialogs_app.command("create-group")
def dialogs_create_group(
    ctx: typer.Context,
    title: str = typer.Option(..., "--title", help="Group title"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    about: str = typer.Option("", "--about", help="Group description"),
) -> None:
    """Create a new Telegram group."""
    _run_dialogs(ctx, "create-group", title=title, phone=phone, about=about)


@dialogs_app.command("pin-message")
def dialogs_pin_message(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    message_id: int = typer.Argument(..., help="Message ID to pin"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    notify: bool = typer.Option(False, "--notify", help="Notify members about pinned message"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Pin a message in a chat."""
    _run_dialogs(ctx, "pin-message", chat_id=chat_id, message_id=message_id, phone=phone, notify=notify, yes=yes)


@dialogs_app.command("react")
def dialogs_react(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    message_id: int = typer.Argument(..., help="Message ID to react on"),
    emoji: str | None = typer.Argument(None, help="Reaction emoji to set; required unless --clear is used"),
    clear: bool = typer.Option(False, "--clear", help="Remove your reaction from the message"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Set or clear your reaction on a message."""
    _run_dialogs(ctx, "react", chat_id=chat_id, message_id=message_id, emoji=emoji, clear=clear, phone=phone, yes=yes)


@dialogs_app.command("unpin-message")
def dialogs_unpin_message(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    message_id: int | None = typer.Option(None, "--message-id", help="Message ID to unpin (omit to unpin all)"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Unpin a message in a chat."""
    _run_dialogs(ctx, "unpin-message", chat_id=chat_id, message_id=message_id, phone=phone, yes=yes)


@dialogs_app.command("download-media")
def dialogs_download_media(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    message_id: int = typer.Argument(..., help="Message ID containing media"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    output_dir: str = typer.Option(".", "--output-dir", help="Directory to save file (default: current dir)"),
) -> None:
    """Download media from a message."""
    _run_dialogs(ctx, "download-media", chat_id=chat_id, message_id=message_id, phone=phone, output_dir=output_dir)


@dialogs_app.command("participants")
def dialogs_participants(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    limit: int = typer.Option(200, "--limit", help="Max participants to fetch (default: 200)"),
    search: str = typer.Option("", "--search", help="Search query to filter participants"),
) -> None:
    """List participants of a channel/group."""
    _run_dialogs(ctx, "participants", chat_id=chat_id, phone=phone, limit=limit, search=search)


@dialogs_app.command("edit-admin")
def dialogs_edit_admin(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    user_id: str = typer.Argument(..., help="User ID or @username to change admin rights for"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    title: str | None = typer.Option(None, "--title", help="Custom admin title"),
    is_admin: bool = typer.Option(True, "--is-admin/--no-admin", help="Promote to admin (default) / demote"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Promote or demote a user as admin."""
    _run_dialogs(
        ctx, "edit-admin", chat_id=chat_id, user_id=user_id, phone=phone,
        title=title, is_admin=is_admin, yes=yes,
    )


@dialogs_app.command("edit-permissions")
def dialogs_edit_permissions(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    user_id: str = typer.Argument(..., help="User ID or @username"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    until_date: str | None = typer.Option(
        None, "--until-date", help="Restriction end date (ISO format, e.g. 2025-12-31)"
    ),
    send_messages: str | None = typer.Option(None, "--send-messages", help="Allow sending messages (true/false)"),
    send_media: str | None = typer.Option(None, "--send-media", help="Allow sending media (true/false)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Restrict or unrestrict a user in a group."""
    _run_dialogs(
        ctx, "edit-permissions", chat_id=chat_id, user_id=user_id, phone=phone,
        until_date=until_date, send_messages=send_messages, send_media=send_media, yes=yes,
    )


@dialogs_app.command("kick")
def dialogs_kick(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    user_id: str = typer.Argument(..., help="User ID or @username to kick"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Kick a participant from a chat."""
    _run_dialogs(ctx, "kick", chat_id=chat_id, user_id=user_id, phone=phone, yes=yes)


@dialogs_app.command("broadcast-stats")
def dialogs_broadcast_stats(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Channel ID or @username"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
) -> None:
    """Get broadcast statistics for a channel."""
    _run_dialogs(ctx, "broadcast-stats", chat_id=chat_id, phone=phone)


@dialogs_app.command("archive")
def dialogs_archive(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
) -> None:
    """Archive a dialog (move to archive folder)."""
    _run_dialogs(ctx, "archive", chat_id=chat_id, phone=phone)


@dialogs_app.command("unarchive")
def dialogs_unarchive(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
) -> None:
    """Unarchive a dialog (move to main folder)."""
    _run_dialogs(ctx, "unarchive", chat_id=chat_id, phone=phone)


@dialogs_app.command("mark-read")
def dialogs_mark_read(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    max_id: int | None = typer.Option(None, "--max-id", help="Mark messages up to this ID as read (default: all)"),
) -> None:
    """Mark messages as read in a chat."""
    _run_dialogs(ctx, "mark-read", chat_id=chat_id, phone=phone, max_id=max_id)


# ---- nested: dialogs queue <action> --------------------------------------- #


def _run_dialogs_queue(ctx: typer.Context, queue_action: str, **ns_kwargs) -> None:
    """Bridge for the nested ``dialogs queue`` group — sets ``dialogs_action=queue``."""
    apply_startup(ctx)
    ns = argparse.Namespace(
        config=ctx.obj.config, dialogs_action="queue", queue_action=queue_action, **ns_kwargs
    )
    run_async(dialogs_cmd._dispatch(ns))


@dialogs_queue_app.command("status")
def dialogs_queue_status(
    ctx: typer.Context,
    command_type: str | None = typer.Option(None, "--command-type", help="Filter by command type, e.g. dialogs.react"),
    phone: str | None = typer.Option(None, "--phone", help="Filter by account phone"),
    limit: int = typer.Option(20, "--limit", help="Recent entries to show (1-100)"),
) -> None:
    """Show pending/running queue status."""
    _run_dialogs_queue(ctx, "status", command_type=command_type, phone=phone, limit=limit)


@dialogs_queue_app.command("cancel")
def dialogs_queue_cancel(
    ctx: typer.Context,
    command_id: int = typer.Argument(..., help="Command id from queue status"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Cancel a pending queue command by id."""
    _run_dialogs_queue(ctx, "cancel", command_id=command_id, yes=yes)


@dialogs_queue_app.command("clear-pending")
def dialogs_queue_clear_pending(
    ctx: typer.Context,
    command_type: str | None = typer.Option(None, "--command-type", help="Filter by command type, e.g. dialogs.react"),
    phone: str | None = typer.Option(None, "--phone", help="Filter by account phone"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Bulk-cancel pending queue commands (optionally filtered)."""
    _run_dialogs_queue(ctx, "clear-pending", command_type=command_type, phone=phone, yes=yes)


# --------------------------------------------------------------------------- #
# pipeline → list / show / add / dry-run-count / edit / delete / toggle / run /
#   generate / generate-stream / runs / run-show / variants / select-variant /
#   auto-select / queue / moderation-list / moderation-view / publish / approve /
#   reject / bulk-approve / bulk-reject / refinement-steps / export / import /
#   templates / from-template / ai-edit / graph
#   + NESTED depth-2: filter (set/show/clear), node (add/replace/remove),
#     edge (add/remove)
#
# Every pipeline leaf builds the argparse Namespace ``pipeline_cmd._dispatch``
# reads and runs it via ``run_async`` — so the Typer path executes the exact
# same logic, including the ``generate-stream`` JSON-Lines streaming and the
# pool lifecycle. The argparse ``append`` (variadic) options are expressed as
# repeated Typer options (``--source 1 --source 2``); see the known-drift note
# on ``_pipeline_argv``.
# --------------------------------------------------------------------------- #

pipeline_app = typer.Typer(no_args_is_help=True, help="Content pipelines")
app.add_typer(pipeline_app, name="pipeline")

# Three nested depth-2 groups mounted via add_typer; the frozen
# ``pipeline filter|node|edge <action>`` paths are the fragile Wave-4 invariant.
pipeline_filter_app = typer.Typer(no_args_is_help=True, help="Manage a pipeline's message filter")
pipeline_app.add_typer(pipeline_filter_app, name="filter")
pipeline_node_app = typer.Typer(no_args_is_help=True, help="Manage pipeline graph nodes")
pipeline_app.add_typer(pipeline_node_app, name="node")
pipeline_edge_app = typer.Typer(no_args_is_help=True, help="Manage pipeline graph edges")
pipeline_app.add_typer(pipeline_edge_app, name="edge")


def _run_pipeline(ctx: typer.Context, pipeline_action: str, **ns_kwargs) -> None:
    """Build the Namespace a pipeline action dispatches on, then run it."""
    apply_startup(ctx)
    ns = argparse.Namespace(
        config=ctx.obj.config, pipeline_action=pipeline_action, **ns_kwargs
    )
    run_async(pipeline_cmd._dispatch(ns))


@pipeline_app.command("list")
def pipeline_list(ctx: typer.Context) -> None:
    """List pipelines."""
    _run_pipeline(ctx, "list")


@pipeline_app.command("show")
def pipeline_show(ctx: typer.Context, id: int = typer.Argument(..., help="Pipeline id")) -> None:
    """Show pipeline details."""
    _run_pipeline(ctx, "show", id=id)


@pipeline_app.command("add")
def pipeline_add(
    ctx: typer.Context,
    name: str = typer.Argument(..., help="Pipeline name"),
    prompt_template: str | None = typer.Option(
        None, "--prompt-template", help="Prompt template (required unless --json-file/--node is used)"
    ),
    json_file: str | None = typer.Option(None, "--json-file"),
    source: list[int] = typer.Option([], "--source", help="Source channel id (repeat for multiple)"),
    target: list[str] = typer.Option([], "--target", help="Target PHONE|DIALOG_ID (repeat for multiple)"),
    llm_model: str | None = typer.Option(None, "--llm-model"),
    image_model: str | None = typer.Option(None, "--image-model"),
    publish_mode: PublishMode = typer.Option(PublishMode.moderated, "--publish-mode"),
    generation_backend: GenerationBackend = typer.Option(GenerationBackend.chain, "--generation-backend"),
    interval: int = typer.Option(60, "--interval"),
    inactive: bool = typer.Option(False, "--inactive"),
    ab_variants: int = typer.Option(1, "--ab-variants"),
    ab_auto_select: bool = typer.Option(False, "--ab-auto-select"),
    node_specs: list[str] = typer.Option([], "--node", help="Node spec (repeat for multiple)"),
    edge: list[str] = typer.Option([], "--edge", help="Explicit edge FROM->TO (repeat)"),
    node_configs: list[str] = typer.Option([], "--node-config", help="Node config NODE=JSON (repeat)"),
    run_after: bool = typer.Option(False, "--run-after"),
    since_value: int = typer.Option(24, "--since-value"),
    since_unit: SinceUnit = typer.Option(SinceUnit.h, "--since-unit"),
) -> None:
    """Add a pipeline."""
    _run_pipeline(
        ctx, "add", name=name, prompt_template=prompt_template, json_file=json_file,
        source=list(source) or None, target=list(target) or None, llm_model=llm_model,
        image_model=image_model, publish_mode=publish_mode.value,
        generation_backend=generation_backend.value, interval=interval, inactive=inactive,
        ab_variants=ab_variants, ab_auto_select=ab_auto_select,
        node_specs=list(node_specs) or None, edge=list(edge) or None,
        node_configs=list(node_configs) or None, run_after=run_after,
        since_value=since_value, since_unit=since_unit.value,
    )


@pipeline_app.command("dry-run-count")
def pipeline_dry_run_count(
    ctx: typer.Context,
    source: list[int] = typer.Option(..., "--source", help="Source channel id (repeat for multiple)"),
    since_value: int = typer.Option(24, "--since-value"),
    since_unit: SinceUnit = typer.Option(SinceUnit.h, "--since-unit"),
) -> None:
    """Count messages for given sources."""
    _run_pipeline(ctx, "dry-run-count", source=list(source), since_value=since_value, since_unit=since_unit.value)


@pipeline_app.command("edit")
def pipeline_edit(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    name: str | None = typer.Option(None, "--name"),
    prompt_template: str | None = typer.Option(None, "--prompt-template"),
    source: list[int] = typer.Option([], "--source"),
    target: list[str] = typer.Option([], "--target"),
    llm_model: str | None = typer.Option(None, "--llm-model"),
    image_model: str | None = typer.Option(None, "--image-model"),
    publish_mode: PublishMode | None = typer.Option(None, "--publish-mode"),
    generation_backend: GenerationBackend | None = typer.Option(None, "--generation-backend"),
    interval: int | None = typer.Option(None, "--interval"),
    active: bool | None = typer.Option(
        None, "--active/--inactive", help="Set active (--active) or inactive (--inactive)"
    ),
    ab_variants: int | None = typer.Option(None, "--ab-variants"),
    ab_auto_select: bool | None = typer.Option(None, "--ab-auto-select/--no-ab-auto-select"),
) -> None:
    """Edit a pipeline."""
    _run_pipeline(
        ctx, "edit", id=id, name=name, prompt_template=prompt_template,
        source=list(source) or None, target=list(target) or None, llm_model=llm_model,
        image_model=image_model,
        publish_mode=publish_mode.value if publish_mode else None,
        generation_backend=generation_backend.value if generation_backend else None,
        interval=interval, active=active, ab_variants=ab_variants, ab_auto_select=ab_auto_select,
    )


@pipeline_app.command("delete")
def pipeline_delete(ctx: typer.Context, id: int = typer.Argument(..., help="Pipeline id")) -> None:
    """Delete a pipeline."""
    _run_pipeline(ctx, "delete", id=id)


@pipeline_app.command("toggle")
def pipeline_toggle(ctx: typer.Context, id: int = typer.Argument(..., help="Pipeline id")) -> None:
    """Toggle pipeline active state."""
    _run_pipeline(ctx, "toggle", id=id)


@pipeline_app.command("run")
def pipeline_run(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    preview: bool = typer.Option(False, "--preview"),
    publish: bool = typer.Option(False, "--publish"),
    limit: int = typer.Option(8, "--limit"),
    max_tokens: int = typer.Option(256, "--max-tokens"),
    temperature: float = typer.Option(0.0, "--temperature"),
) -> None:
    """Run pipeline generation (preview/publish)."""
    _run_pipeline(
        ctx, "run", id=id, preview=preview, publish=publish, limit=limit,
        max_tokens=max_tokens, temperature=temperature,
    )


@pipeline_app.command("generate")
def pipeline_generate(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    max_tokens: int = typer.Option(512, "--max-tokens"),
    temperature: float = typer.Option(0.7, "--temperature"),
    model: str | None = typer.Option(None, "--model"),
    preview: bool = typer.Option(False, "--preview"),
    ab_variants: int | None = typer.Option(None, "--ab-variants"),
    auto_select: bool = typer.Option(False, "--auto-select"),
) -> None:
    """Generate content for a pipeline."""
    _run_pipeline(
        ctx, "generate", id=id, max_tokens=max_tokens, temperature=temperature, model=model,
        preview=preview, ab_variants=ab_variants, auto_select=auto_select,
    )


@pipeline_app.command("generate-stream")
def pipeline_generate_stream(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    model: str | None = typer.Option(None, "--model"),
    max_tokens: int = typer.Option(256, "--max-tokens"),
    temperature: float = typer.Option(0.0, "--temperature"),
    limit: int = typer.Option(8, "--limit"),
) -> None:
    """Generate content for a pipeline, streaming JSON-Lines updates."""
    _run_pipeline(
        ctx, "generate-stream", id=id, model=model, max_tokens=max_tokens,
        temperature=temperature, limit=limit,
    )


@pipeline_app.command("runs")
def pipeline_runs(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    limit: int = typer.Option(20, "--limit"),
    status: str | None = typer.Option(None, "--status"),
) -> None:
    """List generation runs."""
    _run_pipeline(ctx, "runs", id=id, limit=limit, status=status)


@pipeline_app.command("run-show")
def pipeline_run_show(ctx: typer.Context, run_id: int = typer.Argument(..., help="Run id")) -> None:
    """Show generation run details."""
    _run_pipeline(ctx, "run-show", run_id=run_id)


@pipeline_app.command("variants")
def pipeline_variants(ctx: typer.Context, run_id: int = typer.Argument(..., help="Run id")) -> None:
    """List A/B variants."""
    _run_pipeline(ctx, "variants", run_id=run_id)


@pipeline_app.command("select-variant")
def pipeline_select_variant(
    ctx: typer.Context,
    run_id: int = typer.Argument(..., help="Run id"),
    index: int = typer.Argument(..., help="Variant index"),
) -> None:
    """Select an A/B variant."""
    _run_pipeline(ctx, "select-variant", run_id=run_id, index=index)


@pipeline_app.command("auto-select")
def pipeline_auto_select(ctx: typer.Context, run_id: int = typer.Argument(..., help="Run id")) -> None:
    """Auto-select the best A/B variant."""
    _run_pipeline(ctx, "auto-select", run_id=run_id)


@pipeline_app.command("queue")
def pipeline_queue(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    limit: int = typer.Option(20, "--limit"),
) -> None:
    """Show pending moderation queue for a pipeline."""
    _run_pipeline(ctx, "queue", id=id, limit=limit)


@pipeline_app.command("moderation-list")
def pipeline_moderation_list(
    ctx: typer.Context,
    pipeline_id: int | None = typer.Option(None, "--pipeline-id"),
    limit: int = typer.Option(20, "--limit"),
) -> None:
    """List pending moderation runs."""
    _run_pipeline(ctx, "moderation-list", pipeline_id=pipeline_id, limit=limit)


@pipeline_app.command("moderation-view")
def pipeline_moderation_view(ctx: typer.Context, run_id: int = typer.Argument(..., help="Run id")) -> None:
    """Show a moderation run's details."""
    _run_pipeline(ctx, "moderation-view", run_id=run_id)


@pipeline_app.command("publish")
def pipeline_publish(ctx: typer.Context, run_id: int = typer.Argument(..., help="Run id")) -> None:
    """Publish a generation run."""
    _run_pipeline(ctx, "publish", run_id=run_id)


@pipeline_app.command("approve")
def pipeline_approve(ctx: typer.Context, run_id: int = typer.Argument(..., help="Run id")) -> None:
    """Approve a generation run."""
    _run_pipeline(ctx, "approve", run_id=run_id)


@pipeline_app.command("reject")
def pipeline_reject(ctx: typer.Context, run_id: int = typer.Argument(..., help="Run id")) -> None:
    """Reject a generation run."""
    _run_pipeline(ctx, "reject", run_id=run_id)


@pipeline_app.command("bulk-approve")
def pipeline_bulk_approve(
    ctx: typer.Context,
    run_ids: list[int] = typer.Argument(..., help="Run ids"),
) -> None:
    """Approve multiple generation runs."""
    _run_pipeline(ctx, "bulk-approve", run_ids=list(run_ids))


@pipeline_app.command("bulk-reject")
def pipeline_bulk_reject(
    ctx: typer.Context,
    run_ids: list[int] = typer.Argument(..., help="Run ids"),
) -> None:
    """Reject multiple generation runs."""
    _run_pipeline(ctx, "bulk-reject", run_ids=list(run_ids))


@pipeline_app.command("refinement-steps")
def pipeline_refinement_steps(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    steps_json: str | None = typer.Option(None, "--set", help="Set refinement steps (JSON array)"),
) -> None:
    """View or set refinement steps."""
    _run_pipeline(ctx, "refinement-steps", id=id, steps_json=steps_json)


@pipeline_app.command("export")
def pipeline_export(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    output: str | None = typer.Option(None, "--output", "-o"),
    force: bool = typer.Option(False, "--force", "-f"),
) -> None:
    """Export a pipeline as JSON."""
    _run_pipeline(ctx, "export", id=id, output=output, force=force)


@pipeline_app.command("import")
def pipeline_import(
    ctx: typer.Context,
    file: str = typer.Argument(..., help="Path to JSON file"),
    name: str | None = typer.Option(None, "--name"),
) -> None:
    """Import a pipeline from a JSON file."""
    _run_pipeline(ctx, "import", file=file, name=name)


@pipeline_app.command("templates")
def pipeline_templates(
    ctx: typer.Context,
    category: str | None = typer.Option(None, "--category"),
) -> None:
    """List available pipeline templates."""
    _run_pipeline(ctx, "templates", category=category)


@pipeline_app.command("from-template")
def pipeline_from_template(
    ctx: typer.Context,
    template_id: int = typer.Argument(..., help="Template id"),
    name: str = typer.Argument(..., help="Pipeline name"),
    source_ids: str = typer.Option("", "--source-ids"),
    target_refs: str = typer.Option("", "--target-refs"),
) -> None:
    """Create a pipeline from a template."""
    _run_pipeline(
        ctx, "from-template", template_id=template_id, name=name,
        source_ids=source_ids, target_refs=target_refs,
    )


@pipeline_app.command("ai-edit")
def pipeline_ai_edit(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    instruction: str = typer.Argument(..., help="Instruction for the LLM"),
    show: bool = typer.Option(False, "--show"),
) -> None:
    """Edit a pipeline's JSON via an LLM instruction."""
    _run_pipeline(ctx, "ai-edit", id=id, instruction=instruction, show=show)


@pipeline_app.command("graph")
def pipeline_graph(ctx: typer.Context, id: int = typer.Argument(..., help="Pipeline id")) -> None:
    """Show a pipeline's graph (ASCII)."""
    _run_pipeline(ctx, "graph", id=id)


# ---- nested: pipeline filter <action> ------------------------------------- #


@pipeline_filter_app.command("set")
def pipeline_filter_set(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    message_kinds: list[str] = typer.Option([], "--message-kind"),
    service_actions: list[str] = typer.Option([], "--service-action"),
    media_types: list[str] = typer.Option([], "--media-type"),
    sender_kinds: list[str] = typer.Option([], "--sender-kind"),
    keywords: list[str] = typer.Option([], "--keyword"),
    regex: str | None = typer.Option(None, "--regex"),
    forwarded: TriBool | None = typer.Option(None, "--forwarded"),
    has_text: TriBool | None = typer.Option(None, "--has-text"),
) -> None:
    """Set a pipeline's message filter."""
    _run_pipeline(
        ctx, "filter", filter_action="set", id=id,
        message_kinds=list(message_kinds) or None, service_actions=list(service_actions) or None,
        media_types=list(media_types) or None, sender_kinds=list(sender_kinds) or None,
        keywords=list(keywords) or None, regex=regex,
        forwarded=forwarded.value if forwarded else None,
        has_text=has_text.value if has_text else None,
    )


@pipeline_filter_app.command("show")
def pipeline_filter_show(ctx: typer.Context, id: int = typer.Argument(..., help="Pipeline id")) -> None:
    """Show a pipeline's message filter."""
    _run_pipeline(ctx, "filter", filter_action="show", id=id)


@pipeline_filter_app.command("clear")
def pipeline_filter_clear(ctx: typer.Context, id: int = typer.Argument(..., help="Pipeline id")) -> None:
    """Clear a pipeline's message filter."""
    _run_pipeline(ctx, "filter", filter_action="clear", id=id)


# ---- nested: pipeline node <action> --------------------------------------- #


@pipeline_node_app.command("add")
def pipeline_node_add(
    ctx: typer.Context,
    pipeline_id: int = typer.Argument(..., help="Pipeline id"),
    node_spec: str = typer.Argument(..., help="Node spec: type:key=value,..."),
) -> None:
    """Add a node to a pipeline graph."""
    _run_pipeline(ctx, "node", node_action="add", pipeline_id=pipeline_id, node_spec=node_spec)


@pipeline_node_app.command("replace")
def pipeline_node_replace(
    ctx: typer.Context,
    pipeline_id: int = typer.Argument(..., help="Pipeline id"),
    node_id: str = typer.Argument(..., help="Node ID to replace"),
    node_spec: str = typer.Argument(..., help="New node spec: type:key=value,..."),
) -> None:
    """Replace a node in a pipeline graph."""
    _run_pipeline(ctx, "node", node_action="replace", pipeline_id=pipeline_id, node_id=node_id, node_spec=node_spec)


@pipeline_node_app.command("remove")
def pipeline_node_remove(
    ctx: typer.Context,
    pipeline_id: int = typer.Argument(..., help="Pipeline id"),
    node_id: str = typer.Argument(..., help="Node ID to remove"),
) -> None:
    """Remove a node from a pipeline graph."""
    _run_pipeline(ctx, "node", node_action="remove", pipeline_id=pipeline_id, node_id=node_id)


# ---- nested: pipeline edge <action> --------------------------------------- #


@pipeline_edge_app.command("add")
def pipeline_edge_add(
    ctx: typer.Context,
    pipeline_id: int = typer.Argument(..., help="Pipeline id"),
    from_node: str = typer.Argument(..., help="Source node ID"),
    to_node: str = typer.Argument(..., help="Target node ID"),
) -> None:
    """Add an edge to a pipeline graph."""
    _run_pipeline(ctx, "edge", edge_action="add", pipeline_id=pipeline_id, from_node=from_node, to_node=to_node)


@pipeline_edge_app.command("remove")
def pipeline_edge_remove(
    ctx: typer.Context,
    pipeline_id: int = typer.Argument(..., help="Pipeline id"),
    from_node: str = typer.Argument(..., help="Source node ID"),
    to_node: str = typer.Argument(..., help="Target node ID"),
) -> None:
    """Remove an edge from a pipeline graph."""
    _run_pipeline(ctx, "edge", edge_action="remove", pipeline_id=pipeline_id, from_node=from_node, to_node=to_node)


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
    elif command == "analytics":
        argv.append("analytics")
        argv += _analytics_argv(args)
    elif command == "channel":
        argv.append("channel")
        argv += _channel_argv(args)
    elif command == "dialogs":
        argv.append("dialogs")
        argv += _dialogs_argv(args)
    elif command == "pipeline":
        argv.append("pipeline")
        argv += _pipeline_argv(args)
    return argv


def _append_opt(tail: list[str], flag: str, values) -> None:
    """Emit an argparse ``append`` option as repeated Typer flags.

    argparse ``action="append"`` accumulates ``--source 1 --source 2`` into a
    list; Click options can't be variadic (no ``nargs="+"`` on options), so the
    Typer leaf models the same flag as a repeatable option. On the prod
    round-trip we re-emit each list element as its own ``--flag value`` pair.
    """
    for value in values or []:
        tail += [flag, str(value)]


def _pipeline_argv(args: argparse.Namespace) -> list[str]:
    """argv tail for ``pipeline`` — the action plus its flags / positionals.

    Handles the three depth-2 nested groups (``filter`` / ``node`` / ``edge``)
    and the flat ``graph``. Free-text positionals (pipeline name, node specs,
    node/edge ids, ai-edit instruction, import file) are emitted after a ``--``
    separator. argparse ``append`` options re-emit as repeated flags via
    :func:`_append_opt` (the Click variadic-option known-drift, same class as
    Wave 3 ``--files``).
    """
    action = getattr(args, "pipeline_action", None)
    if action is None:
        return []
    tail = [action]

    if action in (
        "show", "delete", "toggle", "run-show", "variants", "auto-select",
        "moderation-view", "publish", "approve", "reject", "graph",
    ):
        # single positional id / run_id
        ident = getattr(args, "run_id", None)
        if ident is None:
            ident = args.id
        tail += ["--", str(ident)]
    elif action == "list":
        pass
    elif action == "add":
        if getattr(args, "prompt_template", None):
            tail += ["--prompt-template", args.prompt_template]
        if getattr(args, "json_file", None):
            tail += ["--json-file", args.json_file]
        _append_opt(tail, "--source", getattr(args, "source", None))
        _append_opt(tail, "--target", getattr(args, "target", None))
        if getattr(args, "llm_model", None):
            tail += ["--llm-model", args.llm_model]
        if getattr(args, "image_model", None):
            tail += ["--image-model", args.image_model]
        if getattr(args, "publish_mode", "moderated") != "moderated":
            tail += ["--publish-mode", args.publish_mode]
        if getattr(args, "generation_backend", "chain") != "chain":
            tail += ["--generation-backend", args.generation_backend]
        if getattr(args, "interval", 60) != 60:
            tail += ["--interval", str(args.interval)]
        if getattr(args, "inactive", False):
            tail.append("--inactive")
        if (getattr(args, "ab_variants", 1) or 1) != 1:
            tail += ["--ab-variants", str(args.ab_variants)]
        if getattr(args, "ab_auto_select", False):
            tail.append("--ab-auto-select")
        _append_opt(tail, "--node", getattr(args, "node_specs", None))
        _append_opt(tail, "--edge", getattr(args, "edge", None))
        _append_opt(tail, "--node-config", getattr(args, "node_configs", None))
        if getattr(args, "run_after", False):
            tail.append("--run-after")
        if getattr(args, "since_value", 24) != 24:
            tail += ["--since-value", str(args.since_value)]
        if getattr(args, "since_unit", "h") != "h":
            tail += ["--since-unit", args.since_unit]
        tail += ["--", args.name]
    elif action == "dry-run-count":
        _append_opt(tail, "--source", getattr(args, "source", None))
        if getattr(args, "since_value", 24) != 24:
            tail += ["--since-value", str(args.since_value)]
        if getattr(args, "since_unit", "h") != "h":
            tail += ["--since-unit", args.since_unit]
    elif action == "edit":
        if getattr(args, "name", None):
            tail += ["--name", args.name]
        if getattr(args, "prompt_template", None):
            tail += ["--prompt-template", args.prompt_template]
        _append_opt(tail, "--source", getattr(args, "source", None))
        _append_opt(tail, "--target", getattr(args, "target", None))
        if getattr(args, "llm_model", None) is not None:
            tail += ["--llm-model", args.llm_model]
        if getattr(args, "image_model", None) is not None:
            tail += ["--image-model", args.image_model]
        if getattr(args, "publish_mode", None):
            tail += ["--publish-mode", args.publish_mode]
        if getattr(args, "generation_backend", None):
            tail += ["--generation-backend", args.generation_backend]
        if getattr(args, "interval", None) is not None:
            tail += ["--interval", str(args.interval)]
        active = getattr(args, "active", None)
        if active is True:
            tail.append("--active")
        elif active is False:
            tail.append("--inactive")
        if getattr(args, "ab_variants", None) is not None:
            tail += ["--ab-variants", str(args.ab_variants)]
        ab_auto = getattr(args, "ab_auto_select", None)
        if ab_auto is True:
            tail.append("--ab-auto-select")
        elif ab_auto is False:
            tail.append("--no-ab-auto-select")
        tail += ["--", str(args.id)]
    elif action == "run":
        if getattr(args, "preview", False):
            tail.append("--preview")
        if getattr(args, "publish", False):
            tail.append("--publish")
        if getattr(args, "limit", 8) != 8:
            tail += ["--limit", str(args.limit)]
        if getattr(args, "max_tokens", 256) != 256:
            tail += ["--max-tokens", str(args.max_tokens)]
        if getattr(args, "temperature", 0.0) != 0.0:
            tail += ["--temperature", str(args.temperature)]
        tail += ["--", str(args.id)]
    elif action == "generate":
        if getattr(args, "max_tokens", 512) != 512:
            tail += ["--max-tokens", str(args.max_tokens)]
        if getattr(args, "temperature", 0.7) != 0.7:
            tail += ["--temperature", str(args.temperature)]
        if getattr(args, "model", None):
            tail += ["--model", args.model]
        if getattr(args, "preview", False):
            tail.append("--preview")
        if getattr(args, "ab_variants", None) is not None:
            tail += ["--ab-variants", str(args.ab_variants)]
        if getattr(args, "auto_select", False):
            tail.append("--auto-select")
        tail += ["--", str(args.id)]
    elif action == "generate-stream":
        if getattr(args, "model", None):
            tail += ["--model", args.model]
        if getattr(args, "max_tokens", 256) != 256:
            tail += ["--max-tokens", str(args.max_tokens)]
        if getattr(args, "temperature", 0.0) != 0.0:
            tail += ["--temperature", str(args.temperature)]
        if getattr(args, "limit", 8) != 8:
            tail += ["--limit", str(args.limit)]
        tail += ["--", str(args.id)]
    elif action == "runs":
        if getattr(args, "limit", 20) != 20:
            tail += ["--limit", str(args.limit)]
        if getattr(args, "status", None):
            tail += ["--status", args.status]
        tail += ["--", str(args.id)]
    elif action == "select-variant":
        tail += ["--", str(args.run_id), str(args.index)]
    elif action == "queue":
        if getattr(args, "limit", 20) != 20:
            tail += ["--limit", str(args.limit)]
        tail += ["--", str(args.id)]
    elif action == "moderation-list":
        if getattr(args, "pipeline_id", None) is not None:
            tail += ["--pipeline-id", str(args.pipeline_id)]
        if getattr(args, "limit", 20) != 20:
            tail += ["--limit", str(args.limit)]
    elif action in ("bulk-approve", "bulk-reject"):
        tail += ["--", *(str(r) for r in args.run_ids)]
    elif action == "refinement-steps":
        if getattr(args, "steps_json", None):
            tail += ["--set", args.steps_json]
        tail += ["--", str(args.id)]
    elif action == "export":
        if getattr(args, "output", None):
            tail += ["--output", args.output]
        if getattr(args, "force", False):
            tail.append("--force")
        tail += ["--", str(args.id)]
    elif action == "import":
        if getattr(args, "name", None):
            tail += ["--name", args.name]
        tail += ["--", args.file]
    elif action == "templates":
        if getattr(args, "category", None):
            tail += ["--category", args.category]
    elif action == "from-template":
        if getattr(args, "source_ids", ""):
            tail += ["--source-ids", args.source_ids]
        if getattr(args, "target_refs", ""):
            tail += ["--target-refs", args.target_refs]
        tail += ["--", str(args.template_id), args.name]
    elif action == "ai-edit":
        if getattr(args, "show", False):
            tail.append("--show")
        tail += ["--", str(args.id), args.instruction]
    elif action == "filter":
        tail += _pipeline_filter_argv(args)
    elif action == "node":
        tail += _pipeline_node_argv(args)
    elif action == "edge":
        tail += _pipeline_edge_argv(args)
    return tail


def _pipeline_filter_argv(args: argparse.Namespace) -> list[str]:
    """argv tail for the nested ``pipeline filter`` group (depth-2)."""
    filter_action = getattr(args, "filter_action", None)
    if not filter_action:
        return []
    tail = [filter_action]
    if filter_action == "set":
        _append_opt(tail, "--message-kind", getattr(args, "message_kinds", None))
        _append_opt(tail, "--service-action", getattr(args, "service_actions", None))
        _append_opt(tail, "--media-type", getattr(args, "media_types", None))
        _append_opt(tail, "--sender-kind", getattr(args, "sender_kinds", None))
        _append_opt(tail, "--keyword", getattr(args, "keywords", None))
        if getattr(args, "regex", None):
            tail += ["--regex", args.regex]
        if getattr(args, "forwarded", None):
            tail += ["--forwarded", args.forwarded]
        if getattr(args, "has_text", None):
            tail += ["--has-text", args.has_text]
        tail += ["--", str(args.id)]
    else:  # show / clear
        tail += ["--", str(args.id)]
    return tail


def _pipeline_node_argv(args: argparse.Namespace) -> list[str]:
    """argv tail for the nested ``pipeline node`` group (depth-2)."""
    node_action = getattr(args, "node_action", None)
    if not node_action:
        return []
    tail = [node_action]
    if node_action == "add":
        tail += ["--", str(args.pipeline_id), args.node_spec]
    elif node_action == "replace":
        tail += ["--", str(args.pipeline_id), args.node_id, args.node_spec]
    elif node_action == "remove":
        tail += ["--", str(args.pipeline_id), args.node_id]
    return tail


def _pipeline_edge_argv(args: argparse.Namespace) -> list[str]:
    """argv tail for the nested ``pipeline edge`` group (depth-2)."""
    edge_action = getattr(args, "edge_action", None)
    if not edge_action:
        return []
    tail = [edge_action, "--", str(args.pipeline_id), args.from_node, args.to_node]
    return tail


def _dialogs_argv(args: argparse.Namespace) -> list[str]:
    """argv tail for ``dialogs`` — the action plus its flags / positionals.

    Handles the depth-2 ``dialogs queue`` nested group. Chat / user / recipient
    identifiers and message text are free text (may start with ``-`` — e.g. a
    negative chat id), so every positional is emitted after a ``--`` separator,
    with options before it. ``leave`` / ``forward`` / ``delete-message`` carry a
    variadic ``nargs="+"`` positional list, emitted as the final tokens.
    """
    action = getattr(args, "dialogs_action", None)
    if action is None:
        return []
    tail = [action]

    if action in ("list", "refresh"):
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
    elif action == "resolve":
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
        tail += ["--", args.identifier]
    elif action == "leave":
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
        if getattr(args, "yes", False):
            tail.append("--yes")
        tail += ["--", *args.dialog_ids]
    elif action == "join":
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
        if getattr(args, "yes", False):
            tail.append("--yes")
        tail += ["--", args.target]
    elif action == "topics":
        tail += ["--channel-id", str(args.channel_id)]
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
    elif action == "cache-clear":
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
    elif action == "cache-status":
        pass
    elif action == "send":
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
        if getattr(args, "yes", False):
            tail.append("--yes")
        tail += ["--", args.recipient, args.text]
    elif action == "forward":
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
        if getattr(args, "yes", False):
            tail.append("--yes")
        tail += ["--", args.from_chat, args.to_chat, *args.message_ids]
    elif action == "edit-message":
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
        if getattr(args, "yes", False):
            tail.append("--yes")
        tail += ["--", args.chat_id, str(args.message_id), args.text]
    elif action == "delete-message":
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
        if getattr(args, "yes", False):
            tail.append("--yes")
        tail += ["--", args.chat_id, *args.message_ids]
    elif action == "create-channel":
        tail += ["--title", args.title]
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
        if getattr(args, "about", ""):
            tail += ["--about", args.about]
        if getattr(args, "username", ""):
            tail += ["--username", args.username]
    elif action == "create-group":
        tail += ["--title", args.title]
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
        if getattr(args, "about", ""):
            tail += ["--about", args.about]
    elif action == "pin-message":
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
        if getattr(args, "notify", False):
            tail.append("--notify")
        if getattr(args, "yes", False):
            tail.append("--yes")
        tail += ["--", args.chat_id, str(args.message_id)]
    elif action == "react":
        if getattr(args, "clear", False):
            tail.append("--clear")
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
        if getattr(args, "yes", False):
            tail.append("--yes")
        tail += ["--", args.chat_id, str(args.message_id)]
        if getattr(args, "emoji", None):
            tail.append(args.emoji)
    elif action == "unpin-message":
        if getattr(args, "message_id", None) is not None:
            tail += ["--message-id", str(args.message_id)]
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
        if getattr(args, "yes", False):
            tail.append("--yes")
        tail += ["--", args.chat_id]
    elif action == "download-media":
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
        if getattr(args, "output_dir", ".") != ".":
            tail += ["--output-dir", args.output_dir]
        tail += ["--", args.chat_id, str(args.message_id)]
    elif action == "participants":
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
        if getattr(args, "limit", 200) != 200:
            tail += ["--limit", str(args.limit)]
        if getattr(args, "search", ""):
            tail += ["--search", args.search]
        tail += ["--", args.chat_id]
    elif action == "edit-admin":
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
        if getattr(args, "title", None):
            tail += ["--title", args.title]
        # tri-state via --is-admin/--no-admin (argparse default True)
        if not getattr(args, "is_admin", True):
            tail.append("--no-admin")
        if getattr(args, "yes", False):
            tail.append("--yes")
        tail += ["--", args.chat_id, args.user_id]
    elif action == "edit-permissions":
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
        if getattr(args, "until_date", None):
            tail += ["--until-date", args.until_date]
        if getattr(args, "send_messages", None) is not None:
            tail += ["--send-messages", args.send_messages]
        if getattr(args, "send_media", None) is not None:
            tail += ["--send-media", args.send_media]
        if getattr(args, "yes", False):
            tail.append("--yes")
        tail += ["--", args.chat_id, args.user_id]
    elif action == "kick":
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
        if getattr(args, "yes", False):
            tail.append("--yes")
        tail += ["--", args.chat_id, args.user_id]
    elif action == "broadcast-stats":
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
        tail += ["--", args.chat_id]
    elif action in ("archive", "unarchive"):
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
        tail += ["--", args.chat_id]
    elif action == "mark-read":
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
        if getattr(args, "max_id", None) is not None:
            tail += ["--max-id", str(args.max_id)]
        tail += ["--", args.chat_id]
    elif action == "queue":
        tail += _dialogs_queue_argv(args)
    return tail


def _dialogs_queue_argv(args: argparse.Namespace) -> list[str]:
    """argv tail for the nested ``dialogs queue`` group (depth-2).

    Returns ``[<queue_action>, …]`` appended after the ``"queue"`` token. The
    only positional is ``cancel``'s integer ``command_id`` (emitted after ``--``
    for symmetry; it is always positive).
    """
    queue_action = getattr(args, "queue_action", None)
    if not queue_action:
        return []
    tail = [queue_action]
    if queue_action == "status":
        if getattr(args, "command_type", None):
            tail += ["--command-type", args.command_type]
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
        if getattr(args, "limit", 20) != 20:
            tail += ["--limit", str(args.limit)]
    elif queue_action == "cancel":
        if getattr(args, "yes", False):
            tail.append("--yes")
        tail += ["--", str(args.command_id)]
    elif queue_action == "clear-pending":
        if getattr(args, "command_type", None):
            tail += ["--command-type", args.command_type]
        if getattr(args, "phone", None):
            tail += ["--phone", args.phone]
        if getattr(args, "yes", False):
            tail.append("--yes")
    return tail


def _channel_argv(args: argparse.Namespace) -> list[str]:
    """argv tail for ``channel`` — the action plus its flags / positionals.

    Handles the depth-2 ``channel tag`` nested group: a ``tag`` action emits
    ``["tag", <tag_action>, …]`` so the nested Typer sub-app routes it. Channel
    identifiers / tag names are free text (may start with ``-``), so every
    positional is emitted after a ``--`` separator to survive Click's option
    parsing — matching the collect/search/messages pattern.
    """
    action = getattr(args, "channel_action", None)
    if action is None:
        return []
    tail = [action]
    if action in ("add", "delete", "toggle", "review-confirm", "review-keep"):
        tail += ["--", args.identifier]
    elif action == "collect":
        if getattr(args, "full", False):
            tail.append("--full")
        tail += ["--", args.identifier]
    elif action == "import":
        tail += ["--", args.source]
    elif action == "stats":
        if getattr(args, "all", False):
            tail.append("--all")
        if getattr(args, "max_channels", None) is not None:
            tail += ["--max-channels", str(args.max_channels)]
        if getattr(args, "identifier", None):
            tail += ["--", args.identifier]
    elif action == "refresh-meta":
        if getattr(args, "all", False):
            tail.append("--all")
        if getattr(args, "identifier", None):
            tail += ["--", args.identifier]
    elif action == "add-bulk":
        tail += ["--phone", args.phone, "--dialog-ids", args.dialog_ids]
    elif action == "list-for-import":
        if getattr(args, "json", False):
            tail.append("--json")
    elif action == "tag":
        tail += _channel_tag_argv(args)
    return tail


def _channel_tag_argv(args: argparse.Namespace) -> list[str]:
    """argv tail for the nested ``channel tag`` group (depth-2).

    Returns ``[<tag_action>, …]`` — appended after the ``"tag"`` token already
    emitted by :func:`_channel_argv`. The positional tag ``name`` / ``tags`` are
    free text, so they follow a ``--`` separator (``set`` carries two
    positionals: pk then the comma-joined tag list).
    """
    tag_action = getattr(args, "tag_action", None)
    if not tag_action:
        return []
    tail = [tag_action]
    if tag_action in ("add", "delete"):
        tail += ["--", args.name]
    elif tag_action == "set":
        tail += ["--", str(args.pk), args.tags]
    elif tag_action == "get":
        tail += ["--", str(args.pk)]
    return tail


def _analytics_argv(args: argparse.Namespace) -> list[str]:
    """argv tail for ``analytics`` — the action plus its flags / positional.

    ``analytics`` has no nested sub-groups; each action maps to a flat Typer
    leaf. A bare ``analytics`` (no action) returns an empty tail so the Typer
    ``analytics_app`` (``no_args_is_help=True``) raises ``NoArgsIsHelpError``
    and ``dispatch_via_typer`` renders help / exits 0 — matching the argparse
    prod path (``main.py`` reparses ``analytics --help`` when the action is
    missing). It must NOT default to ``top``: argparse never ran ``top`` for a
    bare ``analytics``, so doing so would open the DB and emit a top-messages
    report instead of usage (a visible parity regression). The two channel
    actions take a positional ``channel_id`` int, emitted after ``--`` so a
    negative ``analytics channel`` id survives Click.
    """
    action = getattr(args, "analytics_action", None)
    if action is None:
        return []
    tail = [action]
    if action == "top":
        if getattr(args, "limit", 20) != 20:
            tail += ["--limit", str(args.limit)]
        if getattr(args, "date_from", None):
            tail += ["--date-from", args.date_from]
        if getattr(args, "date_to", None):
            tail += ["--date-to", args.date_to]
    elif action in ("content-types", "hourly"):
        if getattr(args, "date_from", None):
            tail += ["--date-from", args.date_from]
        if getattr(args, "date_to", None):
            tail += ["--date-to", args.date_to]
    elif action == "daily":
        if getattr(args, "days", 30) != 30:
            tail += ["--days", str(args.days)]
        if getattr(args, "pipeline_id", None) is not None:
            tail += ["--pipeline-id", str(args.pipeline_id)]
    elif action == "pipeline-stats":
        if getattr(args, "pipeline_id", None) is not None:
            tail += ["--pipeline-id", str(args.pipeline_id)]
    elif action in ("trending-topics", "trending-channels", "trending-emojis"):
        if getattr(args, "days", 7) != 7:
            tail += ["--days", str(args.days)]
        if getattr(args, "limit", 20) != 20:
            tail += ["--limit", str(args.limit)]
    elif action == "velocity":
        if getattr(args, "days", 30) != 30:
            tail += ["--days", str(args.days)]
    elif action == "calendar":
        if getattr(args, "limit", 20) != 20:
            tail += ["--limit", str(args.limit)]
        if getattr(args, "pipeline_id", None) is not None:
            tail += ["--pipeline-id", str(args.pipeline_id)]
    elif action == "channel":
        if getattr(args, "days", 30) != 30:
            tail += ["--days", str(args.days)]
        tail += ["--", str(args.channel_id)]
    elif action == "channel-rating":
        if getattr(args, "useful", None):
            tail += ["--useful", args.useful]
        if getattr(args, "genre", None):
            tail += ["--genre", args.genre]
        if getattr(args, "limit", 50) != 50:
            tail += ["--limit", str(args.limit)]
    elif action == "channel-rate":
        if getattr(args, "model", None):
            tail += ["--model", args.model]
        if getattr(args, "sample_size", 40) != 40:
            tail += ["--sample-size", str(args.sample_size)]
        tail += ["--", str(args.channel_id)]
    return tail


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
