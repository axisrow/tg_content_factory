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

import argparse
from enum import Enum

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
from src.cli.typer_app import app, apply_startup, run_async
from src.filters.criteria import DEFAULT_QUICK_SAMPLE_SIZE

#: Context settings for the commands whose first positional can be a *negative*
#: identifier — Telegram channel / chat ids (``-100123…``), ``@username`` /
#: dialog identifiers, search queries, etc. argparse accepted ``search -100500`` /
#: ``dialogs archive -100… --yes`` directly because no option in this CLI is a
#: negative number, so a ``-N`` token was unambiguously a value. Click instead
#: reads ``-100500`` as an unknown option and errors. ``ignore_unknown_options``
#: restores argparse's behaviour: a ``-N`` token falls through to the positional,
#: options and the negative interleave freely (``-100 --yes`` and ``--yes -100``
#: both work), and a genuinely unknown ``--option`` still errors with exit 2 *when
#: every positional slot is already filled*. This replaces the fragile
#: argv-``--``-insertion the #1125 review rejected (string-munging could not
#: reproduce argparse's free interleaving).
#:
#: **Applied narrowly (#1162 review):** only the ~34 commands whose first
#: positional is a Telegram-id / identifier / query (``channel_id`` / ``chat_id`` /
#: ``identifier`` / ``recipient`` / ``from_chat`` / ``query`` / ``message_id`` /
#: ``source`` / ``target``) carry this. Commands keyed on a *positive* DB primary
#: key (``pipeline <id>``, ``agent context <thread_id>``, ``search-query <id>``, …)
#: keep Click's strict option checking, so an ``--typo`` on those still errors.
#:
#: **Accepted trade-off (#1162 review):** on the negative-capable commands above,
#: ``ignore_unknown_options`` cannot distinguish a real negative value from an
#: unknown dash token, so an unknown option (``-x`` *or* ``--xyz``) that lands in
#: front of an *open* string positional is absorbed as that positional instead of
#: erroring — e.g. ``search --typo`` searches for the literal ``--typo`` rather
#: than exiting 2. This is strictly more permissive (no valid invocation breaks)
#: and the operator's negative-id workflow (Telegram ids are negative everywhere)
#: is the priority; the loss of strict ``--typo`` detection on this subset is the
#: accepted cost.
_NEG_ID_POSITIONAL = {"ignore_unknown_options": True}


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


class PhotoMode(str, Enum):
    """``photo-loader … --mode`` choices — mirrors the argparse ``choices=[…]``.

    Subclasses ``str`` so the command body receives a plain ``"album"``/``"separate"``
    and ``.value`` round-trips cleanly into the *_impl bodies.
    """

    album = "album"
    separate = "separate"




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


@collect_app.command("sample", context_settings=_NEG_ID_POSITIONAL)
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

messages_app = typer.Typer(no_args_is_help=True, help="Read messages from DB or live Telegram")
app.add_typer(messages_app, name="messages")


@messages_app.command("read", context_settings=_NEG_ID_POSITIONAL)
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


@debug_app.command("errors")
def debug_errors(
    ctx: typer.Context,
    as_json: bool = typer.Option(False, "--json", help="Emit raw JSON instead of text"),
) -> None:
    """Show aggregated provider error-recovery stats (#1055)."""
    apply_startup(ctx)
    run_async(debug_cmd.errors_impl(ctx.obj.config, as_json=as_json))


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


@analytics_app.command("channel", context_settings=_NEG_ID_POSITIONAL)
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


@analytics_app.command("channel-rate", context_settings=_NEG_ID_POSITIONAL)
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


@channel_app.command("add", context_settings=_NEG_ID_POSITIONAL)
def channel_add(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Username, link, or numeric ID"),
) -> None:
    """Add a channel."""
    apply_startup(ctx)
    run_async(channel_cmd.add_impl(ctx.obj.config, identifier=identifier))


@channel_app.command("delete", context_settings=_NEG_ID_POSITIONAL)
def channel_delete(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Channel pk, channel_id, or @username"),
) -> None:
    """Delete a channel."""
    apply_startup(ctx)
    run_async(channel_cmd.delete_impl(ctx.obj.config, identifier=identifier))


@channel_app.command("toggle", context_settings=_NEG_ID_POSITIONAL)
def channel_toggle(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Channel pk, channel_id, or @username"),
) -> None:
    """Toggle channel active state."""
    apply_startup(ctx)
    run_async(channel_cmd.toggle_impl(ctx.obj.config, identifier=identifier))


@channel_app.command("collect", context_settings=_NEG_ID_POSITIONAL)
def channel_collect(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Channel pk, channel_id, or @username"),
    full: bool = typer.Option(False, "--full", help="Explicitly backfill the full channel history"),
) -> None:
    """Collect a single channel one-shot."""
    apply_startup(ctx)
    run_async(channel_cmd.collect_impl(ctx.obj.config, identifier=identifier, full=full))


@channel_app.command("stats", context_settings=_NEG_ID_POSITIONAL)
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


@channel_app.command("refresh-meta", context_settings=_NEG_ID_POSITIONAL)
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


@channel_app.command("review-confirm", context_settings=_NEG_ID_POSITIONAL)
def channel_review_confirm(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Channel pk, channel_id, or @username"),
) -> None:
    """Confirm a quarantined channel is dead and deactivate it."""
    apply_startup(ctx)
    run_async(channel_cmd.review_confirm_impl(ctx.obj.config, identifier=identifier))


@channel_app.command("review-keep", context_settings=_NEG_ID_POSITIONAL)
def channel_review_keep(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Channel pk, channel_id, or @username"),
) -> None:
    """Clear a channel's review flag and keep it active."""
    apply_startup(ctx)
    run_async(channel_cmd.review_keep_impl(ctx.obj.config, identifier=identifier))


@channel_app.command("import", context_settings=_NEG_ID_POSITIONAL)
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


@dialogs_app.command("resolve", context_settings=_NEG_ID_POSITIONAL)
def dialogs_resolve(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Identifier to resolve"),
    phone: str | None = typer.Option(None, "--phone", help="Preferred account phone"),
) -> None:
    """Resolve @username, t.me link, or numeric ID."""
    _run_dialogs(ctx, "resolve", identifier=identifier, phone=phone)


@dialogs_app.command("leave", context_settings=_NEG_ID_POSITIONAL)
def dialogs_leave(
    ctx: typer.Context,
    dialog_ids: list[str] = typer.Argument(..., help="Dialog IDs to leave (space- or comma-separated)"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Leave dialogs by ID."""
    _run_dialogs(ctx, "leave", dialog_ids=dialog_ids, phone=phone, yes=yes)


@dialogs_app.command("delete", context_settings=_NEG_ID_POSITIONAL)
def dialogs_delete(
    ctx: typer.Context,
    dialog_ids: list[str] = typer.Argument(..., help="Dialog IDs to delete (space- or comma-separated)"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Permanently delete dialogs by ID (DeleteChannel/DeleteChat)."""
    _run_dialogs(ctx, "delete", dialog_ids=dialog_ids, phone=phone, yes=yes)


@dialogs_app.command("join", context_settings=_NEG_ID_POSITIONAL)
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


@dialogs_app.command("send", context_settings=_NEG_ID_POSITIONAL)
def dialogs_send(
    ctx: typer.Context,
    recipient: str = typer.Argument(..., help="Recipient: @username, phone number, or numeric ID"),
    text: str = typer.Argument(..., help="Message text to send"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Send a direct message to a user or chat."""
    _run_dialogs(ctx, "send", recipient=recipient, text=text, phone=phone, yes=yes)


@dialogs_app.command("forward", context_settings=_NEG_ID_POSITIONAL)
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


@dialogs_app.command("edit-message", context_settings=_NEG_ID_POSITIONAL)
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


@dialogs_app.command("delete-message", context_settings=_NEG_ID_POSITIONAL)
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


@dialogs_app.command("pin-message", context_settings=_NEG_ID_POSITIONAL)
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


@dialogs_app.command("react", context_settings=_NEG_ID_POSITIONAL)
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


@dialogs_app.command("unpin-message", context_settings=_NEG_ID_POSITIONAL)
def dialogs_unpin_message(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    message_id: int | None = typer.Option(None, "--message-id", help="Message ID to unpin (omit to unpin all)"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Unpin a message in a chat."""
    _run_dialogs(ctx, "unpin-message", chat_id=chat_id, message_id=message_id, phone=phone, yes=yes)


@dialogs_app.command("download-media", context_settings=_NEG_ID_POSITIONAL)
def dialogs_download_media(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    message_id: int = typer.Argument(..., help="Message ID containing media"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    output_dir: str = typer.Option(".", "--output-dir", help="Directory to save file (default: current dir)"),
) -> None:
    """Download media from a message."""
    _run_dialogs(ctx, "download-media", chat_id=chat_id, message_id=message_id, phone=phone, output_dir=output_dir)


@dialogs_app.command("participants", context_settings=_NEG_ID_POSITIONAL)
def dialogs_participants(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    limit: int = typer.Option(200, "--limit", help="Max participants to fetch (default: 200)"),
    search: str = typer.Option("", "--search", help="Search query to filter participants"),
) -> None:
    """List participants of a channel/group."""
    _run_dialogs(ctx, "participants", chat_id=chat_id, phone=phone, limit=limit, search=search)


@dialogs_app.command("edit-admin", context_settings=_NEG_ID_POSITIONAL)
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


@dialogs_app.command("edit-permissions", context_settings=_NEG_ID_POSITIONAL)
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


@dialogs_app.command("kick", context_settings=_NEG_ID_POSITIONAL)
def dialogs_kick(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    user_id: str = typer.Argument(..., help="User ID or @username to kick"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
) -> None:
    """Kick a participant from a chat."""
    _run_dialogs(ctx, "kick", chat_id=chat_id, user_id=user_id, phone=phone, yes=yes)


@dialogs_app.command("broadcast-stats", context_settings=_NEG_ID_POSITIONAL)
def dialogs_broadcast_stats(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Channel ID or @username"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
) -> None:
    """Get broadcast statistics for a channel."""
    _run_dialogs(ctx, "broadcast-stats", chat_id=chat_id, phone=phone)


@dialogs_app.command("archive", context_settings=_NEG_ID_POSITIONAL)
def dialogs_archive(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
) -> None:
    """Archive a dialog (move to archive folder)."""
    _run_dialogs(ctx, "archive", chat_id=chat_id, phone=phone)


@dialogs_app.command("unarchive", context_settings=_NEG_ID_POSITIONAL)
def dialogs_unarchive(
    ctx: typer.Context,
    chat_id: str = typer.Argument(..., help="Chat ID or @username"),
    phone: str | None = typer.Option(None, "--phone", help="Account phone (default: first connected)"),
) -> None:
    """Unarchive a dialog (move to main folder)."""
    _run_dialogs(ctx, "unarchive", chat_id=chat_id, phone=phone)


@dialogs_app.command("mark-read", context_settings=_NEG_ID_POSITIONAL)
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
