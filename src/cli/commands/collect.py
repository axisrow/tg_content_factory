from __future__ import annotations

import argparse
import asyncio
import logging

import typer

from src.cli import runtime
from src.cli.commands.common import (
    _NEG_ID_POSITIONAL,
    apply_startup,
    run_async,
)
from src.database.bundles import ChannelBundle
from src.services.collection_service import CollectionService
from src.services.task_enqueuer import TaskEnqueuer
from src.telegram.collector import (
    AllCollectionClientsFloodedError,
    Collector,
    UsernameResolveFloodWaitDeferredError,
    UsernameResolveRateLimitedError,
)


def print_resolve_backoff_warning(pool) -> None:
    """Print active per-account resolve_username backoffs, if any (#790)."""
    get_remaining = getattr(pool, "get_resolve_username_backoff_remaining_sec", None)
    if not callable(get_remaining):
        return
    active: dict[str, int] = {}
    for phone in getattr(pool, "clients", {}) or {}:
        try:
            remaining = int(get_remaining(phone))
        except TypeError:
            return
        except ValueError:
            continue
        if remaining > 0:
            active[str(phone)] = remaining
    if not active:
        return
    parts = ", ".join(f"{phone} ({sec}s left)" for phone, sec in sorted(active.items()))
    print(
        f"Warning: resolve_username Flood Wait active on: {parts}. "
        "Username channels are rotated to free accounts; if every account is "
        "blocked they are deferred."
    )


async def collect_impl(
    config_path: str, *, channel_id: int | None = None, full: bool = False
) -> None:
    """Run one-shot collection: a single ``--channel-id`` or enqueue all channels.

    Shared async body for both CLI entry points — the argparse ``run`` wrapper
    below and the Typer ``collect`` command (``src/cli/typer_commands.py``).
    Driven through the single async-bridge ``run_async`` by its callers, so
    there is no local ``asyncio.run`` in the migrated path.
    """
    config, db = await runtime.init_db(config_path)
    _, pool = await runtime.init_pool(config, db)
    try:
        if not pool.clients:
            logging.error("No connected accounts. Run 'serve' and add accounts via web UI.")
            return

        collector = Collector(pool, db, config.scheduler)

        if channel_id:
            channels = await db.get_channels()
            channel = next((ch for ch in channels if ch.channel_id == channel_id), None)
            if not channel:
                print(f"Channel {channel_id} not found in DB")
                return
            if channel.is_filtered:
                print(f"Channel {channel_id} is filtered and excluded from collection")
                return
            try:
                count = await collector.collect_single_channel(channel, full=full)
            except UsernameResolveFloodWaitDeferredError as exc:
                retry_at = exc.next_available_at.astimezone().isoformat()
                print(f"Username resolve Flood Wait active until {retry_at}; try again later.")
                return
            except UsernameResolveRateLimitedError as exc:
                retry_at = exc.run_after_with_buffer().astimezone().isoformat()
                print(
                    "resolve_username rate-limited "
                    f"on {exc.phone}; deferred until {retry_at}."
                )
                return
            except AllCollectionClientsFloodedError as exc:
                retry_at = exc.next_available_at.astimezone().isoformat()
                print(
                    f"All accounts are flood-waited until {retry_at} "
                    f"(retry in {exc.retry_after_sec}s); try again later."
                )
                return
            print(f"Collected {count} messages from channel {channel_id}")
        else:
            channel_bundle = ChannelBundle.from_database(db)
            collection_service = CollectionService(
                channel_bundle, collector, collection_queue=None
            )
            task_enqueuer = TaskEnqueuer(db, collection_service)
            result = await task_enqueuer.enqueue_all_channels(
                resolve_backoff_remaining_sec=pool.get_resolve_username_backoff_remaining_sec(),
            )
            print(
                f"Enqueued {result.queued_count} channels "
                f"(skipped {result.skipped_existing_count}, "
                f"total {result.total_candidates}). "
                f"Run 'serve' to execute tasks."
            )
            print_resolve_backoff_warning(pool)
    finally:
        await pool.disconnect_all()
        await db.close()


async def collect_sample_impl(config_path: str, *, channel_id: int, limit: int = 10) -> None:
    """Preview the last *limit* messages of a channel without saving to DB.

    Shared async body for the ``collect sample`` sub-command (both CLI entry
    points). Driven through ``run_async`` by its callers.
    """
    config, db = await runtime.init_db(config_path)
    _, pool = await runtime.init_pool(config, db)
    try:
        if not pool.clients:
            logging.error("No connected accounts. Run 'serve' and add accounts via web UI.")
            return

        collector = Collector(pool, db, config.scheduler)

        print(f"Sampling last {limit} messages from channel {channel_id}...\n")
        previews = await collector.sample_channel(channel_id, limit=limit)

        if not previews:
            print("No messages found.")
            return

        for msg in previews:
            date_str = msg["date"].strftime("%Y-%m-%d %H:%M") if msg["date"] else "no date"
            media = msg["media_type"] or "-"
            text = msg["text_preview"] or ""
            preview = repr(text) if text else "(no text)"
            print(f"#{msg['message_id']:<8} {date_str}  {media:<12} {preview}")
    finally:
        await pool.disconnect_all()
        await db.close()


def run(args: argparse.Namespace) -> None:
    if getattr(args, "collect_action", None) == "sample":
        asyncio.run(
            collect_sample_impl(
                args.config, channel_id=args.channel_id, limit=getattr(args, "limit", 10)
            )
        )
        return
    asyncio.run(
        collect_impl(
            args.config,
            channel_id=getattr(args, "channel_id", None),
            full=bool(getattr(args, "full", False)),
        )
    )


# --------------------------------------------------------------------------- #
# collect (+ collect sample)
# --------------------------------------------------------------------------- #

collect_app = typer.Typer(no_args_is_help=False, help="Run one-shot collection")


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
    run_async(collect_impl(ctx.obj.config, channel_id=channel_id, full=full))


@collect_app.command("sample", context_settings=_NEG_ID_POSITIONAL)
def collect_sample(
    ctx: typer.Context,
    channel_id: int = typer.Argument(..., help="Channel ID (numeric)"),
    limit: int = typer.Option(10, "--limit", help="Number of messages to preview (default: 10)"),
) -> None:
    """Preview last N messages without saving to DB."""
    apply_startup(ctx)
    run_async(collect_sample_impl(ctx.obj.config, channel_id=channel_id, limit=limit))
