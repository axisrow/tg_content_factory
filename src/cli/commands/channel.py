from __future__ import annotations

import argparse
import asyncio
import logging
from datetime import timedelta
from pathlib import Path

import typer

from src.cli import runtime
from src.cli.commands.common import (
    _NEG_ID_POSITIONAL,
    apply_startup,
    resolve_channel,
    run_async,
)
from src.models import Channel, CollectionTaskStatus
from src.parsers import deduplicate_identifiers, parse_file, parse_identifiers
from src.services.channel_onboarding import (
    channel_from_resolved_info,
    channel_with_meta,
    enqueue_stats_for_new_channels,
    fetch_channel_meta,
)
from src.services.channel_service import ChannelService
from src.telegram.backends import adapt_transport_session
from src.telegram.collector import (
    RESOLVE_USERNAME_BACKOFF_BUFFER_SEC,
    AllCollectionClientsFloodedError,
    Collector,
    UsernameResolveFloodWaitDeferredError,
    UsernameResolveRateLimitedError,
)


async def _persist_new_channel(db, pool, info, existing_ids, stats_channel_ids, build):
    """Shared import/add-bulk step: dedup against existing_ids; for a new channel
    fetch meta, build it via `build(info, meta)`, persist, and track it for stats.
    Returns the added Channel, or None if it already existed. Callers own the
    resolution source and the per-row console output."""
    if info["channel_id"] in existing_ids:
        return None
    meta = await fetch_channel_meta(pool, int(info["channel_id"]), info.get("channel_type"))
    channel = build(info, meta)
    await db.add_channel(channel)
    existing_ids.add(info["channel_id"])
    if channel.is_active:
        stats_channel_ids.append(channel.channel_id)
    return channel


def _parse_channel_import_source(source: str) -> list[str]:
    source_path = Path(source)
    if source_path.is_file():
        return parse_file(source_path.read_bytes(), source_path.name)
    return parse_identifiers(source)


# --------------------------------------------------------------------------- #
# channel tag (nested depth-2 group) — pure-db helpers
#
# The tag sub-actions act on an already-open ``db`` so they can be called both
# from the Typer command bodies (which open / close the db around them) and from
# the legacy ``_handle_tag`` dispatcher kept for the direct-call command tests.
# --------------------------------------------------------------------------- #


async def tag_list_impl(db) -> None:
    """``channel tag list`` — list every tag name."""
    tags = await db.repos.channels.list_all_tags()
    if not tags:
        print("No tags found.")
        return
    for tag in tags:
        print(f"  {tag}")


async def tag_add_impl(db, *, name: str) -> None:
    """``channel tag add`` — create a tag."""
    await db.repos.channels.create_tag(name)
    print(f"Tag '{name}' created.")


async def tag_delete_impl(db, *, name: str) -> None:
    """``channel tag delete`` — delete a tag."""
    await db.repos.channels.delete_tag(name)
    print(f"Tag '{name}' deleted.")


async def tag_set_impl(db, *, pk: int, tags: str) -> None:
    """``channel tag set`` — replace a channel's tags (comma-separated)."""
    tag_names = [t.strip() for t in tags.split(",") if t.strip()]
    await db.repos.channels.set_channel_tags(pk, tag_names)
    print(f"Tags for channel pk={pk} set to: {', '.join(tag_names)}")


async def tag_get_impl(db, *, pk: int) -> None:
    """``channel tag get`` — show a channel's tags."""
    tags = await db.repos.channels.get_channel_tags(pk)
    if not tags:
        print(f"No tags for channel pk={pk}.")
    else:
        print(f"Tags for channel pk={pk}: {', '.join(tags)}")


async def _handle_tag(args: argparse.Namespace, db) -> None:
    """Legacy nested dispatcher for ``channel tag`` over an open ``db``.

    Kept for the argparse ``run(args)`` adapter and the direct-call command
    tests; the Typer leaves call the ``tag_*_impl`` helpers directly.
    """
    tag_action = getattr(args, "tag_action", None)
    if not tag_action:
        print("Usage: channel tag {list|add|delete|set|get}")
        return

    if tag_action == "list":
        await tag_list_impl(db)
    elif tag_action == "add":
        await tag_add_impl(db, name=args.name)
    elif tag_action == "delete":
        await tag_delete_impl(db, name=args.name)
    elif tag_action == "set":
        await tag_set_impl(db, pk=args.pk, tags=args.tags)
    elif tag_action == "get":
        await tag_get_impl(db, pk=args.pk)


async def _tag_impl(config_path: str, tag_action: str | None, **kwargs) -> None:
    """Open a db, run one ``channel tag`` sub-action, then close.

    Wraps the pure-db tag helpers with the connection lifecycle so the Typer
    leaves get the same own-your-init_db shape as the other channel impls.
    """
    _, db = await runtime.init_db(config_path)
    try:
        if tag_action == "list":
            await tag_list_impl(db)
        elif tag_action == "add":
            await tag_add_impl(db, name=kwargs["name"])
        elif tag_action == "delete":
            await tag_delete_impl(db, name=kwargs["name"])
        elif tag_action == "set":
            await tag_set_impl(db, pk=kwargs["pk"], tags=kwargs["tags"])
        elif tag_action == "get":
            await tag_get_impl(db, pk=kwargs["pk"])
        else:
            print("Usage: channel tag {list|add|delete|set|get}")
    finally:
        await db.close()


# --------------------------------------------------------------------------- #
# channel leaf impls (epic #959, Wave 4 — issue #1124)
#
# Each ``*_impl`` owns its own ``runtime.init_db`` + lazy ``runtime.init_pool``
# and a single ``finally`` that disconnects the pool (if opened) and closes the
# db — replicating the original monolithic ``run(args)`` lifecycle exactly. The
# Typer command bodies call these directly; the ``run(args)`` adapter at the
# bottom forwards the parsed argparse Namespace (grabli #1117 partial fakes).
# --------------------------------------------------------------------------- #


async def list_impl(config_path: str) -> None:
    """``channel list`` — table of every channel with counts and latest stats."""
    _, db = await runtime.init_db(config_path)
    try:
        channels = await db.get_channels_with_counts()
        if not channels:
            print("No channels found.")
            return
        latest_stats = await db.get_latest_stats_for_all()
        fmt = (
            "{:<5} {:<15} {:<25} {:<12} {:<8} {:<10} "
            "{:<12} {:<12} {:<10} {:<10} {:<10} {:<20}"
        )
        header = (
            "ID",
            "Channel ID",
            "Title",
            "Username",
            "Active",
            "Messages",
            "Last msg ID",
            "Subscribers",
            "Avg views",
            "Avg react.",
            "Avg fwd.",
            "Filter",
        )
        print(fmt.format(*header))
        print("-" * 145)
        for ch in channels:
            if ch.is_filtered:
                filt = ch.filter_flags if ch.filter_flags else "Yes"
            else:
                filt = "-"
            st = latest_stats.get(ch.channel_id)
            sub = st.subscriber_count if st and st.subscriber_count is not None else "—"
            avg_v = f"{st.avg_views:.0f}" if st and st.avg_views is not None else "—"
            avg_r = (
                f"{st.avg_reactions:.0f}" if st and st.avg_reactions is not None else "—"
            )
            avg_f = f"{st.avg_forwards:.0f}" if st and st.avg_forwards is not None else "—"
            print(
                fmt.format(
                    ch.id or 0,
                    ch.channel_id,
                    (ch.title or "—")[:25],
                    (ch.username or "—")[:12],
                    "Yes" if ch.is_active else "No",
                    ch.message_count,
                    ch.last_collected_id,
                    sub,
                    avg_v,
                    avg_r,
                    avg_f,
                    filt,
                )
            )
    finally:
        await db.close()


async def add_impl(config_path: str, *, identifier: str) -> None:
    """``channel add`` — resolve and persist a channel by identifier."""
    config, db = await runtime.init_db(config_path)
    pool = None
    try:
        _, pool = await runtime.init_pool(config, db)
        if not pool.clients:
            logging.error("No connected accounts.")
            return
        try:
            info = await pool.resolve_channel(identifier.strip())
        except RuntimeError as exc:
            if str(exc) == "no_client":
                print("ERROR: Нет доступных аккаунтов Telegram.")
                return
            info = None
        except Exception:
            info = None
        if not info:
            print(f"Could not resolve channel: {identifier}")
            return

        existing = await db.get_channel_by_channel_id(int(info["channel_id"]))
        meta = await fetch_channel_meta(pool, int(info["channel_id"]), info.get("channel_type"))
        deactivate = info.get("deactivate", False)
        channel = channel_from_resolved_info(info, meta)
        await db.add_channel(channel)
        if existing is None and channel.is_active:
            await enqueue_stats_for_new_channels(
                db.create_stats_task,
                [channel.channel_id],
                context="cli channel add",
            )
        msg = f"Added channel: {info['title']} ({info['channel_id']})"
        if deactivate:
            msg += f" [WARN: deactivated, type={info['channel_type']}]"
        print(msg)
    finally:
        if pool:
            await pool.disconnect_all()
        await db.close()


async def delete_impl(config_path: str, *, identifier: str) -> None:
    """``channel delete`` — remove a channel resolved locally by identifier."""
    _, db = await runtime.init_db(config_path)
    try:
        channels = await db.get_channels()
        ch = resolve_channel(channels, identifier)
        if not ch:
            print(f"Channel '{identifier}' not found")
            return
        await db.delete_channel(ch.id)
        print(f"Deleted channel '{ch.title}' (pk={ch.id})")
    finally:
        await db.close()


async def toggle_impl(config_path: str, *, identifier: str) -> None:
    """``channel toggle`` — flip a channel's active flag."""
    _, db = await runtime.init_db(config_path)
    try:
        channels = await db.get_channels()
        ch = resolve_channel(channels, identifier)
        if not ch:
            print(f"Channel '{identifier}' not found")
            return
        new_state = not ch.is_active
        await db.set_channel_active(ch.id, new_state)
        print(f"Channel '{ch.title}' (pk={ch.id}): active={new_state}")
    finally:
        await db.close()


async def review_list_impl(config_path: str) -> None:
    """``channel review-list`` — channels quarantined for human review."""
    _, db = await runtime.init_db(config_path)
    try:
        pending = await db.repos.channels.list_channels_for_review()
        if not pending:
            print("No channels are quarantined for review.")
        else:
            print(f"Channels quarantined for review: {len(pending)}")
            for ch in pending:
                print(
                    f"  {ch.id}\t{ch.title} "
                    f"(@{ch.username or ch.channel_id})\t— {ch.review_reason}"
                )
    finally:
        await db.close()


async def review_confirm_impl(config_path: str, *, identifier: str) -> None:
    """``channel review-confirm`` — confirm a quarantined channel is dead."""
    _, db = await runtime.init_db(config_path)
    try:
        channels = await db.get_channels()
        ch = resolve_channel(channels, identifier)
        if not ch:
            print(f"Channel '{identifier}' not found")
            return
        await db.set_channel_active(ch.id, False)
        await db.set_channel_type(ch.channel_id, "unavailable")
        await db.repos.channels.clear_channel_review(ch.id)
        print(f"DEACTIVATED: {ch.title} (pk={ch.id}) — confirmed dead, removed from review")
    finally:
        await db.close()


async def review_keep_impl(config_path: str, *, identifier: str) -> None:
    """``channel review-keep`` — clear a channel's review flag, keep it active."""
    _, db = await runtime.init_db(config_path)
    try:
        channels = await db.get_channels()
        ch = resolve_channel(channels, identifier)
        if not ch:
            print(f"Channel '{identifier}' not found")
            return
        await db.repos.channels.clear_channel_review(ch.id)
        print(f"KEPT ACTIVE: {ch.title} (pk={ch.id}) — cleared from review")
    finally:
        await db.close()


async def import_impl(config_path: str, *, source: str) -> None:
    """``channel import`` — bulk-add channels from a file or identifier list."""
    config, db = await runtime.init_db(config_path)
    pool = None
    try:
        identifiers = await asyncio.to_thread(_parse_channel_import_source, source)

        identifiers = deduplicate_identifiers(identifiers)
        if not identifiers:
            print("No identifiers found in source.")
            return

        _, pool = await runtime.init_pool(config, db)
        if not pool.clients:
            logging.error("No connected accounts.")
            return

        existing = await db.get_channels()
        existing_ids = {ch.channel_id for ch in existing}

        added = skipped = failed = 0
        stats_channel_ids: list[int] = []
        for ident in identifiers:
            try:
                info = await pool.resolve_channel(ident.strip())
            except RuntimeError as exc:
                if str(exc) == "no_client":
                    print("ERROR: Нет доступных аккаунтов Telegram. Импорт прерван.")
                    failed += len(identifiers) - added - skipped - failed
                    break
                info = None
            except Exception as exc:
                logging.warning("Failed to resolve '%s': %s", ident, exc)
                info = None

            if not info:
                print(f"FAIL: {ident} — could not resolve")
                failed += 1
                continue

            channel = await _persist_new_channel(
                db, pool, info, existing_ids, stats_channel_ids,
                channel_from_resolved_info,
            )
            if channel is None:
                print(f"SKIP: {ident} — already exists ({info.get('title', '')})")
                skipped += 1
                continue

            deactivate = info.get("deactivate", False)
            status = f"WARN ({info['channel_type']})" if deactivate else "OK"
            print(f"{status}: {ident} — {info.get('title', '')} ({info['channel_id']})")
            added += 1

        await enqueue_stats_for_new_channels(
            db.create_stats_task,
            stats_channel_ids,
            context="cli channel import",
        )
        print(
            f"\nTotal: {len(identifiers)}, Added: {added}, "
            f"Skipped: {skipped}, Failed: {failed}"
        )
    finally:
        if pool:
            await pool.disconnect_all()
        await db.close()


async def stats_impl(config_path: str, *, all_channels: bool, identifier: str | None, max_channels: int | None) -> None:
    """``channel stats`` — collect stats for one channel or all active ones."""
    config, db = await runtime.init_db(config_path)
    pool = None
    try:
        _, pool = await runtime.init_pool(config, db)
        if not pool.clients:
            logging.error("No connected accounts.")
            return
        collector = Collector(pool, db, config.scheduler)

        if all_channels:
            if max_channels is not None and max_channels <= 0:
                print("--max-channels must be a positive integer")
                return
            result = await collector.collect_all_stats(max_channels=max_channels)
            print(f"Stats collected: {result}")
        elif not identifier:
            print("Specify a channel identifier or use --all")
            return
        else:
            channels = await db.get_channels()
            ch = resolve_channel(channels, identifier)
            if not ch:
                print(f"Channel '{identifier}' not found")
                return
            st = await collector.collect_channel_stats(ch)
            if st:
                print(
                    f"Channel {ch.channel_id} ({ch.title}):\n"
                    f"  Subscribers: {st.subscriber_count}\n"
                    f"  Avg views: {st.avg_views}\n"
                    f"  Avg reactions: {st.avg_reactions}\n"
                    f"  Avg forwards: {st.avg_forwards}"
                )
            else:
                print("No client available to collect stats")
    finally:
        if pool:
            await pool.disconnect_all()
        await db.close()


async def refresh_types_impl(config_path: str) -> None:
    """``channel refresh-types`` — re-resolve channel types for all active channels."""
    config, db = await runtime.init_db(config_path)
    pool = None
    try:
        _, pool = await runtime.init_pool(config, db)
        if not pool.clients:
            logging.error("No connected accounts.")
            return
        channels = await db.get_channels(active_only=True)
        null_type = [ch for ch in channels if ch.channel_type is None]
        print(f"Active channels to check: {len(channels)} (missing type: {len(null_type)})")
        # Pre-fetch dialogs to populate entity cache for channels without username
        prefetch = await pool.get_available_client()
        if prefetch:
            session, phone = prefetch
            session = adapt_transport_session(session, disconnect_on_close=False)
            try:
                print("Pre-fetching dialogs to populate entity cache...")
                await asyncio.wait_for(session.warm_dialog_cache(), timeout=30)
            except Exception as e:
                logging.warning("Failed to pre-fetch dialogs: %s", e)
            finally:
                await pool.release_client(phone)
        updated = failed = deactivated = quarantined = 0
        for ch in channels:
            identifier = ch.username or str(ch.channel_id)
            try:
                # numeric_fallback so a stale @username doesn't deactivate a
                # live channel — gone is retried by numeric id first (#858 review).
                info = await pool.resolve_channel(
                    identifier, signal_gone=True, numeric_fallback=str(ch.channel_id)
                )
            except Exception as e:
                logging.warning("Failed to resolve %s: %s", identifier, e)
                info = None
            # Uncertain (cache-miss vs deleted, owner unknown/unavailable) →
            # quarantine for human review, never silent deactivation (#875 redesign).
            if info and info.get("review"):
                await db.repos.channels.set_channel_review(ch.id, info.get("reason", "uncertain"))
                print(
                    f"QUARANTINE: {ch.title} (@{ch.username or ch.channel_id}) "
                    f"— {info.get('reason', 'uncertain')}"
                )
                quarantined += 1
                continue
            # Definitive not-found → deactivate; transient None → skip and
            # leave active (audit #835/8; old `if info is False` was dead).
            if info and info.get("gone"):
                await db.set_channel_active(ch.id, False)
                await db.set_channel_type(ch.channel_id, "unavailable")
                print(
                    f"DEACTIVATED: {ch.title} (@{ch.username or ch.channel_id}) — not found"
                )
                deactivated += 1
                continue
            if not info or info.get("channel_type") is None:
                print(f"SKIP: {ch.title} ({ch.channel_id}) — type still unknown")
                failed += 1
                continue
            if info.get("deactivate"):
                await db.set_channel_active(ch.id, False)
                await db.set_channel_type(ch.channel_id, info["channel_type"])
                print(f"DEACTIVATED ({info['channel_type']}): {ch.title}")
                deactivated += 1
                continue
            # Resolved live: clear any stale quarantine flag (channel recovered).
            if getattr(ch, "needs_review", False):
                await db.repos.channels.clear_channel_review(ch.id)
            await db.set_channel_type(ch.channel_id, info["channel_type"])
            print(f"OK: {ch.title} → {info['channel_type']}")
            updated += 1
        print(
            f"\nUpdated: {updated}, Deactivated: {deactivated}, "
            f"Quarantined: {quarantined}, Skipped: {failed}"
        )
    finally:
        if pool:
            await pool.disconnect_all()
        await db.close()


async def refresh_meta_impl(config_path: str, *, all_channels: bool, identifier: str | None) -> None:
    """``channel refresh-meta`` — refresh about/linked-chat/comments metadata."""
    config, db = await runtime.init_db(config_path)
    pool = None
    try:
        _, pool = await runtime.init_pool(config, db)
        if not pool.clients:
            logging.error("No connected accounts.")
            return
        if all_channels:
            # Refresh all active channels
            channels = await db.get_channels(active_only=True)
            print(f"Active channels to refresh: {len(channels)}")
            ok = failed = 0
            for ch in channels:
                meta = await pool.fetch_channel_meta(ch.channel_id, ch.channel_type)
                if meta:
                    await db.update_channel_full_meta(
                        ch.channel_id,
                        about=meta["about"],
                        linked_chat_id=meta["linked_chat_id"],
                        has_comments=meta["has_comments"],
                    )
                    print(
                        f"OK: {ch.title} (about_len={len(meta['about'] or '')}, "
                        f"linked={meta['linked_chat_id']}, comments={meta['has_comments']})"
                    )
                    ok += 1
                else:
                    print(f"SKIP: {ch.title}")
                    failed += 1
            print(f"\nRefreshed: {ok}, Failed: {failed}")
        elif identifier:
            # Refresh single channel
            channels = await db.get_channels()
            ch = resolve_channel(channels, identifier)
            if not ch:
                print(f"Channel '{identifier}' not found")
                return
            meta = await pool.fetch_channel_meta(ch.channel_id, ch.channel_type)
            if meta:
                await db.update_channel_full_meta(
                    ch.channel_id,
                    about=meta["about"],
                    linked_chat_id=meta["linked_chat_id"],
                    has_comments=meta["has_comments"],
                )
                print(f"OK: Updated {ch.title}")
                print(f"  about={meta['about'][:60] if meta['about'] else 'N/A'}...")
                print(f"  linked_chat_id={meta['linked_chat_id']}")
                print(f"  has_comments={meta['has_comments']}")
            else:
                print(f"Failed to fetch metadata for {ch.title}")
        else:
            print("Please provide --all or a channel identifier")
    finally:
        if pool:
            await pool.disconnect_all()
        await db.close()


async def add_bulk_impl(config_path: str, *, phone: str, dialog_ids: str) -> None:
    """``channel add-bulk`` — add channels from an account's dialogs by id list."""
    config, db = await runtime.init_db(config_path)
    pool = None
    try:
        _, pool = await runtime.init_pool(config, db)
        if not pool.clients:
            logging.error("No connected accounts.")
            return
        if phone not in pool.clients:
            print(f"Account {phone} not connected.")
            return
        raw_ids = [i.strip() for i in dialog_ids.split(",") if i.strip()]
        parsed_ids = []
        for raw in raw_ids:
            try:
                parsed_ids.append(int(raw))
            except ValueError:
                print(f"Invalid dialog ID: {raw!r}, skipping.")
        if not parsed_ids:
            print("No valid dialog IDs provided.")
            return
        svc = ChannelService(db, pool, None)  # type: ignore[arg-type]
        dialogs_info = await svc.get_my_dialogs(phone)
        info_map = {d["channel_id"]: d for d in dialogs_info}
        existing = await db.get_channels()
        existing_ids = {ch.channel_id for ch in existing}
        added = skipped = failed = 0
        stats_channel_ids: list[int] = []
        def _build_dialog_channel(i, meta):
            channel = Channel(
                channel_id=int(i["channel_id"]),
                title=i["title"],
                username=i.get("username"),
                channel_type=i.get("channel_type"),
                is_active=True,
                created_at=i.get("created_at"),
            )
            return channel_with_meta(channel, meta)

        for did in parsed_ids:
            info = info_map.get(did)
            if not info:
                print(f"SKIP: {did} — not found in dialogs")
                failed += 1
                continue
            channel = await _persist_new_channel(
                db, pool, info, existing_ids, stats_channel_ids, _build_dialog_channel
            )
            if channel is None:
                print(f"SKIP: {did} — already exists ({info.get('title', '')})")
                skipped += 1
                continue
            print(f"OK: {info.get('title', did)} ({info['channel_id']})")
            added += 1
        await enqueue_stats_for_new_channels(
            db.create_stats_task,
            stats_channel_ids,
            context="cli channel add-bulk",
        )
        print(f"\nAdded: {added}, Skipped: {skipped}, Failed: {failed}")
    finally:
        if pool:
            await pool.disconnect_all()
        await db.close()


async def list_for_import_impl(config_path: str, *, as_json: bool) -> None:
    """``channel list-for-import`` — dialogs with an already-added flag."""
    config, db = await runtime.init_db(config_path)
    pool = None
    try:
        _, pool = await runtime.init_pool(config, db)
        svc = ChannelService(db, pool, None)  # type: ignore[arg-type]
        dialogs = await svc.get_dialogs_with_added_flags()
        if as_json:
            import json as _json

            print(_json.dumps(dialogs, ensure_ascii=False, default=str))
            return
        if not dialogs:
            print("No dialogs found.")
            return
        fmt = "{:<15} {:<35} {:<20} {:<8} {:<12}"
        print(fmt.format("Channel ID", "Title", "Username", "Added", "Type"))
        print("-" * 92)
        for d in dialogs:
            print(
                fmt.format(
                    str(d.get("channel_id", "—")),
                    str(d.get("title") or "—")[:35],
                    str(d.get("username") or "—")[:20],
                    "Yes" if d.get("already_added") else "No",
                    str(d.get("channel_type") or "—")[:12],
                )
            )
    finally:
        if pool:
            await pool.disconnect_all()
        await db.close()


async def collect_impl(config_path: str, *, identifier: str, full: bool) -> None:
    """``channel collect`` — one-shot collection of a single channel."""
    config, db = await runtime.init_db(config_path)
    pool = None
    try:
        _, pool = await runtime.init_pool(config, db)
        if not pool.clients:
            logging.error("No connected accounts.")
            return
        channels = await db.get_channels()
        ch = resolve_channel(channels, identifier)
        if not ch:
            print(f"Channel '{identifier}' not found")
            return
        task_id = await db.create_collection_task(ch.channel_id, ch.title)
        await db.update_collection_task(task_id, CollectionTaskStatus.RUNNING)
        collector = Collector(pool, db, config.scheduler)
        try:
            count = await collector.collect_single_channel(
                ch,
                full=bool(full),
                force=True,
            )
            await db.update_collection_task(
                task_id,
                CollectionTaskStatus.COMPLETED,
                messages_collected=count,
            )
            print(f"Collected {count} messages from channel {ch.channel_id}")
        except UsernameResolveFloodWaitDeferredError as exc:
            run_after = exc.next_available_at + timedelta(
                seconds=RESOLVE_USERNAME_BACKOFF_BUFFER_SEC
            )
            note = (
                "Отложено: Flood Wait на resolve_username до "
                f"{run_after.astimezone().isoformat()}"
            )
            await db.reschedule_collection_task(task_id, run_after=run_after, note=note)
            print(
                "Collection deferred: resolve_username Flood Wait "
                f"until {run_after.astimezone().isoformat()}"
            )
        except UsernameResolveRateLimitedError as exc:
            run_after = exc.run_after_with_buffer()
            note = (
                "Отложено: resolve_username rate-limited до "
                f"{run_after.astimezone().isoformat()}"
            )
            await db.reschedule_collection_task(task_id, run_after=run_after, note=note)
            print(
                "Collection deferred: resolve_username rate-limited "
                f"on {exc.phone} until {run_after.astimezone().isoformat()}"
            )
        except AllCollectionClientsFloodedError as exc:
            run_after = exc.next_available_at
            note = (
                "Отложено: все аккаунты в Flood Wait до "
                f"{run_after.astimezone().isoformat()}"
            )
            await db.reschedule_collection_task(task_id, run_after=run_after, note=note)
            print(
                "Collection deferred: all accounts flood-waited until "
                f"{run_after.astimezone().isoformat()} "
                f"(retry in {exc.retry_after_sec}s)"
            )
        except Exception as exc:
            await db.update_collection_task(
                task_id,
                CollectionTaskStatus.FAILED,
                error=str(exc)[:500],
            )
            raise
    finally:
        if pool:
            await pool.disconnect_all()
        await db.close()


# --------------------------------------------------------------------------- #
# argparse adapter — thin dispatch over the *_impl functions
# --------------------------------------------------------------------------- #


def run(args: argparse.Namespace) -> None:
    """Dispatch the parsed argparse Namespace to the matching channel ``*_impl``.

    Reads each flag with ``getattr`` and the parser's declared default so the
    partial Namespaces built by the command tests keep resolving (grabli #1117).
    The Typer command bodies in ``typer_commands.py`` call the ``*_impl``
    functions directly, bypassing this adapter.
    """
    action = getattr(args, "channel_action", None)
    config_path = args.config

    if action == "list":
        coro = list_impl(config_path)
    elif action == "add":
        coro = add_impl(config_path, identifier=args.identifier)
    elif action == "delete":
        coro = delete_impl(config_path, identifier=args.identifier)
    elif action == "toggle":
        coro = toggle_impl(config_path, identifier=args.identifier)
    elif action == "review-list":
        coro = review_list_impl(config_path)
    elif action == "review-confirm":
        coro = review_confirm_impl(config_path, identifier=args.identifier)
    elif action == "review-keep":
        coro = review_keep_impl(config_path, identifier=args.identifier)
    elif action == "import":
        coro = import_impl(config_path, source=args.source)
    elif action == "stats":
        coro = stats_impl(
            config_path,
            all_channels=getattr(args, "all", False),
            identifier=getattr(args, "identifier", None),
            max_channels=getattr(args, "max_channels", None),
        )
    elif action == "refresh-types":
        coro = refresh_types_impl(config_path)
    elif action == "refresh-meta":
        coro = refresh_meta_impl(
            config_path,
            all_channels=getattr(args, "all", False),
            identifier=getattr(args, "identifier", None),
        )
    elif action == "add-bulk":
        coro = add_bulk_impl(config_path, phone=args.phone, dialog_ids=args.dialog_ids)
    elif action == "list-for-import":
        coro = list_for_import_impl(config_path, as_json=getattr(args, "json", False))
    elif action == "tag":
        coro = _tag_impl(
            config_path,
            getattr(args, "tag_action", None),
            name=getattr(args, "name", None),
            pk=getattr(args, "pk", None),
            tags=getattr(args, "tags", None),
        )
    elif action == "collect":
        coro = collect_impl(
            config_path,
            identifier=args.identifier,
            full=bool(getattr(args, "full", False)),
        )
    else:
        coro = list_impl(config_path)

    asyncio.run(coro)


# --------------------------------------------------------------------------- #
# channel → list / add / delete / toggle / collect / stats / refresh-types /
#   refresh-meta / review-list / review-confirm / review-keep / import /
#   add-bulk / list-for-import / tag (NESTED depth-2: list/add/delete/set/get)
# --------------------------------------------------------------------------- #

channel_app = typer.Typer(no_args_is_help=True, help="Channel management")

# Nested depth-2 group: ``channel tag`` is its own Typer added onto the channel
# sub-app via ``add_typer`` — the exact path ``channel tag <action>`` is the
# fragile frozen invariant of Wave 4.
channel_tag_app = typer.Typer(no_args_is_help=True, help="Manage channel tags")
channel_app.add_typer(channel_tag_app, name="tag")


@channel_app.command("list")
def channel_list(ctx: typer.Context) -> None:
    """List channels."""
    apply_startup(ctx)
    run_async(list_impl(ctx.obj.config))


@channel_app.command("add", context_settings=_NEG_ID_POSITIONAL)
def channel_add(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Username, link, or numeric ID"),
) -> None:
    """Add a channel."""
    apply_startup(ctx)
    run_async(add_impl(ctx.obj.config, identifier=identifier))


@channel_app.command("delete", context_settings=_NEG_ID_POSITIONAL)
def channel_delete(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Channel pk, channel_id, or @username"),
) -> None:
    """Delete a channel."""
    apply_startup(ctx)
    run_async(delete_impl(ctx.obj.config, identifier=identifier))


@channel_app.command("toggle", context_settings=_NEG_ID_POSITIONAL)
def channel_toggle(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Channel pk, channel_id, or @username"),
) -> None:
    """Toggle channel active state."""
    apply_startup(ctx)
    run_async(toggle_impl(ctx.obj.config, identifier=identifier))


@channel_app.command("collect", context_settings=_NEG_ID_POSITIONAL)
def channel_collect(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Channel pk, channel_id, or @username"),
    full: bool = typer.Option(False, "--full", help="Explicitly backfill the full channel history"),
) -> None:
    """Collect a single channel one-shot."""
    apply_startup(ctx)
    run_async(collect_impl(ctx.obj.config, identifier=identifier, full=full))


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
        stats_impl(
            ctx.obj.config, all_channels=all_channels, identifier=identifier, max_channels=max_channels
        )
    )


@channel_app.command("refresh-types")
def channel_refresh_types(ctx: typer.Context) -> None:
    """Re-resolve channel types for all active channels."""
    apply_startup(ctx)
    run_async(refresh_types_impl(ctx.obj.config))


@channel_app.command("refresh-meta", context_settings=_NEG_ID_POSITIONAL)
def channel_refresh_meta(
    ctx: typer.Context,
    identifier: str | None = typer.Argument(None, help="Channel pk, channel_id, or @username (omit for all)"),
    all_channels: bool = typer.Option(False, "--all", help="Refresh metadata for all active channels"),
) -> None:
    """Refresh channel metadata."""
    apply_startup(ctx)
    run_async(
        refresh_meta_impl(ctx.obj.config, all_channels=all_channels, identifier=identifier)
    )


@channel_app.command("review-list")
def channel_review_list(ctx: typer.Context) -> None:
    """List channels quarantined for review."""
    apply_startup(ctx)
    run_async(review_list_impl(ctx.obj.config))


@channel_app.command("review-confirm", context_settings=_NEG_ID_POSITIONAL)
def channel_review_confirm(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Channel pk, channel_id, or @username"),
) -> None:
    """Confirm a quarantined channel is dead and deactivate it."""
    apply_startup(ctx)
    run_async(review_confirm_impl(ctx.obj.config, identifier=identifier))


@channel_app.command("review-keep", context_settings=_NEG_ID_POSITIONAL)
def channel_review_keep(
    ctx: typer.Context,
    identifier: str = typer.Argument(..., help="Channel pk, channel_id, or @username"),
) -> None:
    """Clear a channel's review flag and keep it active."""
    apply_startup(ctx)
    run_async(review_keep_impl(ctx.obj.config, identifier=identifier))


@channel_app.command("import", context_settings=_NEG_ID_POSITIONAL)
def channel_import(
    ctx: typer.Context,
    source: str = typer.Argument(..., help="Path to .txt/.csv file, or comma-separated identifiers"),
) -> None:
    """Bulk-import channels from a file or identifier list."""
    apply_startup(ctx)
    run_async(import_impl(ctx.obj.config, source=source))


@channel_app.command("add-bulk")
def channel_add_bulk(
    ctx: typer.Context,
    phone: str = typer.Option(..., "--phone", help="Account phone"),
    dialog_ids: str = typer.Option(..., "--dialog-ids", help="Comma-separated dialog IDs to add as channels"),
) -> None:
    """Add channels from an account's dialogs by id list."""
    apply_startup(ctx)
    run_async(add_bulk_impl(ctx.obj.config, phone=phone, dialog_ids=dialog_ids))


@channel_app.command("list-for-import")
def channel_list_for_import(
    ctx: typer.Context,
    as_json: bool = typer.Option(False, "--json", help="Output as JSON instead of a table"),
) -> None:
    """List dialogs with an already-added flag."""
    apply_startup(ctx)
    run_async(list_for_import_impl(ctx.obj.config, as_json=as_json))


# ---- nested: channel tag <action> ---------------------------------------- #


@channel_tag_app.command("list")
def channel_tag_list(ctx: typer.Context) -> None:
    """List all channel tags."""
    apply_startup(ctx)
    run_async(_tag_impl(ctx.obj.config, "list"))


@channel_tag_app.command("add")
def channel_tag_add(
    ctx: typer.Context,
    name: str = typer.Argument(..., help="Tag name"),
) -> None:
    """Create a channel tag."""
    apply_startup(ctx)
    run_async(_tag_impl(ctx.obj.config, "add", name=name))


@channel_tag_app.command("delete")
def channel_tag_delete(
    ctx: typer.Context,
    name: str = typer.Argument(..., help="Tag name"),
) -> None:
    """Delete a channel tag."""
    apply_startup(ctx)
    run_async(_tag_impl(ctx.obj.config, "delete", name=name))


@channel_tag_app.command("set")
def channel_tag_set(
    ctx: typer.Context,
    pk: int = typer.Argument(..., help="Channel primary key"),
    tags: str = typer.Argument(..., help="Comma-separated tag names"),
) -> None:
    """Replace a channel's tags."""
    apply_startup(ctx)
    run_async(_tag_impl(ctx.obj.config, "set", pk=pk, tags=tags))


@channel_tag_app.command("get")
def channel_tag_get(
    ctx: typer.Context,
    pk: int = typer.Argument(..., help="Channel primary key"),
) -> None:
    """Show a channel's tags."""
    apply_startup(ctx)
    run_async(_tag_impl(ctx.obj.config, "get", pk=pk))
