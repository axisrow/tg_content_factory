from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

from src.cli import runtime
from src.cli.commands.common import resolve_channel
from src.models import Channel, CollectionTaskStatus
from src.parsers import deduplicate_identifiers, parse_file, parse_identifiers
from src.services.channel_service import ChannelService
from src.telegram.backends import adapt_transport_session
from src.telegram.collector import Collector


async def _handle_tag(args: argparse.Namespace, db) -> None:
    tag_action = getattr(args, "tag_action", None)
    if not tag_action:
        print("Usage: channel tag {list|add|delete|set|get}")
        return

    if tag_action == "list":
        tags = await db.repos.channels.list_all_tags()
        if not tags:
            print("No tags found.")
            return
        for tag in tags:
            print(f"  {tag}")

    elif tag_action == "add":
        await db.repos.channels.create_tag(args.name)
        print(f"Tag '{args.name}' created.")

    elif tag_action == "delete":
        await db.repos.channels.delete_tag(args.name)
        print(f"Tag '{args.name}' deleted.")

    elif tag_action == "set":
        tag_names = [t.strip() for t in args.tags.split(",") if t.strip()]
        await db.repos.channels.set_channel_tags(args.pk, tag_names)
        print(f"Tags for channel pk={args.pk} set to: {', '.join(tag_names)}")

    elif tag_action == "get":
        tags = await db.repos.channels.get_channel_tags(args.pk)
        if not tags:
            print(f"No tags for channel pk={args.pk}.")
        else:
            print(f"Tags for channel pk={args.pk}: {', '.join(tags)}")


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        config, db = await runtime.init_db(args.config)
        pool = None

        try:
            if args.channel_action == "list":
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

            elif args.channel_action == "add":
                _, pool = await runtime.init_pool(config, db)
                if not pool.clients:
                    logging.error("No connected accounts.")
                    return
                try:
                    info = await pool.resolve_channel(args.identifier.strip())
                except RuntimeError as exc:
                    if str(exc) == "no_client":
                        print("ERROR: Нет доступных аккаунтов Telegram.")
                        return
                    info = None
                except Exception:
                    info = None
                if not info:
                    print(f"Could not resolve channel: {args.identifier}")
                    return

                # Fetch full metadata
                meta = await pool.fetch_channel_meta(info["channel_id"], info.get("channel_type"))
                deactivate = info.get("deactivate", False)
                await db.add_channel(
                    Channel(
                        channel_id=info["channel_id"],
                        title=info["title"],
                        username=info["username"],
                        channel_type=info.get("channel_type"),
                        is_active=not deactivate,
                        about=meta.get("about") if meta else None,
                        linked_chat_id=meta.get("linked_chat_id") if meta else None,
                        has_comments=meta.get("has_comments", False) if meta else False,
                    )
                )
                msg = f"Added channel: {info['title']} ({info['channel_id']})"
                if deactivate:
                    msg += f" [WARN: deactivated, type={info['channel_type']}]"
                print(msg)

            elif args.channel_action == "delete":
                channels = await db.get_channels()
                ch = resolve_channel(channels, args.identifier)
                if not ch:
                    print(f"Channel '{args.identifier}' not found")
                    return
                await db.delete_channel(ch.id)
                print(f"Deleted channel '{ch.title}' (pk={ch.id})")

            elif args.channel_action == "toggle":
                channels = await db.get_channels()
                ch = resolve_channel(channels, args.identifier)
                if not ch:
                    print(f"Channel '{args.identifier}' not found")
                    return
                new_state = not ch.is_active
                await db.set_channel_active(ch.id, new_state)
                print(f"Channel '{ch.title}' (pk={ch.id}): active={new_state}")

            elif args.channel_action == "import":
                source = args.source
                source_path = Path(source)
                if source_path.is_file():
                    identifiers = parse_file(source_path.read_bytes(), source_path.name)
                else:
                    identifiers = parse_identifiers(source)

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
                    if info["channel_id"] in existing_ids:
                        print(f"SKIP: {ident} — already exists ({info.get('title', '')})")
                        skipped += 1
                        continue

                    deactivate = info.get("deactivate", False)
                    await db.add_channel(
                        Channel(
                            channel_id=info["channel_id"],
                            title=info["title"],
                            username=info["username"],
                            channel_type=info.get("channel_type"),
                            is_active=not deactivate,
                        )
                    )
                    existing_ids.add(info["channel_id"])
                    status = f"WARN ({info['channel_type']})" if deactivate else "OK"
                    print(f"{status}: {ident} — {info.get('title', '')} ({info['channel_id']})")
                    added += 1

                print(
                    f"\nTotal: {len(identifiers)}, Added: {added}, "
                    f"Skipped: {skipped}, Failed: {failed}"
                )

            elif args.channel_action == "stats":
                _, pool = await runtime.init_pool(config, db)
                if not pool.clients:
                    logging.error("No connected accounts.")
                    return
                collector = Collector(pool, db, config.scheduler)

                if args.all:
                    result = await collector.collect_all_stats()
                    print(f"Stats collected: {result}")
                elif not args.identifier:
                    print("Specify a channel identifier or use --all")
                    return
                else:
                    channels = await db.get_channels()
                    ch = resolve_channel(channels, args.identifier)
                    if not ch:
                        print(f"Channel '{args.identifier}' not found")
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

            elif args.channel_action == "refresh-types":
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
                updated = failed = deactivated = 0
                for ch in channels:
                    identifier = ch.username or str(ch.channel_id)
                    try:
                        info = await pool.resolve_channel(identifier)
                    except Exception as e:
                        logging.warning("Failed to resolve %s: %s", identifier, e)
                        info = None
                    if info is False:
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
                    await db.set_channel_type(ch.channel_id, info["channel_type"])
                    print(f"OK: {ch.title} → {info['channel_type']}")
                    updated += 1
                print(f"\nUpdated: {updated}, Deactivated: {deactivated}, Skipped: {failed}")

            elif args.channel_action == "refresh-meta":
                _, pool = await runtime.init_pool(config, db)
                if not pool.clients:
                    logging.error("No connected accounts.")
                    return
                if args.all:
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
                elif args.identifier:
                    # Refresh single channel
                    channels = await db.get_channels()
                    ch = resolve_channel(channels, args.identifier)
                    if not ch:
                        print(f"Channel '{args.identifier}' not found")
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

            elif args.channel_action == "add-bulk":
                _, pool = await runtime.init_pool(config, db)
                if not pool.clients:
                    logging.error("No connected accounts.")
                    return
                phone = args.phone
                if phone not in pool.clients:
                    print(f"Account {phone} not connected.")
                    return
                raw_ids = [i.strip() for i in args.dialog_ids.split(",") if i.strip()]
                dialog_ids = []
                for raw in raw_ids:
                    try:
                        dialog_ids.append(int(raw))
                    except ValueError:
                        print(f"Invalid dialog ID: {raw!r}, skipping.")
                if not dialog_ids:
                    print("No valid dialog IDs provided.")
                    return
                svc = ChannelService(db, pool, None)  # type: ignore[arg-type]
                dialogs_info = await svc.get_my_dialogs(phone)
                info_map = {d["channel_id"]: d for d in dialogs_info}
                existing = await db.get_channels()
                existing_ids = {ch.channel_id for ch in existing}
                added = skipped = failed = 0
                for did in dialog_ids:
                    info = info_map.get(did)
                    if not info:
                        print(f"SKIP: {did} — not found in dialogs")
                        failed += 1
                        continue
                    if info["channel_id"] in existing_ids:
                        print(f"SKIP: {did} — already exists ({info.get('title', '')})")
                        skipped += 1
                        continue
                    await db.add_channel(
                        Channel(
                            channel_id=info["channel_id"],
                            title=info["title"],
                            username=info.get("username"),
                            channel_type=info.get("channel_type"),
                            is_active=True,
                        )
                    )
                    existing_ids.add(info["channel_id"])
                    print(f"OK: {info.get('title', did)} ({info['channel_id']})")
                    added += 1
                print(f"\nAdded: {added}, Skipped: {skipped}, Failed: {failed}")

            elif args.channel_action == "tag":
                await _handle_tag(args, db)

            elif args.channel_action == "collect":
                _, pool = await runtime.init_pool(config, db)
                if not pool.clients:
                    logging.error("No connected accounts.")
                    return
                channels = await db.get_channels()
                ch = resolve_channel(channels, args.identifier)
                if not ch:
                    print(f"Channel '{args.identifier}' not found")
                    return
                task_id = await db.create_collection_task(ch.channel_id, ch.title)
                await db.update_collection_task(task_id, CollectionTaskStatus.RUNNING)
                collector = Collector(pool, db, config.scheduler)
                try:
                    count = await collector.collect_single_channel(ch, full=True, force=True)
                    await db.update_collection_task(
                        task_id,
                        CollectionTaskStatus.COMPLETED,
                        messages_collected=count,
                    )
                    print(f"Collected {count} messages from channel {ch.channel_id}")
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

    asyncio.run(_run())
