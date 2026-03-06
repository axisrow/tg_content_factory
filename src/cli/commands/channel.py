from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

from src.cli import runtime
from src.cli.commands.common import resolve_channel
from src.models import Channel
from src.parsers import deduplicate_identifiers, parse_file, parse_identifiers
from src.telegram.collector import Collector


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
                fmt = "{:<5} {:<15} {:<25} {:<12} {:<8} {:<10} {:<12} {:<20}"
                header = (
                    "ID",
                    "Channel ID",
                    "Title",
                    "Username",
                    "Active",
                    "Messages",
                    "Last msg ID",
                    "Filter",
                )
                print(fmt.format(*header))
                print("-" * 110)
                for ch in channels:
                    if ch.is_filtered:
                        filt = ch.filter_flags if ch.filter_flags else "Yes"
                    else:
                        filt = "-"
                    print(
                        fmt.format(
                            ch.id or 0,
                            ch.channel_id,
                            (ch.title or "—")[:25],
                            (ch.username or "—")[:12],
                            "Yes" if ch.is_active else "No",
                            ch.message_count,
                            ch.last_collected_id,
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

                await db.add_channel(
                    Channel(
                        channel_id=info["channel_id"],
                        title=info["title"],
                        username=info["username"],
                        channel_type=info.get("channel_type"),
                    )
                )
                print(f"Added channel: {info['title']} ({info['channel_id']})")

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

                    await db.add_channel(
                        Channel(
                            channel_id=info["channel_id"],
                            title=info["title"],
                            username=info["username"],
                            channel_type=info.get("channel_type"),
                        )
                    )
                    existing_ids.add(info["channel_id"])
                    print(f"OK: {ident} — {info.get('title', '')} ({info['channel_id']})")
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
                channels = await db.get_channels()
                null_type = [ch for ch in channels if ch.channel_type is None]
                print(f"Channels with missing type: {len(null_type)}")
                # Pre-fetch dialogs to populate entity cache for channels without username
                prefetch = await pool.get_available_client()
                if prefetch:
                    client, phone = prefetch
                    try:
                        print("Pre-fetching dialogs to populate entity cache...")
                        await asyncio.wait_for(client.get_dialogs(), timeout=30)
                    except Exception as e:
                        logging.warning("Failed to pre-fetch dialogs: %s", e)
                    finally:
                        await pool.release_client(phone)
                updated = failed = deactivated = 0
                for ch in null_type:
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
                    await db.add_channel(Channel(
                        channel_id=info["channel_id"],
                        title=info["title"],
                        username=info["username"],
                        channel_type=info["channel_type"],
                    ))
                    print(f"OK: {ch.title} → {info['channel_type']}")
                    updated += 1
                print(f"\nUpdated: {updated}, Deactivated: {deactivated}, Skipped: {failed}")

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
                if ch.is_filtered:
                    title = ch.title or ch.channel_id
                    print(
                        f"Channel '{title}' is filtered"
                        " and excluded from collection"
                    )
                    return
                task_id = await db.create_collection_task(ch.channel_id, ch.title)
                await db.update_collection_task(task_id, "running")
                collector = Collector(pool, db, config.scheduler)
                try:
                    count = await collector.collect_single_channel(ch, full=True)
                    await db.update_collection_task(task_id, "completed", messages_collected=count)
                    print(f"Collected {count} messages from channel {ch.channel_id}")
                except Exception as exc:
                    await db.update_collection_task(task_id, "failed", error=str(exc)[:500])
                    raise
        finally:
            if pool:
                await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())
