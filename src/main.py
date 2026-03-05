from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from dotenv import load_dotenv


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


async def _init_db(config_path: str):
    """Initialize config and database, return (config, db)."""
    from src.config import load_config
    from src.database import Database

    config = load_config(config_path)
    db = Database(config.database.path)
    await db.initialize()
    return config, db


async def _init_pool(config, db):
    """Initialize TelegramAuth + ClientPool, return (auth, pool)."""
    from src.telegram.auth import TelegramAuth
    from src.telegram.client_pool import ClientPool

    api_id = config.telegram.api_id
    api_hash = config.telegram.api_hash
    if api_id == 0 or not api_hash:
        stored_id = await db.get_setting("tg_api_id")
        stored_hash = await db.get_setting("tg_api_hash")
        if stored_id and stored_hash:
            api_id = int(stored_id)
            api_hash = stored_hash

    auth = TelegramAuth(api_id, api_hash)
    pool = ClientPool(auth, db, config.scheduler.max_flood_wait_sec)
    await pool.initialize()
    return auth, pool


def cmd_serve(args: argparse.Namespace) -> None:
    """Start web server."""
    import uvicorn

    from src.config import load_config
    from src.web.app import create_app

    config = load_config(args.config)
    if args.web_pass:
        config.web.password = args.web_pass
    if not config.web.password:
        logging.error("WEB_PASS must be set for web panel authentication")
        sys.exit(1)
    app = create_app(config)
    uvicorn.run(app, host=config.web.host, port=config.web.port)


def cmd_collect(args: argparse.Namespace) -> None:
    """Run one-shot collection."""

    async def _run():
        from src.telegram.collector import Collector

        config, db = await _init_db(args.config)
        _, pool = await _init_pool(config, db)
        try:
            if not pool.clients:
                logging.error("No connected accounts. Run 'serve' and add accounts via web UI.")
                return

            collector = Collector(pool, db, config.scheduler)

            if args.channel_id:
                channels = await db.get_channels()
                channel = next(
                    (ch for ch in channels if ch.channel_id == args.channel_id), None
                )
                if not channel:
                    print(f"Channel {args.channel_id} not found in DB")
                    return
                count = await collector.collect_single_channel(channel, full=True)
                print(f"Collected {count} messages from channel {args.channel_id}")
            else:
                stats = await collector.collect_all_channels()
                print(f"Collection complete: {stats}")
        finally:
            await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())


def cmd_search(args: argparse.Namespace) -> None:
    """Search messages in local DB or via Telegram API."""

    async def _run():
        from src.search.engine import SearchEngine

        config, db = await _init_db(args.config)

        pool = None
        if args.mode in ("telegram", "my_chats", "channel"):
            _, pool = await _init_pool(config, db)
            if not pool.clients:
                logging.error(
                    "No connected accounts. Run 'serve' and add accounts via web UI."
                )
                await db.close()
                return

        try:
            engine = SearchEngine(db, pool)

            if args.mode == "telegram":
                result = await engine.search_telegram(args.query, limit=args.limit)
            elif args.mode == "my_chats":
                result = await engine.search_my_chats(args.query, limit=args.limit)
            elif args.mode == "channel":
                result = await engine.search_in_channel(
                    args.channel_id, args.query, limit=args.limit,
                )
            else:
                result = await engine.search_local(args.query, limit=args.limit)

            print(f"Found {result.total} results for '{result.query}':\n")
            for msg in result.messages:
                text_preview = (msg.text or "")[:200]
                print(f"[{msg.date}] Channel {msg.channel_id}: {text_preview}")
                print("---")
        finally:
            if pool:
                await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())


def _resolve_channel(channels: list, identifier: str):
    """Resolve channel by pk, channel_id, or @username."""
    try:
        num = int(identifier)
        ch = next((c for c in channels if c.id == num), None)
        if ch:
            return ch
        return next((c for c in channels if c.channel_id == num), None)
    except ValueError:
        pass
    uname = identifier.lstrip("@").lower()
    return next((c for c in channels if (c.username or "").lower() == uname), None)


def cmd_channel(args: argparse.Namespace) -> None:
    """Channel management commands."""

    async def _run():
        config, db = await _init_db(args.config)
        pool = None

        try:
            if args.channel_action == "list":
                channels = await db.get_channels_with_counts()
                if not channels:
                    print("No channels found.")
                    return
                fmt = "{:<5} {:<15} {:<25} {:<12} {:<8} {:<10} {:<12}"
                header = ("ID", "Channel ID", "Title", "Username",
                          "Active", "Messages", "Last msg ID")
                print(fmt.format(*header))
                print("-" * 90)
                for ch in channels:
                    print(fmt.format(
                        ch.id or 0,
                        ch.channel_id,
                        (ch.title or "—")[:25],
                        (ch.username or "—")[:12],
                        "Yes" if ch.is_active else "No",
                        ch.message_count,
                        ch.last_collected_id,
                    ))

            elif args.channel_action == "add":
                _, pool = await _init_pool(config, db)
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
                from src.models import Channel
                channel = Channel(
                    channel_id=info["channel_id"],
                    title=info["title"],
                    username=info["username"],
                    channel_type=info.get("channel_type"),
                )
                await db.add_channel(channel)
                print(f"Added channel: {info['title']} ({info['channel_id']})")

            elif args.channel_action == "delete":
                channels = await db.get_channels()
                ch = _resolve_channel(channels, args.identifier)
                if not ch:
                    print(f"Channel '{args.identifier}' not found")
                    return
                await db.delete_channel(ch.id)
                print(f"Deleted channel '{ch.title}' (pk={ch.id})")

            elif args.channel_action == "toggle":
                channels = await db.get_channels()
                ch = _resolve_channel(channels, args.identifier)
                if not ch:
                    print(f"Channel '{args.identifier}' not found")
                    return
                new_state = not ch.is_active
                await db.set_channel_active(ch.id, new_state)
                print(f"Channel '{ch.title}' (pk={ch.id}): active={new_state}")

            elif args.channel_action == "import":
                from pathlib import Path

                from src.parsers import (
                    deduplicate_identifiers,
                    parse_file,
                    parse_identifiers,
                )

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

                _, pool = await _init_pool(config, db)
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
                    from src.models import Channel as Ch
                    ch = Ch(
                        channel_id=info["channel_id"],
                        title=info["title"],
                        username=info["username"],
                        channel_type=info.get("channel_type"),
                    )
                    await db.add_channel(ch)
                    existing_ids.add(info["channel_id"])
                    print(f"OK: {ident} — {info.get('title', '')} ({info['channel_id']})")
                    added += 1

                print(f"\nTotal: {len(identifiers)}, Added: {added}, "
                      f"Skipped: {skipped}, Failed: {failed}")

            elif args.channel_action == "stats":
                from src.telegram.collector import Collector

                _, pool = await _init_pool(config, db)
                if not pool.clients:
                    logging.error("No connected accounts.")
                    return
                collector = Collector(pool, db, config.scheduler)

                if args.all:
                    result = await collector.collect_all_stats()
                    print(f"Stats collected: {result}")
                else:
                    channels = await db.get_channels()
                    ch = _resolve_channel(channels, args.identifier)
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

            elif args.channel_action == "collect":
                from src.telegram.collector import Collector

                _, pool = await _init_pool(config, db)
                if not pool.clients:
                    logging.error("No connected accounts.")
                    return
                channels = await db.get_channels()
                ch = _resolve_channel(channels, args.identifier)
                if not ch:
                    print(f"Channel '{args.identifier}' not found")
                    return
                task_id = await db.create_collection_task(ch.channel_id, ch.title)
                await db.update_collection_task(task_id, "running")
                collector = Collector(pool, db, config.scheduler)
                try:
                    count = await collector.collect_single_channel(ch, full=True)
                    await db.update_collection_task(
                        task_id, "completed", messages_collected=count
                    )
                    print(f"Collected {count} messages from channel {ch.channel_id}")
                except Exception as exc:
                    await db.update_collection_task(
                        task_id, "failed", error=str(exc)[:500]
                    )
                    raise

        finally:
            if pool:
                await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())


def cmd_keyword(args: argparse.Namespace) -> None:
    """Keyword management commands."""

    async def _run():
        _, db = await _init_db(args.config)

        try:
            if args.keyword_action == "list":
                keywords = await db.get_keywords()
                if not keywords:
                    print("No keywords found.")
                    return
                fmt = "{:<5} {:<40} {:<8} {:<8}"
                print(fmt.format("ID", "Pattern", "Regex", "Active"))
                print("-" * 65)
                for kw in keywords:
                    print(fmt.format(
                        kw.id or 0,
                        kw.pattern[:40],
                        "Yes" if kw.is_regex else "No",
                        "Yes" if kw.is_active else "No",
                    ))

            elif args.keyword_action == "add":
                from src.models import Keyword
                kw = Keyword(pattern=args.pattern, is_regex=args.regex)
                kid = await db.add_keyword(kw)
                print(f"Added keyword id={kid}: {args.pattern}"
                      f"{' (regex)' if args.regex else ''}")

            elif args.keyword_action == "delete":
                await db.delete_keyword(args.id)
                print(f"Deleted keyword id={args.id}")

            elif args.keyword_action == "toggle":
                keywords = await db.get_keywords()
                kw = next((k for k in keywords if k.id == args.id), None)
                if not kw:
                    print(f"Keyword id={args.id} not found")
                    return
                new_state = not kw.is_active
                await db.set_keyword_active(args.id, new_state)
                print(f"Keyword id={args.id}: active={new_state}")

        finally:
            await db.close()

    asyncio.run(_run())


def cmd_account(args: argparse.Namespace) -> None:
    """Account management commands."""

    async def _run():
        _, db = await _init_db(args.config)

        try:
            if args.account_action == "list":
                accounts = await db.get_accounts()
                if not accounts:
                    print("No accounts found.")
                    return
                fmt = "{:<5} {:<16} {:<9} {:<8} {:<8}"
                print(fmt.format("ID", "Phone", "Primary", "Active", "Premium"))
                print("-" * 50)
                for acc in accounts:
                    print(fmt.format(
                        acc.id or 0,
                        acc.phone,
                        "Yes" if acc.is_primary else "No",
                        "Yes" if acc.is_active else "No",
                        "Yes" if acc.is_premium else "No",
                    ))

            elif args.account_action == "toggle":
                accounts = await db.get_accounts()
                acc = next((a for a in accounts if a.id == args.id), None)
                if not acc:
                    print(f"Account id={args.id} not found")
                    return
                new_state = not acc.is_active
                await db.set_account_active(args.id, new_state)
                print(f"Account id={args.id} ({acc.phone}): active={new_state}")

            elif args.account_action == "delete":
                await db.delete_account(args.id)
                print(f"Deleted account id={args.id}")

        finally:
            await db.close()

    asyncio.run(_run())


def cmd_scheduler(args: argparse.Namespace) -> None:
    """Scheduler commands."""

    async def _run():
        from src.scheduler.manager import SchedulerManager
        from src.search.engine import SearchEngine
        from src.telegram.collector import Collector

        config, db = await _init_db(args.config)
        _, pool = await _init_pool(config, db)

        try:
            if not pool.clients:
                logging.error("No connected accounts.")
                return

            collector = Collector(pool, db, config.scheduler)
            search_engine = SearchEngine(db, pool)

            if args.scheduler_action == "start":
                manager = SchedulerManager(
                    collector, config.scheduler,
                    search_engine=search_engine, db=db,
                )
                await manager.start()
                print(
                    f"Scheduler started (every {config.scheduler.collect_interval_minutes} min). "
                    "Press Ctrl+C to stop."
                )
                try:
                    while True:
                        await asyncio.sleep(1)
                except (KeyboardInterrupt, asyncio.CancelledError):
                    await manager.stop()
                    print("\nScheduler stopped.")

            elif args.scheduler_action == "trigger":
                stats = await collector.collect_all_channels()
                print(f"Collection complete: {stats}")

            elif args.scheduler_action == "search":
                manager = SchedulerManager(
                    collector, config.scheduler,
                    search_engine=search_engine, db=db,
                )
                stats = await manager.trigger_search_now()
                print(f"Keyword search complete: {stats}")

        finally:
            await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())


def main() -> None:
    load_dotenv()
    setup_logging()

    parser = argparse.ArgumentParser(description="TG Post Search")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    sub = parser.add_subparsers(dest="command")

    # serve
    serve_parser = sub.add_parser("serve", help="Start web server")
    serve_parser.add_argument("--web-pass", help="Web panel password (overrides config)")

    # collect
    collect_parser = sub.add_parser("collect", help="Run one-shot collection")
    collect_parser.add_argument(
        "--channel-id", type=int, default=None,
        help="Collect single channel by channel_id (full mode)",
    )

    # search
    search_parser = sub.add_parser("search", help="Search messages")
    search_parser.add_argument("query", help="Search query")
    search_parser.add_argument("--limit", type=int, default=20, help="Max results")
    search_parser.add_argument(
        "--mode",
        choices=["local", "telegram", "my_chats", "channel"],
        default="local",
        help="Search mode: local, telegram, my_chats, channel",
    )
    search_parser.add_argument(
        "--channel-id", type=int, default=None,
        help="Channel ID for --mode=channel",
    )

    # channel
    ch_parser = sub.add_parser("channel", help="Channel management")
    ch_sub = ch_parser.add_subparsers(dest="channel_action")

    ch_sub.add_parser("list", help="List channels with message counts")

    ch_add = ch_sub.add_parser("add", help="Add channel by identifier")
    ch_add.add_argument("identifier", help="Username, link, or numeric ID")

    ch_del = ch_sub.add_parser("delete", help="Delete channel")
    ch_del.add_argument("identifier", help="Channel pk, channel_id, or @username")

    ch_toggle = ch_sub.add_parser("toggle", help="Toggle channel active state")
    ch_toggle.add_argument("identifier", help="Channel pk, channel_id, or @username")

    ch_collect = ch_sub.add_parser("collect", help="Collect single channel (full)")
    ch_collect.add_argument("identifier", help="Channel pk, channel_id, or @username")

    ch_stats = ch_sub.add_parser("stats", help="Collect channel statistics")
    ch_stats.add_argument(
        "identifier", nargs="?", default=None,
        help="Channel pk, channel_id, or @username",
    )
    ch_stats.add_argument(
        "--all", action="store_true",
        help="Collect stats for all active channels",
    )

    ch_import = ch_sub.add_parser("import", help="Bulk import from file or text")
    ch_import.add_argument("source", help="Path to .txt/.csv file, or comma-separated identifiers")

    # keyword
    kw_parser = sub.add_parser("keyword", help="Keyword management")
    kw_sub = kw_parser.add_subparsers(dest="keyword_action")

    kw_sub.add_parser("list", help="List keywords")

    kw_add = kw_sub.add_parser("add", help="Add keyword")
    kw_add.add_argument("pattern", help="Keyword pattern")
    kw_add.add_argument("--regex", action="store_true", help="Treat pattern as regex")

    kw_del = kw_sub.add_parser("delete", help="Delete keyword")
    kw_del.add_argument("id", type=int, help="Keyword id")

    kw_toggle = kw_sub.add_parser("toggle", help="Toggle keyword active state")
    kw_toggle.add_argument("id", type=int, help="Keyword id")

    # account
    acc_parser = sub.add_parser("account", help="Account management")
    acc_sub = acc_parser.add_subparsers(dest="account_action")

    acc_sub.add_parser("list", help="List accounts")

    acc_toggle = acc_sub.add_parser("toggle", help="Toggle account active state")
    acc_toggle.add_argument("id", type=int, help="Account id")

    acc_del = acc_sub.add_parser("delete", help="Delete account")
    acc_del.add_argument("id", type=int, help="Account id")

    # scheduler
    sched_parser = sub.add_parser("scheduler", help="Scheduler control")
    sched_sub = sched_parser.add_subparsers(dest="scheduler_action")

    sched_sub.add_parser("start", help="Start scheduler (foreground)")
    sched_sub.add_parser("trigger", help="Trigger one-shot collection")
    sched_sub.add_parser("search", help="Run keyword search now")

    args = parser.parse_args()

    commands = {
        "serve": cmd_serve,
        "collect": cmd_collect,
        "search": cmd_search,
        "channel": cmd_channel,
        "keyword": cmd_keyword,
        "account": cmd_account,
        "scheduler": cmd_scheduler,
    }

    handler = commands.get(args.command)
    if handler:
        # Check subcommand is set for nested parsers
        sub_attr = {
            "channel": "channel_action",
            "keyword": "keyword_action",
            "account": "account_action",
            "scheduler": "scheduler_action",
        }
        if args.command in sub_attr and not getattr(args, sub_attr[args.command], None):
            # Print help for the subparser
            parser.parse_args([args.command, "--help"])
        else:
            handler(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
