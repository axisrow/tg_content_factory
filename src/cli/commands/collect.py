from __future__ import annotations

import argparse
import asyncio
import logging

from src.cli import runtime
from src.telegram.collector import Collector


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        config, db = await runtime.init_db(args.config)
        _, pool = await runtime.init_pool(config, db)
        try:
            if not pool.clients:
                logging.error("No connected accounts. Run 'serve' and add accounts via web UI.")
                return

            collector = Collector(pool, db, config.scheduler)

            if args.channel_id:
                channels = await db.get_channels()
                channel = next((ch for ch in channels if ch.channel_id == args.channel_id), None)
                if not channel:
                    print(f"Channel {args.channel_id} not found in DB")
                    return
                if channel.is_filtered:
                    print(
                        f"Channel {args.channel_id} is filtered and excluded from collection"
                    )
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
