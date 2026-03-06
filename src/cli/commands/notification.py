from __future__ import annotations

import argparse
import asyncio

from src.cli import runtime
from src.services.notification_service import NotificationService


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        config, db = await runtime.init_db(args.config)
        _, pool = await runtime.init_pool(config, db)
        svc = NotificationService(
            db, pool,
            config.notifications.bot_name_prefix,
            config.notifications.bot_username_prefix,
        )
        try:
            if args.notification_action == "setup":
                print("Creating notification bot via BotFather...")
                bot = await svc.setup_bot()
                print(f"Bot created: @{bot.bot_username}")
                print("[!] Сохраните токен — он больше не будет показан:")
                print(f"    Token: {bot.bot_token}")
                print(f"Send /start to @{bot.bot_username} in Telegram to activate it.")

            elif args.notification_action == "status":
                bot = await svc.get_status()
                if bot is None:
                    print("No notification bot configured.")
                else:
                    print(f"Bot: @{bot.bot_username}")
                    print(f"Bot ID: {bot.bot_id}")
                    print(f"Created at: {bot.created_at}")

            elif args.notification_action == "delete":
                print("Deleting notification bot via BotFather...")
                await svc.teardown_bot()
                print("Notification bot deleted.")
        finally:
            await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())
