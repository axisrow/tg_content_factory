from __future__ import annotations

import argparse
import asyncio
import inspect
from unittest.mock import Mock

from src.cli import runtime
from src.services.notification_service import NotificationService
from src.services.notification_target_service import NotificationTargetService


async def _describe_target(target_svc):
    describe = getattr(target_svc, "describe_target", None)
    if not callable(describe):
        return None
    status = describe()
    if inspect.isawaitable(status):
        return await status
    if isinstance(status, Mock):
        return None
    return status


def _print_target_status(status) -> None:
    print("Notification target unavailable.")
    print(f"Mode: {getattr(status, 'mode', 'unknown')}")
    print(f"Status: {getattr(status, 'state', 'unknown')}")
    configured_phone = getattr(status, "configured_phone", None)
    effective_phone = getattr(status, "effective_phone", None)
    if configured_phone:
        print(f"Configured phone: {configured_phone}")
    if effective_phone:
        print(f"Effective phone: {effective_phone}")
    message = getattr(status, "message", None)
    if message:
        print(f"Diagnostic: {message}")


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        config, db = await runtime.init_db(args.config)
        _, pool = await runtime.init_pool(config, db)
        target_svc = NotificationTargetService(db, pool)
        svc = NotificationService(
            db,
            target_svc,
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
                target_status = await _describe_target(target_svc)
                if target_status is not None and getattr(target_status, "state", None) != "available":
                    _print_target_status(target_status)
                    return
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

            elif args.notification_action == "test":
                message = getattr(args, "message", "Тестовое уведомление")
                await svc.send_notification(message)
                print("Test notification sent.")

            elif args.notification_action == "set-account":
                phone = args.phone
                await target_svc.set_configured_phone(phone)
                print(f"Notification bot account set to: {phone}")
                return

            elif args.notification_action == "dry-run":
                from datetime import timezone

                last_task = await db.repos.tasks.get_last_completed_collect_task()
                since = None
                if last_task and last_task.completed_at:
                    since = last_task.completed_at.astimezone(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                queries = await db.get_notification_queries(active_only=True)
                filtered = []
                for sq in queries:
                    val = await db.repos.settings.get_setting(
                        f"scheduler_job_disabled:sq_{sq.id}"
                    )
                    if val != "1":
                        filtered.append(sq)
                queries = filtered
                if not queries:
                    print("No active notification queries.")
                    return
                total_matches = 0
                for sq in queries:
                    if since:
                        try:
                            _, total = await db.search_messages_for_query_since(
                                sq, since, limit=0
                            )
                        except Exception:
                            total = 0
                    else:
                        total = 0
                    name = sq.name or sq.query
                    print(f"  {name}: {total} matches")
                    total_matches += total
                print(f"\nTotal: {total_matches} matches (since {since or 'N/A'})")
        finally:
            await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())
