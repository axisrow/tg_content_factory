"""Shared async bodies for the ``notification`` CLI group (epic #959, Wave 2 — #1122).

Migrated off the argparse dispatcher onto the Typer ``app`` (see
``src/cli/typer_commands.py``). Each leaf sub-command is a plain ``async def
*_impl`` here — no local ``asyncio.run`` and no ``argparse.Namespace``.

Every body builds the pool + services itself and tears the pool down on exit
(notification commands touch BotFather via a connected account), mirroring the
original ``run`` lifecycle.
"""

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


async def _build(config_path: str):
    """Build (config, db, pool, target_svc, svc) for a notification command."""
    config, db = await runtime.init_db(config_path)
    _, pool = await runtime.init_pool(config, db)
    target_svc = NotificationTargetService(db, pool)
    svc = NotificationService(
        db,
        target_svc,
        config.notifications.bot_name_prefix,
        config.notifications.bot_username_prefix,
    )
    return config, db, pool, target_svc, svc


async def setup_impl(config_path: str) -> None:
    """Create a personal notification bot via BotFather."""
    _config, db, pool, _target_svc, svc = await _build(config_path)
    try:
        print("Creating notification bot via BotFather...")
        bot = await svc.setup_bot()
        print(f"Bot created: @{bot.bot_username}")
        print("[!] Сохраните токен — он больше не будет показан:")
        print(f"    Token: {bot.bot_token}")
        print(f"Send /start to @{bot.bot_username} in Telegram to activate it.")
    finally:
        await pool.disconnect_all()
        await db.close()


async def status_impl(config_path: str) -> None:
    """Show notification bot status."""
    _config, db, pool, target_svc, svc = await _build(config_path)
    try:
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
    finally:
        await pool.disconnect_all()
        await db.close()


async def delete_impl(config_path: str) -> None:
    """Delete the notification bot via BotFather."""
    _config, db, pool, _target_svc, svc = await _build(config_path)
    try:
        print("Deleting notification bot via BotFather...")
        await svc.teardown_bot()
        print("Notification bot deleted.")
    finally:
        await pool.disconnect_all()
        await db.close()


async def test_impl(config_path: str, *, message: str = "Тестовое уведомление") -> None:
    """Send a test notification message."""
    _config, db, pool, _target_svc, svc = await _build(config_path)
    try:
        await svc.send_notification(message)
        print("Test notification sent.")
    finally:
        await pool.disconnect_all()
        await db.close()


async def set_account_impl(config_path: str, *, phone: str) -> None:
    """Set the account used by the notification bot."""
    _config, db, pool, target_svc, _svc = await _build(config_path)
    try:
        await target_svc.set_configured_phone(phone)
        print(f"Notification bot account set to: {phone}")
    finally:
        await pool.disconnect_all()
        await db.close()


async def dry_run_impl(config_path: str) -> None:
    """Preview notification matches without sending."""
    from datetime import timezone

    _config, db, pool, _target_svc, _svc = await _build(config_path)
    try:
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
        # Match with the SAME engine production uses (regex/substring), not FTS, so the
        # preview agrees with what would actually fire (#838/3). Counts are uncapped —
        # dry_run_counts pages over the whole window so >5000 messages don't undercount.
        from src.services.notification_matcher import dry_run_counts

        counts = await dry_run_counts(db, queries, since)
        total_matches = 0
        for sq in queries:
            total = counts.get(sq.id, 0)
            name = sq.name or sq.query
            print(f"  {name}: {total} matches")
            total_matches += total
        print(f"\nTotal: {total_matches} matches (since {since or 'N/A'})")
    finally:
        await pool.disconnect_all()
        await db.close()


def run(args: argparse.Namespace) -> None:
    """Thin argparse adapter over the ``*_impl`` bodies (legacy dispatch path).

    The production CLI routes ``notification`` through the Typer ``app`` (#1122);
    this wrapper keeps the argparse leaf audit and command-level tests working.
    """
    action = args.notification_action
    if action == "setup":
        asyncio.run(setup_impl(args.config))
    elif action == "status":
        asyncio.run(status_impl(args.config))
    elif action == "delete":
        asyncio.run(delete_impl(args.config))
    elif action == "test":
        asyncio.run(test_impl(args.config, message=getattr(args, "message", "Тестовое уведомление")))
    elif action == "set-account":
        asyncio.run(set_account_impl(args.config, phone=args.phone))
    elif action == "dry-run":
        asyncio.run(dry_run_impl(args.config))
