from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    notif_parser = subparsers.add_parser("notification", help="Personal notification bot management")
    notif_sub = notif_parser.add_subparsers(dest="notification_action")
    notif_sub.add_parser("setup", help="Create personal notification bot via BotFather")
    notif_sub.add_parser("status", help="Show notification bot status")
    notif_sub.add_parser("delete", help="Delete notification bot via BotFather")
    notif_test = notif_sub.add_parser("test", help="Send a test notification message")
    notif_test.add_argument("--message", default="Тестовое уведомление", help="Message text")
    notif_sub.add_parser("dry-run", help="Preview notification matches without sending")
    notif_set_acc = notif_sub.add_parser("set-account", help="Set account for notification bot")
    notif_set_acc.add_argument("--phone", required=True, help="Account phone number")
