from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    photo_parser = subparsers.add_parser("photo-loader", help="Photo upload automation")
    photo_sub = photo_parser.add_subparsers(dest="photo_loader_action")

    photo_dialogs = photo_sub.add_parser("dialogs", help="List dialogs for an account")
    photo_dialogs.add_argument("--phone", required=True, help="Account phone")

    photo_refresh = photo_sub.add_parser("refresh", help="Refresh dialog cache for photo loader")
    photo_refresh.add_argument("--phone", required=True, help="Account phone")

    photo_send = photo_sub.add_parser("send", help="Send photos now")
    photo_send.add_argument("--phone", required=True, help="Account phone")
    photo_send.add_argument("--target", required=True, help="Dialog id")
    photo_send.add_argument("--files", nargs="+", required=True, help="Photo file paths")
    photo_send.add_argument("--mode", choices=["album", "separate"], default="album")
    photo_send.add_argument("--caption", default=None, help="Caption")

    photo_schedule = photo_sub.add_parser("schedule-send", help="Schedule photo send via Telegram")
    photo_schedule.add_argument("--phone", required=True, help="Account phone")
    photo_schedule.add_argument("--target", required=True, help="Dialog id")
    photo_schedule.add_argument("--files", nargs="+", required=True, help="Photo file paths")
    photo_schedule.add_argument("--mode", choices=["album", "separate"], default="album")
    photo_schedule.add_argument("--at", required=True, help="ISO datetime")
    photo_schedule.add_argument("--caption", default=None, help="Caption")

    photo_batch = photo_sub.add_parser("batch-create", help="Create delayed batch from manifest")
    photo_batch.add_argument("--phone", required=True, help="Account phone")
    photo_batch.add_argument("--target", required=True, help="Dialog id")
    photo_batch.add_argument("--manifest", required=True, help="JSON/YAML manifest path")
    photo_batch.add_argument("--caption", default=None, help="Default caption")

    photo_sub.add_parser("batch-list", help="List photo batches")

    photo_items = photo_sub.add_parser("items", help="List photo batch items")
    photo_items.add_argument("--batch-id", type=int, default=None, help="Filter by batch id")
    photo_items.add_argument("--limit", type=int, default=100, help="Max items to show")

    photo_cancel = photo_sub.add_parser("batch-cancel", help="Cancel a photo batch item")
    photo_cancel.add_argument("id", type=int, help="Photo item id")

    photo_auto_create = photo_sub.add_parser("auto-create", help="Create auto-upload job")
    photo_auto_create.add_argument("--phone", required=True, help="Account phone")
    photo_auto_create.add_argument("--target", required=True, help="Dialog id")
    photo_auto_create.add_argument("--folder", required=True, help="Folder path")
    photo_auto_create.add_argument(
        "--interval",
        type=int,
        required=True,
        help="Interval in minutes",
    )
    photo_auto_create.add_argument("--mode", choices=["album", "separate"], default="album")
    photo_auto_create.add_argument("--caption", default=None, help="Caption")

    photo_sub.add_parser("auto-list", help="List auto-upload jobs")

    photo_auto_update = photo_sub.add_parser("auto-update", help="Update auto-upload job")
    photo_auto_update.add_argument("id", type=int, help="Job id")
    photo_auto_update.add_argument("--folder", default=None, help="Folder path")
    photo_auto_update.add_argument("--interval", type=int, default=None, help="Interval in minutes")
    photo_auto_update.add_argument("--mode", choices=["album", "separate"], default=None)
    photo_auto_update.add_argument("--caption", default=None, help="Caption")
    photo_auto_update.add_argument("--active", action="store_true", help="Enable job")
    photo_auto_update.add_argument("--paused", action="store_true", help="Pause job")

    photo_auto_toggle = photo_sub.add_parser("auto-toggle", help="Toggle auto-upload job")
    photo_auto_toggle.add_argument("id", type=int, help="Job id")

    photo_auto_delete = photo_sub.add_parser("auto-delete", help="Delete auto-upload job")
    photo_auto_delete.add_argument("id", type=int, help="Job id")

    photo_sub.add_parser("run-due", help="Run due photo items and auto jobs now")
