from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone

from src.cli import runtime
from src.database.bundles import PhotoLoaderBundle
from src.models import PhotoAutoUploadJob, PhotoSendMode
from src.services.channel_service import ChannelService
from src.services.photo_auto_upload_service import PhotoAutoUploadService
from src.services.photo_publish_service import PhotoPublishService
from src.services.photo_task_service import PhotoTarget, PhotoTaskService


async def _resolve_target(raw: str, pool) -> PhotoTarget:
    try:
        return PhotoTarget(dialog_id=int(raw))
    except ValueError:
        info = await pool.resolve_channel(raw)
        if not info:
            raise ValueError(f"Could not resolve target: {raw}")
        return PhotoTarget(
            dialog_id=int(info["channel_id"]),
            title=info.get("title"),
            target_type=info.get("channel_type"),
        )


def _parse_schedule_at(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.astimezone()
    return dt.astimezone(timezone.utc)


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        config, db = await runtime.init_db(args.config)
        _, pool = await runtime.init_pool(config, db)
        bundle = PhotoLoaderBundle.from_database(db)
        publish = PhotoPublishService(pool)
        tasks = PhotoTaskService(bundle, publish)
        auto = PhotoAutoUploadService(bundle, publish)
        channel_service = ChannelService(db, pool, None)  # type: ignore[arg-type]
        try:
            action = args.photo_loader_action
            if action == "dialogs":
                dialogs = await channel_service.get_my_dialogs(args.phone)
                for dialog in dialogs:
                    print(
                        f"{dialog['channel_id']:>14}  {dialog['channel_type']:<12} "
                        f"{dialog['title']}"
                    )
                return

            if action == "refresh":
                dialogs = await channel_service.get_my_dialogs(args.phone, refresh=True)
                print(f"Dialogs refreshed: {len(dialogs)} total.")
                return

            if action == "send":
                item = await tasks.send_now(
                    phone=args.phone,
                    target=await _resolve_target(args.target, pool),
                    file_paths=args.files,
                    mode=args.mode,
                    caption=args.caption,
                )
                print(f"Sent photo item #{item.id} status={item.status}")
                return

            if action == "schedule-send":
                item = await tasks.schedule_send(
                    phone=args.phone,
                    target=await _resolve_target(args.target, pool),
                    file_paths=args.files,
                    mode=args.mode,
                    schedule_at=_parse_schedule_at(args.at),
                    caption=args.caption,
                )
                print(f"Scheduled photo item #{item.id} status={item.status}")
                return

            if action == "batch-create":
                manifest = tasks.load_manifest(args.manifest)
                batch_id = await tasks.create_batch(
                    phone=args.phone,
                    target=await _resolve_target(args.target, pool),
                    entries=manifest,
                    caption=args.caption,
                )
                print(f"Created photo batch #{batch_id}")
                return

            if action == "batch-list":
                batches = await tasks.list_batches()
                for batch in batches:
                    print(
                        f"#{batch.id} phone={batch.phone} target={batch.target_dialog_id} "
                        f"status={batch.status}"
                    )
                return

            if action == "items":
                if args.batch_id is not None:
                    items = await bundle.list_items_for_batch(args.batch_id, limit=args.limit)
                else:
                    items = await tasks.list_items(limit=args.limit)
                for item in items:
                    print(
                        f"#{item.id} batch={item.batch_id or '-'} phone={item.phone} "
                        f"target={item.target_dialog_id} status={item.status}"
                    )
                return

            if action == "batch-cancel":
                ok = await tasks.cancel_item(args.id)
                print("Cancelled" if ok else "Not cancelled")
                return

            if action == "auto-create":
                target = await _resolve_target(args.target, pool)
                job_id = await auto.create_job(
                    PhotoAutoUploadJob(
                        phone=args.phone,
                        target_dialog_id=target.dialog_id,
                        target_title=target.title,
                        target_type=target.target_type,
                        folder_path=args.folder,
                        send_mode=PhotoSendMode(args.mode),
                        caption=args.caption,
                        interval_minutes=args.interval,
                    )
                )
                print(f"Created auto job #{job_id}")
                return

            if action == "auto-list":
                jobs = await auto.list_jobs()
                for job in jobs:
                    print(
                        f"#{job.id} target={job.target_dialog_id} folder={job.folder_path} "
                        f"interval={job.interval_minutes} active={job.is_active}"
                    )
                return

            if action == "auto-update":
                kwargs = {
                    "folder_path": args.folder,
                    "send_mode": PhotoSendMode(args.mode) if args.mode else None,
                    "caption": args.caption,
                    "interval_minutes": args.interval,
                    "is_active": None,
                }
                if args.active:
                    kwargs["is_active"] = True
                if args.paused:
                    kwargs["is_active"] = False
                await auto.update_job(args.id, **kwargs)
                print(f"Updated auto job #{args.id}")
                return

            if action == "auto-toggle":
                job = await auto.get_job(args.id)
                if not job:
                    print("Auto job not found")
                    return
                await auto.update_job(args.id, is_active=not job.is_active)
                print(f"Toggled auto job #{args.id} to {not job.is_active}")
                return

            if action == "auto-delete":
                await auto.delete_job(args.id)
                print(f"Deleted auto job #{args.id}")
                return

            if action == "run-due":
                items = await tasks.run_due()
                jobs = await auto.run_due()
                print(f"Processed due photo items={items} auto_jobs={jobs}")
                return
        finally:
            await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())
