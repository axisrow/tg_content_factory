"""Shared async bodies for the ``photo-loader`` CLI group (epic #959, Wave 3 — #1123).

Migrated off the argparse dispatcher onto the Typer ``app`` (see
``src/cli/typer_commands.py``). Each leaf sub-command is a plain ``async def
*_impl`` here — no local ``asyncio.run`` and no ``argparse.Namespace``. A thin
``run(args)`` adapter is kept for the argparse leaf audit and existing tests.

Every sub-command needs the photo-loader service stack (pool + publish + task +
auto services); :func:`_run_with_services` builds it once, runs the body, and
tears the pool down — so the per-command bodies stay focused on their own logic.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import TypeVar

import typer

from src.cli import runtime
from src.cli.commands.common import (
    PhotoMode,
    apply_startup,
    run_async,
)
from src.database.bundles import PhotoLoaderBundle
from src.models import PhotoAutoUploadJob, PhotoSendMode
from src.services.channel_service import ChannelService
from src.services.photo_auto_upload_service import PhotoAutoUploadService
from src.services.photo_publish_service import PhotoPublishService
from src.services.photo_task_service import PhotoTarget, PhotoTaskService

T = TypeVar("T")


class _Services:
    """The photo-loader service bundle a sub-command operates on."""

    def __init__(self, db, pool, bundle, publish, tasks, auto, channel_service) -> None:
        self.db = db
        self.pool = pool
        self.bundle = bundle
        self.publish = publish
        self.tasks = tasks
        self.auto = auto
        self.channel_service = channel_service


async def _run_with_services(config_path: str, body: Callable[[_Services], Awaitable[T]]) -> T | None:
    """Build the photo-loader service stack, run ``body``, then tear the pool down.

    A ``ValueError`` from an unresolvable target (bad username, "me" without a
    connected account, …) is reported cleanly and exits 1 — matching the pre-Typer
    behaviour, not a raw traceback.
    """
    config, db = await runtime.init_db(config_path)
    _, pool = await runtime.init_pool(config, db)
    bundle = PhotoLoaderBundle.from_database(db)
    publish = PhotoPublishService(pool)
    tasks = PhotoTaskService(bundle, publish)
    auto = PhotoAutoUploadService(bundle, publish)
    channel_service = ChannelService(db, pool, None)  # type: ignore[arg-type]
    services = _Services(db, pool, bundle, publish, tasks, auto, channel_service)
    try:
        return await body(services)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        await pool.disconnect_all()
        await db.close()


async def _resolve_self_target(pool, phone: str | None) -> PhotoTarget:
    """Resolve "me"/"self" to the account's own Saved Messages dialog id."""
    from src.telegram.backends import adapt_transport_session

    if phone:
        # A specific account was requested. Its Saved Messages id is account-specific,
        # and deferred paths (schedule-send/batch-create/auto-create) persist `phone`
        # separately from this dialog_id. Falling back to another account here would
        # store *that* account's self-id against the requested phone, so a later
        # publish would deliver to the wrong account's chat. Fail instead.
        result = await pool.get_client_by_phone(phone)
        if result is None:
            raise ValueError(
                f"Could not resolve target 'me': account {phone} is not connected/available"
            )
    else:
        result = await pool.get_available_client()
        if result is None:
            raise ValueError("Could not resolve target 'me': no connected Telegram account")
    session, acquired_phone = result
    session = adapt_transport_session(session, disconnect_on_close=False)
    try:
        me = await session.fetch_me()
    finally:
        # Release the lease promptly (mirrors channel.py); the subsequent send/
        # schedule call re-acquires it. disconnect_all() in the finally is the
        # backstop, but explicit release keeps the pool tidy for other callers.
        await pool.release_client(acquired_phone)
    self_id = getattr(me, "id", None)
    if self_id is None:
        raise ValueError("Could not resolve target 'me': failed to fetch account id")
    # target_type="saved" so PhotoPublishService → resolve_dialog_entity maps it to
    # PeerUser (Saved Messages). Any other value (e.g. "user") falls through to
    # PeerChannel(abs(id)), which mis-resolves the self user-id as a channel.
    return PhotoTarget(dialog_id=int(self_id), title="Saved Messages", target_type="saved")


async def _resolve_target(raw: str, pool, phone: str | None = None) -> PhotoTarget:
    # "me"/"self" is the account's Saved Messages — parity with `dialogs send`,
    # which resolves it natively. resolve_channel() rejects it (it is a user, not a
    # channel), so handle it explicitly here instead of crashing in int(raw).
    if raw.strip().lower() in {"me", "self"}:
        return await _resolve_self_target(pool, phone)
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
    from src.utils.datetime import parse_required_schedule_datetime

    return parse_required_schedule_datetime(value)


def _format_eta(seconds: float) -> str:
    if seconds <= 0:
        return "0m"
    minutes = int(round(seconds / 60))
    if minutes < 1:
        return "<1m"
    if minutes < 60:
        return f"{minutes}m"
    hours, mins = divmod(minutes, 60)
    return f"{hours}h {mins}m" if mins else f"{hours}h"


def _make_progress_printer(label: str) -> tuple[Callable[[int, int], None], dict[str, int]]:
    started = time.monotonic()
    state = {"done": 0, "total": 0}

    def _progress(done: int, total: int) -> None:
        state["done"] = done
        state["total"] = total
        remaining = max(total - done, 0)
        eta = 0.0 if done <= 0 else (time.monotonic() - started) * remaining / done
        print(f"[{done}/{total}] {label} processed  (~{_format_eta(eta)} left)")

    return _progress, state


def _print_progress_summary(label: str, processed: int, state: dict[str, int]) -> None:
    total = state["total"] or processed
    if total or processed:
        print(f"Progress: {processed}/{total} {label} processed.")


async def dialogs_impl(config_path: str, *, phone: str) -> None:
    """List dialogs for an account."""
    async def _body(s: _Services) -> None:
        dialogs = await s.channel_service.get_my_dialogs(phone)
        for dialog in dialogs:
            print(
                f"{dialog['channel_id']:>14}  {dialog['channel_type']:<12} {dialog['title']}"
            )
    await _run_with_services(config_path, _body)


async def refresh_impl(config_path: str, *, phone: str) -> None:
    """Refresh the dialog cache for the photo loader."""
    async def _body(s: _Services) -> None:
        dialogs = await s.channel_service.get_my_dialogs(phone, refresh=True)
        print(f"Dialogs refreshed: {len(dialogs)} total.")
    await _run_with_services(config_path, _body)


async def send_impl(
    config_path: str, *, phone: str, target: str, files: list[str], mode: str = "album", caption: str | None = None
) -> None:
    """Send photos now."""
    async def _body(s: _Services) -> None:
        item = await s.tasks.send_now(
            phone=phone,
            target=await _resolve_target(target, s.pool, phone=phone),
            file_paths=files,
            mode=mode,
            caption=caption,
        )
        print(f"Sent photo item #{item.id} status={item.status}")
    await _run_with_services(config_path, _body)


async def schedule_send_impl(
    config_path: str,
    *,
    phone: str,
    target: str,
    files: list[str],
    at: str,
    mode: str = "album",
    caption: str | None = None,
) -> None:
    """Schedule a photo send via Telegram."""
    async def _body(s: _Services) -> None:
        item = await s.tasks.schedule_send(
            phone=phone,
            target=await _resolve_target(target, s.pool, phone=phone),
            file_paths=files,
            mode=mode,
            schedule_at=_parse_schedule_at(at),
            caption=caption,
        )
        print(f"Scheduled photo item #{item.id} status={item.status}")
    await _run_with_services(config_path, _body)


async def batch_create_impl(
    config_path: str, *, phone: str, target: str, manifest: str, caption: str | None = None
) -> None:
    """Create a delayed batch from a manifest."""
    async def _body(s: _Services) -> None:
        entries = s.tasks.load_manifest(manifest)
        batch_id = await s.tasks.create_batch(
            phone=phone,
            target=await _resolve_target(target, s.pool, phone=phone),
            entries=entries,
            caption=caption,
        )
        print(f"Created photo batch #{batch_id}")
    await _run_with_services(config_path, _body)


async def publish_impl(config_path: str, *, batch_id: int) -> None:
    """Publish a held photo batch into the due queue."""
    async def _body(s: _Services) -> None:
        published = await s.tasks.publish_batch(batch_id)
        print(f"Published photo batch #{batch_id}: items={published}")
    await _run_with_services(config_path, _body)


async def batch_list_impl(config_path: str) -> None:
    """List photo batches."""
    async def _body(s: _Services) -> None:
        batches = await s.tasks.list_batches()
        for batch in batches:
            print(
                f"#{batch.id} phone={batch.phone} target={batch.target_dialog_id} "
                f"status={batch.status}"
            )
    await _run_with_services(config_path, _body)


async def items_impl(config_path: str, *, batch_id: int | None = None, limit: int = 100) -> None:
    """List photo batch items."""
    async def _body(s: _Services) -> None:
        if batch_id is not None:
            items = await s.bundle.list_items_for_batch(batch_id, limit=limit)
        else:
            items = await s.tasks.list_items(limit=limit)
        for item in items:
            print(
                f"#{item.id} batch={item.batch_id or '-'} phone={item.phone} "
                f"target={item.target_dialog_id} status={item.status}"
            )
    await _run_with_services(config_path, _body)


async def batch_cancel_impl(config_path: str, *, item_id: int) -> None:
    """Cancel a photo batch item."""
    async def _body(s: _Services) -> None:
        ok = await s.tasks.cancel_item(item_id)
        print("Cancelled" if ok else "Not cancelled")
    await _run_with_services(config_path, _body)


async def auto_create_impl(
    config_path: str,
    *,
    phone: str,
    target: str,
    folder: str,
    interval: int,
    mode: str = "album",
    caption: str | None = None,
) -> None:
    """Create an auto-upload job."""
    async def _body(s: _Services) -> None:
        resolved = await _resolve_target(target, s.pool, phone=phone)
        job_id = await s.auto.create_job(
            PhotoAutoUploadJob(
                phone=phone,
                target_dialog_id=resolved.dialog_id,
                target_title=resolved.title,
                target_type=resolved.target_type,
                folder_path=folder,
                send_mode=PhotoSendMode(mode),
                caption=caption,
                interval_minutes=interval,
            )
        )
        print(f"Created auto job #{job_id}")
    await _run_with_services(config_path, _body)


async def auto_list_impl(config_path: str) -> None:
    """List auto-upload jobs."""
    async def _body(s: _Services) -> None:
        jobs = await s.auto.list_jobs()
        for job in jobs:
            print(
                f"#{job.id} target={job.target_dialog_id} folder={job.folder_path} "
                f"interval={job.interval_minutes} active={job.is_active}"
            )
    await _run_with_services(config_path, _body)


async def auto_update_impl(
    config_path: str,
    *,
    job_id: int,
    folder: str | None = None,
    interval: int | None = None,
    mode: str | None = None,
    caption: str | None = None,
    active: bool = False,
    paused: bool = False,
) -> None:
    """Update an auto-upload job."""
    async def _body(s: _Services) -> None:
        kwargs = {
            "folder_path": folder,
            "send_mode": PhotoSendMode(mode) if mode else None,
            "caption": caption,
            "interval_minutes": interval,
            "is_active": None,
        }
        if active:
            kwargs["is_active"] = True
        if paused:
            kwargs["is_active"] = False
        await s.auto.update_job(job_id, **kwargs)
        print(f"Updated auto job #{job_id}")
    await _run_with_services(config_path, _body)


async def auto_toggle_impl(config_path: str, *, job_id: int) -> None:
    """Toggle an auto-upload job's active state."""
    async def _body(s: _Services) -> None:
        job = await s.auto.get_job(job_id)
        if not job:
            print("Auto job not found")
            return
        await s.auto.update_job(job_id, is_active=not job.is_active)
        print(f"Toggled auto job #{job_id} to {not job.is_active}")
    await _run_with_services(config_path, _body)


async def auto_delete_impl(config_path: str, *, job_id: int) -> None:
    """Delete an auto-upload job."""
    async def _body(s: _Services) -> None:
        await s.auto.delete_job(job_id)
        print(f"Deleted auto job #{job_id}")
    await _run_with_services(config_path, _body)


async def run_due_impl(config_path: str, *, item_id: int | None = None, dry_run: bool = False) -> None:
    """Run due photo items and auto jobs now (or preview them with --dry-run)."""
    async def _body(s: _Services) -> None:
        if dry_run:
            # Preview only: never touch scheduled photo items (the task path has no
            # dry-run); show the auto-job plan and exit before any send/mark.
            previews = await s.auto.run_due(dry_run=True)
            assert isinstance(previews, list)  # narrow run_due's int|list return
            print(f"[dry-run] Would process {len(previews)} due auto job(s); nothing sent.")
            for preview in previews:
                title = preview.target_title or preview.target_dialog_id
                print(
                    f"  job #{preview.job_id} → {title} "
                    f"(dialog_id={preview.target_dialog_id}, mode={preview.send_mode.value}): "
                    f"{len(preview.files)} file(s)"
                )
                for file_path in preview.files:
                    print(f"    - {file_path}")
            return
        item_progress, item_state = _make_progress_printer("photo item")
        items = await s.tasks.run_due(item_id=item_id, on_progress=item_progress)
        _print_progress_summary("photo items", items, item_state)
        jobs = 0
        if item_id is None:
            auto_progress, auto_state = _make_progress_printer("auto job")
            processed = await s.auto.run_due(on_progress=auto_progress)
            assert isinstance(processed, int)  # non-dry-run returns a count
            jobs = processed
            _print_progress_summary("auto jobs", jobs, auto_state)
        print(f"Processed due photo items={items} auto_jobs={jobs}")
    await _run_with_services(config_path, _body)


def run(args: argparse.Namespace) -> None:
    """Thin argparse adapter over the ``*_impl`` bodies (legacy dispatch path).

    The production CLI routes ``photo-loader`` through the Typer ``app`` (#1123);
    this wrapper keeps the argparse leaf audit and command-level tests working. Args
    are read via ``getattr`` defaults so partial test Namespaces stay usable (#1117).
    """
    action = getattr(args, "photo_loader_action", None)
    if action == "dialogs":
        asyncio.run(dialogs_impl(args.config, phone=args.phone))
    elif action == "refresh":
        asyncio.run(refresh_impl(args.config, phone=args.phone))
    elif action == "send":
        asyncio.run(
            send_impl(
                args.config,
                phone=args.phone,
                target=args.target,
                files=args.files,
                mode=getattr(args, "mode", "album"),
                caption=getattr(args, "caption", None),
            )
        )
    elif action == "schedule-send":
        asyncio.run(
            schedule_send_impl(
                args.config,
                phone=args.phone,
                target=args.target,
                files=args.files,
                at=args.at,
                mode=getattr(args, "mode", "album"),
                caption=getattr(args, "caption", None),
            )
        )
    elif action == "batch-create":
        asyncio.run(
            batch_create_impl(
                args.config,
                phone=args.phone,
                target=args.target,
                manifest=args.manifest,
                caption=getattr(args, "caption", None),
            )
        )
    elif action == "publish":
        asyncio.run(publish_impl(args.config, batch_id=args.id))
    elif action == "batch-list":
        asyncio.run(batch_list_impl(args.config))
    elif action == "items":
        asyncio.run(
            items_impl(
                args.config,
                batch_id=getattr(args, "batch_id", None),
                limit=getattr(args, "limit", 100),
            )
        )
    elif action == "batch-cancel":
        asyncio.run(batch_cancel_impl(args.config, item_id=args.id))
    elif action == "auto-create":
        asyncio.run(
            auto_create_impl(
                args.config,
                phone=args.phone,
                target=args.target,
                folder=args.folder,
                interval=args.interval,
                mode=getattr(args, "mode", "album"),
                caption=getattr(args, "caption", None),
            )
        )
    elif action == "auto-list":
        asyncio.run(auto_list_impl(args.config))
    elif action == "auto-update":
        asyncio.run(
            auto_update_impl(
                args.config,
                job_id=args.id,
                folder=getattr(args, "folder", None),
                interval=getattr(args, "interval", None),
                mode=getattr(args, "mode", None),
                caption=getattr(args, "caption", None),
                active=getattr(args, "active", False),
                paused=getattr(args, "paused", False),
            )
        )
    elif action == "auto-toggle":
        asyncio.run(auto_toggle_impl(args.config, job_id=args.id))
    elif action == "auto-delete":
        asyncio.run(auto_delete_impl(args.config, job_id=args.id))
    elif action == "run-due":
        asyncio.run(
            run_due_impl(
                args.config,
                item_id=getattr(args, "item_id", None),
                dry_run=getattr(args, "dry_run", False),
            )
        )


# --------------------------------------------------------------------------- #
# photo-loader → dialogs / refresh / send / schedule-send / batch-create /
#                publish / batch-list / items / batch-cancel / auto-create / auto-list /
#                auto-update / auto-toggle / auto-delete / run-due
# --------------------------------------------------------------------------- #

photo_loader_app = typer.Typer(no_args_is_help=True, help="Photo upload automation")


@photo_loader_app.command("dialogs")
def photo_loader_dialogs(
    ctx: typer.Context,
    phone: str = typer.Option(..., "--phone", help="Account phone"),
) -> None:
    """List dialogs for an account."""
    apply_startup(ctx)
    run_async(dialogs_impl(ctx.obj.config, phone=phone))


@photo_loader_app.command("refresh")
def photo_loader_refresh(
    ctx: typer.Context,
    phone: str = typer.Option(..., "--phone", help="Account phone"),
) -> None:
    """Refresh dialog cache for photo loader."""
    apply_startup(ctx)
    run_async(refresh_impl(ctx.obj.config, phone=phone))


@photo_loader_app.command("send")
def photo_loader_send(
    ctx: typer.Context,
    phone: str = typer.Option(..., "--phone", help="Account phone"),
    target: str = typer.Option(..., "--target", help="Dialog id"),
    # ``--files`` is a repeatable option: ``--files a --files b --files c``.
    # Click options cannot be variadic (``nargs=-1`` is arguments-only), so the
    # argparse ``--files a b c`` (nargs='+') form maps to the repeated flag here.
    # Keeping the ``--files`` flag name (rather than a positional variadic) holds
    # the CLI surface / manifest tuple stable (#1162 drift §2, resolved by keeping
    # the repeated form as the single direct surface once argparse was removed).
    files: list[str] = typer.Option(..., "--files", help="Photo file paths (repeat per file)"),
    mode: PhotoMode = typer.Option(PhotoMode.album, "--mode"),
    caption: str | None = typer.Option(None, "--caption", help="Caption"),
) -> None:
    """Send photos now."""
    apply_startup(ctx)
    run_async(
        send_impl(
            ctx.obj.config, phone=phone, target=target, files=files, mode=mode.value, caption=caption
        )
    )


@photo_loader_app.command("schedule-send")
def photo_loader_schedule_send(
    ctx: typer.Context,
    phone: str = typer.Option(..., "--phone", help="Account phone"),
    target: str = typer.Option(..., "--target", help="Dialog id"),
    files: list[str] = typer.Option(..., "--files", help="Photo file paths (repeat per file)"),
    at: str = typer.Option(..., "--at", help="ISO datetime"),
    mode: PhotoMode = typer.Option(PhotoMode.album, "--mode"),
    caption: str | None = typer.Option(None, "--caption", help="Caption"),
) -> None:
    """Schedule photo send via Telegram."""
    apply_startup(ctx)
    run_async(
        schedule_send_impl(
            ctx.obj.config,
            phone=phone,
            target=target,
            files=files,
            at=at,
            mode=mode.value,
            caption=caption,
        )
    )


@photo_loader_app.command("batch-create")
def photo_loader_batch_create(
    ctx: typer.Context,
    phone: str = typer.Option(..., "--phone", help="Account phone"),
    target: str = typer.Option(..., "--target", help="Dialog id"),
    manifest: str = typer.Option(..., "--manifest", help="JSON/YAML manifest path"),
    caption: str | None = typer.Option(None, "--caption", help="Default caption"),
) -> None:
    """Create delayed batch from manifest."""
    apply_startup(ctx)
    run_async(
        batch_create_impl(
            ctx.obj.config, phone=phone, target=target, manifest=manifest, caption=caption
        )
    )


@photo_loader_app.command("batch-list")
def photo_loader_batch_list(ctx: typer.Context) -> None:
    """List photo batches."""
    apply_startup(ctx)
    run_async(batch_list_impl(ctx.obj.config))


@photo_loader_app.command("publish")
def photo_loader_publish(
    ctx: typer.Context,
    batch_id: int = typer.Argument(..., metavar="batch_id", help="Photo batch id"),
) -> None:
    """Publish a held photo batch into the due queue."""
    apply_startup(ctx)
    run_async(publish_impl(ctx.obj.config, batch_id=batch_id))


@photo_loader_app.command("items")
def photo_loader_items(
    ctx: typer.Context,
    batch_id: int | None = typer.Option(None, "--batch-id", help="Filter by batch id"),
    limit: int = typer.Option(100, "--limit", help="Max items to show"),
) -> None:
    """List photo batch items."""
    apply_startup(ctx)
    run_async(items_impl(ctx.obj.config, batch_id=batch_id, limit=limit))


@photo_loader_app.command("batch-cancel")
def photo_loader_batch_cancel(
    ctx: typer.Context,
    item_id: int = typer.Argument(..., metavar="id", help="Photo item id"),
) -> None:
    """Cancel a photo batch item."""
    apply_startup(ctx)
    run_async(batch_cancel_impl(ctx.obj.config, item_id=item_id))


@photo_loader_app.command("auto-create")
def photo_loader_auto_create(
    ctx: typer.Context,
    phone: str = typer.Option(..., "--phone", help="Account phone"),
    target: str = typer.Option(..., "--target", help="Dialog id"),
    folder: str = typer.Option(..., "--folder", help="Folder path"),
    interval: int = typer.Option(..., "--interval", help="Interval in minutes"),
    mode: PhotoMode = typer.Option(PhotoMode.album, "--mode"),
    caption: str | None = typer.Option(None, "--caption", help="Caption"),
) -> None:
    """Create auto-upload job."""
    apply_startup(ctx)
    run_async(
        auto_create_impl(
            ctx.obj.config,
            phone=phone,
            target=target,
            folder=folder,
            interval=interval,
            mode=mode.value,
            caption=caption,
        )
    )


@photo_loader_app.command("auto-list")
def photo_loader_auto_list(ctx: typer.Context) -> None:
    """List auto-upload jobs."""
    apply_startup(ctx)
    run_async(auto_list_impl(ctx.obj.config))


@photo_loader_app.command("auto-update")
def photo_loader_auto_update(
    ctx: typer.Context,
    job_id: int = typer.Argument(..., metavar="id", help="Job id"),
    folder: str | None = typer.Option(None, "--folder", help="Folder path"),
    interval: int | None = typer.Option(None, "--interval", help="Interval in minutes"),
    mode: PhotoMode | None = typer.Option(None, "--mode"),
    caption: str | None = typer.Option(None, "--caption", help="Caption"),
    active: bool = typer.Option(False, "--active", help="Enable job"),
    paused: bool = typer.Option(False, "--paused", help="Pause job"),
) -> None:
    """Update auto-upload job."""
    apply_startup(ctx)
    run_async(
        auto_update_impl(
            ctx.obj.config,
            job_id=job_id,
            folder=folder,
            interval=interval,
            mode=mode.value if mode else None,
            caption=caption,
            active=active,
            paused=paused,
        )
    )


@photo_loader_app.command("auto-toggle")
def photo_loader_auto_toggle(
    ctx: typer.Context,
    job_id: int = typer.Argument(..., metavar="id", help="Job id"),
) -> None:
    """Toggle auto-upload job."""
    apply_startup(ctx)
    run_async(auto_toggle_impl(ctx.obj.config, job_id=job_id))


@photo_loader_app.command("auto-delete")
def photo_loader_auto_delete(
    ctx: typer.Context,
    job_id: int = typer.Argument(..., metavar="id", help="Job id"),
) -> None:
    """Delete auto-upload job."""
    apply_startup(ctx)
    run_async(auto_delete_impl(ctx.obj.config, job_id=job_id))


@photo_loader_app.command("run-due")
def photo_loader_run_due(
    ctx: typer.Context,
    item_id: int | None = typer.Option(None, "--item-id", help="Run only one due photo item"),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Preview which auto-job files would be posted (where/when) without sending or marking",
    ),
) -> None:
    """Run due photo items and auto jobs now."""
    apply_startup(ctx)
    run_async(run_due_impl(ctx.obj.config, item_id=item_id, dry_run=dry_run))
