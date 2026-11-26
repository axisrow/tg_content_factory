"""``ping`` CLI group — мониторинг затихших DM-контактов (issue #1415, MVP).

Typer-группа (эпик #959): тонкие ``@command``-обёртки над ``*_impl``.
CRUD мониторов — DB-only; ``run-due`` читает живую историю и шлёт пинги через
`TelegramActionService` (тот же путь, что `dialogs read/send`) — реальная
отправка, ручной триггер для проверки. Расписание — scheduler-job `ping_due`,
раз в час ставящая PING_DUE задачу воркеру.
"""

from __future__ import annotations

import sqlite3

import typer

from tg_agent.cli import runtime, worker_handoff
from tg_agent.cli.commands.common import apply_startup, cli_error, run_async
from tg_agent.models import CollectionTaskType, PingMonitor
from tg_agent.services.task_enqueuer import TaskEnqueuer


async def create_impl(
    config_path: str,
    *,
    phone: str,
    dialog_id: int | None,
    username: str | None,
    text: str,
    days: int,
    title: str | None,
) -> None:
    """Поставить 1:1 диалог под мониторинг (по id или @username)."""
    config, db = await runtime.init_db(config_path)
    pool = None
    try:
        resolved_title = title
        if dialog_id is None and username:
            _, pool = await runtime.init_pool(config, db)
            info = await pool.resolve_any_entity(username, phone=phone)
            if not info:
                cli_error(f"Error: could not resolve: {username}")
            dialog_id = int(info["channel_id"])
            resolved_title = resolved_title or info.get("title")
        if dialog_id is None:
            cli_error("Error: --dialog-id or --username is required")

        monitor = PingMonitor(
            phone=phone,
            dialog_id=dialog_id,
            dialog_title=resolved_title,
            silence_threshold_days=days,
            nudge_text=text,
        )
        try:
            monitor_id = await db.repos.ping_monitors.create(monitor)
        except sqlite3.IntegrityError:
            cli_error(
                f"Error: dialog {dialog_id} is already monitored for {phone} "
                "(UNIQUE(phone, dialog_id))"
            )
        print(f"Created ping monitor #{monitor_id} (dialog {dialog_id}, every {days}d of silence)")
    finally:
        if pool is not None:
            await pool.disconnect_all()
        await db.close()


async def list_impl(config_path: str, *, active_only: bool) -> None:
    _, db = await runtime.init_db(config_path)
    try:
        monitors = await db.repos.ping_monitors.list(active_only=active_only)
        if not monitors:
            print("No ping monitors found.")
            return
        fmt = "{:<5} {:<12} {:>12} {:<20} {:>5} {:>5} {:<16} {}"
        print(fmt.format("ID", "Phone", "Dialog", "Title", "Days", "Cyc", "Last nudge", "Active"))
        print("-" * 100)
        for m in monitors:
            print(
                fmt.format(
                    str(m.id),
                    m.phone,
                    str(m.dialog_id),
                    (m.dialog_title or "-")[:20],
                    str(m.silence_threshold_days),
                    str(m.cycle_count),
                    (
                        m.last_nudge_sent_at.strftime("%Y-%m-%d %H:%M")
                        if m.last_nudge_sent_at
                        else "-"
                    ),
                    "yes" if m.is_active else "no",
                )
            )
    finally:
        await db.close()


async def update_impl(
    config_path: str,
    *,
    monitor_id: int,
    text: str | None,
    days: int | None,
    active: bool,
    inactive: bool,
) -> None:
    _, db = await runtime.init_db(config_path)
    try:
        monitor = await db.repos.ping_monitors.get(monitor_id)
        if monitor is None:
            cli_error(f"Ping monitor #{monitor_id} not found")
        is_active = True if active else (False if inactive else None)
        await db.repos.ping_monitors.update(
            monitor_id,
            nudge_text=text,
            silence_threshold_days=days,
            is_active=is_active,
        )
        print(f"Updated ping monitor #{monitor_id}")
    finally:
        await db.close()


async def delete_impl(config_path: str, *, monitor_id: int) -> None:
    _, db = await runtime.init_db(config_path)
    try:
        if not await db.repos.ping_monitors.delete(monitor_id):
            cli_error(f"Ping monitor #{monitor_id} not found")
        print(f"Deleted ping monitor #{monitor_id}")
    finally:
        await db.close()


async def run_due_impl(config_path: str) -> None:
    """Проверить активные мониторы сейчас; при живом демоне — в очередь воркера.

    Воркер и сам гоняет ping_due ежечасно, поэтому параллельный локальный прогон
    прошёл бы то же пороговое правило вторым процессом и отправил nudge дважды
    (окно read_history → send_message → update не межпроцессное). Рука передачи:
    serve_is_running → enqueue PING_DUE, дедуп has_active_task делает наложение
    невозможным (#1574-паттерн HANDS_OFF).
    """
    config, db = await runtime.init_db(config_path)
    pool = None
    try:
        if worker_handoff.serve_is_running(config):
            if await db.repos.tasks.has_active_task(CollectionTaskType.PING_DUE):
                print("A ping_due task is already active; the worker will run it.")
                return
            enqueuer = TaskEnqueuer(db, None)  # type: ignore[arg-type]
            task_id = await enqueuer.enqueue_ping_due()
            print(f"PING_DUE task #{task_id} queued; the worker will run it.")
            return
        _, pool = await runtime.init_pool(config, db)
        from tg_agent.gateway.catalog import auto_reply_guard, shared_project
        from tg_agent.services.ping_monitor_service import PingMonitorService
        from tg_agent.services.telegram_actions import TelegramActionService

        guard = None
        if shared_project(config) and pool.relay is not None:
            # Another project may own automatic replies in a dialog; the
            # gateway answers who, as for the worker (#1868).
            from tg_agent.gateway.client import TelegramGatewayClient

            guard = auto_reply_guard(db, TelegramGatewayClient(pool.relay))
        service = PingMonitorService(
            db.repos.ping_monitors,
            TelegramActionService(pool, db=db),
            auto_reply_guard=guard,
        )
        checked = await service.run_due()
        print(f"Checked {checked} ping monitor(s).")
    finally:
        if pool is not None:
            await pool.disconnect_all()
        await db.close()


ping_app = typer.Typer(no_args_is_help=True, help="Friendly ping for dormant DM chats")


@ping_app.command("create")
def ping_create(
    ctx: typer.Context,
    phone: str = typer.Option(..., "--phone", help="Account phone that owns the dialog"),
    dialog_id: int | None = typer.Option(None, "--dialog-id", help="DM dialog id (user id)"),
    username: str | None = typer.Option(None, "--username", help="Resolve dialog by @username"),
    text: str = typer.Option(..., "--text", help="Friendly ping text"),
    days: int = typer.Option(30, "--days", min=1, help="Base silence threshold in days"),
    title: str | None = typer.Option(None, "--title", help="Dialog title label (optional)"),
) -> None:
    """Start monitoring a dormant 1:1 dialog."""
    apply_startup(ctx)
    run_async(
        create_impl(
            ctx.obj.config,
            phone=phone,
            dialog_id=dialog_id,
            username=username,
            text=text,
            days=days,
            title=title,
        )
    )


@ping_app.command("list")
def ping_list(
    ctx: typer.Context,
    active_only: bool = typer.Option(False, "--active-only", help="Only active monitors"),
) -> None:
    """List ping monitors."""
    apply_startup(ctx)
    run_async(list_impl(ctx.obj.config, active_only=active_only))


@ping_app.command("update")
def ping_update(
    ctx: typer.Context,
    monitor_id: int = typer.Argument(..., metavar="id"),
    text: str | None = typer.Option(None, "--text", help="New ping text"),
    days: int | None = typer.Option(None, "--days", min=1, help="New base threshold in days"),
    active: bool = typer.Option(False, "--active", help="Enable monitor"),
    inactive: bool = typer.Option(False, "--inactive", help="Pause monitor"),
) -> None:
    """Update a ping monitor."""
    apply_startup(ctx)
    run_async(
        update_impl(
            ctx.obj.config,
            monitor_id=monitor_id,
            text=text,
            days=days,
            active=active,
            inactive=inactive,
        )
    )


@ping_app.command("delete")
def ping_delete(
    ctx: typer.Context,
    monitor_id: int = typer.Argument(..., metavar="id"),
) -> None:
    """Delete a ping monitor."""
    apply_startup(ctx)
    run_async(delete_impl(ctx.obj.config, monitor_id=monitor_id))


@ping_app.command("run-due")
def ping_run_due(ctx: typer.Context) -> None:
    """Run the due-check now (reads live history, may send real pings)."""
    apply_startup(ctx)
    run_async(run_due_impl(ctx.obj.config))
