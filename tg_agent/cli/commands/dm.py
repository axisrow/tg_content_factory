"""``dm`` CLI group — черновики ответов на входящие DM (эпик #1416 этап 2.4, #1429).

Typer-группа (эпик #959): тонкие ``@command``-обёртки над ``*_impl``.
Отправка никогда не идёт из CLI напрямую — команда ``dm.send_reply``
ставится в очередь и исполняется воркером с пулом (граница из
``routes/moderation.py``). Атомарный клейм в хендлере защищает от двойной
отправки при конкурентных потребителях и ретраях очереди.
"""

from __future__ import annotations

from typing import Literal

import typer

from tg_agent.cli import runtime
from tg_agent.cli.commands.common import apply_startup, cli_error, run_async
from tg_agent.services.telegram_command_service import TelegramCommandService
from tg_agent.telegram.dm_catchup import (
    DmCatchupSettingsError,
    load_dm_catchup_settings,
)


async def list_impl(config_path: str, *, status: str | None, limit: int) -> None:
    _, db = await runtime.init_db(config_path)
    try:
        drafts = await db.repos.dm_reply_drafts.list_drafts(status=status, limit=limit)
        if not drafts:
            print("No reply drafts found.")
            return
        fmt = "{:<5} {:<10} {:<16} {:<12} {:<11} {:<30}"
        print(fmt.format("ID", "Status", "Chat", "Sent msg", "Updated", "Reply"))
        print("-" * 95)
        for d in drafts:
            print(
                fmt.format(
                    str(d.id),
                    d.status,
                    str(d.chat_id),
                    str(d.sent_message_id if d.sent_message_id is not None else "-"),
                    (d.updated_at.strftime("%Y-%m-%d %H:%M") if d.updated_at else "-"),
                    (d.reply_text[:28] + "…" if len(d.reply_text) > 28 else d.reply_text),
                )
            )
    finally:
        await db.close()


async def create_impl(
    config_path: str,
    *,
    incoming_dm_id: int,
    text: str,
    phone: str | None,
    chat_id: int | None,
) -> None:
    _, db = await runtime.init_db(config_path)
    try:
        target_phone = phone
        target_chat_id = chat_id
        if target_phone is None or target_chat_id is None:
            journal = await db.repos.incoming_dms.get(incoming_dm_id)
            if journal is not None:
                target_phone = target_phone or journal.phone
                target_chat_id = target_chat_id or journal.chat_id
        if not target_phone or target_chat_id is None:
            cli_error(
                f"Не найдена запись журнала incoming_dms #{incoming_dm_id} "
                "(TTL 24ч) — укажите --phone и --chat-id явно."
            )
        draft_id, created = await db.repos.dm_reply_drafts.create_draft(
            incoming_dm_id=incoming_dm_id,
            phone=target_phone,
            chat_id=target_chat_id,
            reply_text=text,
        )
        # Черновик разбирает журнальную строку: помечаем processed, чтобы
        # unprocessed в `dm status` считал только недоразобранное (#1581).
        journal = await db.repos.incoming_dms.get(incoming_dm_id)
        if journal is not None:
            await db.repos.incoming_dms.mark_processed(
                journal.phone, journal.chat_id, [journal.message_id]
            )
        if created:
            print(f"Черновик #{draft_id} создан (статус draft).")
        else:
            print(
                f"Черновик #{draft_id} уже существует — текст не изменён "
                "(обновление возможно только в статусе draft)."
            )
    finally:
        await db.close()


async def decide_impl(
    config_path: str, *, draft_id: int, decision: Literal["approved", "rejected"]
) -> None:
    _, db = await runtime.init_db(config_path)
    try:
        draft = await db.repos.dm_reply_drafts.get(draft_id)
        if draft is None:
            cli_error(f"Черновик #{draft_id} не найден.")
        changed = await db.repos.dm_reply_drafts.decide(draft_id, decision)
        if changed:
            print(f"Черновик #{draft_id}: {draft.status} → {decision}.")
        else:
            # Идемпотентный no-op: отправленный/клеймленный черновик решением
            # владельца назад не переводится — двойная отправка исключена.
            print(
                f"Черновик #{draft_id} остался в статусе '{draft.status}' "
                f"(решения не применяются к sending/sent/unconfirmed)."
            )
    finally:
        await db.close()


async def send_impl(config_path: str, *, draft_id: int) -> None:
    """Поставить отправку одобренного черновика в очередь воркера."""
    _, db = await runtime.init_db(config_path)
    try:
        draft = await db.repos.dm_reply_drafts.get(draft_id)
        if draft is None:
            cli_error(f"Черновик #{draft_id} не найден.")
        if draft.status == "sent":
            print(
                f"Черновик #{draft_id} уже отправлен "
                f"(message_id={draft.sent_message_id}); повторная отправка исключена."
            )
            return
        if draft.status == "unconfirmed":
            # Отправка без подтверждённого id (#1981): ответ мог дойти —
            # повторная отправка исключена, чат сверяет владелец.
            cli_error(
                f"Черновик #{draft_id} отправлен без подтверждения: Telegram не вернул "
                "id сообщения. Проверь чат вручную — повторная отправка исключена."
            )
        if draft.status != "approved":
            cli_error(
                f"Черновик #{draft_id} в статусе '{draft.status}' — "
                "перед отправкой нужен апрув: dm drafts approve."
            )
        command_id = await TelegramCommandService(db).enqueue(
            "dm.send_reply",
            # phone в payload (#1581): generic queue log печатает аккаунт.
            payload={"draft_id": draft_id, "phone": draft.phone},
            requested_by="cli:dm",
        )
        print(
            f"Команда dm.send_reply #{command_id} поставлена в очередь воркера "
            f"(черновик #{draft_id}). Статус: dm drafts list."
        )
    finally:
        await db.close()


def _print_listener_status(payload: dict, updated_at) -> None:
    """Снапшот `dm_listener_status` воркера: живость, аккаунты, догон."""
    stamp = updated_at.isoformat() if updated_at is not None else "-"
    print(f"DM listener (snapshot {stamp}):")
    print(f"  Running: {'yes' if payload.get('running') else 'no'}")
    print(f"  Unprocessed: {payload.get('unprocessed', 0)}")
    accounts = payload.get("accounts") or {}
    if isinstance(accounts, dict):
        for phone, info in sorted(accounts.items()):
            if not isinstance(info, dict):
                continue
            last = info.get("last_update_at") or "no updates yet"
            since = info.get("seconds_since_update")
            age = f" ({since:.1f}s ago)" if isinstance(since, (int, float)) else ""
            print(f"    {phone}: updates={info.get('updates_received', 0)}, last={last}{age}")
    catchup = payload.get("catchup")
    if not isinstance(catchup, dict):
        return
    print(f"Catch-up: Running: {'yes' if catchup.get('running') else 'no'}")
    for phone, run in sorted((catchup.get("last_runs") or {}).items()):
        if not isinstance(run, dict):
            continue
        summary = ", ".join(
            f"{key}={run[key]}"
            for key in (
                "dialogs", "stored", "already", "archived", "skipped", "deferred", "errors"
            )
            if key in run
        )
        print(f"    {phone}: {summary} (finished {run.get('finished_at', '-')})")


async def status_impl(config_path: str) -> None:
    """Статус слушателя DM/догона: снапшот воркера + эффективные настройки (#1581)."""
    _, db = await runtime.init_db(config_path)
    try:
        snapshot = await db.repos.runtime_snapshots.get_snapshot("dm_listener_status")
        if snapshot is None:
            print(
                "No dm_listener_status snapshot — worker down or DM listener never ran."
            )
        else:
            _print_listener_status(snapshot.payload, snapshot.updated_at)
        try:
            settings = await load_dm_catchup_settings(db)
        except DmCatchupSettingsError as exc:
            cli_error(f"Catch-up settings INVALID: {exc}")
        print(
            f"Catch-up settings: Mode: {settings.mode}, "
            f"staleness: {settings.staleness_sec}s, "
            f"overrides: accounts={len(settings.accounts)}, dialogs={len(settings.dialogs)}"
        )
    finally:
        await db.close()


async def journal_list_impl(config_path: str, *, limit: int, offset: int) -> None:
    """Журнал входящих DM (окно TTL 24ч), свежие первыми (#1580)."""
    _, db = await runtime.init_db(config_path)
    try:
        rows = await db.repos.incoming_dms.list(limit=limit, offset=offset)
        if not rows:
            print("Incoming DM journal is empty (24h TTL window).")
            return
        fmt = "{:<5} {:<16} {:<10} {:<8} {:<16} {:<30}"
        print(fmt.format("ID", "Phone", "Chat", "Msg ID", "Received", "Text"))
        print("-" * 95)
        for r in rows:
            received = r.received_at.strftime("%Y-%m-%d %H:%M") if r.received_at else "-"
            text = r.text or ""
            print(
                fmt.format(
                    str(r.id),
                    r.phone,
                    str(r.chat_id),
                    str(r.message_id),
                    received,
                    (text[:28] + "…" if len(text) > 28 else text),
                )
            )
    finally:
        await db.close()


async def archive_list_impl(
    config_path: str,
    *,
    phone: str | None,
    chat_id: int | None,
    limit: int,
) -> None:
    """Постоянный архив DM (обе стороны, без TTL), свежие первыми (#1580)."""
    _, db = await runtime.init_db(config_path)
    try:
        rows = await db.repos.dm_messages.list(
            phone=phone, chat_id=chat_id, limit=limit
        )
        if not rows:
            print("DM archive is empty for the given filter.")
            return
        fmt = "{:<5} {:<16} {:<10} {:<4} {:<16} {:<28}"
        print(fmt.format("ID", "Phone", "Chat", "Dir", "Date", "Text"))
        print("-" * 95)
        for r in rows:
            date = r.message_date.strftime("%Y-%m-%d %H:%M") if r.message_date else "-"
            text = r.text or ""
            print(
                fmt.format(
                    str(r.id),
                    r.phone,
                    str(r.chat_id),
                    "out" if r.out else "in",
                    date,
                    (text[:26] + "…" if len(text) > 26 else text),
                )
            )
    finally:
        await db.close()


dm_app = typer.Typer(no_args_is_help=True, help="Incoming DM: journal, archive, reply drafts")
drafts_app = typer.Typer(no_args_is_help=True, help="Reply drafts: list, decide, queue send")
journal_app = typer.Typer(no_args_is_help=True, help="Incoming DM journal (24h TTL window)")
archive_app = typer.Typer(no_args_is_help=True, help="Persistent DM archive (dm_messages)")
dm_app.add_typer(drafts_app, name="drafts")
dm_app.add_typer(journal_app, name="journal")
dm_app.add_typer(archive_app, name="archive")


@dm_app.command("status")
def dm_status(ctx: typer.Context) -> None:
    """Show DM listener/catch-up status from the worker snapshot + settings."""
    apply_startup(ctx)
    run_async(status_impl(ctx.obj.config))


@drafts_app.command("list")
def drafts_list(
    ctx: typer.Context,
    status: str | None = typer.Option(None, "--status", help="Filter by status"),
    limit: int = typer.Option(100, "--limit", help="Max rows"),
) -> None:
    """List reply drafts (fresh first)."""
    apply_startup(ctx)
    run_async(list_impl(ctx.obj.config, status=status, limit=limit))


@drafts_app.command("create")
def drafts_create(
    ctx: typer.Context,
    incoming_dm_id: int = typer.Argument(..., metavar="incoming-dm-id"),
    text: str = typer.Option(..., "--text", help="Reply text"),
    phone: str | None = typer.Option(
        None, "--phone", help="Account phone if the journal row is already pruned (24h TTL)"
    ),
    chat_id: int | None = typer.Option(
        None, "--chat-id", help="Chat id if the journal row is already pruned (24h TTL)"
    ),
) -> None:
    """Create (or re-draft, while still in draft) a reply for an incoming DM."""
    apply_startup(ctx)
    run_async(
        create_impl(
            ctx.obj.config,
            incoming_dm_id=incoming_dm_id,
            text=text,
            phone=phone,
            chat_id=chat_id,
        )
    )


@drafts_app.command("approve")
def drafts_approve(
    ctx: typer.Context,
    draft_id: int = typer.Argument(..., metavar="id"),
) -> None:
    """Approve a draft for sending (no-op once sending/sent/unconfirmed)."""
    apply_startup(ctx)
    run_async(decide_impl(ctx.obj.config, draft_id=draft_id, decision="approved"))


@drafts_app.command("reject")
def drafts_reject(
    ctx: typer.Context,
    draft_id: int = typer.Argument(..., metavar="id"),
) -> None:
    """Reject a draft — it can never be claimed for sending."""
    apply_startup(ctx)
    run_async(decide_impl(ctx.obj.config, draft_id=draft_id, decision="rejected"))


@drafts_app.command("send")
def drafts_send(
    ctx: typer.Context,
    draft_id: int = typer.Argument(..., metavar="id"),
) -> None:
    """Queue an approved draft for the worker to send (never sends in-process)."""
    apply_startup(ctx)
    run_async(send_impl(ctx.obj.config, draft_id=draft_id))


@journal_app.command("list")
def journal_list(
    ctx: typer.Context,
    limit: int = typer.Option(100, "--limit", help="Max rows"),
    offset: int = typer.Option(0, "--offset", help="Page offset"),
) -> None:
    """List the incoming-DM journal (fresh first, 24h TTL window)."""
    apply_startup(ctx)
    run_async(journal_list_impl(ctx.obj.config, limit=limit, offset=offset))


@archive_app.command("list")
def archive_list(
    ctx: typer.Context,
    phone: str | None = typer.Option(None, "--phone", help="Filter by account phone"),
    chat_id: int | None = typer.Option(None, "--chat-id", help="Filter by dialog chat id"),
    limit: int = typer.Option(100, "--limit", help="Max rows"),
) -> None:
    """List the persistent DM archive (both directions, fresh first)."""
    apply_startup(ctx)
    run_async(
        archive_list_impl(ctx.obj.config, phone=phone, chat_id=chat_id, limit=limit)
    )
