from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from src.agent.runtime_context import AgentRuntimeContext
from src.agent.tools.accounts import get_live_account_info_text
from src.cli import runtime
from src.database.repositories.accounts import AccountSessionDecryptError
from src.models import Account
from src.services.notification_target_service import NotificationTargetService
from src.settings_utils import parse_int_setting
from src.telegram.auth import TelegramAuth, validate_session_string

if TYPE_CHECKING:
    from src.database import Database


def _pending_key(phone: str) -> str:
    return f"auth_pending:{phone}"


async def _load_resolve_backoffs(db, *, now: datetime) -> dict[str, datetime]:
    """Read active per-phone resolve_username backoff deadlines from settings (#790)."""
    from src.telegram.resolve_guard import (
        RESOLVE_BACKOFF_BY_PHONE_SETTING,
        parse_resolve_backoff_setting,
    )

    raw = await db.get_setting(RESOLVE_BACKOFF_BY_PHONE_SETTING)
    return parse_resolve_backoff_setting(raw, now=now)


async def _resolve_credentials(args: argparse.Namespace, config, db) -> tuple[int, str]:
    api_id = getattr(args, "api_id", None) or config.telegram.api_id
    api_hash = getattr(args, "api_hash", None) or config.telegram.api_hash
    if api_id == 0 or not api_hash:
        stored_id = await db.get_setting("tg_api_id")
        stored_hash = await db.get_setting("tg_api_hash")
        if stored_id and stored_hash:
            api_id = parse_int_setting(
                stored_id, setting_name="tg_api_id", default=0,
                logger=logging.getLogger(__name__),
            )
            api_hash = stored_hash
    return api_id, api_hash


async def _run_export_session(args: argparse.Namespace, db: Database) -> None:
    """Print the decrypted plaintext StringSession for an account (SSO export, #828).

    The session string grants full access to the account — it is printed to stdout
    ONLY on this explicit request and is NEVER logged. Selects by ``--id`` or ``--phone``
    (exactly one). Decrypts only the chosen account, so a broken sibling session can't
    abort the lookup — this is the recovery path operators reach for.
    """
    account_id = getattr(args, "id", None)
    phone = getattr(args, "phone", None)
    if (account_id is None) == (phone is None):
        print("ERROR: provide exactly one of --id or --phone")
        return

    # Resolve identity without decrypting the whole DB (a broken sibling session
    # must not crash export of a healthy account, #1143 review).
    summaries = await db.get_account_summaries(active_only=False)
    if account_id is not None:
        match = next((s for s in summaries if s.id == account_id), None)
        target = f"id={account_id}"
    else:
        match = next((s for s in summaries if s.phone == phone), None)
        target = phone
    if match is None:
        print(f"Account {target} not found")
        return

    try:
        session_string = await db.get_decrypted_session(account_id=match.id, phone=None)
    except AccountSessionDecryptError as exc:
        print(f"ERROR: cannot decrypt session for {match.phone} ({exc.status})")
        return
    if not session_string:
        print(f"ERROR: account {match.phone} has no session string to export")
        return

    print("WARNING: this session string grants full access to the account.", file=sys.stderr)
    if getattr(args, "json", False):
        print(json.dumps({"phone": match.phone, "session_string": session_string}))
    else:
        print(session_string)


async def _run_import(args: argparse.Namespace, db: Database) -> None:
    """Add an account from a ready StringSession instead of the login flow (SSO import, #828).

    Validates the session via the telegram layer (CLI must not import telethon directly),
    then takes the normal ``add_account`` path — the repository encrypts at rest as usual.
    The raw session string is never echoed back or logged.
    """
    phone = getattr(args, "phone", None)
    session_string = _read_session_arg(args)
    if not phone or not session_string:
        print("ERROR: provide --phone and a session string (--session-string or --session-string-stdin)")
        return

    if not validate_session_string(session_string):
        print(f"ERROR: invalid Telegram session string for {phone}")
        return

    existing = await db.get_account_summaries(active_only=False)
    # add_account is an ON CONFLICT(phone) UPSERT: importing onto an existing phone
    # would silently overwrite its session and reset is_active/is_premium. Refuse
    # unless --force is given, so an import can't clobber a working account (#1143 review).
    if any(s.phone == phone for s in existing):
        if not getattr(args, "force", False):
            print(
                f"Account {phone} already exists; import would overwrite its session. "
                f"Delete it first, or re-run with --force to replace."
            )
            return
        print(f"WARNING: overwriting existing session for {phone} (--force).", file=sys.stderr)

    is_primary = len(existing) == 0
    account = Account(
        phone=phone,
        session_string=session_string,
        is_primary=is_primary,
        is_premium=False,
    )
    await db.add_account(account)
    print(f"Account {phone} imported successfully (primary={is_primary}).")


def _read_session_arg(args: argparse.Namespace) -> str | None:
    """Resolve the session string from --session-string-stdin (preferred) or --session-string.

    Reading from stdin keeps the secret out of argv / shell history / ``/proc`` (#1143 review).
    """
    if getattr(args, "session_string_stdin", False):
        return sys.stdin.read().strip() or None
    value = getattr(args, "session_string", None)
    return value.strip() if value else None


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        config, db = await runtime.init_db(args.config)
        pool = None
        try:
            if args.account_action == "add":
                args.account_action = "verify-code" if getattr(args, "code", None) else "send-code"

            if args.account_action == "export-session":
                await _run_export_session(args, db)
                return

            if args.account_action == "import":
                await _run_import(args, db)
                return

            if args.account_action == "send-code":
                phone = args.phone
                api_id, api_hash = await _resolve_credentials(args, config, db)
                if api_id == 0 or not api_hash:
                    print("ERROR: API credentials not configured.")
                    print("Provide --api-id and --api-hash, or set them in config/DB.")
                    return

                auth = TelegramAuth(api_id, api_hash)
                try:
                    info = await auth.send_code(phone)
                except Exception as exc:
                    await auth.cleanup()
                    print(f"Error sending auth code: {exc}")
                    return

                await db.set_setting(_pending_key(phone), json.dumps(info))
                await auth.cleanup()
                code_type = info.get("code_type", "Telegram")
                print(f"Code sent to {phone} via {code_type}.")
                print(f"Run: account verify-code --phone {phone} --code CODE")
                return

            elif args.account_action == "verify-code":
                phone = args.phone
                code = args.code
                password_2fa = getattr(args, "password", None) or None

                pending_raw = await db.get_setting(_pending_key(phone))
                if not pending_raw:
                    print(f"ERROR: No pending auth for {phone}. Run 'account send-code' first.")
                    return
                pending = json.loads(pending_raw)
                phone_code_hash = pending["phone_code_hash"]

                api_id, api_hash = await _resolve_credentials(args, config, db)
                if api_id == 0 or not api_hash:
                    print("ERROR: API credentials not configured.")
                    return

                auth = TelegramAuth(api_id, api_hash)
                try:
                    session_string = await auth.sign_in_fresh(
                    phone, code, phone_code_hash,
                    session_str=pending.get("session_str", ""),
                    password_2fa=password_2fa,
                    code_consumed=pending.get("code_consumed", False),
                )
                except ValueError as exc:
                    if "2FA" in str(exc) or "password" in str(exc).lower():
                        pending["code_consumed"] = True
                        await db.set_setting(_pending_key(phone), json.dumps(pending))
                        print(f"2FA required. Re-run with --code {code} --password YOUR_2FA_PASSWORD")
                    else:
                        print(f"Auth failed: {exc}")
                    return
                except Exception as exc:
                    print(f"Auth failed: {exc}")
                    return

                existing = await db.get_account_summaries(active_only=False)
                is_primary = len(existing) == 0

                # Persist the freshly authenticated session to the DB FIRST, before
                # warming the in-memory pool (#449). The inverse order — pool first,
                # DB second — loses the session string permanently if anything between
                # the two calls fails (network, process kill): the pool is rebuilt from
                # the DB on restart, so an unpersisted client vanishes. Premium status
                # is a best-effort enrichment updated afterwards.
                account = Account(
                    phone=phone,
                    session_string=session_string,
                    is_primary=is_primary,
                    is_premium=False,
                )
                await db.add_account(account)

                # Clear the pending-auth key only AFTER the account is durably
                # persisted (#449). Clearing it before add_account opened a data-loss
                # window: a crash between the two autocommit writes would leave no
                # pending key AND no account, so a verify-code retry would hit the
                # "No pending auth" guard above with the session gone. add_account is
                # an idempotent ON CONFLICT(phone) upsert, so a retry after a crash
                # between sign-in and this point safely re-persists the same session.
                await db.set_setting(_pending_key(phone), "")

                is_premium = False
                try:
                    _, pool = await runtime.init_pool(config, db)
                    await pool.add_client(phone, session_string)
                    acquired = await pool.get_client_by_phone(phone)
                    if acquired:
                        session, acquired_phone = acquired
                        try:
                            me = await session.fetch_me()
                            is_premium = bool(getattr(me, "premium", False))
                        except Exception:
                            pass
                        finally:
                            await pool.release_client(acquired_phone)
                    if is_premium:
                        await db.update_account_premium(phone, is_premium)
                except Exception as exc:
                    print(
                        f"Account {phone} saved, but pool warm-up failed "
                        f"(premium unknown): {exc}"
                    )

                print(f"Account {phone} added successfully (primary={is_primary}).")
                return

            elif args.account_action == "info":
                phone_filter = getattr(args, "phone", None) or None
                _, pool = await runtime.init_pool(
                    config, db, phones=(phone_filter,) if phone_filter else None
                )
                ctx = AgentRuntimeContext.build(db=db, config=config, client_pool=pool)
                print(await get_live_account_info_text(ctx, phone_filter or ""))
                return
            if args.account_action == "list":
                accounts = await db.get_account_summaries(active_only=False)
                if not accounts:
                    print("No accounts found.")
                    return
                fmt = "{:<5} {:<16} {:<9} {:<8} {:<8}"
                print(fmt.format("ID", "Phone", "Primary", "Active", "Premium"))
                print("-" * 50)
                for acc in accounts:
                    print(
                        fmt.format(
                            acc.id or 0,
                            acc.phone,
                            "Yes" if acc.is_primary else "No",
                            "Yes" if acc.is_active else "No",
                            "Yes" if acc.is_premium else "No",
                        )
                    )
            elif args.account_action == "toggle":
                accounts = await db.get_account_summaries(active_only=False)
                acc = next((a for a in accounts if a.id == args.id), None)
                if not acc:
                    print(f"Account id={args.id} not found")
                    return
                new_state = not acc.is_active
                await db.set_account_active(args.id, new_state)
                print(f"Account id={args.id} ({acc.phone}): active={new_state}")
            elif args.account_action == "set-primary":
                changed = await db.repos.accounts.set_account_primary(args.id)
                if changed:
                    print(f"Account id={args.id} set as primary")
                else:
                    print(f"Account id={args.id} not found")
            elif args.account_action == "delete":
                accounts = await db.get_account_summaries(active_only=False)
                acc = next((a for a in accounts if a.id == args.id), None)
                if not acc:
                    print(f"Account id={args.id} not found")
                    return
                target_svc = NotificationTargetService(db)
                replacement = getattr(args, "notify_to", None)
                configured = await target_svc.get_configured_phone()
                remaining = [a for a in accounts if a.phone != acc.phone]
                if (
                    replacement is None
                    and configured == acc.phone
                    and len(remaining) >= 2
                    and sys.stdin.isatty()
                ):
                    print("Этот аккаунт используется для уведомлений. На какой переназначить?")
                    print("  0. Primary (по умолчанию)")
                    for idx, other in enumerate(remaining, start=1):
                        marker = " [primary]" if other.is_primary else ""
                        print(f"  {idx}. {other.phone}{marker}")
                    try:
                        choice = input("Номер аккаунта [0]: ").strip()
                    except (EOFError, KeyboardInterrupt):
                        choice = ""
                        print()
                    if choice.isdigit() and 1 <= int(choice) <= len(remaining):
                        replacement = remaining[int(choice) - 1].phone
                try:
                    reassignment = await target_svc.reassign_for_deleted_account(
                        acc.phone, replacement, accounts=accounts
                    )
                except ValueError as exc:
                    print(f"ERROR: {exc}")
                    print("Аккаунт не удалён.")
                    return
                await db.delete_account(args.id)
                print(f"Deleted account id={args.id}")
                if reassignment.action == "reassigned":
                    print(f"Уведомления переназначены на {reassignment.new_phone}.")
                elif reassignment.action == "cleared":
                    print("Аккаунт уведомлений сброшен — используется Primary. Подсказка: --notify-to PHONE.")
            elif args.account_action == "flood-status":
                accounts = await db.get_account_summaries(active_only=False)
                if not accounts:
                    print("No accounts found.")
                    return
                now = datetime.now(timezone.utc)
                resolve_backoffs = await _load_resolve_backoffs(db, now=now)
                fmt = "{:<16} {:<28} {:<14} {:<18}"
                print(fmt.format("Phone", "Flood wait until", "Remaining", "Resolve backoff"))
                print("-" * 78)
                for acc in accounts:
                    if acc.flood_wait_until is None:
                        until_str = "OK"
                        remaining_str = ""
                    else:
                        flood_until = acc.flood_wait_until
                        if flood_until.tzinfo is None:
                            flood_until = flood_until.replace(tzinfo=timezone.utc)
                        if flood_until > now:
                            delta = flood_until - now
                            remaining_str = f"{int(delta.total_seconds())}s"
                            until_str = flood_until.strftime("%Y-%m-%d %H:%M:%S UTC")
                        else:
                            until_str = "OK (expired)"
                            remaining_str = ""
                    resolve_until = resolve_backoffs.get(acc.phone)
                    if resolve_until is None:
                        resolve_str = ""
                    else:
                        resolve_str = f"{int((resolve_until - now).total_seconds())}s"
                    print(fmt.format(acc.phone, until_str, remaining_str, resolve_str))
            elif args.account_action == "flood-clear":
                accounts = await db.get_account_summaries(active_only=False)
                acc = next((a for a in accounts if a.phone == args.phone), None)
                if not acc:
                    print(f"Account {args.phone} not found")
                    return
                await db.update_account_flood(args.phone, None)
                print(f"Flood wait cleared for {args.phone}")
        finally:
            if pool:
                await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())
