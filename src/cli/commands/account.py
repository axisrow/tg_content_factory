from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import datetime, timezone

from src.agent.runtime_context import AgentRuntimeContext
from src.agent.tools.accounts import get_live_account_info_text
from src.cli import runtime
from src.models import Account
from src.settings_utils import parse_int_setting
from src.telegram.auth import TelegramAuth


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


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        config, db = await runtime.init_db(args.config)
        pool = None
        try:
            if args.account_action == "add":
                args.account_action = "verify-code" if getattr(args, "code", None) else "send-code"

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
                await db.delete_account(args.id)
                print(f"Deleted account id={args.id}")
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
