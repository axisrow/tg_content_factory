"""Agent tools for Telegram account management."""

from __future__ import annotations

from typing import Annotated

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.runtime_context import AgentRuntimeContext
from src.agent.tools._registry import _text_response, normalize_phone, require_confirmation, resolve_phone

_NO_LIVE_RUNTIME = "live Telegram runtime unavailable"


def _runtime(kwargs: dict, db, client_pool) -> AgentRuntimeContext:
    ctx = kwargs.get("runtime_context")
    if isinstance(ctx, AgentRuntimeContext):
        return ctx
    return AgentRuntimeContext.build(db=db, client_pool=client_pool, config=kwargs.get("config"))


def _matches_phone(phone: str, phone_filter: str) -> bool:
    if not phone_filter:
        return True
    prefix = phone_filter.rstrip("*")
    return phone == phone_filter or phone.startswith(prefix)


async def get_live_account_info_text(runtime: AgentRuntimeContext, phone: str = "") -> str:
    """Return account info grounded only in the live ClientPool."""
    if not runtime.has_live_telegram:
        return _NO_LIVE_RUNTIME

    phone_filter = normalize_phone(phone.strip()) if phone.strip() else ""
    users = await runtime.client_pool.get_users_info(include_avatar=False)
    if phone_filter:
        users = [u for u in users if _matches_phone(str(u.phone), phone_filter)]
    if not users:
        return "Live Telegram accounts not found for this request: не найдены."

    db_accounts = await runtime.db.get_accounts()
    db_by_phone = {a.phone: a for a in db_accounts}
    lines = [f"Live Telegram accounts ({len(users)}):"]
    for u in users:
        db_account = db_by_phone.get(u.phone)
        name = f"{u.first_name} {u.last_name}".strip() or "-"
        username = f"@{u.username}" if u.username else "-"
        premium = "да" if u.is_premium else "нет"
        active = "да" if getattr(db_account, "is_active", False) else "нет"
        primary = "да" if getattr(db_account, "is_primary", False) else "нет"
        session_present = "да" if getattr(db_account, "session_string", "") else "нет"
        lines.append(
            f"- {u.phone}: {name} ({username}), premium={premium}, "
            f"db_active={active}, db_primary={primary}, session-present={session_present}"
        )
    return "\n".join(lines)


def register(db, client_pool, embedding_service, **kwargs):
    runtime = _runtime(kwargs, db, client_pool)
    tools = []

    @tool(
        "list_accounts",
        "List Telegram accounts from the database only, including stored active/flood status. "
        "This does not prove whether a live Telegram client is currently connected. "
        "Returns id (account_id used by toggle_account/delete_account), phone, and flood_wait status.",
        {},
    )
    async def list_accounts(args):
        try:
            accounts = await db.get_accounts()
            if not accounts:
                return _text_response("Аккаунты не найдены.")
            lines = [f"Аккаунты ({len(accounts)}) в БД:"]
            for a in accounts:
                status = "активен" if a.is_active else "неактивен"
                flood = ""
                if hasattr(a, "flood_wait_until") and a.flood_wait_until:
                    flood = f" [flood_wait до {a.flood_wait_until}]"
                lines.append(f"- id={a.id}, phone={a.phone}, {status}{flood}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения аккаунтов: {e}")

    tools.append(list_accounts)

    @tool(
        "toggle_account",
        "Toggle account active/inactive status. account_id = id from list_accounts.",
        {"account_id": Annotated[int, "ID аккаунта из list_accounts"]},
    )
    async def toggle_account(args):
        account_id = args.get("account_id")
        if account_id is None:
            return _text_response("Ошибка: account_id обязателен.")
        try:
            accounts = await db.get_accounts()
            acc = next((a for a in accounts if a.id == int(account_id)), None)
            if acc is None:
                return _text_response(f"Аккаунт id={account_id} не найден.")
            new_status = not acc.is_active
            await db.set_account_active(int(account_id), new_status)
            status_text = "активирован" if new_status else "деактивирован"
            return _text_response(f"Аккаунт {acc.phone} {status_text}.")
        except Exception as e:
            return _text_response(f"Ошибка переключения аккаунта: {e}")

    tools.append(toggle_account)

    @tool(
        "delete_account",
        "⚠️ DANGEROUS: Delete a Telegram account from the system. "
        "account_id = id from list_accounts. Always ask user for confirmation first.",
        {
            "account_id": Annotated[int, "ID аккаунта из list_accounts"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def delete_account(args):
        account_id = args.get("account_id")
        if account_id is None:
            return _text_response("Ошибка: account_id обязателен.")
        try:
            accounts = await db.get_accounts()
            acc = next((a for a in accounts if a.id == int(account_id)), None)
            name = acc.phone if acc else f"id={account_id}"
            gate = require_confirmation(f"удалит аккаунт '{name}' из системы", args)
            if gate:
                return gate
            await db.delete_account(int(account_id))
            return _text_response(f"Аккаунт '{name}' удалён.")
        except Exception as e:
            return _text_response(f"Ошибка удаления аккаунта: {e}")

    tools.append(delete_account)

    @tool("get_flood_status", "Get database flood-wait status for all accounts; this is not live connection state.", {})
    async def get_flood_status(args):
        try:
            accounts = await db.get_accounts()
            if not accounts:
                return _text_response("Аккаунты не найдены.")
            lines = ["Flood-статус аккаунтов в БД:"]
            for a in accounts:
                flood = "нет ограничений"
                if hasattr(a, "flood_wait_until") and a.flood_wait_until:
                    flood = f"заблокирован до {a.flood_wait_until}"
                lines.append(f"- {a.phone}: {flood}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения flood-статуса: {e}")

    tools.append(get_flood_status)

    @tool(
        "clear_flood_status",
        "Clear flood wait restriction for a specific account. Ask user for confirmation first.",
        {
            "phone": Annotated[str, "Номер телефона аккаунта (например +79001234567)"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
    )
    async def clear_flood_status(args):
        phone, err = await resolve_phone(db, args.get("phone", ""))
        if err:
            return err
        gate = require_confirmation(f"сбросит flood-wait для аккаунта {phone}", args)
        if gate:
            return gate
        try:
            accounts = await db.get_accounts()
            acc = next((a for a in accounts if a.phone == phone), None)
            if acc is None:
                return _text_response(f"Аккаунт {phone} не найден.")
            await db.update_account_flood(phone, None)
            return _text_response(f"Flood-wait для {phone} сброшен.")
        except Exception as e:
            return _text_response(f"Ошибка сброса flood-wait: {e}")

    tools.append(clear_flood_status)

    # ------------------------------------------------------------------
    # get_account_info (READ) — live Telegram account details
    # ------------------------------------------------------------------

    @tool(
        "get_account_info",
        "Get live Telegram account info (name, username, premium status) for connected accounts. "
        "Optionally filter by phone number.",
        {"phone": Annotated[str, "Номер телефона аккаунта (например +79001234567)"]},
    )
    async def get_account_info(args):
        try:
            return _text_response(await get_live_account_info_text(runtime, args.get("phone", "")))
        except Exception as e:
            return _text_response(f"Ошибка получения информации об аккаунтах: {e}")

    tools.append(get_account_info)

    return tools
