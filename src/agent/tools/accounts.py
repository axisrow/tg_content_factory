"""Agent tools for Telegram account management."""

from __future__ import annotations

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._registry import _text_response, require_confirmation, require_pool, resolve_phone


def register(db, client_pool, embedding_service, **kwargs):
    tools = []

    @tool("list_accounts", "List all connected Telegram accounts with their status", {})
    async def list_accounts(args):
        try:
            accounts = await db.get_accounts()
            if not accounts:
                return _text_response("Аккаунты не найдены.")
            lines = [f"Аккаунты ({len(accounts)}):"]
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

    @tool("toggle_account", "Toggle account active/inactive status", {"account_id": int})
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
        "⚠️ DANGEROUS: Delete a Telegram account from the system. Always ask user for confirmation first.",
        {"account_id": int, "confirm": bool},
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

    @tool("get_flood_status", "Get flood wait status for all accounts", {})
    async def get_flood_status(args):
        try:
            accounts = await db.get_accounts()
            if not accounts:
                return _text_response("Аккаунты не найдены.")
            lines = ["Flood-статус аккаунтов:"]
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
        {"phone": str, "confirm": bool},
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
        {"phone": str},
    )
    async def get_account_info(args):
        pool_gate = require_pool(client_pool, "Информация об аккаунтах")
        if pool_gate:
            return pool_gate
        try:
            users = await client_pool.get_users_info(include_avatar=False)
            phone_filter = args.get("phone", "").strip()
            if phone_filter:
                from src.agent.tools._registry import normalize_phone

                phone_filter = normalize_phone(phone_filter)
                users = [u for u in users if u.phone == phone_filter]
            if not users:
                return _text_response("Подключённые аккаунты не найдены.")
            db_accounts = await db.get_accounts()
            active_by_phone = {a.phone: a.is_active for a in db_accounts}
            lines = [f"Аккаунты ({len(users)}):"]
            for u in users:
                name = f"{u.first_name} {u.last_name}".strip() or "—"
                username = f"@{u.username}" if u.username else "—"
                premium = "да" if u.is_premium else "нет"
                active = "да" if active_by_phone.get(u.phone, False) else "нет"
                lines.append(
                    f"- {u.phone}: {name} ({username}), "
                    f"premium={premium}, активен={active}"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения информации об аккаунтах: {e}")

    tools.append(get_account_info)

    return tools
