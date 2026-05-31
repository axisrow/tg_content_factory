"""Agent tools for Telegram account management."""

from __future__ import annotations

from inspect import isawaitable
from typing import Annotated

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.runtime_context import AgentRuntimeContext
from src.agent.tools._registry import (
    _text_response,
    account_session_status,
    available_live_read_phones,
    connected_phones_from_pool,
    get_accounts_with_flood_cleanup,
    is_flood_wait_active,
    normalize_flood_wait_until,
    normalize_phone,
    require_confirmation,
    resolve_phone,
)
from src.services.account_availability import compute_account_availability

_NO_LIVE_RUNTIME = "live Telegram runtime unavailable"

# Human guidance per availability state (#529). Critically distinguishes a
# saved-session reconnect (no SMS/2FA) from interactive Telegram login.
_AVAILABILITY_GUIDANCE = {
    "available": "OK — аккаунт доступен и пригоден к использованию.",
    "flood": "временный flood-wait (ограничение Telegram); дождитесь окончания.",
    "disconnected": (
        "сессия сохранена, но live-клиент не подключён — можно переподключить "
        "СОХРАНЁННУЮ сессию (reconnect); повторный вход по SMS/2FA НЕ требуется."
    ),
    "inactive": "выключен в БД (is_active=false); включите через toggle_account.",
    "session_unavailable": (
        "сохранённая сессия отсутствует или невалидна — требуется ИНТЕРАКТИВНЫЙ "
        "вход в Telegram (через /auth/login?phone=..., с кодом из SMS и 2FA)."
    ),
}


def _runtime(kwargs: dict, db, client_pool) -> AgentRuntimeContext:
    ctx = kwargs.get("runtime_context")
    if isinstance(ctx, AgentRuntimeContext):
        return ctx
    return AgentRuntimeContext.build(db=db, client_pool=client_pool, config=kwargs.get("config"))


def _matches_phone(phone: str, phone_filter: str) -> bool:
    if not phone_filter:
        return True
    if phone_filter.endswith("*"):
        prefix = phone_filter[:-1]
        return bool(prefix) and phone.startswith(prefix)
    return phone == phone_filter


def _format_phones(phones: set[str]) -> str:
    return ", ".join(sorted(phones)) if phones else "-"


def _format_phone_list(phones: list[str]) -> str:
    return ", ".join(phones) if phones else "-"


def _remaining_seconds(account: object) -> int | None:
    from datetime import datetime, timezone

    flood_until = normalize_flood_wait_until(getattr(account, "flood_wait_until", None))
    if flood_until is None:
        return None
    return max(1, int((flood_until - datetime.now(timezone.utc)).total_seconds()))


def _diagnostic_lines(accounts: list[object], client_pool: object | None) -> list[str]:
    active_accounts = [a for a in accounts if getattr(a, "is_active", False)]
    active_phones = [str(getattr(a, "phone", "")) for a in active_accounts if getattr(a, "phone", "")]
    connected = connected_phones_from_pool(client_pool)
    available = available_live_read_phones(active_accounts, connected)
    flood_waited = [
        str(getattr(a, "phone", ""))
        for a in active_accounts
        if getattr(a, "phone", "") and is_flood_wait_active(a)
    ]
    return [
        f"DB active accounts: {len(active_phones)} ({_format_phone_list(active_phones)}).",
        f"Runtime connected phones: {_format_phones(connected)}.",
        f"Available phones: {_format_phone_list(available)}.",
        f"Flood-waited phones: {_format_phone_list(flood_waited)}.",
    ]


async def _get_account_summaries(db: object) -> list[object]:
    for getter_name in ("get_account_summaries", "get_accounts"):
        getter = getattr(db, getter_name, None)
        if not callable(getter):
            continue
        result = getter()
        if isawaitable(result):
            result = await result
        if isinstance(result, (list, tuple)):
            return list(result)
    return []


async def get_live_account_info_text(runtime: AgentRuntimeContext, phone: object = "") -> str:
    """Return account info with DB/runtime/profile-fetch states kept separate."""
    phone_filter = normalize_phone(phone)
    db_accounts = await get_accounts_with_flood_cleanup(runtime.db)
    connected = connected_phones_from_pool(runtime.client_pool)
    if phone_filter:
        connected = {p for p in connected if _matches_phone(p, phone_filter)}

    if not runtime.has_live_telegram:
        details = [_NO_LIVE_RUNTIME]
        if runtime.runtime_kind == "snapshot":
            details.append(
                "worker snapshot видит подключенные телефоны, но этот backend не имеет live Telegram runtime. "
                "Web snapshot runtime can show worker-connected phones, but live Telegram API "
                "is only available in the worker or embedded-worker process."
            )
        else:
            details.append("No live Telegram client pool is attached to this agent backend.")
        if connected:
            details.append(f"Runtime connected phones snapshot: {_format_phones(connected)}.")
        return "\n".join(details)

    try:
        users = await runtime.client_pool.get_users_info(include_avatar=False)
    except Exception:
        users = []
    if phone_filter:
        users = [u for u in users if _matches_phone(str(u.phone), phone_filter)]

    db_by_phone = {a.phone: a for a in db_accounts}
    active_count = sum(1 for a in db_accounts if getattr(a, "is_active", False))
    if not users:
        if connected:
            lines = [
                "Live Telegram account profiles unavailable for this request; profile fetch unavailable.",
                f"DB active accounts: {active_count}.",
                f"Runtime connected phones: {_format_phones(connected)}.",
                "Telegram profile fetch returned no profiles; do not treat this as disconnected.",
            ]
            return "\n".join(lines)
        return "Live Telegram accounts not found for this request: не найдены."

    lines = [f"Live Telegram accounts ({len(users)}):"]
    for u in users:
        db_account = db_by_phone.get(u.phone)
        name = f"{u.first_name} {u.last_name}".strip() or "-"
        username = f"@{u.username}" if u.username else "-"
        premium = "да" if u.is_premium else "нет"
        active = "да" if getattr(db_account, "is_active", False) else "нет"
        primary = "да" if getattr(db_account, "is_primary", False) else "нет"
        session_status = account_session_status(db_account) if db_account else "missing"
        session_present = "да" if db_account and session_status == "ok" else session_status
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
            accounts = await get_accounts_with_flood_cleanup(db)
            if not accounts:
                return _text_response("Аккаунты не найдены.")
            lines = [f"Аккаунты ({len(accounts)}) в БД:"]
            lines.extend(_diagnostic_lines(accounts, client_pool))
            for a in accounts:
                status = "активен" if a.is_active else "неактивен"
                flood = ""
                if is_flood_wait_active(a):
                    remaining = _remaining_seconds(a)
                    suffix = f", remaining={remaining}s" if remaining is not None else ""
                    flood = f" [flood_wait до {a.flood_wait_until}{suffix}]"
                session_status = account_session_status(a)
                session_suffix = "" if session_status == "ok" else f", session_status={session_status}"
                lines.append(f"- id={a.id}, phone={a.phone}, {status}{session_suffix}{flood}")
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
            accounts = await _get_account_summaries(db)
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
            accounts = await _get_account_summaries(db)
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
            accounts = await get_accounts_with_flood_cleanup(db)
            if not accounts:
                return _text_response("Аккаунты не найдены.")
            lines = ["Flood-статус аккаунтов в БД:"]
            lines.extend(_diagnostic_lines(accounts, client_pool))
            for a in accounts:
                flood = "нет ограничений"
                if is_flood_wait_active(a):
                    remaining = _remaining_seconds(a)
                    suffix = f" (remaining {remaining}s)" if remaining is not None else ""
                    flood = f"заблокирован до {a.flood_wait_until}{suffix}"
                lines.append(f"- {a.phone}: {flood}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения flood-статуса: {e}")

    tools.append(get_flood_status)

    @tool(
        "get_account_availability",
        "Report each Telegram account's availability using the SAME source of truth as the "
        "Settings UI (states: available / flood / disconnected / inactive / session_unavailable). "
        "ALWAYS call this BEFORE claiming an account is unavailable or needs re-authorization. "
        "It distinguishes a saved-session reconnect (no SMS/2FA) from interactive Telegram login.",
        {"phone": Annotated[str, "Опционально: телефон или префикс (например +8613... или +861*)"]},
    )
    async def get_account_availability(args):
        try:
            accounts = await get_accounts_with_flood_cleanup(db)
            if not accounts:
                return _text_response("Аккаунты не найдены.")
            phone_filter = normalize_phone(args.get("phone", ""))
            connected = connected_phones_from_pool(client_pool)
            rows = []
            for a in accounts:
                phone = str(getattr(a, "phone", "") or "")
                if not phone or (phone_filter and not _matches_phone(phone, phone_filter)):
                    continue
                avail = compute_account_availability(a, connected=phone in connected)
                state = avail["state"]
                guidance = _AVAILABILITY_GUIDANCE.get(state, state)
                extra = ""
                if state == "flood":
                    remaining = _remaining_seconds(a)
                    if remaining is not None:
                        extra = f" (осталось ~{remaining}s)"
                elif state == "session_unavailable":
                    extra = f" (session_status={account_session_status(a)})"
                rows.append(f"- {phone}: {state}{extra} — {guidance}")
            if not rows:
                return _text_response(f"Аккаунты по фильтру '{phone_filter}' не найдены.")
            header = "Доступность аккаунтов (тот же источник истины, что и Settings UI):"
            return _text_response("\n".join([header, *rows]))
        except Exception as e:
            return _text_response(f"Ошибка получения доступности аккаунтов: {e}")

    tools.append(get_account_availability)

    @tool(
        "clear_flood_status",
        "Clear a STALE/expired flood-wait entry for an account. Refuses to clear an "
        "ACTIVE flood wait — that is a Telegram-mandated pause and must not be bypassed. "
        "Ask user for confirmation first.",
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
            accounts = await _get_account_summaries(db)
            acc = next((a for a in accounts if a.phone == phone), None)
            if acc is None:
                return _text_response(f"Аккаунт {phone} не найден.")
            # Do not let the agent defeat a live Telegram flood wait by clearing
            # it as a retry hack (#597). An active wait is server-mandated; only
            # stale/expired entries may be cleared here. Manual stale-state repair
            # stays available via CLI `account flood-clear`.
            if is_flood_wait_active(acc):
                remaining = _remaining_seconds(acc)
                suffix = f" (осталось ~{remaining}s)" if remaining is not None else ""
                return _text_response(
                    f"Отклонено: у {phone} активен flood-wait до {acc.flood_wait_until}{suffix}. "
                    "Это пауза, предписанная Telegram, — её нельзя обойти сбросом, "
                    "иначе аккаунт получит ещё более длинную блокировку. Дождитесь "
                    "окончания. Для ручного восстановления зависшего состояния есть "
                    "CLI `account flood-clear`."
                )
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
