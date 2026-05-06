"""Tool registry and confirmation helpers for agent tools."""

from __future__ import annotations

import json as _json
import logging
import re as _re
from dataclasses import dataclass
from datetime import datetime, timezone
from inspect import isawaitable
from typing import Any

from src.agent.runtime_context import AgentRuntimeContext
from src.utils.datetime import try_parse_utc_datetime

logger = logging.getLogger(__name__)


def _text_response(text: str) -> dict:
    """Wrap text into MCP tool response format."""
    return {"content": [{"type": "text", "text": text}]}


class ToolInputError(ValueError):
    """User-facing tool argument validation error."""

    def to_response(self) -> dict:
        return _text_response(f"Ошибка: {self}")


@dataclass(slots=True)
class AgentToolContext:
    """Shared dependencies and guards for agent tool handlers."""

    db: object
    config: object | None = None
    client_pool: object | None = None
    scheduler_manager: object | None = None
    embedding_service: object | None = None
    runtime_context: AgentRuntimeContext | None = None

    @classmethod
    def build(
        cls,
        *,
        db: object,
        config: object | None = None,
        client_pool: object | None = None,
        scheduler_manager: object | None = None,
        embedding_service: object | None = None,
        runtime_context: AgentRuntimeContext | None = None,
    ) -> "AgentToolContext":
        runtime_context = runtime_context or AgentRuntimeContext.build(
            db=db,
            config=config,
            client_pool=client_pool,
            scheduler_manager=scheduler_manager,
        )
        return cls(
            db=db,
            config=config,
            client_pool=client_pool,
            scheduler_manager=scheduler_manager,
            embedding_service=embedding_service,
            runtime_context=runtime_context,
        )

    def require_pool(self, action: str = "Эта операция") -> dict | None:
        return require_pool(self.client_pool, action)

    def require_live_runtime(self, action: str = "Эта операция", *, tool_name: str | None = None) -> dict | None:
        return require_live_runtime(self.runtime_context, action, tool_name=tool_name)

    async def resolve_phone(self, raw_phone: str) -> tuple[str, dict | None]:
        return await resolve_phone(self.db, raw_phone)

    async def require_phone_permission(self, phone: str, tool_name: str) -> dict | None:
        return await require_phone_permission(self.db, phone, tool_name)

    def channel_service(self):
        from src.services.channel_service import ChannelService

        return ChannelService(self.db, self.client_pool, None)

    def pipeline_service(self):
        from src.services.pipeline_service import PipelineService

        return PipelineService(self.db)


def get_tool_context(
    kwargs: dict,
    *,
    db: object,
    client_pool: object | None = None,
    embedding_service: object | None = None,
) -> AgentToolContext:
    """Return the shared tool context passed by the registry, or build one for direct tests."""
    ctx = kwargs.get("tool_context")
    if isinstance(ctx, AgentToolContext):
        return ctx
    return AgentToolContext.build(
        db=db,
        config=kwargs.get("config"),
        client_pool=client_pool,
        scheduler_manager=kwargs.get("scheduler_manager"),
        embedding_service=embedding_service,
        runtime_context=kwargs.get("runtime_context"),
    )


def arg_bool(args: dict[str, Any], name: str, default: bool = False) -> bool:
    return bool(args.get(name, default))


def arg_str(args: dict[str, Any], name: str, default: str = "", *, required: bool = False) -> str:
    value = args.get(name, default)
    if value is None:
        value = ""
    value = str(value).strip()
    if required and not value:
        raise ToolInputError(f"{name} обязателен.")
    return value


def arg_int(args: dict[str, Any], name: str, default: int | None = None, *, required: bool = False) -> int | None:
    value = args.get(name, default)
    if value is None or value == "":
        if required:
            raise ToolInputError(f"{name} обязателен.")
        return default
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ToolInputError(f"{name} должен быть целым числом.") from exc


def arg_csv_ints(
    args: dict[str, Any],
    name: str,
    *,
    required: bool = False,
    allow_empty: bool = False,
) -> list[int]:
    value = args.get(name, "")
    if value is None:
        value = ""
    if isinstance(value, (list, tuple)):
        parts = [str(v).strip() for v in value]
    else:
        parts = [part.strip() for part in str(value).split(",")]
    parts = [part for part in parts if part]
    if not parts:
        if required and not allow_empty:
            raise ToolInputError(f"{name} обязателен.")
        return []
    result: list[int] = []
    invalid: list[str] = []
    for part in parts:
        try:
            result.append(int(part))
        except ValueError:
            invalid.append(part)
    if invalid:
        raise ToolInputError(f"{name} должен содержать целые числа через запятую: {', '.join(invalid)}")
    return result


def require_args(args: dict[str, Any], *names: str) -> dict[str, str]:
    values = {name: arg_str(args, name) for name in names}
    missing = [name for name, value in values.items() if not value]
    if missing:
        joined = ", ".join(missing)
        raise ToolInputError(f"{joined} обязательны.")
    return values


def normalize_phone(phone: object) -> str:
    """Ensure phone starts with '+' — models sometimes omit it."""
    if phone is None:
        return ""
    phone = str(phone).strip()
    if phone and not phone.startswith("+"):
        phone = "+" + phone
    return phone


def normalize_flood_wait_until(value: object) -> datetime | None:
    """Return a UTC-aware flood wait timestamp, accepting DB strings and datetimes."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return try_parse_utc_datetime(value)
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        parsed = try_parse_utc_datetime(raw)
        if parsed is None:
            return None
    else:
        return None
    return parsed


def is_flood_wait_active(account: object, now: datetime | None = None) -> bool:
    """True only when the stored flood wait expires in the future."""
    now = now or datetime.now(timezone.utc)
    flood_until = normalize_flood_wait_until(getattr(account, "flood_wait_until", None))
    return flood_until is not None and flood_until > now


def connected_phones_from_pool(client_pool: object | None) -> set[str]:
    """Return phones that the attached runtime reports as connected."""
    if client_pool is None:
        return set()

    instance_attrs = getattr(client_pool, "__dict__", {})
    connected_phones = None
    if isinstance(instance_attrs, dict) and "connected_phones" in instance_attrs:
        connected_phones = instance_attrs["connected_phones"]
    elif callable(getattr(type(client_pool), "connected_phones", None)):
        connected_phones = getattr(client_pool, "connected_phones")

    if callable(connected_phones):
        try:
            phones = connected_phones()
        except Exception:
            phones = set()
        if isinstance(phones, (set, list, tuple)):
            return {str(phone) for phone in phones}

    try:
        clients = getattr(client_pool, "clients")
    except Exception:
        clients = {}
    if isinstance(clients, dict):
        return {str(phone) for phone in clients}
    return set()


def available_phones_from_pool(client_pool: object | None) -> set[str]:
    """Return phones that a snapshot/live pool reports as currently usable."""
    if client_pool is None:
        return set()
    try:
        phones = getattr(client_pool, "available_phones", None)
    except Exception:
        return set()
    if isinstance(phones, (set, list, tuple)):
        return {str(phone) for phone in phones if str(phone)}
    return set()


def flood_waited_phones_from_pool(client_pool: object | None) -> set[str]:
    """Return phones marked flood-waited by a runtime snapshot, if present."""
    if client_pool is None:
        return set()
    try:
        waits = getattr(client_pool, "flood_waits", None)
    except Exception:
        return set()
    if isinstance(waits, dict):
        return {str(phone) for phone in waits if str(phone)}
    return set()


def account_session_status(account: object) -> str:
    status = getattr(account, "session_status", "ok")
    return status if isinstance(status, str) else "ok"


def _pool_reports_connections(client_pool: object | None) -> bool:
    """Whether an empty connected set is a reliable runtime signal."""
    if client_pool is None:
        return True
    instance_attrs = getattr(client_pool, "__dict__", {})
    if isinstance(instance_attrs, dict) and (
        "clients" in instance_attrs or "connected_phones" in instance_attrs
    ):
        return True
    if callable(getattr(type(client_pool), "connected_phones", None)):
        return True
    try:
        return isinstance(getattr(client_pool, "clients"), dict)
    except Exception:
        return False


async def _get_accounts(db: object, *, active_only: bool = False) -> list[object]:
    for getter_name in ("get_account_summaries", "get_accounts"):
        getter = getattr(db, getter_name, None)
        if not callable(getter):
            continue
        try:
            result = getter(active_only=active_only)
        except TypeError:
            result = getter()
        if isawaitable(result):
            result = await result
        if isinstance(result, (list, tuple)):
            return list(result)
    return []


async def _clear_account_flood(db: object, phone: str) -> None:
    updater = getattr(db, "update_account_flood", None)
    if not callable(updater):
        return
    try:
        result = updater(phone, None)
        if isawaitable(result):
            await result
    except Exception:
        logger.debug("Failed to clear expired flood wait for %s", phone, exc_info=True)


async def get_accounts_with_flood_cleanup(
    db: object,
    *,
    active_only: bool = False,
    now: datetime | None = None,
) -> list[object]:
    """Load accounts and clear stale flood_wait_until values before callers inspect them."""
    now = now or datetime.now(timezone.utc)
    accounts = await _get_accounts(db, active_only=active_only)
    for account in accounts:
        flood_until = normalize_flood_wait_until(getattr(account, "flood_wait_until", None))
        if flood_until is not None and flood_until <= now:
            phone = str(getattr(account, "phone", ""))
            if phone:
                await _clear_account_flood(db, phone)
            try:
                account.flood_wait_until = None
            except Exception:
                pass
    return accounts


def _format_phone_list(phones: set[str] | list[str] | tuple[str, ...]) -> str:
    clean = sorted(str(phone) for phone in phones if str(phone))
    return ", ".join(clean) if clean else "-"


def require_live_runtime(
    runtime_context: AgentRuntimeContext | None,
    action: str = "Эта операция",
    *,
    tool_name: str | None = None,
) -> dict | None:
    """Return an explicit error if a tool needs the live Telegram runtime."""
    if runtime_context is not None and runtime_context.has_live_telegram:
        return None
    if runtime_context is None or runtime_context.client_pool is None:
        return require_pool(None, action)

    name = tool_name or action
    if runtime_context.runtime_kind == "snapshot":
        return _text_response(
            f"Ошибка: {name} требует live Telegram runtime. "
            "Текущий backend видит только worker snapshot/cache и не выполняет live Telegram API операции. "
            "Это ограничение runtime, а не признак отключённого аккаунта или отсутствующего чата."
        )
    return _text_response(
        f"Ошибка: {name} требует live Telegram runtime, но он недоступен. "
        "Это ограничение runtime, а не признак отключённого аккаунта или отсутствующего чата."
    )


def available_live_read_phones(
    accounts: list[object],
    connected_phones: set[str],
    *,
    trust_empty_connected: bool = True,
    now: datetime | None = None,
) -> list[str]:
    """Return active DB phones that are connected and not currently flood-waited."""
    now = now or datetime.now(timezone.utc)
    connected_filter = connected_phones if trust_empty_connected or connected_phones else None
    phones: list[str] = []
    for account in accounts:
        phone = str(getattr(account, "phone", ""))
        if not phone:
            continue
        if not getattr(account, "is_active", True):
            continue
        if account_session_status(account) != "ok":
            continue
        if connected_filter is not None and phone not in connected_filter:
            continue
        if is_flood_wait_active(account, now):
            continue
        phones.append(phone)
    return phones


async def resolve_live_read_phone(
    db: object,
    client_pool: object,
    raw_phone: object,
    *,
    tool_name: str,
) -> tuple[str, dict | None]:
    """Resolve a phone for read-only live tools using runtime connection state.

    Explicit phones are never silently replaced.  Omitted phones prefer the
    primary active connected account, then the first available active connected
    account.
    """
    now = datetime.now(timezone.utc)
    phone = normalize_phone(raw_phone)
    accounts = await get_accounts_with_flood_cleanup(db, active_only=True, now=now)
    connected_all = connected_phones_from_pool(client_pool)
    pool_available = available_phones_from_pool(client_pool)
    connected = pool_available or connected_all
    trust_connected = _pool_reports_connections(client_pool)
    available = available_live_read_phones(
        accounts,
        connected,
        trust_empty_connected=trust_connected,
        now=now,
    )
    available_set = set(available)
    flood_waited = {
        str(getattr(account, "phone", ""))
        for account in accounts
        if is_flood_wait_active(account, now)
    }
    flood_waited |= flood_waited_phones_from_pool(client_pool)
    active_phones = {str(getattr(account, "phone", "")) for account in accounts if str(getattr(account, "phone", ""))}
    connected_for_errors = connected_all if trust_connected else active_phones

    if phone:
        reason = None
        if phone not in active_phones:
            reason = "аккаунт не найден среди активных аккаунтов БД"
        elif trust_connected and phone not in connected:
            reason = "аккаунт не подключён в live runtime"
        elif phone in flood_waited:
            reason = "аккаунт временно в flood-wait"
        if reason is not None:
            return "", _text_response(
                f"Ошибка: {tool_name} не может использовать {phone}: {reason}. "
                f"Доступные телефоны: {_format_phone_list(available_set)}. "
                f"Runtime connected phones: {_format_phone_list(connected_for_errors)}."
            )
        return phone, None

    primary = next(
        (
            str(getattr(account, "phone", ""))
            for account in accounts
            if getattr(account, "is_primary", False) and str(getattr(account, "phone", "")) in available_set
        ),
        "",
    )
    if primary:
        return primary, None
    if available:
        return available[0], None

    if not accounts:
        return "", _text_response("Ошибка: нет активных аккаунтов в БД.")
    if trust_connected and not connected:
        return "", _text_response("Ошибка: нет подключённых аккаунтов в live runtime.")
    if flood_waited:
        return "", _text_response(
            f"Ошибка: все подключённые аккаунты временно в flood-wait. "
            f"Flood-waited phones: {_format_phone_list(flood_waited)}."
        )
    return "", _text_response(
        f"Ошибка: нет доступных подключённых активных аккаунтов для {tool_name}. "
        f"DB active accounts: {_format_phone_list(active_phones)}. "
        f"Runtime connected phones: {_format_phone_list(connected_for_errors)}."
    )


def require_confirmation(action_description: str, args: dict) -> dict | None:
    """Return a warning response if confirm is not True, else None (proceed).

    Usage in a tool handler::

        gate = require_confirmation("удалит канал 'X'", args)
        if gate:
            return gate
        # ... execute dangerous action
    """
    if args.get("confirm"):
        return None
    return _text_response(
        f"⚠️ Эта операция {action_description}. "
        f"Подтвердите, вызвав tool повторно с confirm=true."
    )


def require_pool(client_pool: object | None, action: str = "Эта операция") -> dict | None:
    """Return an error response if client_pool is None (CLI mode), else None."""
    if client_pool is not None:
        return None
    return _text_response(
        f"❌ {action} требует Telegram-клиент, который недоступен в CLI-режиме. "
        f"Используйте web-интерфейс."
    )


async def resolve_phone(db: object, raw_phone: object) -> tuple[str, dict | None]:
    """Normalize phone, default to primary account if empty.

    Returns ``(phone, None)`` on success or ``("", error_response)`` on failure.
    """
    phone = normalize_phone(raw_phone)
    if phone:
        return phone, None
    try:
        getter = getattr(db, "get_account_summaries", None)
        result = getter() if callable(getter) else None
        if isawaitable(result):
            result = await result
        if not isinstance(result, (list, tuple)):
            result = await db.get_accounts()
        accounts = list(result)
    except Exception:
        return "", _text_response("Ошибка: не удалось получить список аккаунтов.")
    usable_accounts = [
        account
        for account in accounts
        if account_session_status(account) == "ok"
    ]
    if not usable_accounts:
        return "", _text_response("Ошибка: нет подключённых аккаунтов.")
    primary = next((a for a in usable_accounts if a.is_primary), usable_accounts[0])
    return primary.phone, None


async def require_phone_permission(db: object, phone: str, tool_name: str) -> dict | None:
    """Return helpful response with allowed phones if not permitted, else None.

    If db has no phone permissions configured, returns None (all phones allowed).
    If phone is in allowed list for this tool, returns None (proceed).
    Otherwise: if a PermissionGate is active (TUI/web mode), shows an interactive
    permission dialog instead of a plain error.  Falls back to text error if no gate.
    """
    try:
        from src.agent.tools.permissions import TOOL_PERMISSIONS_SETTING

        raw = await db.get_setting(TOOL_PERMISSIONS_SETTING)
    except Exception:
        logger.warning("Failed to load agent tool permissions; blocking '%s'", tool_name, exc_info=True)
        return _text_response(
            f"❌ Не удалось загрузить ACL для '{tool_name}'. Действие заблокировано до восстановления настроек."
        )
    if not raw:
        return None  # no restrictions configured → allow all
    try:
        perms = _json.loads(raw)
    except (ValueError, TypeError):
        logger.warning("Malformed agent tool permissions JSON; blocking '%s'", tool_name)
        return _text_response(
            f"❌ ACL для '{tool_name}' повреждён. Действие заблокировано до исправления настроек."
        )
    if not isinstance(perms, dict):
        logger.warning("Agent tool permissions JSON is not an object; blocking '%s'", tool_name)
        return _text_response(
            f"❌ ACL для '{tool_name}' повреждён. Действие заблокировано до исправления настроек."
        )
    # Collect phones allowed for this tool
    allowed_phones = [p for p, tools in perms.items() if isinstance(tools, dict) and tools.get(tool_name, False)]
    if not allowed_phones:
        return None  # tool not restricted for any phone → allow all
    # Phone not in perms at all → defaults (all enabled), don't deny based on other phones' config
    if phone and phone not in perms:
        return None
    if phone in allowed_phones:
        return None  # phone is allowed
    # Phone not allowed — try permission gate first
    from src.agent.permission_gate import get_gate, get_request_context

    gate = get_gate()
    if gate is not None and get_request_context() is not None:
        return await gate.check(tool_name, phone)
    # No gate (one-shot CLI mode) — return text error with allowed phones
    phones_str = ", ".join(allowed_phones)
    if not phone:
        msg = (
            f"ℹ️ Для инструмента '{tool_name}' укажи параметр phone. "
            f"Разрешённые телефоны: {phones_str}"
        )
    else:
        msg = (
            f"❌ Телефон {phone} не разрешён для '{tool_name}'. "
            f"Разрешённые телефоны: {phones_str}"
        )
    return _text_response(msg)


_NUMERIC_ID_RE = _re.compile(r"^-?\d+$")


def should_try_dialog_title_lookup(identifier: object) -> bool:
    """Return True when an identifier can reasonably be a dialog title."""
    value = str(identifier or "").strip()
    lowered = value.lower()
    if not value or lowered == "me" or value.startswith("@") or "t.me/" in lowered:
        return False
    return True


async def load_dialogs_for_title_lookup(
    db: object,
    client_pool: object,
    phone: str,
    *,
    refresh: bool = False,
) -> list[dict]:
    """Load account dialogs through the existing ChannelService path."""
    from src.services.channel_service import ChannelService

    svc = ChannelService(db, client_pool, None)
    try:
        if refresh:
            dialogs = await svc.get_my_dialogs(phone, refresh=True)
        else:
            dialogs = await svc.get_my_dialogs(phone)
    except Exception:
        logger.debug(
            "Failed to load dialogs for title lookup phone=%s refresh=%s",
            phone,
            refresh,
            exc_info=True,
        )
        return []
    return [dict(dialog) for dialog in dialogs if isinstance(dialog, dict)]


async def find_dialogs_by_exact_title(
    db: object,
    client_pool: object,
    phone: str,
    title: object,
    *,
    allow_refresh: bool = False,
) -> list[dict]:
    """Find dialogs whose title exactly matches the identifier, case-insensitively."""
    query = str(title or "").strip()
    if not should_try_dialog_title_lookup(query):
        return []
    normalized = query.casefold()
    dialogs = await load_dialogs_for_title_lookup(db, client_pool, phone, refresh=False)
    matches = [
        dialog
        for dialog in dialogs
        if str(dialog.get("title") or "").strip().casefold() == normalized
    ]
    if matches or not allow_refresh:
        return matches
    live_dialogs = await load_dialogs_for_title_lookup(db, client_pool, phone, refresh=True)
    return [
        dialog
        for dialog in live_dialogs
        if str(dialog.get("title") or "").strip().casefold() == normalized
    ]


def format_dialog_title_candidates(title: object, matches: list[dict]) -> dict:
    """Format ambiguous title lookup matches for tool output."""
    query = str(title or "").strip()
    lines = [f"Ошибка: найдено несколько диалогов с названием '{query}'. Уточните chat_id."]
    for dialog in matches[:10]:
        lines.append(
            f"- id={dialog.get('channel_id', '?')}, type={dialog.get('channel_type', '?')}: "
            f"{dialog.get('title', '?')}"
        )
    if len(matches) > 10:
        lines.append(f"... ещё {len(matches) - 10}")
    return _text_response("\n".join(lines))


async def resolve_entity(
    client_pool: object,
    phone: str,
    chat_id: str,
    *,
    is_user: bool = False,
) -> tuple[object, object, dict | None]:
    """Resolve a chat/user entity for Telethon operations with dialog-cache warming fallback.

    For usernames, t.me links, and "me" → uses ``client.get_entity()`` directly (API lookup).
    For numeric IDs → uses ``ClientPool.resolve_dialog_entity()`` which warms the entity cache
    automatically when the entity isn't cached yet (e.g. private groups without a username).

    Returns ``(raw_client, entity, None)`` on success or ``(None, None, error_response)`` on failure.
    Pass ``is_user=True`` when *chat_id* is a user ID (e.g. the ``user_id`` param in admin tools).
    """
    result = await client_pool.get_native_client_by_phone(phone)
    if result is None:
        return None, None, _text_response(f"Клиент для {phone} не найден или flood-wait активен.")
    raw_client, _ = result

    # Non-numeric identifiers: let Telethon resolve via API (username/link/self)
    cid = chat_id.strip()
    if not _NUMERIC_ID_RE.match(cid):
        try:
            entity = await raw_client.get_entity(cid)
            return raw_client, entity, None
        except Exception as e:
            return None, None, _text_response(f"Ошибка: не удалось найти чат/пользователя '{chat_id}': {e}")

    # Numeric ID: use resolve_dialog_entity which handles cache warming
    dialog_id = int(cid)
    session_result = await client_pool.get_client_by_phone(phone)
    if session_result is None:
        return None, None, _text_response(f"Клиент для {phone} не найден или flood-wait активен.")
    session, _ = session_result

    target_type = "dm" if is_user else None
    try:
        entity = await client_pool.resolve_dialog_entity(session, phone, dialog_id, target_type)
        if entity is None:
            raise ValueError("entity is None")
        return raw_client, entity, None
    except (ValueError, TypeError, KeyError):
        pass
    except Exception as e:
        # Propagate flood waits and auth errors — do not retry
        return None, None, _text_response(f"Ошибка: не удалось получить entity для {chat_id}: {e}")

    # Fallback: if not is_user, also try as PeerUser (numeric user DMs without username)
    if not is_user:
        try:
            entity = await client_pool.resolve_dialog_entity(session, phone, dialog_id, "dm")
            if entity is not None:
                return raw_client, entity, None
        except (ValueError, TypeError, KeyError):
            pass
        except Exception as e:
            return None, None, _text_response(f"Ошибка: не удалось получить entity для {chat_id}: {e}")

    return None, None, _text_response(
        f"Ошибка: не удалось найти чат/пользователя с ID {chat_id}. "
        f"Попробуйте сначала обновить кэш диалогов (refresh_dialogs)."
    )
