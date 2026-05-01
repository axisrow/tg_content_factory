"""Tool registry and confirmation helpers for agent tools."""

from __future__ import annotations

import json as _json
import logging
import re as _re
from dataclasses import dataclass
from typing import Any

from src.agent.runtime_context import AgentRuntimeContext

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


def normalize_phone(phone: str) -> str:
    """Ensure phone starts with '+' — models sometimes omit it."""
    phone = phone.strip()
    if phone and not phone.startswith("+"):
        phone = "+" + phone
    return phone


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


async def resolve_phone(db: object, raw_phone: str) -> tuple[str, dict | None]:
    """Normalize phone, default to primary account if empty.

    Returns ``(phone, None)`` on success or ``("", error_response)`` on failure.
    """
    phone = normalize_phone(raw_phone)
    if phone:
        return phone, None
    try:
        accounts = await db.get_accounts()
    except Exception:
        return "", _text_response("Ошибка: не удалось получить список аккаунтов.")
    if not accounts:
        return "", _text_response("Ошибка: нет подключённых аккаунтов.")
    primary = next((a for a in accounts if a.is_primary), accounts[0])
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
