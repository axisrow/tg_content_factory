"""Agent read tools for Telegram command queue diagnostics."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Any

from claude_agent_sdk import tool

from src.agent.tools._registry import (
    ToolInputError,
    _text_response,
    arg_int,
    arg_str,
    normalize_phone,
    require_phone_permission,
)
from src.models import TelegramCommand, TelegramCommandStatus
from src.services.telegram_command_service import TelegramCommandService

_STATUS_LABELS = {
    TelegramCommandStatus.PENDING: "ждёт",
    TelegramCommandStatus.RUNNING: "выполняется",
    TelegramCommandStatus.SUCCEEDED: "выполнено",
    TelegramCommandStatus.FAILED: "ошибка",
    TelegramCommandStatus.CANCELLED: "отменено",
}

_WAIT_REASON_LABELS = {
    "waiting_flood_wait": "из-за flood-wait",
    "waiting_warmup": "из-за прогрева",
    "waiting_rate_limit": "из-за паузы между реакциями",
}

GET_TELEGRAM_QUEUE_STATUS_SCHEMA = {
    "command_type": Annotated[str, "Тип задания, например dialogs.react"],
    "phone": Annotated[str, "Телефон аккаунта для фильтра"],
    "status": Annotated[str, "Статус: pending, running, succeeded, failed, cancelled"],
    "limit": Annotated[int, "Сколько последних заданий показать, максимум 100"],
}


def _parse_status(raw: str) -> TelegramCommandStatus | None:
    if not raw:
        return None
    try:
        return TelegramCommandStatus(raw)
    except ValueError as exc:
        allowed = ", ".join(item.value for item in TelegramCommandStatus)
        raise ToolInputError(f"status должен быть одним из: {allowed}.") from exc


def _format_time(value: datetime | None) -> str:
    if value is None:
        return "-"
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _summary_line(title: str, counts: dict[TelegramCommandStatus, int]) -> str:
    total = sum(counts.values())
    return (
        f"{title}: Всего: {total}. "
        f"Ждёт: {counts.get(TelegramCommandStatus.PENDING, 0)}. "
        f"Выполняется: {counts.get(TelegramCommandStatus.RUNNING, 0)}. "
        f"Выполнено: {counts.get(TelegramCommandStatus.SUCCEEDED, 0)}. "
        f"Ошибок: {counts.get(TelegramCommandStatus.FAILED, 0)}. "
        f"Отменено: {counts.get(TelegramCommandStatus.CANCELLED, 0)}."
    )


def _command_phone(command: TelegramCommand) -> str:
    return str(command.payload.get("phone") or "-")


def _command_target(command: TelegramCommand) -> str:
    payload = command.payload
    if command.command_type == "dialogs.react":
        emoji = payload.get("emoji") or ""
        chat_id = payload.get("chat_id", "-")
        message_id = payload.get("message_id", "-")
        return f"reaction {emoji} в чат {chat_id}, сообщение {message_id}"
    if "recipient" in payload:
        return f"получатель {payload.get('recipient')}"
    if "chat_id" in payload:
        parts = [f"чат {payload.get('chat_id')}"]
        if "message_id" in payload:
            parts.append(f"сообщение {payload.get('message_id')}")
        if "message_ids" in payload:
            parts.append(f"сообщения {payload.get('message_ids')}")
        return ", ".join(parts)
    if "target" in payload:
        return f"цель {payload.get('target')}"
    if "channel_id" in payload:
        return f"канал {payload.get('channel_id')}"
    return "-"


def _command_status_text(command: TelegramCommand, now: datetime) -> str:
    label = _STATUS_LABELS.get(command.status, str(command.status))
    if command.status == TelegramCommandStatus.PENDING and command.run_after is not None:
        run_after = command.run_after.astimezone(timezone.utc)
        if run_after > now:
            return f"{label} до {_format_time(run_after)}"
    return label


def _command_reason(command: TelegramCommand) -> str:
    result_payload = command.result_payload or {}
    state = str(result_payload.get("state") or "")
    reason = _WAIT_REASON_LABELS.get(state)
    if reason:
        return reason
    if command.error:
        return command.error
    detail = result_payload.get("detail")
    return str(detail) if detail else ""


def _format_command_line(command: TelegramCommand, now: datetime) -> str:
    reason = _command_reason(command)
    suffix = f" ({reason})" if reason else ""
    return (
        f"#{command.id} {command.command_type}: {_command_target(command)}; "
        f"телефон {_command_phone(command)} — {_command_status_text(command, now)}{suffix}; "
        f"создано {_format_time(command.created_at)}"
    )


def _reaction_wait_line(state_counts: dict[str, int]) -> str | None:
    parts = [
        f"{count} {_WAIT_REASON_LABELS[state]}"
        for state in _WAIT_REASON_LABELS
        if (count := state_counts.get(state, 0)) > 0
    ]
    if not parts:
        return None
    return "Ожидание реакций: " + ", ".join(parts) + "."


def register_queue_status_tools(db: Any) -> list[Any]:
    tools: list[Any] = []

    @tool(
        "get_telegram_queue_status",
        "Show Telegram command queue status for the agent: totals, reaction delivery status, "
        "recent tasks, delay reasons, and failures. Read-only.",
        GET_TELEGRAM_QUEUE_STATUS_SCHEMA,
    )
    async def get_telegram_queue_status(args):
        try:
            command_type = arg_str(args, "command_type")
            phone = normalize_phone(arg_str(args, "phone"))
            status = _parse_status(arg_str(args, "status"))
            raw_limit = arg_int(args, "limit", 20) or 20
        except ToolInputError as exc:
            return exc.to_response()

        perm_gate = await require_phone_permission(db, phone, "get_telegram_queue_status")
        if perm_gate:
            return perm_gate

        limit = max(1, min(raw_limit, 100))
        service = TelegramCommandService(db)
        try:
            commands = await service.list(
                command_type=command_type or None,
                phone=phone or None,
                status=status,
                limit=limit,
            )
            summary = await service.summary(
                command_type=command_type or None,
                phone=phone or None,
                status=status,
            )
            show_reactions = command_type in {"", "dialogs.react"}
            reaction_summary: dict[TelegramCommandStatus, int] | None = None
            reaction_states: dict[str, int] = {}
            if show_reactions:
                reaction_summary = await service.summary(
                    command_type="dialogs.react",
                    phone=phone or None,
                    status=status,
                )
                reaction_states = await service.result_state_summary(
                    command_type="dialogs.react",
                    phone=phone or None,
                    status=TelegramCommandStatus.PENDING if status is None else status,
                )
        except Exception as exc:
            return _text_response(f"Ошибка получения статуса очереди: {exc}")

        now = datetime.now(timezone.utc)
        lines = [_summary_line("Очередь Telegram-заданий", summary)]
        if show_reactions and reaction_summary is not None:
            lines.append("")
            lines.append(_summary_line("Реакции", reaction_summary))
            reaction_wait = _reaction_wait_line(reaction_states)
            if reaction_wait:
                lines.append(reaction_wait)
        lines.append("")
        if commands:
            lines.append(f"Последние задания ({len(commands)}):")
            lines.extend(_format_command_line(command, now) for command in commands)
        else:
            lines.append("Последние задания: нет.")
        return _text_response("\n".join(lines))

    tools.append(get_telegram_queue_status)
    return tools
