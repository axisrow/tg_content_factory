"""Agent tools for system settings and diagnostics."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated

from claude_agent_sdk import tool

from src.agent.tools._categories import ToolCategory, ToolMeta
from src.agent.tools._registry import _text_response, require_confirmation
from src.services.telegram_command_service import TelegramCommandService

# Permission metadata for this module's tools (#245). Single source of
# truth: permissions.py derives TOOL_CATEGORIES / MODULE_GROUPS /
# PHONE_BINDED_TOOLS from these declarations; invariants in
# tests/test_tool_permissions_autoderive.py keep them in sync with the
# @tool() definitions.
TOOL_GROUPS: list[tuple[str, dict[str, ToolMeta]]] = [
    ("Настройки", {
        "get_settings": ToolMeta(ToolCategory.READ),
        "save_scheduler_settings": ToolMeta(ToolCategory.WRITE),
        "save_agent_settings": ToolMeta(ToolCategory.WRITE),
        "save_filter_settings": ToolMeta(ToolCategory.WRITE),
        "get_system_info": ToolMeta(ToolCategory.READ),
        "get_server_time": ToolMeta(ToolCategory.READ),
    }),
]

def register(db, client_pool, embedding_service, **kwargs):
    # scheduler_manager (a read-only snapshot shim in web-mode) is no longer used here:
    # save_scheduler_settings enqueues a scheduler.reconcile command for the worker instead
    # of calling the no-op shim (#1266). register() still accepts **kwargs for parity with
    # the other tool modules.
    tools = []

    @tool("get_settings", "Get current system settings (scheduler, agent, filters, etc.)", {})
    async def get_settings(args):
        try:
            settings_keys = [
                "collect_interval_minutes",
                "scheduler_enabled",
                "agent_prompt_template",
                "agent_backend_override",
                "semantic_search_model",
            ]
            lines = ["Настройки системы:"]
            for key in settings_keys:
                value = await db.get_setting(key)
                lines.append(f"- {key}: {value or '(не задано)'}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения настроек: {e}")

    tools.append(get_settings)

    @tool(
        "save_agent_settings",
        "⚠️ Update agent settings. backend values: 'claude-agent-sdk' or 'deepagents'. Ask user for confirmation first.",
        {
            "prompt_template": Annotated[str, "Шаблон системного промпта агента"],
            "backend": Annotated[str, "Бэкенд агента: claude-agent-sdk или deepagents"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
    )
    async def save_agent_settings(args):
        gate = require_confirmation("изменит настройки агента", args)
        if gate:
            return gate
        try:
            prompt_template = args.get("prompt_template")
            backend = args.get("backend")
            if prompt_template is not None:
                await db.set_setting("agent_prompt_template", prompt_template)
            if backend is not None:
                await db.set_setting("agent_backend_override", backend)
            return _text_response("Настройки агента сохранены.")
        except Exception as e:
            return _text_response(f"Ошибка сохранения настроек агента: {e}")

    tools.append(save_agent_settings)

    @tool(
        "save_filter_settings",
        "⚠️ Update channel filter thresholds. Ask user for confirmation first.",
        {
            "low_uniqueness_threshold": Annotated[float, "Порог уникальности контента (0.0–1.0)"],
            "low_subscriber_ratio_threshold": Annotated[float, "Порог соотношения подписчиков (0.0–1.0)"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
    )
    async def save_filter_settings(args):
        gate = require_confirmation("изменит пороги фильтров каналов", args)
        if gate:
            return gate
        try:
            for key in ["low_uniqueness_threshold", "low_subscriber_ratio_threshold"]:
                value = args.get(key)
                if value is not None:
                    await db.set_setting(key, str(float(value)))
            return _text_response("Настройки фильтров сохранены.")
        except Exception as e:
            return _text_response(f"Ошибка сохранения настроек фильтров: {e}")

    tools.append(save_filter_settings)

    @tool(
        "save_scheduler_settings",
        "⚠️ Update scheduler collection interval (1–1440 minutes). "
        "Changes take effect immediately if the scheduler is running. Requires confirm=true.",
        {
            "collect_interval_minutes": Annotated[int, "Интервал сбора в минутах (1–1440)"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
    )
    async def save_scheduler_settings(args):
        gate = require_confirmation("изменит интервал планировщика сбора", args)
        if gate:
            return gate
        try:
            interval = int(args.get("collect_interval_minutes", 60))
            interval = max(1, min(1440, interval))
            await db.set_setting("collect_interval_minutes", str(interval))
            # In web-mode ``scheduler_manager`` is the read-only ``SnapshotSchedulerManager``
            # shim whose ``update_interval`` is a no-op — the live ``SchedulerManager`` runs in
            # the separate worker container. Enqueue a ``scheduler.reconcile`` command so the
            # worker re-reads ``collect_interval_minutes`` and rebuilds its IntervalTrigger,
            # exactly like the web settings route does (#1266, matching #1236/#1247 via #1257).
            # ``enqueue`` deduplicates on (type, payload), so repeated reconciles collapse into
            # one pending command.
            await TelegramCommandService(db).enqueue(
                "scheduler.reconcile",
                payload={},
                requested_by="agent:save_scheduler_settings",
            )
            return _text_response(f"Интервал сбора установлен: {interval} мин.")
        except Exception as e:
            return _text_response(f"Ошибка сохранения настроек планировщика: {e}")

    tools.append(save_scheduler_settings)

    @tool("get_system_info", "Get system diagnostics: DB stats, memory, active tasks", {})
    async def get_system_info(args):
        try:
            stats = await db.get_stats()
            lines = ["Системная информация:"]
            for key, value in stats.items():
                lines.append(f"- {key}: {value}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения системной информации: {e}")

    tools.append(get_system_info)

    @tool(
        "get_server_time",
        "Get the current server time (UTC). Returns ISO8601, Unix timestamp and a "
        "human-readable form. No parameters.",
        {},
    )
    async def get_server_time(args):
        now = datetime.now(timezone.utc)
        lines = [
            "Текущее время сервера (UTC):",
            f"- ISO8601: {now.isoformat()}",
            f"- Unix: {int(now.timestamp())}",
            f"- Читаемо: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        ]
        return _text_response("\n".join(lines))

    tools.append(get_server_time)

    return tools
