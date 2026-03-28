"""Agent tools for system settings and diagnostics."""

from __future__ import annotations

from claude_agent_sdk import tool

from src.agent.tools._registry import _text_response, require_confirmation


def register(db, client_pool, embedding_service, **kwargs):
    scheduler_manager = kwargs.get("scheduler_manager")
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
        {"prompt_template": str, "backend": str, "confirm": bool},
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
        {"low_uniqueness_threshold": float, "low_subscriber_ratio_threshold": float, "confirm": bool},
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
        {"collect_interval_minutes": int, "confirm": bool},
    )
    async def save_scheduler_settings(args):
        gate = require_confirmation("изменит интервал планировщика сбора", args)
        if gate:
            return gate
        try:
            interval = int(args.get("collect_interval_minutes", 60))
            interval = max(1, min(1440, interval))
            await db.set_setting("collect_interval_minutes", str(interval))
            if scheduler_manager is not None:
                scheduler_manager.update_interval(interval)
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

    return tools
