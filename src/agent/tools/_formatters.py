"""Shared formatters for read-only agent tool output."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from unittest.mock import Mock


def _value(obj: object, name: str, default: object = None) -> object:
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _display_metric(value: object) -> object:
    return "?" if value is None else value


def _first_value(obj: object, names: Iterable[str]) -> object:
    for name in names:
        value = _value(obj, name)
        if isinstance(value, Mock):
            continue
        if value:
            return value
    return None


def _display_username(value: object) -> str | None:
    if not value:
        return None
    username = str(value).strip()
    if not username:
        return None
    return f"@{username.lstrip('@')}"


def format_channel_identity(
    obj: object,
    *,
    title_names: Iterable[str] = ("channel_title", "title"),
    username_names: Iterable[str] = ("channel_username", "username"),
) -> str:
    channel_id = _value(obj, "channel_id", "?")
    title = _first_value(obj, title_names)
    username = _display_username(_first_value(obj, username_names))
    label_parts = [str(part) for part in (title, username) if part]
    id_part = f"channel_id={channel_id}"
    if not label_parts:
        return id_part
    return f"{' / '.join(label_parts)} ({id_part})"


def format_notification_status(bot: object | None, target_status: object | None = None) -> str:
    lines: list[str]
    if bot is None:
        target_state = _value(target_status, "state") if target_status is not None else None
        if target_status is not None and target_state != "available":
            lines = ["Статус бота уведомлений невозможно проверить: целевой аккаунт недоступен."]
        else:
            lines = ["Бот уведомлений не настроен."]
    else:
        bot_username = _value(bot, "bot_username", "")
        username = f"@{bot_username}" if bot_username else "неизвестен"
        lines = [
            "Бот уведомлений:",
            f"- Username: {username}",
            f"- Bot ID: {_value(bot, 'bot_id', 'неизвестен') or 'неизвестен'}",
            f"- Target user ID: {_value(bot, 'tg_user_id', 'неизвестен')}",
        ]
        tg_username = _value(bot, "tg_username")
        if tg_username:
            lines.append(f"- Target username: @{tg_username}")
        created_at = _value(bot, "created_at")
        if created_at:
            lines.append(f"- Создан: {created_at}")

    if target_status is not None:
        lines.extend(
            [
                "",
                "Целевой аккаунт:",
                f"- Mode: {_value(target_status, 'mode', 'unknown')}",
                f"- Status: {_value(target_status, 'state', 'unknown')}",
            ]
        )
        configured_phone = _value(target_status, "configured_phone")
        effective_phone = _value(target_status, "effective_phone")
        if configured_phone:
            lines.append(f"- Configured phone: {configured_phone}")
        if effective_phone:
            lines.append(f"- Effective phone: {effective_phone}")
        message = _value(target_status, "message")
        if message:
            lines.append(f"- Diagnostic: {message}")

    return "\n".join(lines)


def format_filter_report(report: object) -> str:
    results = list(_value(report, "results", []) or [])
    if not results:
        return "Нет каналов для анализа фильтров. 0 каналов проверено, 0 рекомендовано к фильтрации."

    flagged = [result for result in results if bool(_value(result, "is_filtered", False))]
    lines = [
        f"Анализ фильтров: {len(results)} каналов проверено, "
        f"{len(flagged)} рекомендовано к фильтрации."
    ]
    for result in flagged:
        flags_value = _value(result, "flags", []) or []
        flags = ", ".join(str(flag) for flag in flags_value) if flags_value else "—"
        title = _value(result, "title") or "Без названия"
        lines.append(f"- {title} (id={_value(result, 'channel_id', '?')}): {flags}")
    return "\n".join(lines)


def format_channel_stats(stats: Mapping[int, object], channels: Iterable[object] | None = None) -> str:
    if not stats:
        return "Статистика каналов пока не собрана."

    channels_by_id = {
        int(channel_id): channel
        for channel in channels or []
        if (channel_id := _value(channel, "channel_id")) is not None
    }
    lines = [f"Статистика каналов ({len(stats)}):"]
    for cid, stat in stats.items():
        channel = channels_by_id.get(int(cid))
        title = _value(channel, "title") if channel is not None else None
        username = _value(channel, "username") if channel is not None else None
        label = f"{title or 'Без названия'} "
        if username:
            label += f"(@{username}, "
        else:
            label += "("
        label += f"channel_id={cid})"
        lines.append(
            f"- {label}: "
            f"subscribers={_display_metric(_value(stat, 'subscriber_count'))}, "
            f"avg_views={_display_metric(_value(stat, 'avg_views'))}"
        )
    return "\n".join(lines)
