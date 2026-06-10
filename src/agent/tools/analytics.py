"""Agent tools for content analytics, trends, and calendar."""

from __future__ import annotations

from typing import Annotated

from claude_agent_sdk import tool

from src.agent.tools._categories import ToolCategory, ToolMeta
from src.agent.tools._registry import _text_response

_MAX_TREND_DAYS = 365
_MAX_TREND_LIMIT = 100


def _daily_stat_value(row: object, attr: str, legacy_key: str | None = None, default: int | str = 0) -> object:
    if hasattr(row, attr):
        return getattr(row, attr)
    if isinstance(row, dict):
        return row.get(legacy_key or attr, default)
    return default


def _clamp_positive(value: int, upper: int) -> int:
    return max(1, min(value, upper))




# Permission metadata for this module's tools (#245). Single source of
# truth: permissions.py derives TOOL_CATEGORIES / MODULE_GROUPS /
# PHONE_BINDED_TOOLS from these declarations; invariants in
# tests/test_tool_permissions_autoderive.py keep them in sync with the
# @tool() definitions.
TOOL_GROUPS: list[tuple[str, dict[str, ToolMeta]]] = [
    ("Аналитика", {
        "get_analytics_summary": ToolMeta(ToolCategory.READ),
        "get_pipeline_stats": ToolMeta(ToolCategory.READ),
        "get_daily_stats": ToolMeta(ToolCategory.READ),
        "get_trending_topics": ToolMeta(ToolCategory.READ),
        "get_trending_channels": ToolMeta(ToolCategory.READ),
        "get_message_velocity": ToolMeta(ToolCategory.READ),
        "get_peak_hours": ToolMeta(ToolCategory.READ),
        "get_calendar": ToolMeta(ToolCategory.READ),
        "get_top_messages": ToolMeta(ToolCategory.READ),
        "get_content_type_stats": ToolMeta(ToolCategory.READ),
        "get_hourly_activity": ToolMeta(ToolCategory.READ),
        "get_trending_emojis": ToolMeta(ToolCategory.READ),
        "get_channel_analytics": ToolMeta(ToolCategory.READ),
    }),
]

def register(db, client_pool, embedding_service, **kwargs):
    tools = []

    @tool("get_analytics_summary", "Get overall content analytics: generations, published, pending, rejected", {})
    async def get_analytics_summary(args):
        try:
            from src.services.content_analytics_service import ContentAnalyticsService

            svc = ContentAnalyticsService(db)
            summary = await svc.get_summary()
            lines = [
                "Аналитика контента:",
                f"- Всего генераций: {summary.get('total_generations', 0)}",
                f"- Опубликовано: {summary.get('total_published', 0)}",
                f"- На модерации: {summary.get('total_pending', 0)}",
                f"- Отклонено: {summary.get('total_rejected', 0)}",
                f"- Пайплайнов: {summary.get('pipelines_count', 0)}",
            ]
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения аналитики: {e}")

    tools.append(get_analytics_summary)

    @tool(
        "get_pipeline_stats",
        "Get detailed statistics for a specific pipeline or all pipelines",
        {"pipeline_id": Annotated[int, "ID пайплайна для фильтрации"]},
    )
    async def get_pipeline_stats(args):
        try:
            from src.services.content_analytics_service import ContentAnalyticsService

            svc = ContentAnalyticsService(db)
            pipeline_id = args.get("pipeline_id")
            stats = await svc.get_pipeline_stats(
                pipeline_id=int(pipeline_id) if pipeline_id is not None else None
            )
            if not stats:
                return _text_response("Статистика пайплайнов не найдена.")
            lines = ["Статистика пайплайнов:"]
            for s in stats:
                success_rate = getattr(s, "success_rate", 0) or 0
                lines.append(
                    f"- {s.pipeline_name} (id={getattr(s, 'pipeline_id', '?')}): "
                    f"генераций={s.total_generations}, опубл.={s.total_published}, "
                    f"отклон.={getattr(s, 'total_rejected', 0)}, "
                    f"на модерации={getattr(s, 'pending_moderation', 0)}, "
                    f"success_rate={success_rate:.0%}"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения статистики пайплайнов: {e}")

    tools.append(get_pipeline_stats)

    @tool(
        "get_daily_stats",
        "Get daily content generation statistics over a time period",
        {
            "days": Annotated[int, "Количество дней для анализа"],
            "pipeline_id": Annotated[int, "ID пайплайна для фильтрации"],
        },
    )
    async def get_daily_stats(args):
        try:
            from src.services.content_analytics_service import ContentAnalyticsService

            svc = ContentAnalyticsService(db)
            days = int(args.get("days", 30))
            pipeline_id = args.get("pipeline_id")
            rows = await svc.get_daily_stats(
                days=days,
                pipeline_id=int(pipeline_id) if pipeline_id is not None else None,
            )
            if not rows:
                return _text_response("Нет данных за указанный период.")
            lines = [f"Ежедневная статистика за {days} дней:"]
            for row in rows:
                date = _daily_stat_value(row, "date", default="")
                generations = _daily_stat_value(row, "generations", "count")
                publications = _daily_stat_value(row, "publications", "published")
                lines.append(f"- {date}: генераций={generations}, опубл.={publications}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения дневной статистики: {e}")

    tools.append(get_daily_stats)

    @tool(
        "get_trending_topics",
        "Get trending topics/keywords from collected messages over the last N days",
        {
            "days": Annotated[int, "Количество дней для анализа"],
            "limit": Annotated[int, "Максимальное количество результатов"],
        },
    )
    async def get_trending_topics(args):
        try:
            from src.services.trend_service import TrendService

            svc = TrendService(db)
            days = _clamp_positive(int(args.get("days", 7)), _MAX_TREND_DAYS)
            limit = _clamp_positive(int(args.get("limit", 20)), _MAX_TREND_LIMIT)
            topics = await svc.get_trending_topics(days=days, limit=limit)
            if not topics:
                return _text_response("Трендовые темы не найдены.")
            lines = [f"Тренды за {days} дней (топ-{limit}):"]
            for t in topics:
                lines.append(f"- {t.keyword}: {t.count} упоминаний")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения трендов: {e}")

    tools.append(get_trending_topics)

    @tool(
        "get_trending_channels",
        "Get top channels by message activity over the last N days",
        {
            "days": Annotated[int, "Количество дней для анализа"],
            "limit": Annotated[int, "Максимальное количество результатов"],
        },
    )
    async def get_trending_channels(args):
        try:
            from src.services.trend_service import TrendService

            svc = TrendService(db)
            days = _clamp_positive(int(args.get("days", 7)), _MAX_TREND_DAYS)
            limit = _clamp_positive(int(args.get("limit", 20)), _MAX_TREND_LIMIT)
            channels = await svc.get_trending_channels(days=days, limit=limit)
            if not channels:
                return _text_response("Данные о каналах не найдены.")
            lines = [f"Топ каналов за {days} дней:"]
            for ch in channels:
                message_count = getattr(ch, "message_count", getattr(ch, "count", 0))
                lines.append(
                    f"- {ch.title} (id={getattr(ch, 'channel_id', '?')}): "
                    f"{message_count} сообщений"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения трендов каналов: {e}")

    tools.append(get_trending_channels)

    @tool(
        "get_message_velocity",
        "Get message volume over time (messages per day) for the last N days",
        {"days": Annotated[int, "Количество дней для анализа"]},
    )
    async def get_message_velocity(args):
        try:
            from src.services.trend_service import TrendService

            svc = TrendService(db)
            days = _clamp_positive(int(args.get("days", 30)), _MAX_TREND_DAYS)
            velocity = await svc.get_message_velocity(days=days)
            if not velocity:
                return _text_response("Данные о скорости сообщений не найдены.")
            lines = [f"Скорость сообщений за {days} дней:"]
            for v in velocity:
                lines.append(f"- {v.date}: {v.count} сообщений")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения скорости сообщений: {e}")

    tools.append(get_message_velocity)

    @tool(
        "get_peak_hours",
        "Get peak activity hours across all channels",
        {"days": Annotated[int, "Количество дней для анализа"]},
    )
    async def get_peak_hours(args):
        try:
            from src.services.trend_service import TrendService

            svc = TrendService(db)
            days = _clamp_positive(int(args.get("days", 30)), _MAX_TREND_DAYS)
            hours = await svc.get_peak_hours(days=days)
            if not hours:
                return _text_response("Данные о пиковых часах не найдены.")
            lines = [f"Пиковые часы активности за {days} дней:"]
            for h in hours:
                bar = "█" * max(1, h.count // 10)
                lines.append(f"- {h.hour:02d}:00 — {h.count} сообщений {bar}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения пиковых часов: {e}")

    tools.append(get_peak_hours)

    @tool(
        "get_calendar",
        "Get upcoming scheduled content publications",
        {
            "limit": Annotated[int, "Максимальное количество результатов"],
            "pipeline_id": Annotated[int, "ID пайплайна для фильтрации"],
        },
    )
    async def get_calendar(args):
        try:
            from src.services.content_calendar_service import ContentCalendarService

            svc = ContentCalendarService(db)
            limit = int(args.get("limit", 20))
            pipeline_id = args.get("pipeline_id")
            events = await svc.get_upcoming(
                limit=limit,
                pipeline_id=int(pipeline_id) if pipeline_id is not None else None,
            )
            if not events:
                return _text_response("Нет запланированных публикаций.")
            lines = [f"Ближайшие публикации ({len(events)}):"]
            for e in events:
                scheduled = getattr(e, "scheduled_time", None) or getattr(e, "created_at", "unknown")
                preview = getattr(e, "preview", "") or ""
                lines.append(
                    f"- run_id={e.run_id}, pipeline={e.pipeline_name} "
                    f"(id={getattr(e, 'pipeline_id', '?')}), "
                    f"статус={e.moderation_status}, дата={scheduled}, "
                    f"превью: {preview[:100]}"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения календаря: {e}")

    tools.append(get_calendar)

    # ------------------------------------------------------------------
    # get_top_messages  (top messages by reactions)
    # ------------------------------------------------------------------

    @tool(
        "get_top_messages",
        "Get top messages ranked by total reactions count. "
        "Optional date filters: date_from, date_to (YYYY-MM-DD).",
        {
            "limit": Annotated[int, "Максимальное количество результатов"],
            "date_from": Annotated[str, "Начало периода в формате YYYY-MM-DD"],
            "date_to": Annotated[str, "Конец периода в формате YYYY-MM-DD"],
        },
    )
    async def get_top_messages(args):
        try:
            limit = min(int(args.get("limit", 20)), 200)
            date_from = args.get("date_from")
            date_to = args.get("date_to")
            rows = await db.get_top_messages(limit=limit, date_from=date_from, date_to=date_to)
            if not rows:
                return _text_response("Сообщения с реакциями не найдены.")
            lines = [f"Топ-{len(rows)} сообщений по реакциям:"]
            for i, row in enumerate(rows, 1):
                channel = row.get("channel_title") or row.get("channel_username") or str(row.get("channel_id", ""))
                text = (row.get("text") or "")[:80].replace("\n", " ")
                lines.append(f"{i}. [{row['total_reactions']} реакций] {channel}: {text}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения топ-сообщений: {e}")

    tools.append(get_top_messages)

    # ------------------------------------------------------------------
    # get_content_type_stats  (engagement by media type)
    # ------------------------------------------------------------------

    @tool(
        "get_content_type_stats",
        "Get message counts and average reactions grouped by content type (text, photo, video, etc.).",
        {
            "date_from": Annotated[str, "Начало периода в формате YYYY-MM-DD"],
            "date_to": Annotated[str, "Конец периода в формате YYYY-MM-DD"],
        },
    )
    async def get_content_type_stats(args):
        try:
            date_from = args.get("date_from")
            date_to = args.get("date_to")
            rows = await db.get_engagement_by_media_type(date_from=date_from, date_to=date_to)
            if not rows:
                return _text_response("Нет данных по типам контента.")
            lines = ["Статистика по типам контента:"]
            for row in rows:
                lines.append(
                    f"- {row['content_type']}: {row['message_count']} сообщений, "
                    f"ср. реакций: {row['avg_reactions']:.1f}"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения статистики контента: {e}")

    tools.append(get_content_type_stats)

    # ------------------------------------------------------------------
    # get_hourly_activity  (message distribution by hour)
    # ------------------------------------------------------------------

    @tool(
        "get_hourly_activity",
        "Get message distribution by hour of day (UTC). Shows when channels are most active.",
        {
            "date_from": Annotated[str, "Начало периода в формате YYYY-MM-DD"],
            "date_to": Annotated[str, "Конец периода в формате YYYY-MM-DD"],
        },
    )
    async def get_hourly_activity(args):
        try:
            date_from = args.get("date_from")
            date_to = args.get("date_to")
            rows = await db.get_hourly_activity(date_from=date_from, date_to=date_to)
            if not rows:
                return _text_response("Нет данных по часовой активности.")
            lines = ["Активность по часам (UTC):"]
            for row in rows:
                bar = "█" * min(max(1, row["message_count"] // 10), 30)
                lines.append(
                    f"- {row['hour']:02d}:00 — {row['message_count']} сообщений, "
                    f"ср. реакций: {row['avg_reactions']:.1f} {bar}"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения часовой активности: {e}")

    tools.append(get_hourly_activity)

    @tool(
        "get_trending_emojis",
        "Get the most-used reaction emojis across collected channels over the last N days.",
        {
            "days": Annotated[int, "Период в днях (по умолчанию 7)"],
            "limit": Annotated[int, "Максимум эмодзи (по умолчанию 15)"],
        },
    )
    async def get_trending_emojis(args):
        try:
            from src.services.trend_service import TrendService

            days = _clamp_positive(int(args.get("days", 7)), _MAX_TREND_DAYS)
            limit = _clamp_positive(int(args.get("limit", 15)), _MAX_TREND_LIMIT)
            emojis = await TrendService(db).get_trending_emojis(days=days, limit=limit)
            if not emojis:
                return _text_response("Нет данных по трендовым эмодзи.")
            lines = [f"Трендовые эмодзи за {days} дн.:"]
            for item in emojis:
                lines.append(f"- {item.emoji}: {item.count}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения трендовых эмодзи: {e}")

    tools.append(get_trending_emojis)

    @tool(
        "get_channel_analytics",
        "Get an analytics overview for a single channel: subscribers and deltas, ERR, post "
        "frequency, average views/forwards/reactions. channel_id = Telegram numeric ID.",
        {
            "channel_id": Annotated[int, "Числовой Telegram ID канала"],
            "days": Annotated[int, "Период в днях (по умолчанию 30)"],
        },
    )
    async def get_channel_analytics(args):
        channel_id = args.get("channel_id")
        if not channel_id:
            return _text_response("Ошибка: channel_id обязателен.")
        try:
            from src.services.channel_analytics_service import ChannelAnalyticsService

            days = int(args.get("days", 30))
            ov = await ChannelAnalyticsService(db).get_channel_overview(int(channel_id), days=days)
            if ov.title is None and ov.subscriber_count is None:
                return _text_response(f"Канал channel_id={channel_id} не найден или без данных.")
            title = ov.title or "(без названия)"
            lines = [
                f"Аналитика канала: {title} (channel_id={ov.channel_id}), период {days} дн.",
                f"- Подписчиков: {ov.subscriber_count if ov.subscriber_count is not None else '—'} "
                f"(Δ {ov.subscriber_delta if ov.subscriber_delta is not None else '—'})",
                f"- ERR: {f'{ov.err:.2f}%' if ov.err is not None else '—'}",
                f"- Постов: всего {ov.total_posts}, сегодня {ov.posts_today}, "
                f"неделя {ov.posts_week}, месяц {ov.posts_month}",
                f"- Ср. просмотры: {ov.avg_views if ov.avg_views is not None else '—'}, "
                f"ср. репосты: {ov.avg_forwards if ov.avg_forwards is not None else '—'}, "
                f"ср. реакции: {ov.avg_reactions if ov.avg_reactions is not None else '—'}",
            ]
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка аналитики канала: {e}")

    tools.append(get_channel_analytics)

    return tools
