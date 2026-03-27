"""Agent tools for content analytics, trends, and calendar."""

from __future__ import annotations

from claude_agent_sdk import tool

from src.agent.tools._registry import _text_response


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
        {"pipeline_id": int},
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
                lines.append(
                    f"- {s.pipeline_name} (id={s.pipeline_id}): "
                    f"генераций={s.total_generations}, опубл.={s.total_published}, "
                    f"отклон.={s.total_rejected}, на модерации={s.pending_moderation}, "
                    f"success_rate={s.success_rate:.0%}"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения статистики пайплайнов: {e}")

    tools.append(get_pipeline_stats)

    @tool(
        "get_daily_stats",
        "Get daily content generation statistics over a time period",
        {"days": int, "pipeline_id": int},
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
                lines.append(f"- {row['date']}: генераций={row.get('count', 0)}, опубл.={row.get('published', 0)}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения дневной статистики: {e}")

    tools.append(get_daily_stats)

    @tool(
        "get_trending_topics",
        "Get trending topics/keywords from collected messages over the last N days",
        {"days": int, "limit": int},
    )
    async def get_trending_topics(args):
        try:
            from src.services.trend_service import TrendService

            svc = TrendService(db)
            days = int(args.get("days", 7))
            limit = int(args.get("limit", 20))
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
        {"days": int, "limit": int},
    )
    async def get_trending_channels(args):
        try:
            from src.services.trend_service import TrendService

            svc = TrendService(db)
            days = int(args.get("days", 7))
            limit = int(args.get("limit", 20))
            channels = await svc.get_trending_channels(days=days, limit=limit)
            if not channels:
                return _text_response("Данные о каналах не найдены.")
            lines = [f"Топ каналов за {days} дней:"]
            for ch in channels:
                lines.append(f"- {ch.title} (id={ch.channel_id}): {ch.count} сообщений")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения трендов каналов: {e}")

    tools.append(get_trending_channels)

    @tool(
        "get_message_velocity",
        "Get message volume over time (messages per day) for the last N days",
        {"days": int},
    )
    async def get_message_velocity(args):
        try:
            from src.services.trend_service import TrendService

            svc = TrendService(db)
            days = int(args.get("days", 30))
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

    @tool("get_peak_hours", "Get peak activity hours across all channels", {})
    async def get_peak_hours(args):
        try:
            from src.services.trend_service import TrendService

            svc = TrendService(db)
            hours = await svc.get_peak_hours()
            if not hours:
                return _text_response("Данные о пиковых часах не найдены.")
            lines = ["Пиковые часы активности:"]
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
        {"limit": int, "pipeline_id": int},
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
                scheduled = e.scheduled_time or e.created_at
                lines.append(
                    f"- run_id={e.run_id}, pipeline={e.pipeline_name} (id={e.pipeline_id}), "
                    f"статус={e.moderation_status}, дата={scheduled}, "
                    f"превью: {e.preview[:100]}"
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
        {"limit": int, "date_from": str, "date_to": str},
    )
    async def get_top_messages(args):
        try:
            limit = int(args.get("limit", 20))
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
        {"date_from": str, "date_to": str},
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
        {"date_from": str, "date_to": str},
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
                bar = "█" * max(1, row["message_count"] // 10)
                lines.append(
                    f"- {row['hour']:02d}:00 — {row['message_count']} сообщений, "
                    f"ср. реакций: {row['avg_reactions']:.1f} {bar}"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения часовой активности: {e}")

    tools.append(get_hourly_activity)

    return tools
