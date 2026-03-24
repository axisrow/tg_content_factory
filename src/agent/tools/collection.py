"""Agent tools for channel collection management."""

from __future__ import annotations

from claude_agent_sdk import tool

from src.agent.tools._registry import _text_response, require_pool


def register(db, client_pool, embedding_service, **kwargs):
    tools = []

    @tool(
        "collect_channel",
        "Enqueue a single channel for message collection by its primary key (pk). "
        "Use force=true to collect even if the channel is filtered.",
        {"pk": int, "force": bool},
    )
    async def collect_channel(args):
        gate = require_pool(client_pool, "Сбор сообщений")
        if gate:
            return gate
        pk = args.get("pk")
        if pk is None:
            return _text_response("Ошибка: pk обязателен.")
        try:
            ch = await db.get_channel_by_pk(int(pk))
            if ch is None:
                return _text_response(f"Канал pk={pk} не найден.")
            force = bool(args.get("force", False))
            if ch.is_filtered and not force:
                return _text_response(
                    f"Канал '{ch.title}' отфильтрован. Используйте force=true для принудительного сбора."
                )
            await db.create_collection_task(ch.channel_id, ch.title, channel_username=ch.username)
            return _text_response(f"Канал '{ch.title}' поставлен в очередь на сбор.")
        except Exception as e:
            return _text_response(f"Ошибка постановки канала в очередь: {e}")

    tools.append(collect_channel)

    @tool(
        "collect_all_channels",
        "Enqueue all active (non-filtered) channels for message collection.",
        {},
    )
    async def collect_all_channels(args):
        gate = require_pool(client_pool, "Сбор сообщений")
        if gate:
            return gate
        try:
            channels = await db.get_channels(active_only=True, include_filtered=False)
            if not channels:
                return _text_response("Нет активных каналов для сбора.")
            count = 0
            for ch in channels:
                await db.create_collection_task(ch.channel_id, ch.title, channel_username=ch.username)
                count += 1
            return _text_response(f"Поставлено в очередь на сбор: {count} каналов.")
        except Exception as e:
            return _text_response(f"Ошибка постановки каналов в очередь: {e}")

    tools.append(collect_all_channels)

    @tool(
        "collect_channel_stats",
        "Enqueue statistics collection for a single channel by its primary key (pk).",
        {"pk": int},
    )
    async def collect_channel_stats(args):
        gate = require_pool(client_pool, "Сбор статистики")
        if gate:
            return gate
        pk = args.get("pk")
        if pk is None:
            return _text_response("Ошибка: pk обязателен.")
        try:
            ch = await db.get_channel_by_pk(int(pk))
            if ch is None:
                return _text_response(f"Канал pk={pk} не найден.")
            from src.models import StatsAllTaskPayload

            payload = StatsAllTaskPayload(channel_ids=[ch.channel_id], batch_size=1)
            await db.create_stats_task(payload)
            return _text_response(f"Сбор статистики для '{ch.title}' поставлен в очередь.")
        except Exception as e:
            return _text_response(f"Ошибка постановки сбора статистики: {e}")

    tools.append(collect_channel_stats)

    @tool(
        "collect_all_stats",
        "Enqueue statistics collection for all active channels.",
        {},
    )
    async def collect_all_stats(args):
        gate = require_pool(client_pool, "Сбор статистики")
        if gate:
            return gate
        try:
            channels = await db.get_channels(active_only=True, include_filtered=False)
            if not channels:
                return _text_response("Нет активных каналов для сбора статистики.")
            from src.models import StatsAllTaskPayload

            payload = StatsAllTaskPayload(
                channel_ids=[ch.channel_id for ch in channels],
                batch_size=20,
            )
            await db.create_stats_task(payload)
            return _text_response(f"Сбор статистики поставлен в очередь для {len(channels)} каналов.")
        except Exception as e:
            return _text_response(f"Ошибка постановки сбора статистики: {e}")

    tools.append(collect_all_stats)

    return tools
