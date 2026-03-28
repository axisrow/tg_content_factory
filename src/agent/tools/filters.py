"""Agent tools for channel filter management."""

from __future__ import annotations

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._registry import _text_response, require_confirmation


def register(db, client_pool, embedding_service, **kwargs):
    tools = []

    @tool(
        "analyze_filters",
        "Analyze all channels and compute filter scores (low_uniqueness, low_subscriber_ratio, "
        "cross_channel_spam, non_cyrillic, chat_noise). Shows which channels should be filtered.",
        {},
    )
    async def analyze_filters(args):
        try:
            from src.filters.analyzer import ChannelAnalyzer

            analyzer = ChannelAnalyzer(db)
            report = await analyzer.analyze_all()
            if not report.results:
                return _text_response("Нет каналов для анализа фильтров.")
            flagged = [r for r in report.results if r.should_filter]
            lines = [
                f"Анализ фильтров: {len(report.results)} каналов проверено, "
                f"{len(flagged)} рекомендовано к фильтрации."
            ]
            for r in flagged:
                flags = ", ".join(r.flags) if r.flags else "—"
                lines.append(f"- {r.title} (id={r.channel_id}): {flags}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка анализа фильтров: {e}")

    tools.append(analyze_filters)

    @tool(
        "apply_filters",
        "⚠️ DANGEROUS: Run analyze_filters and mark flagged channels as filtered (skipped during collection). "
        "Always ask user for confirmation first.",
        {"confirm": bool},
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def apply_filters(args):
        gate = require_confirmation("применит фильтры и пометит каналы как отфильтрованные", args)
        if gate:
            return gate
        try:
            from src.filters.analyzer import ChannelAnalyzer

            analyzer = ChannelAnalyzer(db)
            report = await analyzer.analyze_all()
            count = await analyzer.apply_filters(report)
            return _text_response(f"Фильтры применены: {count} каналов помечены как отфильтрованные.")
        except Exception as e:
            return _text_response(f"Ошибка применения фильтров: {e}")

    tools.append(apply_filters)

    @tool(
        "reset_filters",
        "⚠️ DANGEROUS: Reset all channel filters — unmark all filtered channels. "
        "Always ask user for confirmation first.",
        {"confirm": bool},
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def reset_filters(args):
        gate = require_confirmation("сбросит все фильтры каналов", args)
        if gate:
            return gate
        try:
            from src.filters.analyzer import ChannelAnalyzer

            analyzer = ChannelAnalyzer(db)
            count = await analyzer.reset_filters()
            return _text_response(f"Фильтры сброшены: {count} каналов разблокированы.")
        except Exception as e:
            return _text_response(f"Ошибка сброса фильтров: {e}")

    tools.append(reset_filters)

    @tool(
        "toggle_channel_filter",
        "Toggle filter status for a specific channel. pk = DB primary key — get it from list_channels.",
        {"pk": int},
    )
    async def toggle_channel_filter(args):
        pk = args.get("pk")
        if pk is None:
            return _text_response("Ошибка: pk обязателен.")
        try:
            ch = await db.get_channel_by_pk(int(pk))
            if ch is None:
                return _text_response(f"Канал pk={pk} не найден.")
            new_filtered = not ch.is_filtered
            await db.set_channel_filtered(int(pk), new_filtered)
            status = "отфильтрован" if new_filtered else "разблокирован"
            return _text_response(f"Канал '{ch.title}' теперь {status}.")
        except Exception as e:
            return _text_response(f"Ошибка переключения фильтра: {e}")

    tools.append(toggle_channel_filter)

    @tool(
        "purge_filtered_channels",
        "⚠️ DANGEROUS: Soft-delete messages from filtered channels. "
        "pks = comma-separated DB primary keys from list_channels; "
        "omit to purge all filtered channels. Always ask user for confirmation first.",
        {"pks": str, "confirm": bool},
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def purge_filtered_channels(args):
        pks_str = args.get("pks", "")
        desc = "удалит сообщения из отфильтрованных каналов"
        if pks_str:
            desc = f"удалит сообщения из каналов pks=[{pks_str}]"
        gate = require_confirmation(desc, args)
        if gate:
            return gate
        try:
            from src.services.filter_deletion_service import FilterDeletionService

            svc = FilterDeletionService(db)
            if pks_str:
                pks = [int(x.strip()) for x in pks_str.split(",") if x.strip()]
                result = await svc.purge_channels_by_pks(pks)
            else:
                result = await svc.purge_all_filtered()
            return _text_response(
                f"Очистка завершена: {result.purged_count} каналов очищено, "
                f"{result.total_messages_deleted} сообщений удалено."
            )
        except Exception as e:
            return _text_response(f"Ошибка очистки каналов: {e}")

    tools.append(purge_filtered_channels)

    @tool(
        "hard_delete_channels",
        "⚠️ DANGEROUS: Permanently delete channels and ALL their data (irreversible). "
        "pks = comma-separated DB primary keys from list_channels. "
        "Always ask user for confirmation first.",
        {"pks": str, "confirm": bool},
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def hard_delete_channels(args):
        pks_str = args.get("pks", "")
        if not pks_str:
            return _text_response("Ошибка: pks обязателен (через запятую).")
        gate = require_confirmation(f"БЕЗВОЗВРАТНО удалит каналы pks=[{pks_str}] и все их данные", args)
        if gate:
            return gate
        try:
            from src.services.filter_deletion_service import FilterDeletionService

            svc = FilterDeletionService(db)
            pks = [int(x.strip()) for x in pks_str.split(",") if x.strip()]
            result = await svc.hard_delete_channels_by_pks(pks)
            return _text_response(
                f"Удаление завершено: {result.purged_count} каналов удалено безвозвратно."
            )
        except Exception as e:
            return _text_response(f"Ошибка удаления каналов: {e}")

    tools.append(hard_delete_channels)

    @tool(
        "precheck_filters",
        "⚠️ Pre-filter channels by subscriber ratio (no Telegram API needed). "
        "Marks channels as filtered. Ask user for confirmation first.",
        {"confirm": bool},
    )
    async def precheck_filters(args):
        gate = require_confirmation(
            "пометит каналы как filtered по subscriber ratio (bulk операция)", args
        )
        if gate:
            return gate
        try:
            from src.filters.analyzer import ChannelAnalyzer

            analyzer = ChannelAnalyzer(db)
            count = await analyzer.precheck_subscriber_ratio()
            return _text_response(
                f"Pre-filter применён: {count} каналов отмечены как filtered (low_subscriber_ratio)."
            )
        except Exception as e:
            return _text_response(f"Ошибка pre-filter: {e}")

    tools.append(precheck_filters)

    return tools
