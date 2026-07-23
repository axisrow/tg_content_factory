"""Agent tools for channel filter management."""

from __future__ import annotations

from typing import Annotated

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._categories import ToolCategory, ToolMeta
from src.agent.tools._formatters import format_filter_report
from src.agent.tools._registry import (
    ToolInputError,
    _text_response,
    arg_bool,
    arg_int,
    require_confirmation,
)

# Permission metadata for this module's tools (#245). Single source of
# truth: permissions.py derives TOOL_CATEGORIES / MODULE_GROUPS /
# PHONE_BINDED_TOOLS from these declarations; invariants in
# tests/test_tool_permissions_autoderive.py keep them in sync with the
# @tool() definitions.
TOOL_GROUPS: list[tuple[str, dict[str, ToolMeta]]] = [
    ("Фильтры", {
        "analyze_filters": ToolMeta(ToolCategory.READ),
        "apply_filters": ToolMeta(ToolCategory.WRITE),
        "reset_filters": ToolMeta(ToolCategory.WRITE),
        "toggle_channel_filter": ToolMeta(ToolCategory.WRITE),
        "purge_filtered_channels": ToolMeta(ToolCategory.DELETE),
        "hard_delete_channels": ToolMeta(ToolCategory.DELETE),
        "precheck_filters": ToolMeta(ToolCategory.WRITE),
        "purge_channel_messages": ToolMeta(ToolCategory.DELETE),
    }),
]


def _build_deletion_service(db):
    """Build FilterDeletionService with a channel_service wired in.

    hard_delete_channels needs a ChannelService to delete channels; purge does
    not (it only touches messages via db). ChannelService.delete() works with a
    null pool/queue (pure DB op), matching the CLI's _build_deletion_service
    (src/cli/commands/filter.py). Without channel_service the tool always
    raised RuntimeError (#1290).
    """
    from typing import cast

    from src.database.bundles import ChannelBundle
    from src.services.channel_service import ChannelService
    from src.services.filter_deletion_service import FilterDeletionService
    from src.telegram.client_pool import ClientPool

    channel_service = ChannelService(ChannelBundle.from_database(db), cast("ClientPool", None), queue=None)
    return FilterDeletionService(db, channel_service)


def register(db, client_pool, embedding_service, **kwargs):
    tools = []

    @tool(
        "analyze_filters",
        "Analyze all channels and compute filter scores (low_uniqueness, low_subscriber_ratio, "
        "cross_channel_spam, non_cyrillic, chat_noise). Shows which channels should be filtered. "
        "Set quick=true for a fast sampled run (last N messages/channel, no cross-dupe) on large DBs.",
        {
            "quick": Annotated[
                bool,
                "Быстрый режим: семпл последних N сообщений на канал + без cross-dupe (секунды вместо минут)",
            ],
            "sample_size": Annotated[
                int,
                "Сколько последних сообщений семплировать в quick-режиме (по умолчанию 300; игнорируется без quick)",
            ],
        },
    )
    async def analyze_filters(args):
        try:
            from src.filters.analyzer import ChannelAnalyzer

            quick = arg_bool(args, "quick")
            raw_sample = args.get("sample_size")
            sample_size = int(raw_sample) if raw_sample not in (None, "") else None
            analyzer = ChannelAnalyzer(db)
            report = await analyzer.analyze_all(quick=quick, sample_size=sample_size)
            return _text_response(format_filter_report(report))
        except Exception as e:
            return _text_response(f"Ошибка анализа фильтров: {e}")

    tools.append(analyze_filters)

    @tool(
        "apply_filters",
        "⚠️ DANGEROUS: Run analyze_filters and mark flagged channels as filtered (skipped during collection). "
        "Always ask user for confirmation first.",
        {"confirm": Annotated[bool, "Установите true для подтверждения действия"]},
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
        {"confirm": Annotated[bool, "Установите true для подтверждения действия"]},
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
        {"pk": Annotated[int, "ID записи в БД (первичный ключ из list_channels)"]},
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
        {
            "pks": Annotated[str, "ID записей через запятую (первичные ключи из list_channels)"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
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
            msg = (
                f"Очистка завершена: {result.purged_count} каналов очищено, "
                f"{result.total_messages_deleted} сообщений удалено."
            )
            # Surface real per-channel failures instead of reporting full success (#676 review).
            if result.errors:
                msg += f"\n⚠️ Ошибки ({len(result.errors)}): " + "; ".join(result.errors)
            return _text_response(msg)
        except Exception as e:
            return _text_response(f"Ошибка очистки каналов: {e}")

    tools.append(purge_filtered_channels)

    @tool(
        "hard_delete_channels",
        "⚠️ DANGEROUS: Permanently delete channels and ALL their data (irreversible). "
        "pks = comma-separated DB primary keys from list_channels. "
        "Always ask user for confirmation first.",
        {
            "pks": Annotated[str, "ID записей через запятую (первичные ключи из list_channels)"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def hard_delete_channels(args):
        pks_str = args.get("pks", "")
        if not pks_str:
            return _text_response("Ошибка: pks обязателен (через запятую).")
        gate = require_confirmation(f"БЕЗВОЗВРАТНО удалит каналы pks=[{pks_str}] и все их данные", args)
        if gate:
            return gate
        # Dev-mode gate — hard-delete is irreversible, match CLI/web which both
        # require agent_dev_mode_enabled. Without it the tool would permanently
        # erase channels in production (#1290 acceptance criteria).
        dev_mode = (await db.get_setting("agent_dev_mode_enabled") or "0") == "1"
        if not dev_mode:
            return _text_response(
                "Hard-delete требует режим разработчика. "
                "Включите его в Настройки → Режим разработчика."
            )
        try:
            svc = _build_deletion_service(db)
            pks = [int(x.strip()) for x in pks_str.split(",") if x.strip()]
            result = await svc.hard_delete_channels_by_pks(pks)
            msg = f"Удаление завершено: {result.purged_count} каналов удалено безвозвратно."
            if result.errors:
                msg += f"\n⚠️ Ошибки ({len(result.errors)}): " + "; ".join(result.errors)
            return _text_response(msg)
        except Exception as e:
            return _text_response(f"Ошибка удаления каналов: {e}")

    tools.append(hard_delete_channels)

    @tool(
        "precheck_filters",
        "⚠️ Pre-filter channels by subscriber ratio (no Telegram API needed). "
        "Marks channels as filtered. Ask user for confirmation first.",
        {"confirm": Annotated[bool, "Установите true для подтверждения действия"]},
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

    @tool(
        "purge_channel_messages",
        "⚠️ DANGEROUS: Delete all collected messages of ONE channel. "
        "channel_id = Telegram numeric ID (from list_channels), NOT the DB pk. "
        "Requires confirm=true.",
        {
            "channel_id": Annotated[int, "Числовой Telegram ID канала (НЕ pk)"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def purge_channel_messages(args):
        try:
            channel_id = arg_int(args, "channel_id", required=True)
        except ToolInputError as exc:
            return exc.to_response()
        gate = require_confirmation(f"удалит все сообщения канала channel_id={channel_id}", args)
        if gate:
            return gate
        try:
            deleted = await db.delete_messages_for_channel(channel_id)
            return _text_response(f"Удалено {deleted} сообщений канала channel_id={channel_id}.")
        except Exception as e:
            return _text_response(f"Ошибка очистки сообщений канала: {e}")

    tools.append(purge_channel_messages)

    return tools
