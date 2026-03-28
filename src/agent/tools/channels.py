from __future__ import annotations

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._registry import _text_response, require_confirmation, require_pool


def register(db, client_pool, embedding_service, **kwargs):
    """Register channel-related agent tools."""
    tools = []

    # ------------------------------------------------------------------
    # list_channels (READ)
    # ------------------------------------------------------------------

    @tool(
        "list_channels",
        "List Telegram channels in the database. Optionally filter by active_only or include_filtered.",
        {"active_only": bool, "include_filtered": bool},
    )
    async def list_channels(args):
        active_only = bool(args.get("active_only", False))
        include_filtered = bool(args.get("include_filtered", True))
        try:
            channels = await db.get_channels(active_only=active_only, include_filtered=include_filtered)
            if not channels:
                return _text_response("Каналы не найдены.")
            lines = [f"Каналы ({len(channels)}):"]
            for ch in channels:
                status = "активен" if ch.is_active else "неактивен"
                filtered = " [отфильтрован]" if ch.is_filtered else ""
                ch_type = ch.channel_type or "unknown"
                lines.append(
                    f"- {ch.title} (@{ch.username}, channel_id={ch.channel_id}, "
                    f"{status}{filtered}, type={ch_type})"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения каналов: {e}")

    tools.append(list_channels)

    # ------------------------------------------------------------------
    # get_channel_stats (READ)
    # ------------------------------------------------------------------

    @tool(
        "get_channel_stats",
        "Get subscriber counts and statistics for all channels.",
        {},
    )
    async def get_channel_stats(args):
        try:
            stats = await db.repos.channels.get_latest_stats_for_all()
            if not stats:
                return _text_response("Статистика каналов пока не собрана.")
            lines = [f"Статистика каналов ({len(stats)}):"]
            for cid, s in stats.items():
                lines.append(
                    f"- channel_id={cid}: "
                    f"subscribers={s.subscriber_count or '?'}, "
                    f"avg_views={s.avg_views or '?'}"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения статистики каналов: {e}")

    tools.append(get_channel_stats)

    # ------------------------------------------------------------------
    # add_channel (WRITE + confirm)
    # ------------------------------------------------------------------

    @tool(
        "add_channel",
        "Add a Telegram channel by identifier (t.me link, @username, or numeric ID). Requires confirmation.",
        {"identifier": str, "confirm": bool},
    )
    async def add_channel(args):
        identifier = args.get("identifier")
        if not identifier:
            return _text_response("Ошибка: identifier обязателен.")
        gate = require_confirmation(f"добавит канал по идентификатору '{identifier}'", args)
        if gate:
            return gate
        try:
            from src.services.channel_service import ChannelService

            svc = ChannelService(db, client_pool, None)
            added = await svc.add_by_identifier(identifier)
            if added:
                return _text_response(f"Канал '{identifier}' успешно добавлен.")
            return _text_response(f"Канал '{identifier}' уже существует или не удалось добавить.")
        except Exception as e:
            return _text_response(f"Ошибка добавления канала: {e}")

    tools.append(add_channel)

    # ------------------------------------------------------------------
    # delete_channel (DELETE + confirm)
    # ------------------------------------------------------------------

    @tool(
        "delete_channel",
        "DANGEROUS: Permanently delete a channel and all its messages. Always ask user for confirmation first.",
        {"pk": int, "confirm": bool},
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def delete_channel(args):
        pk = args.get("pk")
        if pk is None:
            return _text_response("Ошибка: pk обязателен.")
        ch = await db.get_channel_by_pk(int(pk))
        name = ch.title if ch else f"id={pk}"
        gate = require_confirmation(f"удалит канал '{name}' и все его сообщения", args)
        if gate:
            return gate
        try:
            from src.services.channel_service import ChannelService

            svc = ChannelService(db, client_pool, None)
            await svc.delete(int(pk))
            return _text_response(f"Канал '{name}' удалён.")
        except Exception as e:
            return _text_response(f"Ошибка удаления канала: {e}")

    tools.append(delete_channel)

    # ------------------------------------------------------------------
    # toggle_channel (WRITE)
    # ------------------------------------------------------------------

    @tool(
        "toggle_channel",
        "Toggle channel active/inactive status by primary key.",
        {"pk": int},
    )
    async def toggle_channel(args):
        pk = args.get("pk")
        if pk is None:
            return _text_response("Ошибка: pk обязателен.")
        try:
            from src.services.channel_service import ChannelService

            svc = ChannelService(db, client_pool, None)
            await svc.toggle(int(pk))
            ch = await db.get_channel_by_pk(int(pk))
            if ch:
                status = "активен" if ch.is_active else "неактивен"
                return _text_response(f"Канал '{ch.title}' теперь {status}.")
            return _text_response(f"Статус канала pk={pk} переключён.")
        except Exception as e:
            return _text_response(f"Ошибка переключения канала: {e}")

    tools.append(toggle_channel)

    # ------------------------------------------------------------------
    # import_channels (WRITE + confirm)
    # ------------------------------------------------------------------

    @tool(
        "import_channels",
        "Bulk import channels from a text string (t.me links, @usernames, numeric IDs). Requires confirmation.",
        {"text": str, "confirm": bool},
    )
    async def import_channels(args):
        text = args.get("text")
        if not text:
            return _text_response("Ошибка: text обязателен.")
        from src.parsers import extract_identifiers

        identifiers = extract_identifiers(text)
        if not identifiers:
            return _text_response("Не удалось распознать идентификаторы каналов в тексте.")
        gate = require_confirmation(f"импортирует {len(identifiers)} канал(ов): {', '.join(identifiers[:5])}...", args)
        if gate:
            return gate
        try:
            from src.services.channel_service import ChannelService

            svc = ChannelService(db, client_pool, None)
            added = 0
            errors = []
            for ident in identifiers:
                try:
                    result = await svc.add_by_identifier(ident)
                    if result:
                        added += 1
                except Exception as e:
                    errors.append(f"{ident}: {e}")
            lines = [f"Импорт завершён. Добавлено: {added}/{len(identifiers)}."]
            if errors:
                lines.append(f"Ошибки ({len(errors)}):")
                for err in errors[:10]:
                    lines.append(f"  - {err}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка импорта каналов: {e}")

    tools.append(import_channels)

    # ------------------------------------------------------------------
    # refresh_channel_types (WRITE + confirm)
    # ------------------------------------------------------------------

    @tool(
        "refresh_channel_types",
        "Refresh channel_type for all active channels using Telegram API. Requires a connected Telegram client.",
        {"confirm": bool},
    )
    async def refresh_channel_types(args):
        pool_gate = require_pool(client_pool, "Обновление типов каналов")
        if pool_gate:
            return pool_gate
        gate = require_confirmation("обновит типы всех активных каналов через Telegram API", args)
        if gate:
            return gate
        try:
            channels = await db.get_channels(active_only=True)
            null_type = [ch for ch in channels if ch.channel_type is None]
            updated = 0
            failed = 0
            for ch in channels:
                identifier = ch.username or str(ch.channel_id)
                try:
                    info = await client_pool.resolve_channel(identifier)
                except Exception:
                    info = None
                if info is False:
                    await db.set_channel_active(ch.id, False)
                    await db.set_channel_type(ch.channel_id, "unavailable")
                    failed += 1
                    continue
                if not info or info.get("channel_type") is None:
                    failed += 1
                    continue
                await db.set_channel_type(ch.channel_id, info["channel_type"])
                updated += 1
            return _text_response(
                f"Обновление типов завершено.\n"
                f"Всего каналов: {len(channels)} (без типа: {len(null_type)}).\n"
                f"Обновлено: {updated}, не удалось: {failed}."
            )
        except Exception as e:
            return _text_response(f"Ошибка обновления типов каналов: {e}")

    tools.append(refresh_channel_types)

    # ------------------------------------------------------------------
    # refresh_channel_meta (WRITE + confirm) — about, linked_chat_id, has_comments
    # ------------------------------------------------------------------

    @tool(
        "refresh_channel_meta",
        "Refresh channel metadata (about, linked_chat_id, has_comments) from Telegram. "
        "Pass identifier for a single channel, or omit to refresh all active channels. Requires confirmation.",
        {"identifier": str, "confirm": bool},
    )
    async def refresh_channel_meta(args):
        pool_gate = require_pool(client_pool, "Обновление метаданных каналов")
        if pool_gate:
            return pool_gate
        identifier = args.get("identifier")
        if identifier:
            desc = f"обновит метаданные канала '{identifier}'"
        else:
            desc = "обновит метаданные всех активных каналов"
        gate = require_confirmation(desc, args)
        if gate:
            return gate
        try:
            if identifier:
                channels = await db.get_channels()
                ch = next(
                    (c for c in channels
                     if str(c.channel_id) == str(identifier)
                     or (c.username and c.username.lower() == identifier.lstrip("@").lower())),
                    None,
                )
                if not ch:
                    return _text_response(f"Канал '{identifier}' не найден.")
                meta = await client_pool.fetch_channel_meta(ch.channel_id, ch.channel_type)
                if not meta:
                    return _text_response(f"Не удалось получить метаданные для '{ch.title}'.")
                await db.update_channel_full_meta(
                    ch.channel_id,
                    about=meta["about"],
                    linked_chat_id=meta["linked_chat_id"],
                    has_comments=meta["has_comments"],
                )
                about = meta["about"] or ""
                about_preview = about[:60] + ("..." if len(about) > 60 else "")
                return _text_response(
                    f"Метаданные обновлены: {ch.title}\n"
                    f"  about: {about_preview}\n"
                    f"  linked_chat_id: {meta['linked_chat_id']}\n"
                    f"  has_comments: {meta['has_comments']}"
                )
            else:
                channels = await db.get_channels(active_only=True)
                ok = failed = 0
                for ch in channels:
                    meta = await client_pool.fetch_channel_meta(ch.channel_id, ch.channel_type)
                    if meta:
                        try:
                            await db.update_channel_full_meta(
                                ch.channel_id,
                                about=meta["about"],
                                linked_chat_id=meta["linked_chat_id"],
                                has_comments=meta["has_comments"],
                            )
                            ok += 1
                        except Exception:
                            failed += 1
                    else:
                        failed += 1
                return _text_response(
                    f"Обновление метаданных завершено.\n"
                    f"Всего каналов: {len(channels)}. Обновлено: {ok}, не удалось: {failed}."
                )
        except Exception as e:
            return _text_response(f"Ошибка обновления метаданных: {e}")

    tools.append(refresh_channel_meta)

    return tools
