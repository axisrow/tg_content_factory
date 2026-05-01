from __future__ import annotations

from typing import Annotated

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._formatters import format_channel_identity, format_channel_stats
from src.agent.tools._registry import (
    ToolInputError,
    _text_response,
    arg_bool,
    arg_int,
    arg_str,
    get_tool_context,
    require_confirmation,
)


def register(db, client_pool, embedding_service, **kwargs):
    """Register channel-related agent tools."""
    ctx = get_tool_context(kwargs, db=db, client_pool=client_pool, embedding_service=embedding_service)
    tools = []

    # ------------------------------------------------------------------
    # list_channels (READ)
    # ------------------------------------------------------------------

    @tool(
        "list_channels",
        "List Telegram channels saved in the local database. Each row includes pk "
        "(DB primary key used by collect_channel/delete_channel/toggle_channel), "
        "channel_id (Telegram numeric ID), title, username, channel_type, and is_filtered status.",
        {
            "active_only": Annotated[bool, "Показывать только активные записи"],
            "include_filtered": Annotated[bool, "Включить отфильтрованные каналы в результат"],
        },
    )
    async def list_channels(args):
        active_only = arg_bool(args, "active_only", False)
        include_filtered = arg_bool(args, "include_filtered", True)
        try:
            channels = await db.get_channels(active_only=active_only, include_filtered=include_filtered)
            if not channels:
                return _text_response("Каналы не найдены.")
            lines = [f"Каналы ({len(channels)}):"]
            for ch in channels:
                status = "активен" if ch.is_active else "неактивен"
                filtered = " [отфильтрован]" if getattr(ch, "is_filtered", False) else ""
                ch_type = getattr(ch, "channel_type", None) or "unknown"
                lines.append(f"- {format_channel_identity(ch)}: {status}{filtered}, type={ch_type}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения каналов: {e}")

    tools.append(list_channels)

    # ------------------------------------------------------------------
    # get_channel_stats (READ)
    # ------------------------------------------------------------------

    @tool(
        "get_channel_stats",
        "Get latest subscriber counts and avg_views for all channels. "
        "Run collect_channel_stats first if stats are stale.",
        {},
    )
    async def get_channel_stats(args):
        try:
            stats = await db.get_latest_stats_for_all()
            channels = await db.get_channels(active_only=False, include_filtered=True)
            return _text_response(format_channel_stats(stats, channels))
        except Exception as e:
            return _text_response(f"Ошибка получения статистики каналов: {e}")

    tools.append(get_channel_stats)

    # ------------------------------------------------------------------
    # add_channel (WRITE + confirm)
    # ------------------------------------------------------------------

    @tool(
        "add_channel",
        "Add a Telegram channel to the local database by identifier (t.me link, @username, or numeric ID). "
        "After adding, use list_channels to get the pk, then call collect_channel to start message collection. "
        "Requires confirmation.",
        {
            "identifier": Annotated[str, "Идентификатор канала (t.me ссылка, @username или числовой ID)"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
    )
    async def add_channel(args):
        try:
            identifier = arg_str(args, "identifier", required=True)
        except ToolInputError as exc:
            return exc.to_response()
        gate = require_confirmation(f"добавит канал по идентификатору '{identifier}'", args)
        if gate:
            return gate
        try:
            svc = ctx.channel_service()
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
        "DANGEROUS: Permanently delete a channel and all its messages from the DB. "
        "pk = DB primary key — get it from list_channels. Always ask user for confirmation first.",
        {
            "pk": Annotated[int, "ID записи в БД (первичный ключ из list_channels)"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def delete_channel(args):
        try:
            pk = arg_int(args, "pk", required=True)
        except ToolInputError as exc:
            return exc.to_response()
        ch = await db.get_channel_by_pk(pk)
        name = ch.title if ch else f"id={pk}"
        gate = require_confirmation(f"удалит канал '{name}' и все его сообщения", args)
        if gate:
            return gate
        try:
            svc = ctx.channel_service()
            await svc.delete(pk)
            return _text_response(f"Канал '{name}' удалён.")
        except Exception as e:
            return _text_response(f"Ошибка удаления канала: {e}")

    tools.append(delete_channel)

    # ------------------------------------------------------------------
    # toggle_channel (WRITE)
    # ------------------------------------------------------------------

    @tool(
        "toggle_channel",
        "Toggle channel active/inactive status. pk = DB primary key — get it from list_channels.",
        {"pk": Annotated[int, "ID записи в БД (первичный ключ из list_channels)"]},
    )
    async def toggle_channel(args):
        try:
            pk = arg_int(args, "pk", required=True)
        except ToolInputError as exc:
            return exc.to_response()
        try:
            svc = ctx.channel_service()
            await svc.toggle(pk)
            ch = await db.get_channel_by_pk(pk)
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
        "Bulk-import channels from a text string (t.me links, @usernames, or numeric IDs, any separator). "
        "After import, use collect_all_channels to start collection. Requires confirmation.",
        {
            "text": Annotated[str, "Идентификаторы каналов (t.me ссылки, @username, ID через любой разделитель)"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
    )
    async def import_channels(args):
        try:
            text = arg_str(args, "text", required=True)
        except ToolInputError as exc:
            return exc.to_response()
        from src.parsers import extract_identifiers

        identifiers = extract_identifiers(text)
        if not identifiers:
            return _text_response("Не удалось распознать идентификаторы каналов в тексте.")
        gate = require_confirmation(f"импортирует {len(identifiers)} канал(ов): {', '.join(identifiers[:5])}...", args)
        if gate:
            return gate
        try:
            svc = ctx.channel_service()
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
        "Refresh channel_type (channel/group/supergroup/unavailable) for all active channels "
        "via Telegram API. Requires a connected Telegram client. Requires confirmation.",
        {"confirm": Annotated[bool, "Установите true для подтверждения действия"]},
    )
    async def refresh_channel_types(args):
        pool_gate = ctx.require_pool("Обновление типов каналов")
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
        "Pass identifier (channel_id as string, or @username) for one channel, or omit to refresh all. "
        "Requires confirmation.",
        {
            "identifier": Annotated[str, "Идентификатор канала (t.me ссылка, @username или числовой ID)"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
    )
    async def refresh_channel_meta(args):
        pool_gate = ctx.require_pool("Обновление метаданных каналов")
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

    # ------------------------------------------------------------------
    # Tags
    # ------------------------------------------------------------------

    @tool(
        "list_tags",
        "List all channel tags defined in the system. "
        "Use set_channel_tags to assign tags to a channel.",
        {},
    )
    async def list_tags(args):
        try:
            tags = await db.repos.channels.list_all_tags()
            if not tags:
                return _text_response("Теги не найдены.")
            return _text_response(f"Теги ({len(tags)}): {', '.join(tags)}")
        except Exception as e:
            return _text_response(f"Ошибка получения тегов: {e}")

    tools.append(list_tags)

    @tool(
        "create_tag",
        "Create a new channel tag. Requires confirm=true.",
        {
            "name": Annotated[str, "Название тега"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
    )
    async def create_tag(args):
        try:
            name = arg_str(args, "name", required=True)
        except ToolInputError as exc:
            return exc.to_response()
        gate = require_confirmation(f"создаст тег '{name}'", args)
        if gate:
            return gate
        try:
            await db.repos.channels.create_tag(name)
            return _text_response(f"Тег '{name}' создан.")
        except Exception as e:
            return _text_response(f"Ошибка создания тега: {e}")

    tools.append(create_tag)

    @tool(
        "delete_tag",
        "⚠️ DANGEROUS: Delete a tag and remove it from all channels. Requires confirm=true.",
        {
            "name": Annotated[str, "Название тега"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def delete_tag(args):
        try:
            name = arg_str(args, "name", required=True)
        except ToolInputError as exc:
            return exc.to_response()
        gate = require_confirmation(f"удалит тег '{name}' у всех каналов", args)
        if gate:
            return gate
        try:
            await db.repos.channels.delete_tag(name)
            return _text_response(f"Тег '{name}' удалён.")
        except Exception as e:
            return _text_response(f"Ошибка удаления тега: {e}")

    tools.append(delete_tag)

    @tool(
        "set_channel_tags",
        "Assign tags to a channel (replaces all existing tags). "
        "pk = DB primary key from list_channels. tags = comma-separated tag names (empty to clear all).",
        {
            "pk": Annotated[int, "ID записи в БД (первичный ключ из list_channels)"],
            "tags": Annotated[str, "Теги через запятую (пустая строка — очистить)"],
        },
    )
    async def set_channel_tags(args):
        try:
            pk = arg_int(args, "pk", required=True)
        except ToolInputError as exc:
            return exc.to_response()
        raw = args.get("tags", "")
        tag_names = [t.strip() for t in str(raw).split(",") if t.strip()]
        try:
            ch = await db.get_channel_by_pk(pk)
            if ch is None:
                return _text_response(f"Канал pk={pk} не найден.")
            await db.repos.channels.set_channel_tags(pk, tag_names)
            if tag_names:
                return _text_response(f"Теги канала '{ch.title}' обновлены: {', '.join(tag_names)}")
            return _text_response(f"Теги канала '{ch.title}' очищены.")
        except Exception as e:
            return _text_response(f"Ошибка установки тегов: {e}")

    tools.append(set_channel_tags)

    return tools
