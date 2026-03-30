"""Agent tools for managing saved search queries."""

from __future__ import annotations

from typing import Annotated

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._registry import _text_response, require_confirmation


def register(db, client_pool, embedding_service, **kwargs):
    tools = []

    # ------------------------------------------------------------------
    # READ
    # ------------------------------------------------------------------

    @tool(
        "list_search_queries",
        "List saved search queries. Optionally show only active ones.",
        {"active_only": Annotated[bool, "Показывать только активные запросы"]},
    )
    async def list_search_queries(args):
        try:
            from src.services.search_query_service import SearchQueryService

            svc = SearchQueryService(db)
            active_only = bool(args.get("active_only", False))
            queries = await svc.list(active_only=active_only)
            if not queries:
                return _text_response("Поисковые запросы не найдены.")
            lines = [f"Поисковые запросы ({len(queries)}):"]
            for sq in queries:
                status = "активен" if sq.is_active else "неактивен"
                flags = []
                if sq.is_regex:
                    flags.append("regex")
                if sq.is_fts:
                    flags.append("fts")
                if sq.notify_on_collect:
                    flags.append("notify")
                flags_str = f" [{', '.join(flags)}]" if flags else ""
                lines.append(
                    f"- id={sq.id}: '{sq.query}' interval={sq.interval_minutes}m "
                    f"{status}{flags_str}"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения поисковых запросов: {e}")

    tools.append(list_search_queries)

    @tool(
        "get_search_query",
        "Get full details of a saved search query. sq_id from list_search_queries.",
        {"sq_id": Annotated[int, "ID поискового запроса из list_search_queries"]},
    )
    async def get_search_query(args):
        sq_id = args.get("sq_id")
        if sq_id is None:
            return _text_response("Ошибка: sq_id обязателен.")
        try:
            from src.services.search_query_service import SearchQueryService

            svc = SearchQueryService(db)
            sq = await svc.get(int(sq_id))
            if sq is None:
                return _text_response(f"Поисковый запрос id={sq_id} не найден.")
            lines = [
                f"id: {sq.id}",
                f"query: {sq.query}",
                f"interval_minutes: {sq.interval_minutes}",
                f"is_active: {sq.is_active}",
                f"is_regex: {sq.is_regex}",
                f"is_fts: {sq.is_fts}",
                f"notify_on_collect: {sq.notify_on_collect}",
            ]
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения поискового запроса: {e}")

    tools.append(get_search_query)

    # ------------------------------------------------------------------
    # WRITE + confirm
    # ------------------------------------------------------------------

    @tool(
        "add_search_query",
        "Add a saved search query. is_fts=true for FTS5 full-text search (default: substring). "
        "notify_on_collect=true to send notification on new matches. "
        "track_stats=true to record daily match counts for get_search_query_stats. Requires confirm=true.",
        {
            "query": Annotated[str, "Текст поискового запроса"],
            "interval_minutes": Annotated[int, "Интервал выполнения запроса в минутах"],
            "is_regex": Annotated[bool, "Использовать регулярное выражение"],
            "is_fts": Annotated[bool, "Использовать полнотекстовый поиск FTS5"],
            "notify_on_collect": Annotated[bool, "Отправлять уведомление при новых совпадениях"],
            "track_stats": Annotated[bool, "Записывать ежедневную статистику совпадений"],
            "exclude_patterns": Annotated[str, "Паттерны исключения через запятую"],
            "max_length": Annotated[int, "Максимальная длина сообщения для совпадения"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
    )
    async def add_search_query(args):
        query = args.get("query")
        if not query:
            return _text_response("Ошибка: query обязателен.")
        gate = require_confirmation(f"добавит поисковый запрос '{query}'", args)
        if gate:
            return gate
        try:
            from src.services.search_query_service import SearchQueryService

            svc = SearchQueryService(db)
            interval = int(args.get("interval_minutes", 60))
            is_regex = bool(args.get("is_regex", False))
            is_fts = bool(args.get("is_fts", False))
            notify = bool(args.get("notify_on_collect", False))
            track_stats = bool(args.get("track_stats", True))
            exclude_patterns = args.get("exclude_patterns", "")
            max_length = args.get("max_length")
            sq_id = await svc.add(
                query,
                interval_minutes=interval,
                is_regex=is_regex,
                is_fts=is_fts,
                notify_on_collect=notify,
                track_stats=track_stats,
                exclude_patterns=exclude_patterns or "",
                max_length=int(max_length) if max_length is not None else None,
            )
            return _text_response(f"Поисковый запрос создан (id={sq_id}).")
        except Exception as e:
            return _text_response(f"Ошибка добавления поискового запроса: {e}")

    tools.append(add_search_query)

    @tool(
        "edit_search_query",
        "Edit an existing search query. Provide sq_id and fields to change. Requires confirm=true.",
        {
            "sq_id": Annotated[int, "ID поискового запроса из list_search_queries"],
            "query": Annotated[str, "Текст поискового запроса"],
            "interval_minutes": Annotated[int, "Интервал выполнения запроса в минутах"],
            "is_regex": Annotated[bool, "Использовать регулярное выражение"],
            "is_fts": Annotated[bool, "Использовать полнотекстовый поиск FTS5"],
            "notify_on_collect": Annotated[bool, "Отправлять уведомление при новых совпадениях"],
            "track_stats": Annotated[bool, "Записывать ежедневную статистику совпадений"],
            "exclude_patterns": Annotated[str, "Паттерны исключения через запятую"],
            "max_length": Annotated[int, "Максимальная длина сообщения для совпадения"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
    )
    async def edit_search_query(args):
        sq_id = args.get("sq_id")
        if sq_id is None:
            return _text_response("Ошибка: sq_id обязателен.")
        gate = require_confirmation(f"изменит поисковый запрос id={sq_id}", args)
        if gate:
            return gate
        try:
            from src.services.search_query_service import SearchQueryService

            svc = SearchQueryService(db)
            existing = await svc.get(int(sq_id))
            if existing is None:
                return _text_response(f"Поисковый запрос id={sq_id} не найден.")
            query = args.get("query", existing.query)
            interval = int(args.get("interval_minutes", existing.interval_minutes))
            is_regex = bool(args.get("is_regex", existing.is_regex))
            is_fts = bool(args.get("is_fts", existing.is_fts))
            notify = bool(args.get("notify_on_collect", existing.notify_on_collect))
            track_stats = bool(args.get("track_stats", getattr(existing, "track_stats", True)))
            exclude_patterns = args.get("exclude_patterns", getattr(existing, "exclude_patterns", ""))
            max_length_raw = args.get("max_length", getattr(existing, "max_length", None))
            ok = await svc.update(
                int(sq_id),
                query,
                interval,
                is_regex=is_regex,
                is_fts=is_fts,
                notify_on_collect=notify,
                track_stats=track_stats,
                exclude_patterns=exclude_patterns or "",
                max_length=int(max_length_raw) if max_length_raw is not None else None,
            )
            if ok:
                return _text_response(f"Поисковый запрос id={sq_id} обновлён.")
            return _text_response(f"Не удалось обновить запрос id={sq_id}.")
        except Exception as e:
            return _text_response(f"Ошибка редактирования поискового запроса: {e}")

    tools.append(edit_search_query)

    @tool(
        "delete_search_query",
        "⚠️ DANGEROUS: Delete a saved search query. Requires confirm=true.",
        {
            "sq_id": Annotated[int, "ID поискового запроса из list_search_queries"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def delete_search_query(args):
        sq_id = args.get("sq_id")
        if sq_id is None:
            return _text_response("Ошибка: sq_id обязателен.")
        gate = require_confirmation(f"удалит поисковый запрос id={sq_id}", args)
        if gate:
            return gate
        try:
            from src.services.search_query_service import SearchQueryService

            svc = SearchQueryService(db)
            await svc.delete(int(sq_id))
            return _text_response(f"Поисковый запрос id={sq_id} удалён.")
        except Exception as e:
            return _text_response(f"Ошибка удаления поискового запроса: {e}")

    tools.append(delete_search_query)

    # ------------------------------------------------------------------
    # WRITE (no confirm needed)
    # ------------------------------------------------------------------

    @tool(
        "toggle_search_query",
        "Toggle a search query active/inactive.",
        {"sq_id": Annotated[int, "ID поискового запроса из list_search_queries"]},
    )
    async def toggle_search_query(args):
        sq_id = args.get("sq_id")
        if sq_id is None:
            return _text_response("Ошибка: sq_id обязателен.")
        try:
            from src.services.search_query_service import SearchQueryService

            svc = SearchQueryService(db)
            existing = await svc.get(int(sq_id))
            if existing is None:
                return _text_response(f"Поисковый запрос id={sq_id} не найден.")
            await svc.toggle(int(sq_id))
            new_status = "деактивирован" if existing.is_active else "активирован"
            return _text_response(f"Поисковый запрос id={sq_id} {new_status}.")
        except Exception as e:
            return _text_response(f"Ошибка переключения поискового запроса: {e}")

    tools.append(toggle_search_query)

    @tool(
        "run_search_query",
        "Run a search query once and return the number of matches found today.",
        {"sq_id": Annotated[int, "ID поискового запроса из list_search_queries"]},
    )
    async def run_search_query(args):
        sq_id = args.get("sq_id")
        if sq_id is None:
            return _text_response("Ошибка: sq_id обязателен.")
        try:
            from src.services.search_query_service import SearchQueryService

            svc = SearchQueryService(db)
            count = await svc.run_once(int(sq_id))
            return _text_response(f"Запрос id={sq_id} выполнен: найдено {count} совпадений.")
        except Exception as e:
            return _text_response(f"Ошибка выполнения поискового запроса: {e}")

    tools.append(run_search_query)

    # ------------------------------------------------------------------
    # get_search_query_stats (READ)
    # ------------------------------------------------------------------

    @tool(
        "get_search_query_stats",
        "Get daily match statistics for a search query over the last N days. "
        "Only works if track_stats=true was set when creating the query. sq_id from list_search_queries.",
        {
            "sq_id": Annotated[int, "ID поискового запроса из list_search_queries"],
            "days": Annotated[int, "Количество дней для анализа"],
        },
    )
    async def get_search_query_stats(args):
        sq_id = args.get("sq_id")
        if sq_id is None:
            return _text_response("Ошибка: sq_id обязателен.")
        days = max(1, int(args.get("days", 30)))
        try:
            from src.services.search_query_service import SearchQueryService

            svc = SearchQueryService(db)
            stats = await svc.get_daily_stats(int(sq_id), days)
            if not stats:
                return _text_response(f"Нет статистики для запроса id={sq_id} за {days} дней.")
            max_count = max(s.count for s in stats)
            lines = [f"Статистика запроса id={sq_id} за {days} дней:"]
            for s in stats:
                bar_len = int(s.count / max_count * 30) if max_count else 0
                bar = "█" * bar_len
                lines.append(f"  {s.day}  {bar} {s.count}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения статистики: {e}")

    tools.append(get_search_query_stats)

    return tools
