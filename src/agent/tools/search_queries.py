"""Agent tools for managing saved search queries."""

from __future__ import annotations

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._registry import _text_response, require_confirmation


def register(db, client_pool, embedding_service):
    tools = []

    # ------------------------------------------------------------------
    # READ
    # ------------------------------------------------------------------

    @tool(
        "list_search_queries",
        "List saved search queries. Optionally show only active ones.",
        {"active_only": bool},
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
        "Get full details of a saved search query by its ID.",
        {"sq_id": int},
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
        "Add a new saved search query. Requires confirm=true.",
        {
            "query": str,
            "interval_minutes": int,
            "is_regex": bool,
            "is_fts": bool,
            "notify_on_collect": bool,
            "confirm": bool,
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
            sq_id = await svc.add(
                query,
                interval_minutes=interval,
                is_regex=is_regex,
                is_fts=is_fts,
                notify_on_collect=notify,
            )
            return _text_response(f"Поисковый запрос создан (id={sq_id}).")
        except Exception as e:
            return _text_response(f"Ошибка добавления поискового запроса: {e}")

    tools.append(add_search_query)

    @tool(
        "edit_search_query",
        "Edit an existing search query. Provide sq_id and fields to change. Requires confirm=true.",
        {
            "sq_id": int,
            "query": str,
            "interval_minutes": int,
            "is_regex": bool,
            "is_fts": bool,
            "notify_on_collect": bool,
            "confirm": bool,
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
            ok = await svc.update(
                int(sq_id),
                query,
                interval,
                is_regex=is_regex,
                is_fts=is_fts,
                notify_on_collect=notify,
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
        {"sq_id": int, "confirm": bool},
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
        {"sq_id": int},
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
        {"sq_id": int},
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

    return tools
