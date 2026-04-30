"""Sync tool wrappers for Deepagents backend.

Deepagents/LangChain agents run tools synchronously in a separate thread.
These wrappers bridge async DB/service calls via asyncio.run().
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TypeVar

from src.agent.runtime_context import AgentRuntimeContext

_T = TypeVar("_T")
logger = logging.getLogger(__name__)


def _run_sync(tool_name: str, operation: Callable[[], Awaitable[_T]]) -> _T:
    """Run an async operation synchronously (must be called outside event loop)."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(operation())
    raise RuntimeError(f"Deepagents tool '{tool_name}' cannot run inside an active event loop")


def build_deepagents_tools(
    db,
    client_pool=None,
    config=None,
    runtime_context: AgentRuntimeContext | None = None,
) -> list[Callable]:  # noqa: C901
    """Build all sync tools for the deepagents backend.

    Returns a list of callables compatible with LangChain tool registration.
    """
    tools: list[Callable] = []
    runtime_context = runtime_context or AgentRuntimeContext.build(
        db=db,
        config=config,
        client_pool=client_pool,
    )

    # === Search ===

    def search_messages(query_text: str, limit: int = 20) -> str:
        """Full-text search in collected messages stored in the local DB."""
        try:
            messages, total = _run_sync(
                "search_messages", lambda: db.search_messages(query_text, limit=limit)
            )
        except Exception as exc:
            logger.warning("Deepagents search_messages failed: %s", exc)
            return f"Ошибка поиска: {exc}"
        if not messages:
            return f"Ничего не найдено по запросу: {query_text}"
        lines = [f"Найдено {total} сообщений по запросу '{query_text}':"]
        for m in messages:
            preview = (m.text or "").replace("\n", " ")[:200]
            lines.append(f"- [{m.date}] channel_id={m.channel_id}: {preview}")
        return "\n".join(lines)

    tools.append(search_messages)

    def semantic_search(query_text: str, limit: int = 10) -> str:
        """Search collected messages by semantic (embedding) similarity.

        Requires index_messages first and an embedding API key.
        """
        try:
            from src.services.embedding_service import EmbeddingService

            svc = EmbeddingService(db, config=config)
            embedding = _run_sync("semantic_embed", lambda: svc.embed_query(query_text))
            messages, total = _run_sync(
                "semantic_search", lambda: db.search_semantic_messages(embedding, limit=limit)
            )
        except Exception as exc:
            return f"Ошибка семантического поиска: {exc}"
        if not messages:
            return f"Семантически похожие сообщения не найдены: {query_text}"
        lines = [f"Семантически найдено {total} сообщений:"]
        for m in messages:
            preview = (m.text or "").replace("\n", " ")[:200]
            lines.append(f"- [{m.date}] channel_id={m.channel_id}: {preview}")
        return "\n".join(lines)

    tools.append(semantic_search)

    def index_messages() -> str:
        """Create semantic embeddings for all not-yet-indexed messages. Required before semantic_search works."""
        try:
            from src.services.embedding_service import EmbeddingService

            svc = EmbeddingService(db, config=config)
            count = _run_sync("index_messages", svc.index_pending_messages)
            return f"Проиндексировано: {count} сообщений."
        except Exception as exc:
            return f"Ошибка индексации: {exc}"

    tools.append(index_messages)

    # === Channels ===

    def list_channels(active_only: bool = False) -> str:
        """List channels in DB. Each row has pk (for collect/delete/toggle), channel_id (Telegram ID), title."""
        try:
            channels = _run_sync("list_channels", lambda: db.get_channels(active_only=active_only))
        except Exception as exc:
            return f"Ошибка получения каналов: {exc}"
        if not channels:
            return "Каналы не найдены."
        lines = [f"Каналы ({len(channels)}):"]
        for ch in channels:
            status = "активен" if ch.is_active else "неактивен"
            lines.append(f"- {ch.title} (id={ch.channel_id}, {status})")
        return "\n".join(lines)

    tools.append(list_channels)

    def get_channel_stats() -> str:
        """Get subscriber counts and statistics for all channels."""
        try:
            stats = _run_sync("get_channel_stats", db.repos.channels.get_latest_stats_for_all)
        except Exception as exc:
            return f"Ошибка статистики каналов: {exc}"
        if not stats:
            return "Статистика каналов не собрана."
        lines = [f"Статистика ({len(stats)} каналов):"]
        for cid, s in stats.items():
            lines.append(f"- channel_id={cid}: subscribers={s.subscriber_count or '?'}")
        return "\n".join(lines)

    tools.append(get_channel_stats)

    def add_channel(identifier: str) -> str:
        """Add a channel to DB by identifier (t.me link, @username, or numeric ID).

        Then use list_channels to get pk and collect_channel to collect messages.
        """
        try:
            from src.services.channel_service import ChannelService

            svc = ChannelService(db, client_pool, None)
            result = _run_sync("add_channel", lambda: svc.add_by_identifier(identifier))
            return f"Канал добавлен: {result}" if result else f"Не удалось добавить канал: {identifier}"
        except Exception as exc:
            return f"Ошибка добавления канала: {exc}"

    tools.append(add_channel)

    def delete_channel(pk: int) -> str:
        """⚠️ DANGEROUS: Delete a channel permanently. pk = DB primary key from list_channels."""
        try:
            from src.services.channel_service import ChannelService

            svc = ChannelService(db, client_pool, None)
            _run_sync("delete_channel", lambda: svc.delete(pk))
            return f"Канал pk={pk} удалён."
        except Exception as exc:
            return f"Ошибка удаления канала: {exc}"

    tools.append(delete_channel)

    def toggle_channel(pk: int) -> str:
        """Toggle channel active/inactive status. pk = DB primary key from list_channels."""
        try:
            from src.services.channel_service import ChannelService

            svc = ChannelService(db, client_pool, None)
            _run_sync("toggle_channel", lambda: svc.toggle(pk))
            return f"Канал pk={pk} переключён."
        except Exception as exc:
            return f"Ошибка переключения канала: {exc}"

    tools.append(toggle_channel)

    # === Pipelines ===

    def list_pipelines(active_only: bool = False) -> str:
        """List all content pipelines with settings."""
        try:
            from src.services.pipeline_service import PipelineService

            svc = PipelineService(db)
            pipelines = _run_sync("list_pipelines", lambda: svc.list(active_only=active_only))
        except Exception as exc:
            return f"Ошибка получения пайплайнов: {exc}"
        if not pipelines:
            return "Пайплайны не найдены."
        lines = [f"Пайплайны ({len(pipelines)}):"]
        for p in pipelines:
            status = "активен" if p.is_active else "неактивен"
            lines.append(f"- id={p.id}: {p.name} [{status}] model={p.llm_model or 'default'}")
        return "\n".join(lines)

    tools.append(list_pipelines)

    def get_pipeline_detail(pipeline_id: int) -> str:
        """Get detailed info about a specific pipeline."""
        try:
            from src.services.pipeline_service import PipelineService

            svc = PipelineService(db)
            detail = _run_sync("get_pipeline_detail", lambda: svc.get_detail(pipeline_id))
        except Exception as exc:
            return f"Ошибка получения деталей пайплайна: {exc}"
        if not detail:
            return f"Пайплайн id={pipeline_id} не найден."
        p = detail.get("pipeline")
        return f"Пайплайн: {p.name} (id={p.id}), model={p.llm_model}, active={p.is_active}"

    tools.append(get_pipeline_detail)

    def toggle_pipeline(pipeline_id: int) -> str:
        """Toggle pipeline active/inactive."""
        try:
            from src.services.pipeline_service import PipelineService

            svc = PipelineService(db)
            _run_sync("toggle_pipeline", lambda: svc.toggle(pipeline_id))
            return f"Пайплайн id={pipeline_id} переключён."
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(toggle_pipeline)

    def delete_pipeline(pipeline_id: int) -> str:
        """⚠️ DANGEROUS: Delete a pipeline permanently."""
        try:
            from src.services.pipeline_service import PipelineService

            svc = PipelineService(db)
            _run_sync("delete_pipeline", lambda: svc.delete(pipeline_id))
            return f"Пайплайн id={pipeline_id} удалён."
        except Exception as exc:
            return f"Ошибка удаления пайплайна: {exc}"

    tools.append(delete_pipeline)

    def run_pipeline(pipeline_id: int) -> str:
        """Trigger content generation for a pipeline."""
        try:
            from src.search.engine import SearchEngine
            from src.services.content_generation_service import ContentGenerationService
            from src.services.pipeline_service import PipelineService
            from src.services.provider_service import build_provider_service

            svc = PipelineService(db)
            pipeline = _run_sync("run_pipeline_get", lambda: svc.get(pipeline_id))
            if not pipeline:
                return f"Пайплайн id={pipeline_id} не найден."
            engine = SearchEngine(db, config=config)
            image_service = _build_image_service_sync()
            provider_service = _run_sync("run_pipeline_provider_service", lambda: build_provider_service(db, config))
            gen_svc = ContentGenerationService(
                db,
                engine,
                config=config,
                image_service=image_service,
                client_pool=client_pool,
                provider_service=provider_service,
            )
            run = _run_sync("run_pipeline", lambda: gen_svc.generate(pipeline))
            preview = (run.generated_text or "")[:300]
            return f"Генерация завершена (run id={run.id}). Превью:\n{preview}"
        except Exception as exc:
            return f"Ошибка запуска пайплайна: {exc}"

    tools.append(run_pipeline)

    def list_pipeline_runs(pipeline_id: int, limit: int = 20) -> str:
        """List generation runs for a pipeline."""
        try:
            runs = _run_sync(
                "list_pipeline_runs",
                lambda: db.repos.generation_runs.list_by_pipeline(pipeline_id, limit=limit),
            )
        except Exception as exc:
            return f"Ошибка: {exc}"
        if not runs:
            return f"Нет runs для пайплайна id={pipeline_id}."
        lines = [f"Runs ({len(runs)}):"]
        for r in runs:
            lines.append(f"- id={r.id}, status={r.status}, moderation={r.moderation_status}")
        return "\n".join(lines)

    tools.append(list_pipeline_runs)

    def get_pipeline_run(run_id: int) -> str:
        """Get detailed info about a generation run."""
        try:
            run = _run_sync("get_pipeline_run", lambda: db.repos.generation_runs.get(run_id))
        except Exception as exc:
            return f"Ошибка: {exc}"
        if not run:
            return f"Run id={run_id} не найден."
        preview = (run.generated_text or "")[:500]
        return (
            f"Run id={run.id}, pipeline_id={run.pipeline_id}\n"
            f"Status: {run.status}, Moderation: {run.moderation_status}\n"
            f"Text:\n{preview}"
        )

    tools.append(get_pipeline_run)

    # === Moderation ===

    def list_pending_moderation(limit: int = 20) -> str:
        """List generation runs awaiting moderation."""
        try:
            runs = _run_sync(
                "list_pending_moderation",
                lambda: db.repos.generation_runs.list_pending_moderation(limit=limit),
            )
        except Exception as exc:
            return f"Ошибка: {exc}"
        if not runs:
            return "Нет черновиков на модерации."
        lines = [f"На модерации ({len(runs)}):"]
        for r in runs:
            preview = (r.generated_text or "")[:150]
            lines.append(f"- run_id={r.id}, pipeline_id={r.pipeline_id}: {preview}")
        return "\n".join(lines)

    tools.append(list_pending_moderation)

    def approve_run(run_id: int) -> str:
        """Approve a generation run for publishing. Then use publish_pipeline_run to publish it."""
        try:
            _run_sync("approve_run", lambda: db.repos.generation_runs.set_moderation_status(run_id, "approved"))
            return f"Run id={run_id} одобрен."
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(approve_run)

    def reject_run(run_id: int) -> str:
        """Reject a generation run."""
        try:
            _run_sync("reject_run", lambda: db.repos.generation_runs.set_moderation_status(run_id, "rejected"))
            return f"Run id={run_id} отклонён."
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(reject_run)

    def bulk_approve_runs(run_ids_csv: str) -> str:
        """Approve multiple runs. Pass comma-separated IDs like '1,2,3'."""
        try:
            ids = [int(x.strip()) for x in run_ids_csv.split(",") if x.strip()]
            for rid in ids:
                _run_sync(f"approve_{rid}", lambda r=rid: db.repos.generation_runs.set_moderation_status(r, "approved"))
            return f"Одобрено: {len(ids)} runs."
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(bulk_approve_runs)

    def bulk_reject_runs(run_ids_csv: str) -> str:
        """Reject multiple runs. Pass comma-separated IDs like '1,2,3'."""
        try:
            ids = [int(x.strip()) for x in run_ids_csv.split(",") if x.strip()]
            for rid in ids:
                _run_sync(f"reject_{rid}", lambda r=rid: db.repos.generation_runs.set_moderation_status(r, "rejected"))
            return f"Отклонено: {len(ids)} runs."
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(bulk_reject_runs)

    # === Search Queries ===

    def list_search_queries(active_only: bool = False) -> str:
        """List saved search queries."""
        try:
            from src.services.search_query_service import SearchQueryService

            svc = SearchQueryService(db)
            queries = _run_sync("list_sq", lambda: svc.list(active_only=active_only))
        except Exception as exc:
            return f"Ошибка: {exc}"
        if not queries:
            return "Поисковые запросы не найдены."
        lines = [f"Поисковые запросы ({len(queries)}):"]
        for q in queries:
            status = "вкл" if q.is_active else "выкл"
            lines.append(f"- id={q.id}: '{q.query}' [{status}] interval={q.interval_minutes}мин")
        return "\n".join(lines)

    tools.append(list_search_queries)

    def toggle_search_query(sq_id: int) -> str:
        """Toggle a search query active/inactive."""
        try:
            from src.services.search_query_service import SearchQueryService

            svc = SearchQueryService(db)
            _run_sync("toggle_sq", lambda: svc.toggle(sq_id))
            return f"Поисковый запрос id={sq_id} переключён."
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(toggle_search_query)

    def delete_search_query(sq_id: int) -> str:
        """⚠️ DANGEROUS: Delete a search query permanently."""
        try:
            from src.services.search_query_service import SearchQueryService

            svc = SearchQueryService(db)
            _run_sync("delete_sq", lambda: svc.delete(sq_id))
            return f"Поисковый запрос id={sq_id} удалён."
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(delete_search_query)

    def run_search_query(sq_id: int) -> str:
        """Execute a search query immediately."""
        try:
            from src.services.search_query_service import SearchQueryService

            svc = SearchQueryService(db)
            count = _run_sync("run_sq", lambda: svc.run_once(sq_id))
            return f"Запрос id={sq_id} выполнен: {count} совпадений."
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(run_search_query)

    # === Accounts ===

    def list_accounts() -> str:
        """List Telegram accounts from the database only. Not live Telegram connection state."""
        try:
            accounts = _run_sync("list_accounts", db.get_accounts)
        except Exception as exc:
            return f"Ошибка: {exc}"
        if not accounts:
            return "Аккаунты не найдены."
        lines = [f"Аккаунты ({len(accounts)}) в БД:"]
        for a in accounts:
            status = "активен" if a.is_active else "неактивен"
            lines.append(f"- id={a.id}, phone={a.phone}, {status}")
        return "\n".join(lines)

    tools.append(list_accounts)

    def toggle_account(account_id: int) -> str:
        """Toggle account active/inactive. account_id from list_accounts."""
        try:
            accounts = _run_sync("toggle_acc_get", db.get_accounts)
            acc = next((a for a in accounts if a.id == account_id), None)
            if not acc:
                return f"Аккаунт id={account_id} не найден."
            _run_sync("toggle_acc", lambda: db.set_account_active(account_id, not acc.is_active))
            return f"Аккаунт {acc.phone} переключён."
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(toggle_account)

    def delete_account(account_id: int) -> str:
        """⚠️ DANGEROUS: Delete a Telegram account from the system. account_id from list_accounts."""
        try:
            _run_sync("delete_acc", lambda: db.delete_account(account_id))
            return f"Аккаунт id={account_id} удалён."
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(delete_account)

    def get_flood_status() -> str:
        """Get database flood-wait status for all accounts; this is not live connection state."""
        try:
            accounts = _run_sync("flood_status", db.get_accounts)
        except Exception as exc:
            return f"Ошибка: {exc}"
        if not accounts:
            return "Аккаунты не найдены."
        lines = ["Flood-статус в БД:"]
        for a in accounts:
            flood = getattr(a, "flood_wait_until", None) or "нет"
            lines.append(f"- {a.phone}: {flood}")
        return "\n".join(lines)

    tools.append(get_flood_status)

    def get_account_info(phone: str = "") -> str:
        """Get live Telegram account info. Requires live runtime; use before account/reconnect diagnostics."""
        try:
            from src.agent.tools.accounts import get_live_account_info_text

            return runtime_context.run_sync(
                "get_account_info",
                lambda: get_live_account_info_text(runtime_context, phone),
            )
        except Exception as exc:
            return f"Ошибка получения информации об аккаунтах: {exc}"

    tools.append(get_account_info)

    # === Filters ===

    def analyze_filters() -> str:
        """Analyze channels and compute filter scores (low_uniqueness, spam, non_cyrillic, etc.)."""
        try:
            from src.filters.analyzer import ChannelAnalyzer

            analyzer = ChannelAnalyzer(db)
            report = _run_sync("analyze_filters", analyzer.analyze_all)
            flagged = [r for r in report.results if r.should_filter]
            return f"Анализ: {len(report.results)} проверено, {len(flagged)} к фильтрации."
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(analyze_filters)

    def apply_filters() -> str:
        """⚠️ DANGEROUS: Run analyze_filters and mark flagged channels as filtered."""
        try:
            from src.filters.analyzer import ChannelAnalyzer

            analyzer = ChannelAnalyzer(db)
            report = _run_sync("analyze", analyzer.analyze_all)
            count = _run_sync("apply", lambda: analyzer.apply_filters(report))
            return f"Фильтры применены: {count} каналов помечены."
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(apply_filters)

    def reset_filters() -> str:
        """⚠️ DANGEROUS: Reset all channel filters."""
        try:
            from src.filters.analyzer import ChannelAnalyzer

            analyzer = ChannelAnalyzer(db)
            count = _run_sync("reset_filters", analyzer.reset_filters)
            return f"Фильтры сброшены: {count} каналов разблокированы."
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(reset_filters)

    def toggle_channel_filter(pk: int) -> str:
        """Toggle filter status for a channel. pk = DB primary key from list_channels."""
        try:
            ch = _run_sync("get_ch", lambda: db.get_channel_by_pk(pk))
            if not ch:
                return f"Канал pk={pk} не найден."
            _run_sync("toggle_filter", lambda: db.set_channel_filtered(pk, not ch.is_filtered))
            return f"Фильтр канала '{ch.title}' переключён."
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(toggle_channel_filter)

    # === Analytics ===

    def get_analytics_summary() -> str:
        """Get overall content analytics summary."""
        try:
            from src.services.content_analytics_service import ContentAnalyticsService

            svc = ContentAnalyticsService(db)
            s = _run_sync("analytics", svc.get_summary)
            return (
                f"Аналитика:\n- Генераций: {s.get('total_generations', 0)}\n"
                f"- Опубликовано: {s.get('total_published', 0)}\n"
                f"- На модерации: {s.get('total_pending', 0)}\n"
                f"- Отклонено: {s.get('total_rejected', 0)}"
            )
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(get_analytics_summary)

    def get_pipeline_stats(pipeline_id: int = 0) -> str:
        """Get pipeline statistics. Pass 0 for all pipelines."""
        try:
            from src.services.content_analytics_service import ContentAnalyticsService

            svc = ContentAnalyticsService(db)
            pid = pipeline_id if pipeline_id else None
            stats = _run_sync("pipeline_stats", lambda: svc.get_pipeline_stats(pipeline_id=pid))
        except Exception as exc:
            return f"Ошибка: {exc}"
        if not stats:
            return "Статистика не найдена."
        lines = ["Статистика пайплайнов:"]
        for s in stats:
            lines.append(f"- {s.pipeline_name}: генераций={s.total_generations}, опубл.={s.total_published}")
        return "\n".join(lines)

    tools.append(get_pipeline_stats)

    def get_trending_topics(days: int = 7, limit: int = 20) -> str:
        """Get trending topics/keywords from collected messages."""
        try:
            from src.services.trend_service import TrendService

            svc = TrendService(db)
            topics = _run_sync("trends", lambda: svc.get_trending_topics(days=days, limit=limit))
        except Exception as exc:
            return f"Ошибка: {exc}"
        if not topics:
            return "Тренды не найдены."
        lines = [f"Тренды за {days} дней:"]
        for t in topics:
            lines.append(f"- {t.keyword}: {t.count}")
        return "\n".join(lines)

    tools.append(get_trending_topics)

    def get_trending_channels(days: int = 7, limit: int = 20) -> str:
        """Get top channels by activity."""
        try:
            from src.services.trend_service import TrendService

            svc = TrendService(db)
            channels = _run_sync("trend_ch", lambda: svc.get_trending_channels(days=days, limit=limit))
        except Exception as exc:
            return f"Ошибка: {exc}"
        if not channels:
            return "Данные не найдены."
        lines = [f"Топ каналов за {days} дней:"]
        for ch in channels:
            lines.append(f"- {ch.title}: {ch.count} сообщений")
        return "\n".join(lines)

    tools.append(get_trending_channels)

    def get_message_velocity(days: int = 30) -> str:
        """Get message volume per day."""
        try:
            from src.services.trend_service import TrendService

            svc = TrendService(db)
            velocity = _run_sync("velocity", lambda: svc.get_message_velocity(days=days))
        except Exception as exc:
            return f"Ошибка: {exc}"
        if not velocity:
            return "Данные не найдены."
        lines = [f"Скорость за {days} дней:"]
        for v in velocity:
            lines.append(f"- {v.date}: {v.count}")
        return "\n".join(lines)

    tools.append(get_message_velocity)

    def get_peak_hours() -> str:
        """Get peak activity hours."""
        try:
            from src.services.trend_service import TrendService

            svc = TrendService(db)
            hours = _run_sync("peak", svc.get_peak_hours)
        except Exception as exc:
            return f"Ошибка: {exc}"
        if not hours:
            return "Данные не найдены."
        lines = ["Пиковые часы:"]
        for h in hours:
            lines.append(f"- {h.hour:02d}:00 — {h.count}")
        return "\n".join(lines)

    tools.append(get_peak_hours)

    def get_calendar(limit: int = 20) -> str:
        """Get upcoming content publications."""
        try:
            from src.services.content_calendar_service import ContentCalendarService

            svc = ContentCalendarService(db)
            events = _run_sync("calendar", lambda: svc.get_upcoming(limit=limit))
        except Exception as exc:
            return f"Ошибка: {exc}"
        if not events:
            return "Нет запланированных публикаций."
        lines = [f"Ближайшие публикации ({len(events)}):"]
        for e in events:
            lines.append(f"- run_id={e.run_id}, pipeline={e.pipeline_name}, статус={e.moderation_status}")
        return "\n".join(lines)

    tools.append(get_calendar)

    def get_daily_stats(days: int = 30) -> str:
        """Get daily content generation statistics."""
        try:
            from src.services.content_analytics_service import ContentAnalyticsService

            svc = ContentAnalyticsService(db)
            rows = _run_sync("daily_stats", lambda: svc.get_daily_stats(days=days))
        except Exception as exc:
            return f"Ошибка: {exc}"
        if not rows:
            return "Нет данных."
        lines = [f"Статистика за {days} дней:"]
        for r in rows:
            lines.append(f"- {r['date']}: {r.get('count', 0)} генераций")
        return "\n".join(lines)

    tools.append(get_daily_stats)

    # === Scheduler ===

    def get_scheduler_status() -> str:
        """Get scheduler status and job info."""
        try:
            from src.scheduler.service import SchedulerManager

            mgr = SchedulerManager(db)
            _run_sync("sched_load", mgr.load_settings)
            running = mgr.is_running
            return f"Планировщик: {'запущен' if running else 'остановлен'}, интервал={mgr.interval_minutes}мин"
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(get_scheduler_status)

    def toggle_scheduler_job(job_id: str) -> str:
        """Toggle a scheduler job on/off."""
        try:
            from src.scheduler.service import SchedulerManager

            mgr = SchedulerManager(db)
            _run_sync("sched_load2", mgr.load_settings)
            current = _run_sync("sched_check", lambda: mgr.is_job_enabled(job_id))
            _run_sync("sched_toggle", lambda: mgr.sync_job_state(job_id, not current))
            return f"Задача '{job_id}' {'выключена' if current else 'включена'}."
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(toggle_scheduler_job)

    # === Notifications ===

    def get_notification_status() -> str:
        """Get notification bot status."""
        try:
            from src.services.notification_service import NotificationService
            from src.services.notification_target_service import NotificationTargetService
            svc = NotificationService(db, NotificationTargetService(db, client_pool))
            bot = _run_sync("notif_status", svc.get_status)
            if not bot:
                return "Бот уведомлений не настроен."
            return f"Бот: @{bot.bot_username}, chat_id={bot.chat_id}"
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(get_notification_status)

    # === Images ===

    def _build_image_service_sync():
        """Build ImageGenerationService with DB providers + env fallback (sync)."""
        from src.services.image_generation_service import ImageGenerationService

        if db and config:
            try:
                from src.services.image_provider_service import ImageProviderService

                svc = ImageProviderService(db, config)

                async def _load():
                    configs = await svc.load_provider_configs()
                    return svc.build_adapters(configs)

                adapters = _run_sync("load_image_providers", _load)
                if adapters:
                    return ImageGenerationService(adapters=adapters)
            except Exception:
                logger.warning("Failed to load image providers from DB", exc_info=True)
        return ImageGenerationService()

    def generate_image(prompt: str, model: str = "") -> str:
        """Generate an image from text prompt."""
        try:
            svc = _build_image_service_sync()
            result = _run_sync("gen_image", lambda: svc.generate(model=model or None, text=prompt))
            return f"Изображение: {result}" if result else "Генерация не вернула результат."
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(generate_image)

    def list_image_providers() -> str:
        """List configured image generation providers."""
        try:
            svc = _build_image_service_sync()
            names = svc.adapter_names
            return f"Провайдеры: {', '.join(names)}" if names else "Провайдеры не настроены."
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(list_image_providers)

    # === Settings ===

    def get_settings() -> str:
        """Get current system settings."""
        try:
            keys = ["collect_interval_minutes", "scheduler_enabled", "agent_backend_override"]
            lines = ["Настройки:"]
            for key in keys:
                val = _run_sync(f"setting_{key}", lambda k=key: db.get_setting(k))
                lines.append(f"- {key}: {val or '(не задано)'}")
            return "\n".join(lines)
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(get_settings)

    def get_system_info() -> str:
        """Get system diagnostics."""
        try:
            stats = _run_sync("sys_info", db.get_stats)
            lines = ["Система:"]
            for k, v in stats.items():
                lines.append(f"- {k}: {v}")
            return "\n".join(lines)
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(get_system_info)

    # === Agent Threads ===

    def list_agent_threads() -> str:
        """List agent chat threads."""
        try:
            threads = _run_sync("threads", db.get_agent_threads)
        except Exception as exc:
            return f"Ошибка: {exc}"
        if not threads:
            return "Треды не найдены."
        lines = [f"Треды ({len(threads)}):"]
        for t in threads:
            lines.append(f"- id={t['id']}: {t['title']}")
        return "\n".join(lines)

    tools.append(list_agent_threads)

    def create_agent_thread(title: str = "Новый тред") -> str:
        """Create a new agent chat thread."""
        try:
            tid = _run_sync("create_thread", lambda: db.create_agent_thread(title))
            return f"Тред создан: id={tid}"
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(create_agent_thread)

    def delete_agent_thread(thread_id: int) -> str:
        """⚠️ DANGEROUS: Delete a thread and all messages."""
        try:
            _run_sync("delete_thread", lambda: db.delete_agent_thread(thread_id))
            return f"Тред id={thread_id} удалён."
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(delete_agent_thread)

    def rename_agent_thread(thread_id: int, title: str) -> str:
        """Rename an agent chat thread."""
        try:
            _run_sync("rename_thread", lambda: db.rename_agent_thread(thread_id, title))
            return f"Тред id={thread_id} переименован в '{title}'."
        except Exception as exc:
            return f"Ошибка: {exc}"

    tools.append(rename_agent_thread)

    def get_thread_messages(thread_id: int, limit: int = 50) -> str:
        """Get messages from an agent thread."""
        try:
            messages = _run_sync("thread_msgs", lambda: db.get_agent_messages(thread_id))
            messages = messages[-limit:]
        except Exception as exc:
            return f"Ошибка: {exc}"
        if not messages:
            return f"Нет сообщений в треде id={thread_id}."
        lines = [f"Сообщения ({len(messages)}):"]
        for m in messages:
            content = (m.get("content", "") or "")[:150]
            lines.append(f"[{m.get('role', '?')}]: {content}")
        return "\n".join(lines)

    tools.append(get_thread_messages)

    return tools
