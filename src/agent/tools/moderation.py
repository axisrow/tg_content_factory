"""Agent tools for content moderation (approve / reject generation runs)."""

from __future__ import annotations

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._registry import _text_response, require_confirmation


def register(db, client_pool, embedding_service, **kwargs):
    tools = []

    # ------------------------------------------------------------------
    # READ
    # ------------------------------------------------------------------

    @tool(
        "list_pending_moderation",
        "List generation runs awaiting moderation. Filter by pipeline_id. "
        "Use run_id from results with get_pipeline_run to view full text, then approve_run or reject_run.",
        {"pipeline_id": int, "limit": int},
    )
    async def list_pending_moderation(args):
        try:
            pipeline_id = args.get("pipeline_id")
            limit = int(args.get("limit", 20))
            runs = await db.repos.generation_runs.list_pending_moderation(
                pipeline_id=int(pipeline_id) if pipeline_id is not None else None,
                limit=limit,
            )
            if not runs:
                return _text_response("Нет черновиков на модерации.")
            lines = [f"На модерации ({len(runs)} шт.):"]
            for r in runs:
                preview = (r.generated_text or "")[:200]
                lines.append(
                    f"- run_id={r.id}, pipeline_id={r.pipeline_id}, "
                    f"created={r.created_at}: {preview}"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения очереди модерации: {e}")

    tools.append(list_pending_moderation)

    @tool(
        "view_moderation_run",
        "View full details of a specific generation run for moderation review: "
        "generated text, pipeline name, status, quality score. "
        "run_id from list_pending_moderation. Then use approve_run or reject_run.",
        {"run_id": int},
    )
    async def view_moderation_run(args):
        run_id = args.get("run_id")
        if run_id is None:
            return _text_response("Ошибка: run_id обязателен.")
        try:
            run = await db.repos.generation_runs.get(int(run_id))
            if run is None:
                return _text_response(f"Run id={run_id} не найден.")
            lines = [
                f"Run id={run.id} (pipeline_id={run.pipeline_id})",
                f"  Статус: {run.status}",
                f"  Модерация: {run.moderation_status}",
                f"  Создан: {run.created_at}",
                "",
                "Текст для проверки:",
                run.generated_text or "(пусто)",
            ]
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения run: {e}")

    tools.append(view_moderation_run)

    # ------------------------------------------------------------------
    # WRITE
    # ------------------------------------------------------------------

    @tool(
        "approve_run",
        "Approve a generation run for publishing. Then use publish_pipeline_run to publish it. "
        "run_id from list_pending_moderation or list_pipeline_runs.",
        {"run_id": int},
    )
    async def approve_run(args):
        run_id = args.get("run_id")
        if run_id is None:
            return _text_response("Ошибка: run_id обязателен.")
        try:
            run = await db.repos.generation_runs.get(int(run_id))
            if run is None:
                return _text_response(f"Run id={run_id} не найден.")
            await db.repos.generation_runs.set_moderation_status(int(run_id), "approved")
            return _text_response(f"Run id={run_id} одобрен для публикации.")
        except Exception as e:
            return _text_response(f"Ошибка одобрения: {e}")

    tools.append(approve_run)

    @tool(
        "reject_run",
        "Reject a generation run. Provide the run_id.",
        {"run_id": int},
    )
    async def reject_run(args):
        run_id = args.get("run_id")
        if run_id is None:
            return _text_response("Ошибка: run_id обязателен.")
        try:
            run = await db.repos.generation_runs.get(int(run_id))
            if run is None:
                return _text_response(f"Run id={run_id} не найден.")
            await db.repos.generation_runs.set_moderation_status(int(run_id), "rejected")
            return _text_response(f"Run id={run_id} отклонён.")
        except Exception as e:
            return _text_response(f"Ошибка отклонения: {e}")

    tools.append(reject_run)

    # ------------------------------------------------------------------
    # BULK WRITE + confirm
    # ------------------------------------------------------------------

    @tool(
        "bulk_approve_runs",
        "Approve multiple generation runs at once. Provide comma-separated run_ids (e.g. '1,2,3'). "
        "Requires confirm=true.",
        {"run_ids": str, "confirm": bool},
        annotations=ToolAnnotations(destructiveHint=False),
    )
    async def bulk_approve_runs(args):
        raw = args.get("run_ids", "")
        try:
            ids = [int(x.strip()) for x in raw.split(",") if x.strip()]
        except ValueError:
            return _text_response("Ошибка: run_ids должны быть числами через запятую.")
        if not ids:
            return _text_response("Ошибка: run_ids пуст.")
        gate = require_confirmation(f"одобрит {len(ids)} run(s) для публикации", args)
        if gate:
            return gate
        try:
            approved = []
            for rid in ids:
                await db.repos.generation_runs.set_moderation_status(rid, "approved")
                approved.append(rid)
            return _text_response(f"Одобрено {len(approved)} run(s): {approved}")
        except Exception as e:
            return _text_response(f"Ошибка массового одобрения: {e}")

    tools.append(bulk_approve_runs)

    @tool(
        "bulk_reject_runs",
        "Reject multiple generation runs at once. Provide comma-separated run_ids (e.g. '1,2,3'). "
        "Requires confirm=true.",
        {"run_ids": str, "confirm": bool},
        annotations=ToolAnnotations(destructiveHint=False),
    )
    async def bulk_reject_runs(args):
        raw = args.get("run_ids", "")
        try:
            ids = [int(x.strip()) for x in raw.split(",") if x.strip()]
        except ValueError:
            return _text_response("Ошибка: run_ids должны быть числами через запятую.")
        if not ids:
            return _text_response("Ошибка: run_ids пуст.")
        gate = require_confirmation(f"отклонит {len(ids)} run(s)", args)
        if gate:
            return gate
        try:
            rejected = []
            for rid in ids:
                await db.repos.generation_runs.set_moderation_status(rid, "rejected")
                rejected.append(rid)
            return _text_response(f"Отклонено {len(rejected)} run(s): {rejected}")
        except Exception as e:
            return _text_response(f"Ошибка массового отклонения: {e}")

    tools.append(bulk_reject_runs)

    return tools
