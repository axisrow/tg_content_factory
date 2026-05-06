from __future__ import annotations

from typing import Any

from claude_agent_sdk import tool

from src.agent.tools._registry import ToolInputError, _text_response, arg_bool, arg_int
from src.agent.tools.pipeline_schemas import (
    GET_PIPELINE_DETAIL_SCHEMA,
    GET_PIPELINE_QUEUE_SCHEMA,
    GET_REFINEMENT_STEPS_SCHEMA,
    LIST_PIPELINES_SCHEMA,
)


def register_pipeline_read_tools(db: Any, ctx: Any) -> list[Any]:
    tools: list[Any] = []

    @tool(
        "list_pipelines",
        "List all content pipelines with id, name, model, publish_mode (auto/moderated), schedule, "
        "and backend. Use pipeline_id from this list for other pipeline tools.",
        LIST_PIPELINES_SCHEMA,
    )
    async def list_pipelines(args):
        try:
            svc = ctx.pipeline_service()
            active_only = arg_bool(args, "active_only", False)
            pipelines = await svc.list(active_only=active_only)
            if not pipelines:
                return _text_response("Пайплайны не найдены.")
            lines = [f"Пайплайны ({len(pipelines)}):"]
            for pipeline in pipelines:
                status = "активен" if pipeline.is_active else "неактивен"
                model = getattr(pipeline, "llm_model", None) or "default"
                cron = getattr(pipeline, "schedule_cron", None) or "manual"
                generation_backend = getattr(pipeline, "generation_backend", None)
                publish_mode = getattr(pipeline, "publish_mode", None)
                backend = getattr(generation_backend, "value", generation_backend) or "chain"
                publish = getattr(publish_mode, "value", publish_mode) or "auto"
                lines.append(
                    f"- id={pipeline.id}: {pipeline.name} [{status}] model={model} "
                    f"publish={publish} schedule={cron} backend={backend}"
                )
            return _text_response("\n".join(lines))
        except Exception as exc:
            return _text_response(f"Ошибка получения пайплайнов: {exc}")

    tools.append(list_pipelines)

    @tool(
        "get_pipeline_detail",
        "Get detailed information about a specific pipeline including sources, targets, and channel names.",
        GET_PIPELINE_DETAIL_SCHEMA,
    )
    async def get_pipeline_detail(args):
        try:
            pipeline_id = arg_int(args, "pipeline_id", required=True)
        except ToolInputError as exc:
            return exc.to_response()
        try:
            svc = ctx.pipeline_service()
            detail = await svc.get_detail(pipeline_id)
            if detail is None:
                return _text_response(f"Пайплайн id={pipeline_id} не найден.")
            pipeline = detail["pipeline"]
            source_titles = detail.get("source_titles", [])
            target_refs = detail.get("target_refs", [])
            prompt_template = getattr(pipeline, "prompt_template", "") or ""
            publish_mode = getattr(pipeline, "publish_mode", None)
            generation_backend = getattr(pipeline, "generation_backend", None)
            lines = [
                f"Пайплайн: {pipeline.name} (id={pipeline.id})",
                f"  Статус: {'активен' if pipeline.is_active else 'неактивен'}",
                f"  LLM модель: {getattr(pipeline, 'llm_model', None) or 'default'}",
                f"  Публикация: {getattr(publish_mode, 'value', publish_mode) or 'auto'}",
                f"  Бэкенд: {getattr(generation_backend, 'value', generation_backend) or 'chain'}",
                f"  Расписание: {getattr(pipeline, 'schedule_cron', None) or 'manual'}",
                f"  Интервал генерации: {getattr(pipeline, 'generate_interval_minutes', '?')} мин.",
                f"  Шаблон промпта: {prompt_template[:200]}{'...' if len(prompt_template) > 200 else ''}",
                f"  Источники ({len(source_titles)}): {', '.join(source_titles) or '—'}",
                f"  Цели ({len(target_refs)}): {', '.join(target_refs) or '—'}",
            ]
            return _text_response("\n".join(lines))
        except Exception as exc:
            return _text_response(f"Ошибка получения деталей пайплайна: {exc}")

    tools.append(get_pipeline_detail)

    @tool(
        "get_pipeline_queue",
        "List pending and running generation runs across all pipelines (the generation queue). "
        "Shows run_id, pipeline_id, status, and text preview. "
        "Use get_pipeline_run to see full text of a specific run.",
        GET_PIPELINE_QUEUE_SCHEMA,
    )
    async def get_pipeline_queue(args):
        try:
            limit = arg_int(args, "limit", 20) or 20
            runs = await db.repos.generation_runs.list_by_status(["pending", "running"], limit=limit)
            if not runs:
                return _text_response("Очередь генерации пуста.")
            lines = [f"Очередь генерации ({len(runs)} шт.):"]
            for run in runs:
                preview = (run.generated_text or "")[:100]
                lines.append(
                    f"- run_id={run.id}, pipeline_id={run.pipeline_id}, status={run.status}: {preview}"
                )
            return _text_response("\n".join(lines))
        except Exception as exc:
            return _text_response(f"Ошибка получения очереди: {exc}")

    tools.append(get_pipeline_queue)

    @tool(
        "get_refinement_steps",
        "Get the refinement (post-processing) steps for a pipeline. "
        "Each step has a name and a prompt template with {text} placeholder.",
        GET_REFINEMENT_STEPS_SCHEMA,
    )
    async def get_refinement_steps(args):
        try:
            pipeline_id = arg_int(args, "pipeline_id", required=True)
        except ToolInputError as exc:
            return exc.to_response()
        try:
            pipeline = await db.repos.content_pipelines.get_by_id(pipeline_id)
            if pipeline is None:
                return _text_response(f"Пайплайн id={pipeline_id} не найден.")
            steps = pipeline.refinement_steps or []
            if not steps:
                return _text_response(f"Пайплайн id={pipeline_id} не имеет шагов рефайнмента.")
            lines = [f"Шаги рефайнмента пайплайна id={pipeline_id} ({len(steps)} шт.):"]
            for index, step in enumerate(steps, 1):
                prompt_preview = step.get("prompt", "")[:150]
                lines.append(f"  {index}. {step.get('name', '—')}: {prompt_preview}")
            return _text_response("\n".join(lines))
        except Exception as exc:
            return _text_response(f"Ошибка получения шагов рефайнмента: {exc}")

    tools.append(get_refinement_steps)
    return tools
