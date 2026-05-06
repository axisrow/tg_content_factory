from __future__ import annotations

from typing import Any

from claude_agent_sdk import tool

from src.agent.tools._pipeline_runtime import build_image_service
from src.agent.tools._registry import _text_response, require_confirmation
from src.agent.tools.pipeline_schemas import (
    GENERATE_DRAFT_SCHEMA,
    GET_PIPELINE_RUN_SCHEMA,
    LIST_PIPELINE_RUNS_SCHEMA,
    PUBLISH_PIPELINE_RUN_SCHEMA,
    RUN_PIPELINE_SCHEMA,
)


def register_pipeline_run_tools(db: Any, client_pool: Any, config: Any, ctx: Any) -> list[Any]:
    tools: list[Any] = []

    @tool(
        "run_pipeline",
        "Trigger content generation for a pipeline. Returns a preview of the generated text. "
        "If publish_mode=auto, the run is published immediately; "
        "otherwise use approve_run + publish_pipeline_run.",
        RUN_PIPELINE_SCHEMA,
    )
    async def run_pipeline(args):
        pipeline_id = args.get("pipeline_id")
        if pipeline_id is None:
            return _text_response("Ошибка: pipeline_id обязателен.")
        try:
            from src.search.engine import SearchEngine
            from src.services.content_generation_service import ContentGenerationService
            from src.services.pipeline_service import PipelineService
            from src.services.provider_service import build_provider_service

            svc = PipelineService(db)
            pipeline = await svc.get(int(pipeline_id))
            if pipeline is None:
                return _text_response(f"Пайплайн id={pipeline_id} не найден.")
            if not pipeline.is_active:
                return _text_response(f"Пайплайн '{pipeline.name}' неактивен.")

            engine = SearchEngine(db, config=config)
            image_service = await build_image_service(db, config)
            provider_service = await build_provider_service(db, config)
            gen_svc = ContentGenerationService(
                db,
                engine,
                config=config,
                image_service=image_service,
                provider_service=provider_service,
            )
            run = await gen_svc.generate(pipeline)

            preview = (run.generated_text or "")[:500]
            mod = run.moderation_status or "n/a"
            return _text_response(
                f"Генерация завершена (run id={run.id}).\n"
                f"Статус модерации: {mod}\n\n"
                f"Превью:\n{preview}"
            )
        except Exception as exc:
            return _text_response(f"Ошибка запуска пайплайна: {exc}")

    tools.append(run_pipeline)

    @tool(
        "generate_draft",
        "Generate a draft from a query using RAG (returns draft text and citations). "
        "Optionally use a pipeline's prompt template and model.",
        GENERATE_DRAFT_SCHEMA,
    )
    async def generate_draft(args):
        query = args.get("query", "")
        pipeline_id = args.get("pipeline_id")
        limit = int(args.get("limit", 8))
        try:
            from src.search.engine import SearchEngine
            from src.services.generation_service import GenerationService
            from src.services.pipeline_service import PipelineService
            from src.services.provider_service import build_provider_service

            engine = SearchEngine(db, config=config)
            prompt_template = None
            llm_model = None
            channel_id = None
            if pipeline_id is not None:
                svc = PipelineService(db)
                pipeline = await svc.get(int(pipeline_id))
                if pipeline is not None:
                    prompt_template = pipeline.prompt_template
                    llm_model = pipeline.llm_model
                    scope = await svc.get_retrieval_scope(pipeline)
                    channel_id = scope.channel_id
                    if not query:
                        query = scope.query
            provider_service = await build_provider_service(db, config)
            provider_callable = provider_service.get_provider_callable(llm_model)

            gen = GenerationService(engine, provider_callable=provider_callable)
            result = await gen.generate(
                query=query,
                limit=limit,
                prompt_template=prompt_template,
                channel_id=channel_id,
            )
            text = result.get("generated_text", "")
            citations = result.get("citations", [])
            content = f"Generated draft:\n\n{text}\n\nCitations:\n" + "\n".join(
                f"- {citation['channel_title']} id={citation['message_id']} date={citation['date']}"
                for citation in citations
            )
        except Exception as exc:
            content = f"Ошибка генерации: {exc}"
        return _text_response(content)

    tools.append(generate_draft)

    @tool(
        "list_pipeline_runs",
        "List generation runs for a pipeline. "
        "Filter by status (pending/completed/approved/rejected). "
        "Use run_id from results with get_pipeline_run, approve_run, publish_pipeline_run.",
        LIST_PIPELINE_RUNS_SCHEMA,
    )
    async def list_pipeline_runs(args):
        pipeline_id = args.get("pipeline_id")
        if pipeline_id is None:
            return _text_response("Ошибка: pipeline_id обязателен.")
        try:
            limit = int(args.get("limit", 20))
            status_filter = args.get("status")
            fetch_limit = limit * 10 if status_filter else limit
            runs = await db.repos.generation_runs.list_by_pipeline(int(pipeline_id), limit=fetch_limit)
            if status_filter:
                runs = [run for run in runs if run.status == status_filter or run.moderation_status == status_filter]
                runs = runs[:limit]
            if not runs:
                return _text_response(f"Нет генераций для пайплайна id={pipeline_id}.")
            lines = [f"Генерации пайплайна id={pipeline_id} ({len(runs)} шт.):"]
            for run in runs:
                preview = (getattr(run, "generated_text", None) or "")[:150]
                result_kind = getattr(run, "result_kind", None)
                result_count = getattr(run, "result_count", None)
                if result_kind is not None and result_count is not None:
                    result_part = f"result={result_kind}:{result_count}, "
                else:
                    result_part = ""
                lines.append(
                    f"- run_id={run.id}, status={run.status}, moderation={run.moderation_status}, "
                    f"{result_part}created={getattr(run, 'created_at', 'unknown')}: {preview}"
                )
            return _text_response("\n".join(lines))
        except Exception as exc:
            return _text_response(f"Ошибка получения генераций: {exc}")

    tools.append(list_pipeline_runs)

    @tool(
        "get_pipeline_run",
        "Get full details of a specific generation run including generated text, status, and quality score.",
        GET_PIPELINE_RUN_SCHEMA,
    )
    async def get_pipeline_run(args):
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
                f"  Качество: "
                f"{run.quality_score if hasattr(run, 'quality_score') and run.quality_score else 'n/a'}",
            ]
            result_kind = getattr(run, "result_kind", None)
            result_count = getattr(run, "result_count", None)
            if result_kind is not None and result_count is not None:
                from src.services.pipeline_result import result_kind_label

                lines.append(
                    f"  Результат: {result_kind_label(result_kind)} "
                    f"({result_kind}:{result_count})"
                )
            lines.extend(
                [
                    f"  Создан: {getattr(run, 'created_at', 'unknown')}",
                    f"  Обновлён: {getattr(run, 'updated_at', 'unknown')}",
                    "",
                    "Текст:",
                    getattr(run, "generated_text", None) or "(пусто)",
                ]
            )
            return _text_response("\n".join(lines))
        except Exception as exc:
            return _text_response(f"Ошибка получения run: {exc}")

    tools.append(get_pipeline_run)

    @tool(
        "publish_pipeline_run",
        "Publish a generation run to its pipeline target channels. "
        "Approve the run first via approve_run. run_id from list_pipeline_runs. "
        "Requires Telegram client and confirm=true.",
        PUBLISH_PIPELINE_RUN_SCHEMA,
    )
    async def publish_pipeline_run(args):
        gate = ctx.require_pool("Публикация контента")
        if gate:
            return gate
        gate = require_confirmation("опубликует генерацию в целевые каналы", args)
        if gate:
            return gate
        run_id = args.get("run_id")
        if run_id is None:
            return _text_response("Ошибка: run_id обязателен.")
        try:
            from src.services.pipeline_service import PipelineService
            from src.services.publish_service import PublishService

            run = await db.repos.generation_runs.get(int(run_id))
            if run is None:
                return _text_response(f"Run id={run_id} не найден.")
            svc = PipelineService(db)
            pipeline = await svc.get(run.pipeline_id)
            if pipeline is None:
                return _text_response(f"Пайплайн id={run.pipeline_id} не найден.")

            publish_svc = PublishService(db, client_pool)
            results = await publish_svc.publish_run(run, pipeline)
            ok_count = sum(1 for result in results if result.success)
            fail_count = len(results) - ok_count
            lines = [f"Публикация run id={run_id}: {ok_count} успешно, {fail_count} ошибок."]
            for result in results:
                if not result.success:
                    lines.append(f"  Ошибка: {result.error}")
            return _text_response("\n".join(lines))
        except Exception as exc:
            return _text_response(f"Ошибка публикации: {exc}")

    tools.append(publish_pipeline_run)
    return tools
