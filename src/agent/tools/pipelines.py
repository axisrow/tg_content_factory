"""Agent tools for content pipeline management."""

from __future__ import annotations

import logging
from typing import Annotated

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._registry import (
    ToolInputError,
    _text_response,
    arg_bool,
    arg_csv_ints,
    arg_int,
    arg_str,
    get_tool_context,
    require_confirmation,
)

logger = logging.getLogger(__name__)


def register(db, client_pool, embedding_service, **kwargs):
    config = kwargs.get("config")
    ctx = get_tool_context(kwargs, db=db, client_pool=client_pool, embedding_service=embedding_service)
    tools = []

    def _parse_target_refs(raw: str):
        from src.services.pipeline_service import PipelineTargetRef

        target_refs = []
        for part in raw.split(","):
            part = part.strip()
            if not part:
                continue
            if "|" not in part:
                raise ToolInputError(f"Неверный формат target_ref: '{part}'. Ожидается 'phone|dialog_id'.")
            phone, dialog_id = part.split("|", 1)
            try:
                target_refs.append(PipelineTargetRef(phone=phone.strip(), dialog_id=int(dialog_id.strip())))
            except ValueError as exc:
                raise ToolInputError(f"dialog_id в target_ref '{part}' должен быть целым числом.") from exc
        return target_refs

    async def _build_image_service():
        """Build ImageGenerationService with DB providers + env fallback."""
        from src.services.image_generation_service import ImageGenerationService

        if db and config:
            try:
                from src.services.image_provider_service import ImageProviderService

                svc = ImageProviderService(db, config)
                configs = await svc.load_provider_configs()
                adapters = svc.build_adapters(configs)
                if adapters:
                    return ImageGenerationService(adapters=adapters)
            except Exception:
                logger.warning("Failed to load image providers from DB", exc_info=True)
        return ImageGenerationService()

    # ------------------------------------------------------------------
    # Read tools
    # ------------------------------------------------------------------

    @tool(
        "list_pipelines",
        "List all content pipelines with id, name, model, publish_mode (auto/moderated), schedule, "
        "and backend. Use pipeline_id from this list for other pipeline tools.",
        {"active_only": Annotated[bool, "Показывать только активные"]},
    )
    async def list_pipelines(args):
        try:
            svc = ctx.pipeline_service()
            active_only = arg_bool(args, "active_only", False)
            pipelines = await svc.list(active_only=active_only)
            if not pipelines:
                return _text_response("Пайплайны не найдены.")
            lines = [f"Пайплайны ({len(pipelines)}):"]
            for p in pipelines:
                status = "активен" if p.is_active else "неактивен"
                model = p.llm_model or "default"
                cron = p.schedule_cron or "manual"
                backend = getattr(p.generation_backend, "value", p.generation_backend) or "chain"
                publish = getattr(p.publish_mode, "value", p.publish_mode) or "auto"
                lines.append(
                    f"- id={p.id}: {p.name} [{status}] model={model} "
                    f"publish={publish} schedule={cron} backend={backend}"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения пайплайнов: {e}")

    tools.append(list_pipelines)

    @tool(
        "get_pipeline_detail",
        "Get detailed information about a specific pipeline including sources, targets, and channel names.",
        {"pipeline_id": Annotated[int, "ID пайплайна из list_pipelines"]},
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
            p = detail["pipeline"]
            source_titles = detail.get("source_titles", [])
            target_refs = detail.get("target_refs", [])
            lines = [
                f"Пайплайн: {p.name} (id={p.id})",
                f"  Статус: {'активен' if p.is_active else 'неактивен'}",
                f"  LLM модель: {p.llm_model or 'default'}",
                f"  Публикация: {getattr(p.publish_mode, 'value', p.publish_mode)}",
                f"  Бэкенд: {getattr(p.generation_backend, 'value', p.generation_backend) or 'chain'}",
                f"  Расписание: {p.schedule_cron or 'manual'}",
                f"  Интервал генерации: {p.generate_interval_minutes} мин.",
                f"  Шаблон промпта: {p.prompt_template[:200]}{'...' if len(p.prompt_template) > 200 else ''}",
                f"  Источники ({len(source_titles)}): {', '.join(source_titles) or '—'}",
                f"  Цели ({len(target_refs)}): {', '.join(target_refs) or '—'}",
            ]
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения деталей пайплайна: {e}")

    tools.append(get_pipeline_detail)

    @tool(
        "get_pipeline_queue",
        "List pending and running generation runs across all pipelines (the generation queue). "
        "Shows run_id, pipeline_id, status, and text preview. "
        "Use get_pipeline_run to see full text of a specific run.",
        {"limit": Annotated[int, "Максимальное количество результатов"]},
    )
    async def get_pipeline_queue(args):
        try:
            limit = arg_int(args, "limit", 20) or 20
            runs = await db.repos.generation_runs.list_by_status(["pending", "running"], limit=limit)
            if not runs:
                return _text_response("Очередь генерации пуста.")
            lines = [f"Очередь генерации ({len(runs)} шт.):"]
            for r in runs:
                preview = (r.generated_text or "")[:100]
                lines.append(
                    f"- run_id={r.id}, pipeline_id={r.pipeline_id}, status={r.status}: {preview}"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения очереди: {e}")

    tools.append(get_pipeline_queue)

    @tool(
        "get_refinement_steps",
        "Get the refinement (post-processing) steps for a pipeline. "
        "Each step has a name and a prompt template with {text} placeholder.",
        {"pipeline_id": Annotated[int, "ID пайплайна из list_pipelines"]},
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
            for i, step in enumerate(steps, 1):
                prompt_preview = step.get("prompt", "")[:150]
                lines.append(f"  {i}. {step.get('name', '—')}: {prompt_preview}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения шагов рефайнмента: {e}")

    tools.append(get_refinement_steps)

    # ------------------------------------------------------------------
    # Write tools
    # ------------------------------------------------------------------

    @tool(
        "add_pipeline",
        "Create a new content pipeline. source_channel_ids = comma-separated Telegram channel IDs "
        "(from list_channels). target_refs = comma-separated 'phone|dialog_id' pairs. "
        "publish_mode: 'auto' or 'moderated'. Requires confirm=true.",
        {
            "name": Annotated[str, "Название пайплайна"],
            "prompt_template": Annotated[str, "Шаблон промпта для генерации контента"],
            "source_channel_ids": Annotated[str, "Telegram ID каналов-источников через запятую"],
            "target_refs": Annotated[str, "Цели публикации через запятую в формате phone|dialog_id"],
            "llm_model": Annotated[str, "Модель LLM для генерации (например claude-sonnet-4-20250514)"],
            "publish_mode": Annotated[str, "Режим публикации: auto или moderated"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
    )
    async def add_pipeline(args):
        gate = require_confirmation("создаст новый пайплайн", args)
        if gate:
            return gate
        try:
            name = arg_str(args, "name")
            prompt_template = arg_str(args, "prompt_template")
            source_str = arg_str(args, "source_channel_ids")
            target_str = arg_str(args, "target_refs")
            if not name or not prompt_template or not source_str or not target_str:
                return _text_response(
                    "Ошибка: name, prompt_template, source_channel_ids и target_refs обязательны."
                )
            source_ids = arg_csv_ints(args, "source_channel_ids", required=True)
            target_refs = _parse_target_refs(target_str)

            svc = ctx.pipeline_service()
            llm_model = args.get("llm_model")
            publish_mode = args.get("publish_mode", "moderated")
            pipeline_id = await svc.add(
                name=name,
                prompt_template=prompt_template,
                source_channel_ids=source_ids,
                target_refs=target_refs,
                llm_model=llm_model,
                publish_mode=publish_mode,
            )
            return _text_response(f"Пайплайн '{name}' создан (id={pipeline_id}).")
        except ToolInputError as e:
            return e.to_response()
        except Exception as e:
            return _text_response(f"Ошибка создания пайплайна: {e}")

    tools.append(add_pipeline)

    @tool(
        "edit_pipeline",
        "Edit an existing pipeline. All fields are optional except pipeline_id. "
        "source_channel_ids and target_refs are comma-separated strings. Requires confirm=true.",
        {
            "pipeline_id": Annotated[int, "ID пайплайна из list_pipelines"],
            "name": Annotated[str, "Название пайплайна"],
            "prompt_template": Annotated[str, "Шаблон промпта для генерации контента"],
            "source_channel_ids": Annotated[str, "Telegram ID каналов-источников через запятую"],
            "target_refs": Annotated[str, "Цели публикации через запятую в формате phone|dialog_id"],
            "llm_model": Annotated[str, "Модель LLM для генерации (например claude-sonnet-4-20250514)"],
            "publish_mode": Annotated[str, "Режим публикации: auto или moderated"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
    )
    async def edit_pipeline(args):
        gate = require_confirmation("изменит настройки пайплайна", args)
        if gate:
            return gate
        try:
            pipeline_id = arg_int(args, "pipeline_id", required=True)
        except ToolInputError as exc:
            return exc.to_response()
        try:
            from src.services.pipeline_service import PipelineTargetRef

            svc = ctx.pipeline_service()
            existing = await svc.get_detail(pipeline_id)
            if existing is None:
                return _text_response(f"Пайплайн id={pipeline_id} не найден.")
            p = existing["pipeline"]

            name = args.get("name", p.name).strip()
            prompt_template = args.get("prompt_template", p.prompt_template).strip()
            llm_model = args.get("llm_model", p.llm_model)
            publish_mode = args.get("publish_mode", getattr(p.publish_mode, "value", p.publish_mode))

            source_str = args.get("source_channel_ids")
            if source_str:
                source_ids = arg_csv_ints(args, "source_channel_ids", required=True)
            else:
                source_ids = existing["source_ids"]

            target_str = args.get("target_refs")
            if target_str:
                target_refs = _parse_target_refs(target_str)
            else:
                target_refs = [
                    PipelineTargetRef(phone=t.phone, dialog_id=t.dialog_id) for t in existing["targets"]
                ]

            ok = await svc.update(
                pipeline_id,
                name=name,
                prompt_template=prompt_template,
                source_channel_ids=source_ids,
                target_refs=target_refs,
                llm_model=llm_model,
                publish_mode=publish_mode,
            )
            if ok:
                return _text_response(f"Пайплайн '{name}' (id={pipeline_id}) обновлён.")
            return _text_response(f"Не удалось обновить пайплайн id={pipeline_id}.")
        except ToolInputError as e:
            return e.to_response()
        except Exception as e:
            return _text_response(f"Ошибка редактирования пайплайна: {e}")

    tools.append(edit_pipeline)

    @tool(
        "toggle_pipeline",
        "Toggle pipeline active/inactive status.",
        {"pipeline_id": Annotated[int, "ID пайплайна из list_pipelines"]},
    )
    async def toggle_pipeline(args):
        try:
            pipeline_id = arg_int(args, "pipeline_id", required=True)
        except ToolInputError as exc:
            return exc.to_response()
        try:
            svc = ctx.pipeline_service()
            ok = await svc.toggle(pipeline_id)
            if not ok:
                return _text_response(f"Пайплайн id={pipeline_id} не найден.")
            pipeline = await svc.get(pipeline_id)
            status = "активирован" if pipeline and pipeline.is_active else "деактивирован"
            name = pipeline.name if pipeline else f"id={pipeline_id}"
            return _text_response(f"Пайплайн '{name}' {status}.")
        except Exception as e:
            return _text_response(f"Ошибка переключения пайплайна: {e}")

    tools.append(toggle_pipeline)

    @tool(
        "delete_pipeline",
        "⚠️ DANGEROUS: Delete a content pipeline permanently. Requires confirm=true.",
        {
            "pipeline_id": Annotated[int, "ID пайплайна из list_pipelines"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def delete_pipeline(args):
        try:
            pipeline_id = arg_int(args, "pipeline_id", required=True)
        except ToolInputError as exc:
            return exc.to_response()
        try:
            svc = ctx.pipeline_service()
            pipeline = await svc.get(pipeline_id)
            name = pipeline.name if pipeline else f"id={pipeline_id}"
            gate = require_confirmation(f"безвозвратно удалит пайплайн '{name}'", args)
            if gate:
                return gate
            await svc.delete(pipeline_id)
            return _text_response(f"Пайплайн '{name}' удалён.")
        except Exception as e:
            return _text_response(f"Ошибка удаления пайплайна: {e}")

    tools.append(delete_pipeline)

    # ------------------------------------------------------------------
    # Generation & runs
    # ------------------------------------------------------------------

    @tool(
        "run_pipeline",
        "Trigger content generation for a pipeline. Returns a preview of the generated text. "
        "If publish_mode=auto, the run is published immediately; "
        "otherwise use approve_run + publish_pipeline_run.",
        {"pipeline_id": Annotated[int, "ID пайплайна из list_pipelines"]},
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
            image_service = await _build_image_service()
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
        except Exception as e:
            return _text_response(f"Ошибка запуска пайплайна: {e}")

    tools.append(run_pipeline)

    @tool(
        "generate_draft",
        "Generate a draft from a query using RAG (returns draft text and citations). "
        "Optionally use a pipeline's prompt template and model.",
        {
            "query": Annotated[str, "Запрос для генерации черновика"],
            "pipeline_id": Annotated[int, "ID пайплайна из list_pipelines"],
            "limit": Annotated[int, "Максимальное количество результатов"],
        },
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
                f"- {c['channel_title']} id={c['message_id']} date={c['date']}" for c in citations
            )
        except Exception as e:
            content = f"Ошибка генерации: {e}"
        return _text_response(content)

    tools.append(generate_draft)

    @tool(
        "list_pipeline_runs",
        "List generation runs for a pipeline. "
        "Filter by status (pending/completed/approved/rejected). "
        "Use run_id from results with get_pipeline_run, approve_run, publish_pipeline_run.",
        {
            "pipeline_id": Annotated[int, "ID пайплайна из list_pipelines"],
            "limit": Annotated[int, "Максимальное количество результатов"],
            "status": Annotated[str, "Фильтр по статусу (pending/completed/approved/rejected)"],
        },
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
                runs = [r for r in runs if r.status == status_filter or r.moderation_status == status_filter]
                runs = runs[:limit]
            if not runs:
                return _text_response(f"Нет генераций для пайплайна id={pipeline_id}.")
            lines = [f"Генерации пайплайна id={pipeline_id} ({len(runs)} шт.):"]
            for r in runs:
                preview = (r.generated_text or "")[:150]
                result_kind = getattr(r, "result_kind", None)
                result_count = getattr(r, "result_count", None)
                if result_kind is not None and result_count is not None:
                    result_part = f"result={result_kind}:{result_count}, "
                else:
                    result_part = ""
                lines.append(
                    f"- run_id={r.id}, status={r.status}, moderation={r.moderation_status}, "
                    f"{result_part}created={r.created_at}: {preview}"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения генераций: {e}")

    tools.append(list_pipeline_runs)

    @tool(
        "get_pipeline_run",
        "Get full details of a specific generation run including generated text, status, and quality score.",
        {"run_id": Annotated[int, "ID генерации из list_pipeline_runs"]},
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
                f"  Качество: {run.quality_score if hasattr(run, 'quality_score') and run.quality_score else 'n/a'}",
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
                    f"  Создан: {run.created_at}",
                    f"  Обновлён: {run.updated_at}",
                    "",
                    "Текст:",
                    run.generated_text or "(пусто)",
                ]
            )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения run: {e}")

    tools.append(get_pipeline_run)

    @tool(
        "publish_pipeline_run",
        "Publish a generation run to its pipeline target channels. "
        "Approve the run first via approve_run. run_id from list_pipeline_runs. "
        "Requires Telegram client and confirm=true.",
        {
            "run_id": Annotated[int, "ID генерации из list_pipeline_runs"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
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
            ok_count = sum(1 for r in results if r.success)
            fail_count = len(results) - ok_count
            lines = [f"Публикация run id={run_id}: {ok_count} успешно, {fail_count} ошибок."]
            for r in results:
                if not r.success:
                    lines.append(f"  Ошибка: {r.error}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка публикации: {e}")

    tools.append(publish_pipeline_run)

    @tool(
        "set_refinement_steps",
        "⚠️ Set the refinement (post-processing) steps for a pipeline. "
        "steps_json is a JSON array: [{\"name\": \"Step name\", \"prompt\": \"...{text}...\"}]. "
        "Pass an empty array to clear all steps. Requires confirm=true.",
        {
            "pipeline_id": Annotated[int, "ID пайплайна из list_pipelines"],
            "steps_json": Annotated[str, "JSON-массив шагов: [{name, prompt}]"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
    )
    async def set_refinement_steps(args):
        pipeline_id = args.get("pipeline_id")
        if pipeline_id is None:
            return _text_response("Ошибка: pipeline_id обязателен.")
        gate = require_confirmation(f"обновит шаги рефайнмента пайплайна id={pipeline_id}", args)
        if gate:
            return gate
        try:
            import json as _json
            raw = args.get("steps_json", "[]")
            steps = _json.loads(raw)
            if not isinstance(steps, list):
                return _text_response("Ошибка: steps_json должен быть JSON-массивом.")
            validated = [
                {"name": str(s.get("name", "")).strip(), "prompt": str(s.get("prompt", "")).strip()}
                for s in steps
                if isinstance(s, dict) and str(s.get("prompt", "")).strip()
            ]
            dropped = len(steps) - len(validated)
            if dropped > 0:
                return _text_response(
                    f"Ошибка: {dropped} из {len(steps)} шагов не содержат поле 'prompt' и не могут быть сохранены. "
                    f"Убедитесь что каждый элемент имеет ключ 'prompt' с непустым значением."
                )
            pipeline = await db.repos.content_pipelines.get_by_id(int(pipeline_id))
            if pipeline is None:
                return _text_response(f"Пайплайн id={pipeline_id} не найден.")
            await db.repos.content_pipelines.set_refinement_steps(int(pipeline_id), validated)
            return _text_response(
                f"Шаги рефайнмента пайплайна id={pipeline_id} обновлены ({len(validated)} шт.)."
            )
        except _json.JSONDecodeError as e:
            return _text_response(f"Ошибка парсинга steps_json: {e}")
        except Exception as e:
            return _text_response(f"Ошибка обновления шагов рефайнмента: {e}")

    tools.append(set_refinement_steps)

    # ------------------------------------------------------------------
    # JSON import / export / templates / AI-edit
    # ------------------------------------------------------------------

    @tool(
        "export_pipeline_json",
        "Export a pipeline as a JSON dict (the node-based DAG config and legacy fields). "
        "Use this to inspect the full pipeline structure or to save it for re-import.",
        {"pipeline_id": Annotated[int, "ID пайплайна из list_pipelines"]},
    )
    async def export_pipeline_json(args):
        pipeline_id = args.get("pipeline_id")
        if pipeline_id is None:
            return _text_response("Ошибка: pipeline_id обязателен.")
        try:
            import json as _json

            from src.services.pipeline_service import PipelineService

            svc = PipelineService(db)
            data = await svc.export_json(int(pipeline_id))
            if data is None:
                return _text_response(f"Пайплайн id={pipeline_id} не найден.")
            return _text_response(_json.dumps(data, ensure_ascii=False, indent=2))
        except Exception as e:
            return _text_response(f"Ошибка экспорта пайплайна: {e}")

    tools.append(export_pipeline_json)

    @tool(
        "import_pipeline_json",
        "Create a new pipeline by importing a JSON dict (previously exported via export_pipeline_json). "
        "Pass the JSON as a string. Requires confirm=true.",
        {
            "json_text": Annotated[str, "JSON строка с конфигурацией пайплайна"],
            "name_override": Annotated[str, "Переопределить имя пайплайна (опционально)"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
    )
    async def import_pipeline_json(args):
        gate = require_confirmation("создаст новый пайплайн из JSON", args)
        if gate:
            return gate
        json_text = args.get("json_text", "").strip()
        if not json_text:
            return _text_response("Ошибка: json_text обязателен.")
        try:
            import json as _json

            from src.services.pipeline_service import PipelineService

            data = _json.loads(json_text)
            svc = PipelineService(db)
            name_override = args.get("name_override") or None
            pipeline_id = await svc.import_json(data, name_override=name_override)
            return _text_response(f"Пайплайн импортирован (id={pipeline_id}).")
        except Exception as e:
            return _text_response(f"Ошибка импорта пайплайна: {e}")

    tools.append(import_pipeline_json)

    @tool(
        "list_pipeline_templates",
        "List all available pipeline templates (built-in and custom). "
        "Returns template id, name, category, description and node types.",
        {"category": Annotated[str, "Фильтр по категории (content/automation/moderation/monitoring)"]},
    )
    async def list_pipeline_templates(args):
        try:
            from src.services.pipeline_service import PipelineService

            svc = PipelineService(db)
            category = args.get("category") or None
            templates = await svc.list_templates(category=category)
            if not templates:
                return _text_response("Шаблонов не найдено.")
            lines = [f"Шаблоны пайплайнов ({len(templates)} шт.):"]
            for tpl in templates:
                node_types = ", ".join(n.type.value for n in tpl.template_json.nodes)
                builtin = " [builtin]" if tpl.is_builtin else ""
                lines.append(
                    f"- id={tpl.id}: [{tpl.category}] {tpl.name}{builtin}\n"
                    f"  {tpl.description}\n"
                    f"  Ноды: {node_types}"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения шаблонов: {e}")

    tools.append(list_pipeline_templates)

    @tool(
        "create_pipeline_from_template",
        "Create a new pipeline from a built-in template. "
        "Use list_pipeline_templates to find template_id. "
        "source_channel_ids and target_refs are comma-separated strings. Requires confirm=true.",
        {
            "template_id": Annotated[int, "ID шаблона из list_pipeline_templates"],
            "name": Annotated[str, "Название нового пайплайна"],
            "source_channel_ids": Annotated[str, "Telegram ID каналов-источников через запятую"],
            "target_refs": Annotated[str, "Цели публикации через запятую в формате phone|dialog_id"],
            "llm_model": Annotated[str, "LLM модель (опционально)"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
    )
    async def create_pipeline_from_template(args):
        gate = require_confirmation("создаст новый пайплайн из шаблона", args)
        if gate:
            return gate
        template_id = args.get("template_id")
        name = args.get("name", "").strip()
        if template_id is None or not name:
            return _text_response("Ошибка: template_id и name обязательны.")
        try:
            from src.services.pipeline_service import PipelineService, PipelineTargetRef

            svc = PipelineService(db)
            source_str = args.get("source_channel_ids", "")
            source_ids = [int(x.strip()) for x in source_str.split(",") if x.strip()]
            target_str = args.get("target_refs", "")
            target_refs = []
            for part in target_str.split(","):
                part = part.strip()
                if "|" not in part:
                    continue
                phone, dialog_id = part.split("|", 1)
                target_refs.append(PipelineTargetRef(phone=phone.strip(), dialog_id=int(dialog_id.strip())))

            overrides = {}
            if args.get("llm_model"):
                overrides["llm_model"] = args["llm_model"]

            pipeline_id = await svc.create_from_template(
                int(template_id),
                name=name,
                source_ids=source_ids,
                target_refs=target_refs,
                overrides=overrides,
            )
            return _text_response(f"Пайплайн '{name}' создан из шаблона (id={pipeline_id}).")
        except Exception as e:
            return _text_response(f"Ошибка создания пайплайна из шаблона: {e}")

    tools.append(create_pipeline_from_template)

    @tool(
        "ai_edit_pipeline",
        "Edit a pipeline's node-based JSON configuration using a natural language instruction. "
        "The LLM will modify the pipeline graph based on your instruction. Requires confirm=true.",
        {
            "pipeline_id": Annotated[int, "ID пайплайна из list_pipelines"],
            "instruction": Annotated[str, "Инструкция на естественном языке (например: добавь шаг генерации картинки)"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
    )
    async def ai_edit_pipeline(args):
        pipeline_id = args.get("pipeline_id")
        instruction = args.get("instruction", "").strip()
        if pipeline_id is None or not instruction:
            return _text_response("Ошибка: pipeline_id и instruction обязательны.")
        gate = require_confirmation(f"изменит конфигурацию пайплайна id={pipeline_id} через AI", args)
        if gate:
            return gate
        try:
            import json as _json

            from src.services.pipeline_service import PipelineService

            svc = PipelineService(db)
            result = await svc.edit_via_llm(int(pipeline_id), instruction, db, config=config)
            if result["ok"]:
                preview = _json.dumps(result["pipeline_json"], ensure_ascii=False, indent=2)[:800]
                return _text_response(
                    f"Пайплайн id={pipeline_id} обновлён через AI.\n\nОбновлённый JSON (превью):\n{preview}"
                )
            return _text_response(f"Ошибка AI-редактирования: {result['error']}")
        except Exception as e:
            return _text_response(f"Ошибка AI-редактирования: {e}")

    tools.append(ai_edit_pipeline)

    return tools
