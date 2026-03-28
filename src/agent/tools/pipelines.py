"""Agent tools for content pipeline management."""

from __future__ import annotations

import logging

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._registry import _text_response, require_confirmation, require_pool

logger = logging.getLogger(__name__)


def register(db, client_pool, embedding_service, **kwargs):
    config = kwargs.get("config")
    tools = []

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
        {"active_only": bool},
    )
    async def list_pipelines(args):
        try:
            from src.services.pipeline_service import PipelineService

            svc = PipelineService(db)
            active_only = bool(args.get("active_only", False))
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
        {"pipeline_id": int},
    )
    async def get_pipeline_detail(args):
        pipeline_id = args.get("pipeline_id")
        if pipeline_id is None:
            return _text_response("Ошибка: pipeline_id обязателен.")
        try:
            from src.services.pipeline_service import PipelineService

            svc = PipelineService(db)
            detail = await svc.get_detail(int(pipeline_id))
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

    # ------------------------------------------------------------------
    # Write tools
    # ------------------------------------------------------------------

    @tool(
        "add_pipeline",
        "Create a new content pipeline. source_channel_ids = comma-separated Telegram channel IDs "
        "(from list_channels). target_refs = comma-separated 'phone|dialog_id' pairs. "
        "publish_mode: 'auto' or 'moderated'. Requires confirm=true.",
        {
            "name": str,
            "prompt_template": str,
            "source_channel_ids": str,
            "target_refs": str,
            "llm_model": str,
            "publish_mode": str,
            "confirm": bool,
        },
    )
    async def add_pipeline(args):
        gate = require_confirmation("создаст новый пайплайн", args)
        if gate:
            return gate
        try:
            from src.services.pipeline_service import PipelineService, PipelineTargetRef

            name = args.get("name", "").strip()
            prompt_template = args.get("prompt_template", "").strip()
            source_str = args.get("source_channel_ids", "")
            target_str = args.get("target_refs", "")
            if not name or not prompt_template or not source_str or not target_str:
                return _text_response(
                    "Ошибка: name, prompt_template, source_channel_ids и target_refs обязательны."
                )
            source_ids = [int(x.strip()) for x in source_str.split(",") if x.strip()]
            target_refs = []
            for part in target_str.split(","):
                part = part.strip()
                if "|" not in part:
                    return _text_response(f"Неверный формат target_ref: '{part}'. Ожидается 'phone|dialog_id'.")
                phone, dialog_id = part.split("|", 1)
                target_refs.append(PipelineTargetRef(phone=phone.strip(), dialog_id=int(dialog_id.strip())))

            svc = PipelineService(db)
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
        except Exception as e:
            return _text_response(f"Ошибка создания пайплайна: {e}")

    tools.append(add_pipeline)

    @tool(
        "edit_pipeline",
        "Edit an existing pipeline. All fields are optional except pipeline_id. "
        "source_channel_ids and target_refs are comma-separated strings. Requires confirm=true.",
        {
            "pipeline_id": int,
            "name": str,
            "prompt_template": str,
            "source_channel_ids": str,
            "target_refs": str,
            "llm_model": str,
            "publish_mode": str,
            "confirm": bool,
        },
    )
    async def edit_pipeline(args):
        gate = require_confirmation("изменит настройки пайплайна", args)
        if gate:
            return gate
        pipeline_id = args.get("pipeline_id")
        if pipeline_id is None:
            return _text_response("Ошибка: pipeline_id обязателен.")
        try:
            from src.services.pipeline_service import PipelineService, PipelineTargetRef

            svc = PipelineService(db)
            existing = await svc.get_detail(int(pipeline_id))
            if existing is None:
                return _text_response(f"Пайплайн id={pipeline_id} не найден.")
            p = existing["pipeline"]

            name = args.get("name", p.name).strip()
            prompt_template = args.get("prompt_template", p.prompt_template).strip()
            llm_model = args.get("llm_model", p.llm_model)
            publish_mode = args.get("publish_mode", getattr(p.publish_mode, "value", p.publish_mode))

            source_str = args.get("source_channel_ids")
            if source_str:
                source_ids = [int(x.strip()) for x in source_str.split(",") if x.strip()]
            else:
                source_ids = existing["source_ids"]

            target_str = args.get("target_refs")
            if target_str:
                target_refs = []
                for part in target_str.split(","):
                    part = part.strip()
                    if "|" not in part:
                        return _text_response(
                            f"Неверный формат target_ref: '{part}'. Ожидается 'phone|dialog_id'."
                        )
                    phone, dialog_id = part.split("|", 1)
                    target_refs.append(PipelineTargetRef(phone=phone.strip(), dialog_id=int(dialog_id.strip())))
            else:
                target_refs = [
                    PipelineTargetRef(phone=t.phone, dialog_id=t.dialog_id) for t in existing["targets"]
                ]

            ok = await svc.update(
                int(pipeline_id),
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
        except Exception as e:
            return _text_response(f"Ошибка редактирования пайплайна: {e}")

    tools.append(edit_pipeline)

    @tool(
        "toggle_pipeline",
        "Toggle pipeline active/inactive status.",
        {"pipeline_id": int},
    )
    async def toggle_pipeline(args):
        pipeline_id = args.get("pipeline_id")
        if pipeline_id is None:
            return _text_response("Ошибка: pipeline_id обязателен.")
        try:
            from src.services.pipeline_service import PipelineService

            svc = PipelineService(db)
            ok = await svc.toggle(int(pipeline_id))
            if not ok:
                return _text_response(f"Пайплайн id={pipeline_id} не найден.")
            pipeline = await svc.get(int(pipeline_id))
            status = "активирован" if pipeline and pipeline.is_active else "деактивирован"
            name = pipeline.name if pipeline else f"id={pipeline_id}"
            return _text_response(f"Пайплайн '{name}' {status}.")
        except Exception as e:
            return _text_response(f"Ошибка переключения пайплайна: {e}")

    tools.append(toggle_pipeline)

    @tool(
        "delete_pipeline",
        "⚠️ DANGEROUS: Delete a content pipeline permanently. Requires confirm=true.",
        {"pipeline_id": int, "confirm": bool},
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def delete_pipeline(args):
        pipeline_id = args.get("pipeline_id")
        if pipeline_id is None:
            return _text_response("Ошибка: pipeline_id обязателен.")
        try:
            from src.services.pipeline_service import PipelineService

            svc = PipelineService(db)
            pipeline = await svc.get(int(pipeline_id))
            name = pipeline.name if pipeline else f"id={pipeline_id}"
            gate = require_confirmation(f"безвозвратно удалит пайплайн '{name}'", args)
            if gate:
                return gate
            await svc.delete(int(pipeline_id))
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
        {"pipeline_id": int},
    )
    async def run_pipeline(args):
        pipeline_id = args.get("pipeline_id")
        if pipeline_id is None:
            return _text_response("Ошибка: pipeline_id обязателен.")
        try:
            from src.search.engine import SearchEngine
            from src.services.content_generation_service import ContentGenerationService
            from src.services.pipeline_service import PipelineService

            svc = PipelineService(db)
            pipeline = await svc.get(int(pipeline_id))
            if pipeline is None:
                return _text_response(f"Пайплайн id={pipeline_id} не найден.")
            if not pipeline.is_active:
                return _text_response(f"Пайплайн '{pipeline.name}' неактивен.")

            engine = SearchEngine(db, config=config)
            image_service = await _build_image_service()
            gen_svc = ContentGenerationService(db, engine, image_service=image_service)
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
        {"query": str, "pipeline_id": int, "limit": int},
    )
    async def generate_draft(args):
        query = args.get("query", "")
        pipeline_id = args.get("pipeline_id")
        limit = int(args.get("limit", 8))
        try:
            from src.search.engine import SearchEngine
            from src.services.generation_service import GenerationService
            from src.services.pipeline_service import PipelineService
            from src.services.provider_service import AgentProviderService

            engine = SearchEngine(db, config=config)
            prompt_template = None
            llm_model = None
            if pipeline_id is not None:
                svc = PipelineService(db)
                pipeline = await svc.get(int(pipeline_id))
                if pipeline is not None:
                    prompt_template = pipeline.prompt_template
                    llm_model = pipeline.llm_model
                    if not query:
                        query = prompt_template or pipeline.name or ""
            provider_service = AgentProviderService(db)
            provider_callable = provider_service.get_provider_callable(llm_model)

            gen = GenerationService(engine, provider_callable=provider_callable)
            result = await gen.generate(query=query, limit=limit, prompt_template=prompt_template)
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
        {"pipeline_id": int, "limit": int, "status": str},
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
                lines.append(
                    f"- run_id={r.id}, status={r.status}, moderation={r.moderation_status}, "
                    f"created={r.created_at}: {preview}"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения генераций: {e}")

    tools.append(list_pipeline_runs)

    @tool(
        "get_pipeline_run",
        "Get full details of a specific generation run including generated text, status, and quality score.",
        {"run_id": int},
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
                f"  Создан: {run.created_at}",
                f"  Обновлён: {run.updated_at}",
                "",
                "Текст:",
                run.generated_text or "(пусто)",
            ]
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения run: {e}")

    tools.append(get_pipeline_run)

    @tool(
        "publish_pipeline_run",
        "Publish a generation run to its pipeline target channels. "
        "Approve the run first via approve_run. run_id from list_pipeline_runs. "
        "Requires Telegram client and confirm=true.",
        {"run_id": int, "confirm": bool},
    )
    async def publish_pipeline_run(args):
        gate = require_pool(client_pool, "Публикация контента")
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

    return tools
