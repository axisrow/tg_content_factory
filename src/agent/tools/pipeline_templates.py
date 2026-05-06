from __future__ import annotations

import json
from typing import Any

from claude_agent_sdk import tool

from src.agent.tools._pipeline_runtime import parse_agent_target_refs
from src.agent.tools._registry import _text_response, require_confirmation
from src.agent.tools.pipeline_schemas import (
    AI_EDIT_PIPELINE_SCHEMA,
    CREATE_PIPELINE_FROM_TEMPLATE_SCHEMA,
    EXPORT_PIPELINE_JSON_SCHEMA,
    IMPORT_PIPELINE_JSON_SCHEMA,
    LIST_PIPELINE_TEMPLATES_SCHEMA,
)


def register_pipeline_template_tools(db: Any, config: Any) -> list[Any]:
    tools: list[Any] = []

    @tool(
        "export_pipeline_json",
        "Export a pipeline as a JSON dict (the node-based DAG config and legacy fields). "
        "Use this to inspect the full pipeline structure or to save it for re-import.",
        EXPORT_PIPELINE_JSON_SCHEMA,
    )
    async def export_pipeline_json(args):
        pipeline_id = args.get("pipeline_id")
        if pipeline_id is None:
            return _text_response("Ошибка: pipeline_id обязателен.")
        try:
            from src.services.pipeline_service import PipelineService

            svc = PipelineService(db)
            data = await svc.export_json(int(pipeline_id))
            if data is None:
                return _text_response(f"Пайплайн id={pipeline_id} не найден.")
            return _text_response(json.dumps(data, ensure_ascii=False, indent=2))
        except Exception as exc:
            return _text_response(f"Ошибка экспорта пайплайна: {exc}")

    tools.append(export_pipeline_json)

    @tool(
        "import_pipeline_json",
        "Create a new pipeline by importing a JSON dict (previously exported via export_pipeline_json). "
        "Pass the JSON as a string. Requires confirm=true.",
        IMPORT_PIPELINE_JSON_SCHEMA,
    )
    async def import_pipeline_json(args):
        gate = require_confirmation("создаст новый пайплайн из JSON", args)
        if gate:
            return gate
        json_text = args.get("json_text", "").strip()
        if not json_text:
            return _text_response("Ошибка: json_text обязателен.")
        try:
            from src.services.pipeline_service import PipelineService

            data = json.loads(json_text)
            svc = PipelineService(db)
            name_override = args.get("name_override") or None
            pipeline_id = await svc.import_json(data, name_override=name_override)
            return _text_response(f"Пайплайн импортирован (id={pipeline_id}).")
        except Exception as exc:
            return _text_response(f"Ошибка импорта пайплайна: {exc}")

    tools.append(import_pipeline_json)

    @tool(
        "list_pipeline_templates",
        "List all available pipeline templates (built-in and custom). "
        "Returns template id, name, category, description and node types.",
        LIST_PIPELINE_TEMPLATES_SCHEMA,
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
            for template in templates:
                node_types = ", ".join(node.type.value for node in template.template_json.nodes)
                builtin = " [builtin]" if template.is_builtin else ""
                lines.append(
                    f"- id={template.id}: [{template.category}] {template.name}{builtin}\n"
                    f"  {template.description}\n"
                    f"  Ноды: {node_types}"
                )
            return _text_response("\n".join(lines))
        except Exception as exc:
            return _text_response(f"Ошибка получения шаблонов: {exc}")

    tools.append(list_pipeline_templates)

    @tool(
        "create_pipeline_from_template",
        "Create a new pipeline from a built-in template. "
        "Use list_pipeline_templates to find template_id. "
        "source_channel_ids and target_refs are comma-separated strings. Requires confirm=true.",
        CREATE_PIPELINE_FROM_TEMPLATE_SCHEMA,
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
            from src.services.pipeline_service import PipelineService

            svc = PipelineService(db)
            source_str = args.get("source_channel_ids", "")
            source_ids = [int(item.strip()) for item in source_str.split(",") if item.strip()]
            target_str = args.get("target_refs", "")
            target_parts = [part.strip() for part in target_str.split(",") if "|" in part]
            target_refs = parse_agent_target_refs(",".join(target_parts)) if target_parts else []

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
        except Exception as exc:
            return _text_response(f"Ошибка создания пайплайна из шаблона: {exc}")

    tools.append(create_pipeline_from_template)

    @tool(
        "ai_edit_pipeline",
        "Edit a pipeline's node-based JSON configuration using a natural language instruction. "
        "The LLM will modify the pipeline graph based on your instruction. Requires confirm=true.",
        AI_EDIT_PIPELINE_SCHEMA,
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
            from src.services.pipeline_service import PipelineService

            svc = PipelineService(db)
            result = await svc.edit_via_llm(int(pipeline_id), instruction, db, config=config)
            if result["ok"]:
                preview = json.dumps(result["pipeline_json"], ensure_ascii=False, indent=2)[:800]
                return _text_response(
                    f"Пайплайн id={pipeline_id} обновлён через AI.\n\nОбновлённый JSON (превью):\n{preview}"
                )
            return _text_response(f"Ошибка AI-редактирования: {result['error']}")
        except Exception as exc:
            return _text_response(f"Ошибка AI-редактирования: {exc}")

    tools.append(ai_edit_pipeline)
    return tools
