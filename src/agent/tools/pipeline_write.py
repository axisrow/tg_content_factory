from __future__ import annotations

import json
from typing import Any

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._pipeline_runtime import parse_agent_target_refs
from src.agent.tools._registry import (
    ToolInputError,
    _text_response,
    arg_csv_ints,
    arg_int,
    arg_str,
    require_confirmation,
)
from src.agent.tools.pipeline_schemas import (
    ADD_PIPELINE_SCHEMA,
    DELETE_PIPELINE_SCHEMA,
    EDIT_PIPELINE_SCHEMA,
    SET_REFINEMENT_STEPS_SCHEMA,
    TOGGLE_PIPELINE_SCHEMA,
)


def register_pipeline_write_tools(db: Any, ctx: Any) -> list[Any]:
    tools: list[Any] = []

    @tool(
        "add_pipeline",
        "Create a new content pipeline. source_channel_ids = comma-separated Telegram channel IDs "
        "(from list_channels). target_refs = comma-separated 'phone|dialog_id' pairs. "
        "publish_mode: 'auto' or 'moderated'. Requires confirm=true.",
        ADD_PIPELINE_SCHEMA,
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
            target_refs = parse_agent_target_refs(target_str)

            svc = ctx.pipeline_service()
            pipeline_id = await svc.add(
                name=name,
                prompt_template=prompt_template,
                source_channel_ids=source_ids,
                target_refs=target_refs,
                llm_model=args.get("llm_model"),
                publish_mode=args.get("publish_mode", "moderated"),
            )
            return _text_response(f"Пайплайн '{name}' создан (id={pipeline_id}).")
        except ToolInputError as exc:
            return exc.to_response()
        except Exception as exc:
            return _text_response(f"Ошибка создания пайплайна: {exc}")

    tools.append(add_pipeline)

    @tool(
        "edit_pipeline",
        "Edit an existing pipeline. All fields are optional except pipeline_id. "
        "source_channel_ids and target_refs are comma-separated strings. Requires confirm=true.",
        EDIT_PIPELINE_SCHEMA,
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
            pipeline = existing["pipeline"]

            name = args.get("name", pipeline.name).strip()
            prompt_template = args.get("prompt_template", pipeline.prompt_template).strip()
            llm_model = args.get("llm_model", pipeline.llm_model)
            publish_mode = args.get("publish_mode", getattr(pipeline.publish_mode, "value", pipeline.publish_mode))

            source_str = args.get("source_channel_ids")
            source_ids = (
                arg_csv_ints(args, "source_channel_ids", required=True)
                if source_str
                else existing["source_ids"]
            )

            target_str = args.get("target_refs")
            if target_str:
                target_refs = parse_agent_target_refs(target_str)
            else:
                target_refs = [
                    PipelineTargetRef(phone=target.phone, dialog_id=target.dialog_id)
                    for target in existing["targets"]
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
        except ToolInputError as exc:
            return exc.to_response()
        except Exception as exc:
            return _text_response(f"Ошибка редактирования пайплайна: {exc}")

    tools.append(edit_pipeline)

    @tool(
        "toggle_pipeline",
        "Toggle pipeline active/inactive status.",
        TOGGLE_PIPELINE_SCHEMA,
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
        except Exception as exc:
            return _text_response(f"Ошибка переключения пайплайна: {exc}")

    tools.append(toggle_pipeline)

    @tool(
        "delete_pipeline",
        "⚠️ DANGEROUS: Delete a content pipeline permanently. Requires confirm=true.",
        DELETE_PIPELINE_SCHEMA,
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
        except Exception as exc:
            return _text_response(f"Ошибка удаления пайплайна: {exc}")

    tools.append(delete_pipeline)
    return tools


def register_refinement_write_tools(db: Any) -> list[Any]:
    tools: list[Any] = []

    @tool(
        "set_refinement_steps",
        "⚠️ Set the refinement (post-processing) steps for a pipeline. "
        "steps_json is a JSON array: [{\"name\": \"Step name\", \"prompt\": \"...{text}...\"}]. "
        "Pass an empty array to clear all steps. Requires confirm=true.",
        SET_REFINEMENT_STEPS_SCHEMA,
    )
    async def set_refinement_steps(args):
        pipeline_id = args.get("pipeline_id")
        if pipeline_id is None:
            return _text_response("Ошибка: pipeline_id обязателен.")
        gate = require_confirmation(f"обновит шаги рефайнмента пайплайна id={pipeline_id}", args)
        if gate:
            return gate
        try:
            raw = args.get("steps_json", "[]")
            steps = json.loads(raw)
            if not isinstance(steps, list):
                return _text_response("Ошибка: steps_json должен быть JSON-массивом.")
            validated = [
                {"name": str(step.get("name", "")).strip(), "prompt": str(step.get("prompt", "")).strip()}
                for step in steps
                if isinstance(step, dict) and str(step.get("prompt", "")).strip()
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
        except json.JSONDecodeError as exc:
            return _text_response(f"Ошибка парсинга steps_json: {exc}")
        except Exception as exc:
            return _text_response(f"Ошибка обновления шагов рефайнмента: {exc}")

    tools.append(set_refinement_steps)
    return tools
