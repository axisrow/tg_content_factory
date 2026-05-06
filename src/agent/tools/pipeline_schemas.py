from __future__ import annotations

from typing import Annotated

CONFIRM_ARG = Annotated[bool, "Установите true для подтверждения действия"]
PIPELINE_ID_ARG = Annotated[int, "ID пайплайна из list_pipelines"]
RUN_ID_ARG = Annotated[int, "ID генерации из list_pipeline_runs"]
LIMIT_ARG = Annotated[int, "Максимальное количество результатов"]

LIST_PIPELINES_SCHEMA = {"active_only": Annotated[bool, "Показывать только активные"]}
GET_PIPELINE_DETAIL_SCHEMA = {"pipeline_id": PIPELINE_ID_ARG}
GET_PIPELINE_QUEUE_SCHEMA = {"limit": LIMIT_ARG}
GET_REFINEMENT_STEPS_SCHEMA = {"pipeline_id": PIPELINE_ID_ARG}

ADD_PIPELINE_SCHEMA = {
    "name": Annotated[str, "Название пайплайна"],
    "prompt_template": Annotated[str, "Шаблон промпта для генерации контента"],
    "source_channel_ids": Annotated[str, "Telegram ID каналов-источников через запятую"],
    "target_refs": Annotated[str, "Цели публикации через запятую в формате phone|dialog_id"],
    "llm_model": Annotated[str, "Модель LLM для генерации (например claude-sonnet-4-20250514)"],
    "publish_mode": Annotated[str, "Режим публикации: auto или moderated"],
    "confirm": CONFIRM_ARG,
}

EDIT_PIPELINE_SCHEMA = {
    "pipeline_id": PIPELINE_ID_ARG,
    "name": Annotated[str, "Название пайплайна"],
    "prompt_template": Annotated[str, "Шаблон промпта для генерации контента"],
    "source_channel_ids": Annotated[str, "Telegram ID каналов-источников через запятую"],
    "target_refs": Annotated[str, "Цели публикации через запятую в формате phone|dialog_id"],
    "llm_model": Annotated[str, "Модель LLM для генерации (например claude-sonnet-4-20250514)"],
    "publish_mode": Annotated[str, "Режим публикации: auto или moderated"],
    "confirm": CONFIRM_ARG,
}

TOGGLE_PIPELINE_SCHEMA = {"pipeline_id": PIPELINE_ID_ARG}
DELETE_PIPELINE_SCHEMA = {"pipeline_id": PIPELINE_ID_ARG, "confirm": CONFIRM_ARG}

SET_REFINEMENT_STEPS_SCHEMA = {
    "pipeline_id": PIPELINE_ID_ARG,
    "steps_json": Annotated[str, "JSON-массив шагов: [{name, prompt}]"],
    "confirm": CONFIRM_ARG,
}

RUN_PIPELINE_SCHEMA = {"pipeline_id": PIPELINE_ID_ARG}

GENERATE_DRAFT_SCHEMA = {
    "query": Annotated[str, "Запрос для генерации черновика"],
    "pipeline_id": PIPELINE_ID_ARG,
    "limit": LIMIT_ARG,
}

LIST_PIPELINE_RUNS_SCHEMA = {
    "pipeline_id": PIPELINE_ID_ARG,
    "limit": LIMIT_ARG,
    "status": Annotated[str, "Фильтр по статусу (pending/completed/approved/rejected)"],
}

GET_PIPELINE_RUN_SCHEMA = {"run_id": RUN_ID_ARG}
PUBLISH_PIPELINE_RUN_SCHEMA = {"run_id": RUN_ID_ARG, "confirm": CONFIRM_ARG}

EXPORT_PIPELINE_JSON_SCHEMA = {"pipeline_id": PIPELINE_ID_ARG}

IMPORT_PIPELINE_JSON_SCHEMA = {
    "json_text": Annotated[str, "JSON строка с конфигурацией пайплайна"],
    "name_override": Annotated[str, "Переопределить имя пайплайна (опционально)"],
    "confirm": CONFIRM_ARG,
}

LIST_PIPELINE_TEMPLATES_SCHEMA = {
    "category": Annotated[str, "Фильтр по категории (content/automation/moderation/monitoring)"],
}

CREATE_PIPELINE_FROM_TEMPLATE_SCHEMA = {
    "template_id": Annotated[int, "ID шаблона из list_pipeline_templates"],
    "name": Annotated[str, "Название нового пайплайна"],
    "source_channel_ids": Annotated[str, "Telegram ID каналов-источников через запятую"],
    "target_refs": Annotated[str, "Цели публикации через запятую в формате phone|dialog_id"],
    "llm_model": Annotated[str, "LLM модель (опционально)"],
    "confirm": CONFIRM_ARG,
}

AI_EDIT_PIPELINE_SCHEMA = {
    "pipeline_id": PIPELINE_ID_ARG,
    "instruction": Annotated[str, "Инструкция на естественном языке (например: добавь шаг генерации картинки)"],
    "confirm": CONFIRM_ARG,
}
