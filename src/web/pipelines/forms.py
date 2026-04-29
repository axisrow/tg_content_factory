from __future__ import annotations

from dataclasses import dataclass

from fastapi import UploadFile

from src.services.pipeline_filters import normalize_filter_config
from src.services.pipeline_service import PipelineTargetRef, PipelineValidationError


@dataclass(frozen=True)
class CreateWizardForm:
    name: str
    pipeline_json: str
    source_channel_ids: list[int]
    target_refs: list[str]
    generate_interval_minutes: int
    is_active: str
    run_after: str
    since_value: int
    since_unit: str
    account_phone: str


@dataclass(frozen=True)
class PipelineCreateForm:
    name: str
    prompt_template: str
    source_channel_ids: list[int]
    target_refs: list[str]
    llm_model: str
    image_model: str
    publish_mode: str
    generation_backend: str
    generate_interval_minutes: int
    is_active: bool


@dataclass(frozen=True)
class PipelineEditForm:
    name: str
    prompt_template: str
    source_channel_ids: list[int]
    target_refs: list[str]
    llm_model: str
    image_model: str
    publish_mode: str
    generation_backend: str
    generate_interval_minutes: int
    is_active: bool
    react_emoji: str
    filter_present: str
    filter_message_kinds: list[str]
    filter_service_actions: list[str]
    filter_media_types: list[str]
    filter_sender_kinds: list[str]
    filter_keywords: str
    filter_regex: str
    filter_has_text: str
    dag_source_channel_ids: list[int]
    account_phone: str


@dataclass(frozen=True)
class PipelineRunForm:
    since_value: int
    since_unit: str


@dataclass(frozen=True)
class PipelineGenerateForm:
    model: str
    max_tokens: int
    temperature: float


@dataclass(frozen=True)
class PipelineTemplateCreateForm:
    template_id: int | None
    name: str
    source_channel_ids: list[int]
    target_refs: list[str]
    llm_model: str
    image_model: str
    generate_interval_minutes: int


@dataclass(frozen=True)
class PipelineImportForm:
    json_file: UploadFile | None
    json_text: str
    name_override: str


def parse_target_refs(values: list[str]) -> list[PipelineTargetRef]:
    refs: list[PipelineTargetRef] = []
    for value in values:
        phone, separator, raw_dialog_id = value.partition("|")
        if not separator:
            raise PipelineValidationError("Некорректный формат цели pipeline.")
        try:
            dialog_id = int(raw_dialog_id)
        except ValueError as exc:
            raise PipelineValidationError("Некорректный dialog id для pipeline target.") from exc
        refs.append(PipelineTargetRef(phone=phone, dialog_id=dialog_id))
    return refs


def get_filter_config(pipeline) -> dict | None:
    graph = getattr(pipeline, "pipeline_json", None)
    if graph is None:
        return None
    node = next((item for item in graph.nodes if item.type.value == "filter"), None)
    if node is None:
        return None
    return normalize_filter_config(node.config)


def build_filter_config_from_form(
    *,
    filter_present: str,
    filter_message_kinds: list[str],
    filter_service_actions: list[str],
    filter_media_types: list[str],
    filter_sender_kinds: list[str],
    filter_keywords: str,
    filter_regex: str,
    filter_has_text: str,
) -> dict | None:
    if not filter_present:
        return None
    keywords = [item.strip() for item in filter_keywords.splitlines() if item.strip()]
    return normalize_filter_config(
        {
            "type": "message_filter",
            "message_kinds": filter_message_kinds,
            "service_actions": filter_service_actions,
            "media_types": filter_media_types,
            "sender_kinds": filter_sender_kinds,
            "keywords": keywords,
            "regex": filter_regex.strip(),
            "has_text": True if filter_has_text else None,
        }
    )
