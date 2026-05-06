from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from types import UnionType
from typing import Any, TypeVar, Union, get_args, get_origin

from fastapi import Request
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter
from starlette.datastructures import UploadFile

from src.services.pipeline_filters import normalize_filter_config
from src.services.pipeline_refs import parse_pipeline_target_refs
from src.services.pipeline_service import PipelineTargetRef


class _FrozenForm(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True, str_strip_whitespace=True)


TForm = TypeVar("TForm", bound=BaseModel)


def validate_form_model(model: type[TForm], data: Mapping[str, object]) -> TForm:
    return TypeAdapter(model).validate_python(dict(data))


def _is_list_field(annotation: object) -> bool:
    return get_origin(annotation) is list


def _allows_none(annotation: object) -> bool:
    if annotation is None:
        return True
    origin = get_origin(annotation)
    if origin in {UnionType, Union}:
        return type(None) in get_args(annotation)
    return False


def _is_empty_upload(value: object) -> bool:
    return isinstance(value, UploadFile) and not value.filename


def _form_data_for_model(model: type[BaseModel], form: Mapping[str, Any]) -> dict[str, object]:
    data: dict[str, object] = {}
    getlist = getattr(form, "getlist", None)
    for name, field in model.model_fields.items():
        if name not in form:
            continue
        if _is_list_field(field.annotation):
            values = list(getlist(name) if getlist else [form[name]])
            data[name] = [value for value in values if not _is_empty_upload(value)]
            continue
        value = form[name]
        if _allows_none(field.annotation) and _is_empty_upload(value):
            data[name] = None
        else:
            data[name] = value
    return data


def form_model_dependency(model: type[TForm]) -> Callable[[Request], Awaitable[TForm]]:
    async def dependency(request: Request) -> TForm:
        form = await request.form()
        return validate_form_model(model, _form_data_for_model(model, form))

    return dependency


class CreateWizardForm(_FrozenForm):
    name: str = ""
    pipeline_json: str = ""
    source_channel_ids: list[int] = Field(default_factory=list)
    target_refs: list[str] = Field(default_factory=list)
    generate_interval_minutes: int = 60
    is_active: str = ""
    run_after: str = ""
    since_value: int = 24
    since_unit: str = "h"
    account_phone: str = ""


class PipelineCreateForm(_FrozenForm):
    name: str = ""
    prompt_template: str = ""
    source_channel_ids: list[int] = Field(default_factory=list)
    target_refs: list[str] = Field(default_factory=list)
    llm_model: str = ""
    image_model: str = ""
    publish_mode: str = "moderated"
    generation_backend: str = "chain"
    generate_interval_minutes: int = 60
    is_active: bool = False


class PipelineEditForm(_FrozenForm):
    name: str = ""
    prompt_template: str = ""
    source_channel_ids: list[int] = Field(default_factory=list)
    target_refs: list[str] = Field(default_factory=list)
    llm_model: str = ""
    image_model: str = ""
    publish_mode: str = "moderated"
    generation_backend: str = "chain"
    generate_interval_minutes: int = 60
    is_active: bool = False
    react_emoji: str = ""
    filter_present: str = ""
    filter_message_kinds: list[str] = Field(default_factory=list)
    filter_service_actions: list[str] = Field(default_factory=list)
    filter_media_types: list[str] = Field(default_factory=list)
    filter_sender_kinds: list[str] = Field(default_factory=list)
    filter_keywords: str = ""
    filter_regex: str = ""
    filter_has_text: str = ""
    dag_source_channel_ids: list[int] = Field(default_factory=list)
    account_phone: str = ""


class PipelineRunForm(_FrozenForm):
    since_value: int = 24
    since_unit: str = "h"


class PipelineGenerateForm(_FrozenForm):
    model: str = ""
    max_tokens: int = 256
    temperature: float = 0.0


class PipelinePublishForm(_FrozenForm):
    run_id: int | None = None


class PipelineTemplateCreateForm(_FrozenForm):
    template_id: int | None = None
    name: str = ""
    source_channel_ids: list[int] = Field(default_factory=list)
    target_refs: list[str] = Field(default_factory=list)
    llm_model: str = ""
    image_model: str = ""
    generate_interval_minutes: int = 60


class PipelineImportForm(_FrozenForm):
    json_file: UploadFile | None = None
    json_text: str = ""
    name_override: str = ""


def parse_target_refs(values: list[str]) -> list[PipelineTargetRef]:
    return parse_pipeline_target_refs(
        values,
        missing_separator_message="Некорректный формат цели pipeline.",
        invalid_dialog_id_message="Некорректный dialog id для pipeline target.",
    )


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
