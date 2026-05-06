from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any, TypeVar

from fastapi import Request
from pydantic import BaseModel, ConfigDict, ValidationError

from src.services.embedding_service import (
    DEFAULT_EMBEDDINGS_BATCH_SIZE,
    DEFAULT_EMBEDDINGS_MODEL,
    DEFAULT_EMBEDDINGS_PROVIDER,
)

CREDENTIALS_MASK = "••••••••"


class SettingsFormError(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


FormMapping = Mapping[str, Any]
TForm = TypeVar("TForm", bound=BaseModel)
SettingsFormResult = TForm | SettingsFormError


class _FrozenForm(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True, str_strip_whitespace=True)


class SchedulerSettingsForm(_FrozenForm):
    interval_minutes: int


class SemanticSearchSettingsForm(_FrozenForm):
    provider: str
    model: str
    base_url: str
    api_key: str | None = None
    batch_size: int
    reset_index: bool


class SemanticIndexForm(_FrozenForm):
    reset_index: bool


class AgentToolPermissionsForm(_FrozenForm):
    phone: str | None
    permissions: dict[str, bool]


class AgentSettingsForm(_FrozenForm):
    form_scope: str
    wants_dev_mode: bool
    disclaimer_accepted: bool
    backend_override: str | None
    prompt_template: str | None
    tool_permissions: AgentToolPermissionsForm | None


class ProviderAddForm(_FrozenForm):
    provider: str


class ProviderConfigForm(_FrozenForm):
    raw: FormMapping


class FiltersForm(_FrozenForm):
    min_subscribers: str
    auto_delete_filtered: bool
    auto_delete_on_collect: bool


class NotificationAccountForm(_FrozenForm):
    selected_phone: str


class CredentialsForm(_FrozenForm):
    api_id: str
    api_hash: str


class ImageProviderSaveForm(_FrozenForm):
    raw: FormMapping
    default_model: str


class TranslationSettingsForm(_FrozenForm):
    provider: str
    model: str
    target_lang: str
    source_filter: str
    auto_on_collect: bool


class TranslationRunForm(_FrozenForm):
    target_lang: str


def settings_form_dependency(
    parser: Callable[[FormMapping], TForm],
) -> Callable[[Request], Awaitable[TForm | SettingsFormError]]:
    async def dependency(request: Request) -> TForm | SettingsFormError:
        try:
            return parser(await request.form())
        except SettingsFormError as exc:
            return exc

    return dependency


def _text(form: FormMapping, key: str, default: object = "") -> str:
    return str(form.get(key, default)).strip()


def _checked(form: FormMapping, key: str) -> bool:
    return bool(form.get(key))


def _validate_strings(model: type[TForm], data: Mapping[str, object], code: str = "invalid_value") -> TForm:
    try:
        return model.model_validate_strings({key: str(value) for key, value in data.items() if value is not None})
    except ValidationError as exc:
        raise SettingsFormError(code) from exc


def parse_scheduler_form(form: FormMapping) -> SchedulerSettingsForm:
    parsed = _validate_strings(
        SchedulerSettingsForm,
        {"interval_minutes": form.get("collect_interval_minutes", 60)},
    )
    return parsed.model_copy(update={"interval_minutes": max(1, min(1440, parsed.interval_minutes))})


def parse_semantic_search_form(form: FormMapping) -> SemanticSearchSettingsForm:
    provider = _text(form, "semantic_embeddings_provider", DEFAULT_EMBEDDINGS_PROVIDER)
    model = _text(form, "semantic_embeddings_model", DEFAULT_EMBEDDINGS_MODEL)
    base_url = _text(form, "semantic_embeddings_base_url")
    api_key = _text(form, "semantic_embeddings_api_key") if "semantic_embeddings_api_key" in form else None
    reset_index = _text(form, "semantic_reset_index") == "1"
    if not provider or not model:
        raise SettingsFormError("semantic_invalid_value")
    parsed = _validate_strings(
        SemanticSearchSettingsForm,
        {
            "provider": provider,
            "model": model,
            "base_url": base_url,
            "api_key": api_key,
            "batch_size": form.get("semantic_embeddings_batch_size", DEFAULT_EMBEDDINGS_BATCH_SIZE),
            "reset_index": reset_index,
        },
        code="semantic_invalid_value",
    )
    return parsed.model_copy(update={"batch_size": max(1, min(1000, parsed.batch_size))})


def parse_semantic_index_form(form: FormMapping) -> SemanticIndexForm:
    return _validate_strings(SemanticIndexForm, {"reset_index": _text(form, "semantic_reset_index") == "1"})


def parse_agent_form(form: FormMapping) -> AgentSettingsForm:
    form_scope = _text(form, "agent_form_scope", "dev_mode")
    backend_override = _text(form, "agent_backend_override") if "agent_backend_override" in form else None
    tool_permissions = None
    if form_scope == "tool_permissions":
        from src.agent.tools.permissions import TOOL_CATEGORIES

        permissions = {
            tool_name: form.get(f"tool_perm__{tool_name}") == "1"
            for tool_name in TOOL_CATEGORIES
        }
        phone = _text(form, "phone") or None
        tool_permissions = AgentToolPermissionsForm(phone=phone, permissions=permissions)

    return AgentSettingsForm(
        form_scope=form_scope,
        wants_dev_mode=_text(form, "agent_dev_mode_enabled") == "1",
        disclaimer_accepted=_text(form, "agent_dev_mode_disclaimer") == "1",
        backend_override=backend_override,
        prompt_template=str(form.get("agent_prompt_template") or "") if form_scope == "prompt_template" else None,
        tool_permissions=tool_permissions,
    )


def parse_provider_add_form(form: FormMapping) -> ProviderAddForm:
    return _validate_strings(ProviderAddForm, {"provider": _text(form, "provider")})


def parse_provider_config_form(form: FormMapping) -> ProviderConfigForm:
    return ProviderConfigForm(raw=form)


def parse_filters_form(form: FormMapping) -> FiltersForm:
    min_subscribers = _text(form, "min_subscribers_filter", "0")
    if not min_subscribers.isdigit():
        raise SettingsFormError("invalid_value")
    return _validate_strings(
        FiltersForm,
        {
            "min_subscribers": min_subscribers,
            "auto_delete_filtered": _checked(form, "auto_delete_filtered"),
            "auto_delete_on_collect": _checked(form, "auto_delete_on_collect"),
        },
    )


def parse_notification_account_form(form: FormMapping) -> NotificationAccountForm:
    return _validate_strings(NotificationAccountForm, {"selected_phone": _text(form, "notification_account_phone")})


def parse_credentials_form(form: FormMapping) -> CredentialsForm:
    return _validate_strings(CredentialsForm, {"api_id": _text(form, "api_id"), "api_hash": _text(form, "api_hash")})


def parse_image_provider_save_form(form: FormMapping) -> ImageProviderSaveForm:
    return ImageProviderSaveForm(raw=form, default_model=_text(form, "default_image_model"))


def parse_translation_form(form: FormMapping) -> TranslationSettingsForm:
    return _validate_strings(
        TranslationSettingsForm,
        {
            "provider": _text(form, "translation_provider"),
            "model": _text(form, "translation_model"),
            "target_lang": _text(form, "translation_target_lang").lower(),
            "source_filter": _text(form, "translation_source_filter").lower(),
            "auto_on_collect": _checked(form, "translation_auto_on_collect"),
        },
    )


def parse_translation_run_form(form: FormMapping) -> TranslationRunForm:
    return _validate_strings(TranslationRunForm, {"target_lang": _text(form, "target_lang", "en").lower() or "en"})
