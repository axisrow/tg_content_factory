from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

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


@dataclass(frozen=True)
class SchedulerSettingsForm:
    interval_minutes: int


@dataclass(frozen=True)
class SemanticSearchSettingsForm:
    provider: str
    model: str
    base_url: str
    api_key: str | None
    batch_size: int
    reset_index: bool


@dataclass(frozen=True)
class SemanticIndexForm:
    reset_index: bool


@dataclass(frozen=True)
class AgentToolPermissionsForm:
    phone: str | None
    permissions: dict[str, bool]


@dataclass(frozen=True)
class AgentSettingsForm:
    form_scope: str
    wants_dev_mode: bool
    disclaimer_accepted: bool
    backend_override: str | None
    prompt_template: str | None
    tool_permissions: AgentToolPermissionsForm | None


@dataclass(frozen=True)
class ProviderAddForm:
    provider: str


@dataclass(frozen=True)
class ProviderConfigForm:
    raw: FormMapping


@dataclass(frozen=True)
class FiltersForm:
    min_subscribers: str
    auto_delete_filtered: bool
    auto_delete_on_collect: bool


@dataclass(frozen=True)
class NotificationAccountForm:
    selected_phone: str


@dataclass(frozen=True)
class CredentialsForm:
    api_id: str
    api_hash: str


@dataclass(frozen=True)
class ImageProviderSaveForm:
    raw: FormMapping
    default_model: str


@dataclass(frozen=True)
class TranslationSettingsForm:
    provider: str
    model: str
    target_lang: str
    source_filter: str
    auto_on_collect: bool


@dataclass(frozen=True)
class TranslationRunForm:
    target_lang: str


def _text(form: FormMapping, key: str, default: object = "") -> str:
    return str(form.get(key, default)).strip()


def _checked(form: FormMapping, key: str) -> bool:
    return bool(form.get(key))


def parse_scheduler_form(form: FormMapping) -> SchedulerSettingsForm:
    try:
        interval = int(form.get("collect_interval_minutes", 60))
    except (TypeError, ValueError) as exc:
        raise SettingsFormError("invalid_value") from exc
    return SchedulerSettingsForm(interval_minutes=max(1, min(1440, interval)))


def parse_semantic_search_form(form: FormMapping) -> SemanticSearchSettingsForm:
    provider = _text(form, "semantic_embeddings_provider", DEFAULT_EMBEDDINGS_PROVIDER)
    model = _text(form, "semantic_embeddings_model", DEFAULT_EMBEDDINGS_MODEL)
    base_url = _text(form, "semantic_embeddings_base_url")
    api_key = _text(form, "semantic_embeddings_api_key") if "semantic_embeddings_api_key" in form else None
    reset_index = _text(form, "semantic_reset_index") == "1"
    if not provider or not model:
        raise SettingsFormError("semantic_invalid_value")
    try:
        batch_size = int(str(form.get("semantic_embeddings_batch_size", DEFAULT_EMBEDDINGS_BATCH_SIZE)))
    except (TypeError, ValueError) as exc:
        raise SettingsFormError("semantic_invalid_value") from exc
    return SemanticSearchSettingsForm(
        provider=provider,
        model=model,
        base_url=base_url,
        api_key=api_key,
        batch_size=max(1, min(1000, batch_size)),
        reset_index=reset_index,
    )


def parse_semantic_index_form(form: FormMapping) -> SemanticIndexForm:
    return SemanticIndexForm(reset_index=_text(form, "semantic_reset_index") == "1")


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
    return ProviderAddForm(provider=_text(form, "provider"))


def parse_provider_config_form(form: FormMapping) -> ProviderConfigForm:
    return ProviderConfigForm(raw=form)


def parse_filters_form(form: FormMapping) -> FiltersForm:
    min_subscribers = _text(form, "min_subscribers_filter", "0")
    if not min_subscribers.isdigit():
        raise SettingsFormError("invalid_value")
    return FiltersForm(
        min_subscribers=min_subscribers,
        auto_delete_filtered=_checked(form, "auto_delete_filtered"),
        auto_delete_on_collect=_checked(form, "auto_delete_on_collect"),
    )


def parse_notification_account_form(form: FormMapping) -> NotificationAccountForm:
    return NotificationAccountForm(selected_phone=_text(form, "notification_account_phone"))


def parse_credentials_form(form: FormMapping) -> CredentialsForm:
    return CredentialsForm(
        api_id=_text(form, "api_id"),
        api_hash=_text(form, "api_hash"),
    )


def parse_image_provider_save_form(form: FormMapping) -> ImageProviderSaveForm:
    return ImageProviderSaveForm(raw=form, default_model=_text(form, "default_image_model"))


def parse_translation_form(form: FormMapping) -> TranslationSettingsForm:
    return TranslationSettingsForm(
        provider=_text(form, "translation_provider"),
        model=_text(form, "translation_model"),
        target_lang=_text(form, "translation_target_lang").lower(),
        source_filter=_text(form, "translation_source_filter").lower(),
        auto_on_collect=_checked(form, "translation_auto_on_collect"),
    )


def parse_translation_run_form(form: FormMapping) -> TranslationRunForm:
    return TranslationRunForm(target_lang=_text(form, "target_lang", "en").lower() or "en")

