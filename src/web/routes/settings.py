from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from src.web import deps
from src.web.settings.forms import (
    AgentSettingsForm,
    CredentialsForm,
    FiltersForm,
    ImageProviderSaveForm,
    NotificationAccountForm,
    ProviderAddForm,
    ProviderConfigForm,
    SchedulerSettingsForm,
    SemanticIndexForm,
    SemanticSearchSettingsForm,
    SettingsFormError,
    TranslationRunForm,
    TranslationSettingsForm,
    parse_agent_form,
    parse_credentials_form,
    parse_filters_form,
    parse_image_provider_save_form,
    parse_notification_account_form,
    parse_provider_add_form,
    parse_provider_config_form,
    parse_scheduler_form,
    parse_semantic_index_form,
    parse_semantic_search_form,
    parse_translation_form,
    parse_translation_run_form,
    settings_form_dependency,
)
from src.web.settings.handlers import (
    handle_add_agent_provider,
    handle_add_image_provider,
    handle_delete_agent_provider,
    handle_delete_image_provider,
    handle_delete_notification_bot,
    handle_notification_bot_status,
    handle_probe_agent_provider_model,
    handle_refresh_agent_provider_models,
    handle_refresh_all_agent_provider_models,
    handle_run_semantic_index,
    handle_save_agent_providers,
    handle_save_agent_settings,
    handle_save_credentials,
    handle_save_filters,
    handle_save_image_providers,
    handle_save_notification_account,
    handle_save_scheduler_settings,
    handle_save_semantic_search_settings,
    handle_save_translation_settings,
    handle_settings_page,
    handle_setup_notification_bot,
    handle_test_all_agent_provider_models,
    handle_test_all_agent_provider_models_status,
    handle_test_notification,
    handle_translation_backfill_lang,
    handle_translation_run_batch,
)
from src.web.settings.responses import (
    SettingsFlash,
    settings_flash_response,
    settings_json_response,
    settings_result_response,
)

router = APIRouter()

SchedulerFormDep = Annotated[
    SchedulerSettingsForm | SettingsFormError,
    Depends(settings_form_dependency(parse_scheduler_form)),
]
SemanticSearchFormDep = Annotated[
    SemanticSearchSettingsForm | SettingsFormError,
    Depends(settings_form_dependency(parse_semantic_search_form)),
]
SemanticIndexFormDep = Annotated[
    SemanticIndexForm | SettingsFormError,
    Depends(settings_form_dependency(parse_semantic_index_form)),
]
AgentFormDep = Annotated[AgentSettingsForm | SettingsFormError, Depends(settings_form_dependency(parse_agent_form))]
ProviderAddFormDep = Annotated[
    ProviderAddForm | SettingsFormError,
    Depends(settings_form_dependency(parse_provider_add_form)),
]
ProviderConfigFormDep = Annotated[
    ProviderConfigForm | SettingsFormError,
    Depends(settings_form_dependency(parse_provider_config_form)),
]
FiltersFormDep = Annotated[FiltersForm | SettingsFormError, Depends(settings_form_dependency(parse_filters_form))]
NotificationAccountFormDep = Annotated[
    NotificationAccountForm | SettingsFormError,
    Depends(settings_form_dependency(parse_notification_account_form)),
]
CredentialsFormDep = Annotated[
    CredentialsForm | SettingsFormError,
    Depends(settings_form_dependency(parse_credentials_form)),
]
ImageProviderSaveFormDep = Annotated[
    ImageProviderSaveForm | SettingsFormError,
    Depends(settings_form_dependency(parse_image_provider_save_form)),
]
TranslationFormDep = Annotated[
    TranslationSettingsForm | SettingsFormError,
    Depends(settings_form_dependency(parse_translation_form)),
]
TranslationRunFormDep = Annotated[
    TranslationRunForm | SettingsFormError,
    Depends(settings_form_dependency(parse_translation_run_form)),
]


def _settings_form_error_response(exc: SettingsFormError):
    return settings_flash_response(SettingsFlash(error=exc.code))


def _form_error_response(form: object):
    if isinstance(form, SettingsFormError):
        return _settings_form_error_response(form)
    return None


@router.get("/", response_class=HTMLResponse)
async def settings_page(request: Request):
    context = await handle_settings_page(request)
    return deps.get_templates(request).TemplateResponse(request, "settings.html", context)


@router.post("/save-scheduler")
async def save_scheduler_settings(request: Request, form: SchedulerFormDep):
    if response := _form_error_response(form):
        return response
    return settings_flash_response(await handle_save_scheduler_settings(request, form))


@router.post("/save-semantic-search")
async def save_semantic_search_settings(request: Request, form: SemanticSearchFormDep):
    if response := _form_error_response(form):
        return response
    return settings_flash_response(await handle_save_semantic_search_settings(request, form))


@router.post("/semantic-index")
async def run_semantic_index(request: Request, form: SemanticIndexFormDep):
    if response := _form_error_response(form):
        return response
    return settings_flash_response(await handle_run_semantic_index(request, form))


@router.post("/save-agent")
async def save_agent_settings(request: Request, form: AgentFormDep):
    if response := _form_error_response(form):
        return response
    return settings_flash_response(await handle_save_agent_settings(request, form))


@router.post("/agent-providers/add")
async def add_agent_provider(request: Request, form: ProviderAddFormDep):
    if response := _form_error_response(form):
        return response
    return settings_flash_response(await handle_add_agent_provider(request, form))


@router.post("/agent-providers/save")
async def save_agent_providers(request: Request, form: ProviderConfigFormDep):
    if response := _form_error_response(form):
        return response
    return settings_flash_response(await handle_save_agent_providers(request, form))


@router.post("/agent-providers/{provider_name}/delete")
async def delete_agent_provider(request: Request, provider_name: str):
    return settings_flash_response(await handle_delete_agent_provider(request, provider_name))


@router.post("/agent-providers/{provider_name}/refresh")
async def refresh_agent_provider_models(request: Request, provider_name: str, form: ProviderConfigFormDep):
    if response := _form_error_response(form):
        return response
    return settings_json_response(await handle_refresh_agent_provider_models(request, provider_name, form))


@router.post("/agent-providers/refresh-all")
async def refresh_all_agent_provider_models(request: Request, form: ProviderConfigFormDep):
    if response := _form_error_response(form):
        return response
    return settings_json_response(await handle_refresh_all_agent_provider_models(request, form))


@router.post("/agent-providers/{provider_name}/probe")
async def probe_agent_provider_model(request: Request, provider_name: str, form: ProviderConfigFormDep):
    if response := _form_error_response(form):
        return response
    return settings_json_response(await handle_probe_agent_provider_model(request, provider_name, form))


@router.post("/agent-providers/test-all")
async def test_all_agent_provider_models(request: Request, form: ProviderConfigFormDep):
    if response := _form_error_response(form):
        return response
    return settings_json_response(await handle_test_all_agent_provider_models(request, form))


@router.get("/agent-providers/test-all/status")
async def test_all_agent_provider_models_status(request: Request):
    return settings_json_response(await handle_test_all_agent_provider_models_status(request))


@router.post("/save-filters")
async def save_filters(request: Request, form: FiltersFormDep):
    if response := _form_error_response(form):
        return response
    return settings_flash_response(await handle_save_filters(request, form))


@router.post("/save-notification-account")
async def save_notification_account(request: Request, form: NotificationAccountFormDep):
    if response := _form_error_response(form):
        return response
    return settings_flash_response(await handle_save_notification_account(request, form))


@router.post("/save-credentials")
async def save_credentials(request: Request, form: CredentialsFormDep):
    if response := _form_error_response(form):
        return response
    return settings_flash_response(await handle_save_credentials(request, form))


@router.post("/notifications/setup")
async def setup_notification_bot(request: Request):
    return settings_result_response(await handle_setup_notification_bot(request))


@router.get("/notifications/status")
async def notification_bot_status(request: Request):
    return settings_json_response(await handle_notification_bot_status(request))


@router.post("/notifications/delete")
async def delete_notification_bot(request: Request):
    return settings_result_response(await handle_delete_notification_bot(request))


@router.post("/notifications/test")
async def test_notification(request: Request):
    return settings_result_response(await handle_test_notification(request))


@router.post("/image-providers/add")
async def add_image_provider(request: Request, form: ProviderAddFormDep):
    if response := _form_error_response(form):
        return response
    return settings_flash_response(await handle_add_image_provider(request, form))


@router.post("/image-providers/save")
async def save_image_providers(request: Request, form: ImageProviderSaveFormDep):
    if response := _form_error_response(form):
        return response
    return settings_flash_response(await handle_save_image_providers(request, form))


@router.post("/image-providers/{provider_name}/delete")
async def delete_image_provider(request: Request, provider_name: str):
    return settings_flash_response(await handle_delete_image_provider(request, provider_name))


@router.post("/save-translation")
async def save_translation_settings(request: Request, form: TranslationFormDep):
    if response := _form_error_response(form):
        return response
    return settings_flash_response(await handle_save_translation_settings(request, form))


@router.post("/translation-backfill")
async def translation_backfill_lang(request: Request):
    return settings_flash_response(await handle_translation_backfill_lang(request))


@router.post("/translation-run")
async def translation_run_batch(request: Request, form: TranslationRunFormDep):
    if response := _form_error_response(form):
        return response
    return settings_flash_response(await handle_translation_run_batch(request, form))


# Account toggle/delete endpoints live in src/web/routes/accounts.py.
