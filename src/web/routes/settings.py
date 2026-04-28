from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from src.web import deps
from src.web.settings.forms import (
    SettingsFormError,
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


def _settings_form_error_response(exc: SettingsFormError):
    return settings_flash_response(SettingsFlash(error=exc.code))


@router.get("/", response_class=HTMLResponse)
async def settings_page(request: Request):
    context = await handle_settings_page(request)
    return deps.get_templates(request).TemplateResponse(request, "settings.html", context)


@router.post("/save-scheduler")
async def save_scheduler_settings(request: Request):
    try:
        form = parse_scheduler_form(await request.form())
    except SettingsFormError as exc:
        return _settings_form_error_response(exc)
    return settings_flash_response(await handle_save_scheduler_settings(request, form))


@router.post("/save-semantic-search")
async def save_semantic_search_settings(request: Request):
    try:
        form = parse_semantic_search_form(await request.form())
    except SettingsFormError as exc:
        return _settings_form_error_response(exc)
    return settings_flash_response(await handle_save_semantic_search_settings(request, form))


@router.post("/semantic-index")
async def run_semantic_index(request: Request):
    form = parse_semantic_index_form(await request.form())
    return settings_flash_response(await handle_run_semantic_index(request, form))


@router.post("/save-agent")
async def save_agent_settings(request: Request):
    try:
        form = parse_agent_form(await request.form())
    except SettingsFormError as exc:
        return _settings_form_error_response(exc)
    return settings_flash_response(await handle_save_agent_settings(request, form))


@router.post("/agent-providers/add")
async def add_agent_provider(request: Request):
    form = parse_provider_add_form(await request.form())
    return settings_flash_response(await handle_add_agent_provider(request, form))


@router.post("/agent-providers/save")
async def save_agent_providers(request: Request):
    form = parse_provider_config_form(await request.form())
    return settings_flash_response(await handle_save_agent_providers(request, form))


@router.post("/agent-providers/{provider_name}/delete")
async def delete_agent_provider(request: Request, provider_name: str):
    return settings_flash_response(await handle_delete_agent_provider(request, provider_name))


@router.post("/agent-providers/{provider_name}/refresh")
async def refresh_agent_provider_models(request: Request, provider_name: str):
    form = parse_provider_config_form(await request.form())
    return settings_json_response(await handle_refresh_agent_provider_models(request, provider_name, form))


@router.post("/agent-providers/refresh-all")
async def refresh_all_agent_provider_models(request: Request):
    form = parse_provider_config_form(await request.form())
    return settings_json_response(await handle_refresh_all_agent_provider_models(request, form))


@router.post("/agent-providers/{provider_name}/probe")
async def probe_agent_provider_model(request: Request, provider_name: str):
    form = parse_provider_config_form(await request.form())
    return settings_json_response(await handle_probe_agent_provider_model(request, provider_name, form))


@router.post("/agent-providers/test-all")
async def test_all_agent_provider_models(request: Request):
    form = parse_provider_config_form(await request.form())
    return settings_json_response(await handle_test_all_agent_provider_models(request, form))


@router.get("/agent-providers/test-all/status")
async def test_all_agent_provider_models_status(request: Request):
    return settings_json_response(await handle_test_all_agent_provider_models_status(request))


@router.post("/save-filters")
async def save_filters(request: Request):
    try:
        form = parse_filters_form(await request.form())
    except SettingsFormError as exc:
        return _settings_form_error_response(exc)
    return settings_flash_response(await handle_save_filters(request, form))


@router.post("/save-notification-account")
async def save_notification_account(request: Request):
    form = parse_notification_account_form(await request.form())
    return settings_flash_response(await handle_save_notification_account(request, form))


@router.post("/save-credentials")
async def save_credentials(request: Request):
    form = parse_credentials_form(await request.form())
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
async def add_image_provider(request: Request):
    form = parse_provider_add_form(await request.form())
    return settings_flash_response(await handle_add_image_provider(request, form))


@router.post("/image-providers/save")
async def save_image_providers(request: Request):
    form = parse_image_provider_save_form(await request.form())
    return settings_flash_response(await handle_save_image_providers(request, form))


@router.post("/image-providers/{provider_name}/delete")
async def delete_image_provider(request: Request, provider_name: str):
    return settings_flash_response(await handle_delete_image_provider(request, provider_name))


@router.post("/save-translation")
async def save_translation_settings(request: Request):
    form = parse_translation_form(await request.form())
    return settings_flash_response(await handle_save_translation_settings(request, form))


@router.post("/translation-backfill")
async def translation_backfill_lang(request: Request):
    return settings_flash_response(await handle_translation_backfill_lang(request))


@router.post("/translation-run")
async def translation_run_batch(request: Request):
    form = parse_translation_run_form(await request.form())
    return settings_flash_response(await handle_translation_run_batch(request, form))


# Account toggle/delete endpoints live in src/web/routes/accounts.py.
