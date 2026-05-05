from __future__ import annotations

import asyncio
import logging
import os
from datetime import UTC, datetime
from types import SimpleNamespace

from fastapi import Request

from src.agent.manager import AgentManager
from src.agent.prompt_template import (
    AGENT_PROMPT_TEMPLATE_SETTING,
    ALLOWED_TEMPLATE_VARIABLES,
    DEFAULT_AGENT_PROMPT_TEMPLATE,
    PromptTemplateError,
    validate_prompt_template,
)
from src.agent.provider_registry import PROVIDER_ORDER, ProviderRuntimeConfig
from src.agent.provider_registry import provider_spec as deepagents_provider_spec
from src.models import AccountSessionStatus
from src.services.agent_provider_service import (
    ProviderConfigService,
    ProviderModelCacheEntry,
    ProviderModelCompatibilityRecord,
)
from src.services.embedding_service import (
    DEFAULT_EMBEDDINGS_BATCH_SIZE,
    DEFAULT_EMBEDDINGS_MODEL,
    DEFAULT_EMBEDDINGS_PROVIDER,
    EMBEDDINGS_API_KEY_SETTING,
    EMBEDDINGS_BASE_URL_SETTING,
    EMBEDDINGS_BATCH_SIZE_SETTING,
    EMBEDDINGS_MODEL_SETTING,
    EMBEDDINGS_PROVIDER_SETTING,
    LAST_EMBEDDED_ID_SETTING,
    EmbeddingService,
)
from src.services.image_provider_service import (
    IMAGE_PROVIDER_ORDER,
    IMAGE_PROVIDER_SPECS,
    ImageProviderService,
    image_provider_spec,
)
from src.settings_utils import parse_int_setting
from src.utils.datetime import parse_datetime
from src.web import deps
from src.web.settings.forms import (
    CREDENTIALS_MASK,
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
    TranslationRunForm,
    TranslationSettingsForm,
)
from src.web.settings.responses import SettingsFlash, SettingsJson

logger = logging.getLogger("src.web.routes.settings")


def _image_provider_service(request: Request) -> ImageProviderService:
    return ImageProviderService(deps.get_db(request), request.app.state.config)


def _wants_json(request: Request) -> bool:
    return "application/json" in request.headers.get("accept", "")


def _agent_provider_service(request: Request) -> ProviderConfigService:
    return ProviderConfigService(deps.get_db(request), request.app.state.config)


async def _notification_snapshot_payload(request: Request) -> dict[str, object]:
    snapshot = await deps.get_db(request).repos.runtime_snapshots.get_snapshot("notification_target_status")
    payload = snapshot.payload if snapshot is not None else {}
    return payload if isinstance(payload, dict) else {}


async def _notification_snapshot_bot(request: Request):
    payload = await _notification_snapshot_payload(request)
    raw_bot = payload.get("bot")
    if not isinstance(raw_bot, dict) or not raw_bot.get("configured"):
        return None
    return SimpleNamespace(
        bot_username=raw_bot.get("bot_username"),
        bot_id=raw_bot.get("bot_id"),
        created_at=(
            parse_datetime(raw_bot["created_at"])
            if isinstance(raw_bot.get("created_at"), str)
            else None
        ),
    )


async def _enqueue_notification_command(
    request: Request,
    command_type: str,
    *,
    requested_by: str,
    redirect_code: str,
    payload: dict[str, object] | None = None,
) -> SettingsFlash | SettingsJson:
    command_id = await deps.telegram_command_service(request).enqueue(
        command_type,
        payload=payload or {},
        requested_by=requested_by,
    )
    if _wants_json(request):
        return SettingsJson({"status": "queued", "command_id": command_id}, status_code=202)
    return SettingsFlash(msg=redirect_code, extra={"command_id": command_id})


async def _reload_llm_providers(request: Request) -> None:
    """Reload the shared LLM provider service from DB after settings change."""
    svc = getattr(request.app.state, "llm_provider_service", None)
    if svc is not None:
        try:
            await svc.reload_db_providers()
        except Exception:
            logger.warning("Failed to reload LLM providers from DB", exc_info=True)


async def _semantic_settings_context(request: Request) -> dict[str, object]:
    db = deps.get_db(request)
    last_embedded_id = parse_int_setting(
        await db.get_setting(LAST_EMBEDDED_ID_SETTING),
        setting_name=LAST_EMBEDDED_ID_SETTING,
        default=0,
        logger=logger,
    )
    batch_size = parse_int_setting(
        await db.get_setting(EMBEDDINGS_BATCH_SIZE_SETTING),
        setting_name=EMBEDDINGS_BATCH_SIZE_SETTING,
        default=DEFAULT_EMBEDDINGS_BATCH_SIZE,
        logger=logger,
    )
    embedding_dimensions = await db.repos.messages.get_embedding_dimensions()
    embeddings_count = await db.repos.messages.count_embeddings()
    return {
        "semantic_embeddings_provider": (
            await db.get_setting(EMBEDDINGS_PROVIDER_SETTING) or DEFAULT_EMBEDDINGS_PROVIDER
        ),
        "semantic_embeddings_model": (
            await db.get_setting(EMBEDDINGS_MODEL_SETTING) or DEFAULT_EMBEDDINGS_MODEL
        ),
        "semantic_embeddings_api_key": (
            CREDENTIALS_MASK if await db.get_setting(EMBEDDINGS_API_KEY_SETTING) else ""
        ),
        "semantic_embeddings_base_url": await db.get_setting(EMBEDDINGS_BASE_URL_SETTING) or "",
        "semantic_embeddings_batch_size": batch_size,
        "semantic_last_embedded_id": last_embedded_id,
        "semantic_embedding_dimensions": embedding_dimensions,
        "semantic_embeddings_count": embeddings_count,
    }


def _settings_agent_manager(request: Request) -> tuple[AgentManager, bool]:
    manager = deps.get_agent_manager(request)
    if manager is not None:
        return manager, True
    pool = getattr(request.app.state, "pool", None)
    return AgentManager(deps.get_db(request), request.app.state.config, client_pool=pool), False


async def _dev_mode_enabled(request: Request) -> bool:
    return (await deps.get_db(request).get_setting("agent_dev_mode_enabled") or "0") == "1"


async def _require_agent_dev_mode(
    request: Request,
    *,
    json_mode: bool = False,
) -> SettingsFlash | SettingsJson | None:
    if await _dev_mode_enabled(request):
        return None
    if json_mode:
        return SettingsJson({"ok": False, "error": "Developer mode is required."}, status_code=403)
    return SettingsFlash(error="agent_dev_mode_required")


def _bulk_test_lock(request: Request) -> asyncio.Lock:
    lock = getattr(request.app.state, "agent_provider_bulk_test_lock", None)
    if lock is None:
        lock = asyncio.Lock()
        request.app.state.agent_provider_bulk_test_lock = lock
    return lock


def _bulk_test_status_payload(request: Request) -> dict[str, object]:
    status = getattr(request.app.state, "agent_provider_bulk_test_status", None)
    if status is None:
        status = {
            "running": False,
            "started_at": "",
            "finished_at": "",
            "current_provider": "",
            "current_model": "",
            "completed_probes": 0,
            "total_probes": 0,
            "summary": {"supported": 0, "unsupported": 0, "unknown": 0},
            "providers": {},
            "catalog_path": "",
            "error": "",
            "recent_events": [],
        }
        request.app.state.agent_provider_bulk_test_status = status
    return status


def _replace_bulk_test_status(request: Request, status: dict[str, object]) -> None:
    request.app.state.agent_provider_bulk_test_status = status


def _bulk_test_recent_event(status: dict[str, object], message: str) -> None:
    events = list(status.get("recent_events", []))
    events.append(f"{datetime.now(UTC).astimezone().strftime('%H:%M:%S')} {message}")
    status["recent_events"] = events[-12:]


async def _provider_configs_from_bulk_form(
    form: ProviderConfigForm,
    service: ProviderConfigService,
) -> list[ProviderRuntimeConfig]:
    existing = await service.load_provider_configs()
    if any(str(key).startswith("provider_present__") for key in form.raw.keys()):
        return service.parse_provider_form(form.raw, existing)
    return existing


async def _probe_provider_config(
    service: ProviderConfigService,
    manager: AgentManager,
    cfg: ProviderRuntimeConfig,
    *,
    probe_kind: str,
    force: bool = False,
) -> ProviderModelCompatibilityRecord:
    return await service.ensure_model_compatibility(
        cfg,
        probe_runner=lambda current_cfg, current_probe_kind: manager.probe_provider_config(
            current_cfg,
            probe_kind=current_probe_kind,
        ),
        probe_kind=probe_kind,
        force=force,
    )


async def _run_bulk_test_job(
    request: Request,
    configs: list[ProviderRuntimeConfig] | None = None,
) -> None:
    service = _agent_provider_service(request)
    manager, is_persistent_manager = _settings_agent_manager(request)
    if configs is None:
        configs = await service.load_provider_configs()
    status = {
        "running": True,
        "started_at": datetime.now(UTC).isoformat(),
        "finished_at": "",
        "current_provider": "",
        "current_model": "",
        "completed_probes": 0,
        "total_probes": 0,
        "summary": {"supported": 0, "unsupported": 0, "unknown": 0},
        "providers": {},
        "catalog_path": "",
        "error": "",
        "recent_events": [],
    }
    try:
        _replace_bulk_test_status(request, status)
        logger.info("Bulk compatibility test started: providers=%d", len(configs))
        total_probes = 0
        entries_by_provider: dict[str, ProviderModelCacheEntry] = {}
        for cfg in configs:
            entry = await service.refresh_models_for_provider(cfg.provider, cfg)
            entries_by_provider[cfg.provider] = entry
            total_probes += len(entry.models)
        status["total_probes"] = total_probes
        _bulk_test_recent_event(status, f"Запущено тестирование {total_probes} моделей.")

        for cfg in configs:
            status["current_provider"] = cfg.provider
            entry = entries_by_provider[cfg.provider]
            logger.info(
                "Bulk compatibility refresh: provider=%s selected_model=%s",
                cfg.provider,
                cfg.selected_model or "<empty>",
            )
            logger.info(
                "Bulk compatibility models loaded: provider=%s source=%s count=%d error=%s",
                cfg.provider,
                entry.source,
                len(entry.models),
                entry.error or "",
            )
            provider_results: list[dict[str, str]] = []
            provider_summary = {"supported": 0, "unsupported": 0, "unknown": 0}
            status["providers"][cfg.provider] = {
                "models": provider_results,
                "source": entry.source,
                "summary": provider_summary,
            }
            for model in entry.models:
                status["current_model"] = model
                _bulk_test_recent_event(status, f"Тестируется {cfg.provider} / {model}")
                model_cfg = ProviderRuntimeConfig(
                    provider=cfg.provider,
                    enabled=cfg.enabled,
                    priority=cfg.priority,
                    selected_model=model,
                    plain_fields=dict(cfg.plain_fields),
                    secret_fields=dict(cfg.secret_fields),
                )
                validation_error = service.validate_provider_config(model_cfg)
                if validation_error:
                    logger.info(
                        (
                            "Bulk compatibility probe blocked: provider=%s "
                            "model=%s status=unsupported reason=%s"
                        ),
                        cfg.provider,
                        model,
                        validation_error,
                    )
                    record = ProviderModelCompatibilityRecord(
                        model=model,
                        status="unsupported",
                        reason=validation_error,
                        config_fingerprint=service.config_fingerprint(model_cfg),
                        probe_kind="dev-bulk",
                    )
                else:
                    logger.info(
                        "Bulk compatibility probe started: provider=%s model=%s",
                        cfg.provider,
                        model,
                    )
                    record = await _probe_provider_config(
                        service,
                        manager,
                        model_cfg,
                        probe_kind="dev-bulk",
                        force=True,
                    )
                    logger.info(
                        (
                            "Bulk compatibility probe finished: provider=%s "
                            "model=%s status=%s reason=%s"
                        ),
                        cfg.provider,
                        record.model or model,
                        record.status,
                        record.reason or "",
                    )
                status["summary"][record.status] = status["summary"].get(record.status, 0) + 1
                provider_summary[record.status] = provider_summary.get(record.status, 0) + 1
                status["completed_probes"] = int(status["completed_probes"]) + 1
                provider_results.append(
                    {
                        "model": record.model,
                        "status": record.status,
                        "reason": record.reason,
                        "tested_at": record.tested_at,
                    }
                )
            logger.info(
                (
                    "Bulk compatibility provider summary: provider=%s "
                    "supported=%d unsupported=%d unknown=%d"
                ),
                cfg.provider,
                provider_summary["supported"],
                provider_summary["unsupported"],
                provider_summary["unknown"],
            )
            _bulk_test_recent_event(
                status,
                (
                    f"Провайдер {cfg.provider} завершён: "
                    f"supported={provider_summary['supported']}, "
                    f"unsupported={provider_summary['unsupported']}, "
                    f"unknown={provider_summary['unknown']}"
                ),
            )

        cache = await service.load_model_cache()
        catalog_path = await service.export_compatibility_catalog(configs, cache)
        status["catalog_path"] = str(catalog_path)
        if is_persistent_manager:
            await manager.refresh_settings_cache(preflight=True)
        logger.info(
            "Bulk compatibility test finished: supported=%d unsupported=%d unknown=%d catalog=%s",
            status["summary"]["supported"],
            status["summary"]["unsupported"],
            status["summary"]["unknown"],
            catalog_path,
        )
        _bulk_test_recent_event(
            status,
            (
                f"Тестирование завершено. supported={status['summary']['supported']}, "
                f"unsupported={status['summary']['unsupported']}, "
                f"unknown={status['summary']['unknown']}"
            ),
        )
    except Exception as exc:
        status["error"] = str(exc)
        logger.exception("Bulk compatibility test failed")
        _bulk_test_recent_event(status, f"Ошибка: {exc}")
    finally:
        status["running"] = False
        status["finished_at"] = datetime.now(UTC).isoformat()
        status["current_provider"] = ""
        status["current_model"] = ""


async def handle_settings_page(request: Request) -> dict[str, object]:
    auth = deps.get_auth(request)
    db = deps.get_db(request)
    pool = deps.get_pool(request)
    api_id_raw = await db.get_setting("tg_api_id") or ""
    api_hash_raw = await db.get_setting("tg_api_hash") or ""
    min_subscribers_filter = parse_int_setting(
        await db.get_setting("min_subscribers_filter"),
        setting_name="min_subscribers_filter",
        default=0,
        logger=logger,
    )
    auto_delete_filtered = (await db.get_setting("auto_delete_filtered") or "0") == "1"
    auto_delete_on_collect = (await db.get_setting("auto_delete_on_collect") or "0") == "1"
    saved_interval = await db.get_setting("collect_interval_minutes")
    agent_dev_mode_enabled = (await db.get_setting("agent_dev_mode_enabled") or "0") == "1"
    agent_backend_override = await db.get_setting("agent_backend_override") or "auto"
    agent_prompt_template = (
        await db.get_setting(AGENT_PROMPT_TEMPLATE_SETTING) or DEFAULT_AGENT_PROMPT_TEMPLATE
    )
    if agent_backend_override not in {"auto", "claude", "deepagents"}:
        agent_backend_override = "auto"
    config = request.app.state.config
    provider_service = _agent_provider_service(request)
    telegram_credentials_from_env = bool(
        os.environ.get("TG_API_ID", "").strip().isdigit()
        and os.environ.get("TG_API_HASH", "").strip()
    )
    collect_interval_minutes = parse_int_setting(
        saved_interval,
        setting_name="collect_interval_minutes",
        default=config.scheduler.collect_interval_minutes,
        logger=logger,
    )
    accounts = await db.get_account_summaries()
    telegram_session_warning = any(
        account.session_status != AccountSessionStatus.OK for account in accounts
    )
    now = datetime.now(UTC)
    for account in accounts:
        if account.flood_wait_until is not None:
            flood_until = account.flood_wait_until
            if flood_until.tzinfo is None:
                flood_until = flood_until.replace(tzinfo=UTC)
            if flood_until <= now:
                await db.update_account_flood(account.phone, None)
                account.flood_wait_until = None
    connected_phones = set(pool.clients.keys())
    account_status: dict[str, dict[str, object]] = {}
    flooded_connected = []
    for account in accounts:
        connected = account.phone in connected_phones
        if account.session_status != AccountSessionStatus.OK:
            state = "session_unavailable"
            remaining_seconds = 0
        elif not account.is_active:
            state = "inactive"
            remaining_seconds = 0
        elif not connected:
            state = "disconnected"
            remaining_seconds = 0
        elif account.flood_wait_until is not None:
            flood_until = account.flood_wait_until
            if flood_until.tzinfo is None:
                flood_until = flood_until.replace(tzinfo=UTC)
            remaining_seconds = max(0, int((flood_until - now).total_seconds()))
            if remaining_seconds > 0:
                state = "flood"
                flooded_connected.append(flood_until)
            else:
                state = "available"
        else:
            state = "available"
            remaining_seconds = 0
        account_status[account.phone] = {
            "state": state,
            "remaining_seconds": remaining_seconds,
            "remaining_minutes": max(1, remaining_seconds // 60) if remaining_seconds else 0,
        }
    all_accounts_flooded = bool(flooded_connected) and len(flooded_connected) == len(
        [
            acc
            for acc in accounts
            if acc.is_active
            and acc.phone in connected_phones
            and acc.session_status == AccountSessionStatus.OK
        ]
    )
    next_available_at = min(flooded_connected) if flooded_connected else None
    notification_target = await deps.get_notification_target_service(request).describe_target()
    notification_bot = await _notification_snapshot_bot(request)
    notification_bot_error = ""
    provider_configs = await provider_service.load_provider_configs()
    provider_cache = await provider_service.load_model_cache()
    provider_views = provider_service.build_provider_views(provider_configs, provider_cache)
    configured_names = {cfg.provider for cfg in provider_configs}
    available_provider_options = [
        provider_service.provider_specs[name]
        for name in PROVIDER_ORDER
        if name not in configured_names
    ]
    img_provider_service = _image_provider_service(request)
    img_provider_configs = await img_provider_service.load_provider_configs()
    img_provider_views = img_provider_service.build_provider_views(img_provider_configs)
    configured_img_names = {cfg.provider for cfg in img_provider_configs}
    available_img_options = [
        IMAGE_PROVIDER_SPECS[name] for name in IMAGE_PROVIDER_ORDER if name not in configured_img_names
    ]
    semantic_context = await _semantic_settings_context(request)

    translation_provider = await db.get_setting("translation_provider") or ""
    translation_model = await db.get_setting("translation_model") or ""
    translation_target_lang = await db.get_setting("translation_target_lang") or ""
    translation_source_filter = await db.get_setting("translation_source_filter") or ""
    translation_auto_on_collect = await db.get_setting("translation_auto_on_collect") or "0"
    language_stats = await db.repos.messages.get_language_stats()

    from src.agent.tools.permissions import (
        build_template_context,
        load_tool_permissions_all_phones,
    )

    phone_permissions = await load_tool_permissions_all_phones(db, accounts)
    phone_perm_contexts = {
        phone: build_template_context(perms)
        for phone, perms in phone_permissions.items()
    }

    return {
        "is_configured": auth.is_configured,
        "telegram_credentials_from_env": telegram_credentials_from_env,
        "api_id": CREDENTIALS_MASK if api_id_raw else "",
        "api_hash": CREDENTIALS_MASK if api_hash_raw else "",
        "min_subscribers_filter": min_subscribers_filter,
        "auto_delete_filtered": auto_delete_filtered,
        "auto_delete_on_collect": auto_delete_on_collect,
        "accounts": accounts,
        "telegram_session_warning": telegram_session_warning,
        "account_status": account_status,
        "account_phones": [acc.phone for acc in accounts],
        "connected_phones": connected_phones,
        "all_accounts_flooded": all_accounts_flooded,
        "next_available_at": next_available_at,
        "notification_target": notification_target,
        "notification_selected_phone": notification_target.configured_phone or "",
        "notification_bot": notification_bot,
        "notification_bot_error": notification_bot_error,
        "collect_interval_minutes": collect_interval_minutes,
        "agent_dev_mode_enabled": agent_dev_mode_enabled,
        "agent_backend_override": agent_backend_override,
        "agent_prompt_template": agent_prompt_template,
        "agent_fallback_model": config.agent.fallback_model or os.environ.get("AGENT_FALLBACK_MODEL", "").strip(),
        "agent_prompt_template_variables": sorted(ALLOWED_TEMPLATE_VARIABLES),
        "agent_provider_writes_enabled": provider_service.writes_enabled,
        "agent_provider_views": provider_views,
        "agent_provider_options": available_provider_options,
        "img_provider_writes_enabled": img_provider_service.writes_enabled,
        "img_provider_views": img_provider_views,
        "img_provider_options": available_img_options,
        "default_image_model": await db.get_setting("default_image_model") or "",
        **semantic_context,
        "phone_perm_contexts": phone_perm_contexts,
        "translation_provider": translation_provider,
        "translation_model": translation_model,
        "translation_target_lang": translation_target_lang,
        "translation_source_filter": translation_source_filter,
        "translation_auto_on_collect": translation_auto_on_collect,
        "language_stats": language_stats,
    }


async def handle_save_scheduler_settings(
    request: Request,
    form: SchedulerSettingsForm,
) -> SettingsFlash:
    db = deps.get_db(request)
    await db.set_setting("collect_interval_minutes", str(form.interval_minutes))
    scheduler = getattr(request.app.state, "scheduler", None)
    if scheduler:
        scheduler.update_interval(form.interval_minutes)
    return SettingsFlash(msg="scheduler_saved")


async def handle_save_semantic_search_settings(
    request: Request,
    form: SemanticSearchSettingsForm,
) -> SettingsFlash:
    db = deps.get_db(request)
    current_values = {
        EMBEDDINGS_PROVIDER_SETTING: await db.get_setting(EMBEDDINGS_PROVIDER_SETTING)
        or DEFAULT_EMBEDDINGS_PROVIDER,
        EMBEDDINGS_MODEL_SETTING: await db.get_setting(EMBEDDINGS_MODEL_SETTING)
        or DEFAULT_EMBEDDINGS_MODEL,
        EMBEDDINGS_BASE_URL_SETTING: await db.get_setting(EMBEDDINGS_BASE_URL_SETTING) or "",
        EMBEDDINGS_API_KEY_SETTING: await db.get_setting(EMBEDDINGS_API_KEY_SETTING) or "",
    }
    changed = (
        current_values[EMBEDDINGS_PROVIDER_SETTING] != form.provider
        or current_values[EMBEDDINGS_MODEL_SETTING] != form.model
        or current_values[EMBEDDINGS_BASE_URL_SETTING] != form.base_url
    )
    await db.set_setting(EMBEDDINGS_PROVIDER_SETTING, form.provider)
    await db.set_setting(EMBEDDINGS_MODEL_SETTING, form.model)
    await db.set_setting(EMBEDDINGS_BASE_URL_SETTING, form.base_url)
    await db.set_setting(EMBEDDINGS_BATCH_SIZE_SETTING, str(form.batch_size))
    if form.api_key is not None and form.api_key != CREDENTIALS_MASK:
        await db.set_setting(EMBEDDINGS_API_KEY_SETTING, form.api_key)
    if changed or form.reset_index:
        await db.repos.messages.reset_embeddings_index()
        deps.get_search_engine(request).invalidate_numpy_index()
    return SettingsFlash(msg="semantic_saved")


async def handle_run_semantic_index(request: Request, form: SemanticIndexForm) -> SettingsFlash:
    db = deps.get_db(request)
    if not deps.get_search_engine(request).semantic_available:
        return SettingsFlash(error="semantic_unavailable")
    if form.reset_index:
        await db.repos.messages.reset_embeddings_index()
    indexed = await EmbeddingService(db, request.app.state.config).index_pending_messages()
    if indexed > 0 or form.reset_index:
        deps.get_search_engine(request).invalidate_numpy_index()
    return SettingsFlash(msg="semantic_indexed", extra={"indexed": indexed})


async def handle_save_agent_settings(request: Request, form: AgentSettingsForm) -> SettingsFlash:
    db = deps.get_db(request)

    if form.form_scope == "tool_permissions":
        from src.agent.tools.permissions import save_tool_permissions

        if form.tool_permissions is None:
            return SettingsFlash(error="invalid_value")
        await save_tool_permissions(
            db,
            form.tool_permissions.permissions,
            phone=form.tool_permissions.phone,
        )
        return SettingsFlash(msg="tool_permissions_saved", fragment="pane-tool-permissions")

    current_dev_mode = (await db.get_setting("agent_dev_mode_enabled") or "0") == "1"
    current_backend_override = await db.get_setting("agent_backend_override") or "auto"
    current_prompt_template = (
        await db.get_setting(AGENT_PROMPT_TEMPLATE_SETTING) or DEFAULT_AGENT_PROMPT_TEMPLATE
    )

    if form.backend_override is None:
        backend_override = current_backend_override
    else:
        backend_override = form.backend_override
    if backend_override not in {"auto", "claude", "deepagents"}:
        backend_override = "auto"

    if form.form_scope == "backend_override":
        dev_mode_enabled = current_dev_mode
    else:
        if not form.wants_dev_mode:
            dev_mode_enabled = False
        elif form.disclaimer_accepted:
            dev_mode_enabled = True
        else:
            dev_mode_enabled = current_dev_mode

    if form.form_scope == "prompt_template":
        prompt_template = form.prompt_template or ""
        if not prompt_template.strip():
            prompt_template = DEFAULT_AGENT_PROMPT_TEMPLATE
        try:
            validate_prompt_template(prompt_template)
        except PromptTemplateError as exc:
            logger.warning("Rejected invalid agent prompt template: %s", exc)
            return SettingsFlash(error="agent_prompt_template_invalid")
    else:
        prompt_template = current_prompt_template

    if not dev_mode_enabled:
        backend_override = "auto"

    if backend_override != "auto" and dev_mode_enabled:
        if backend_override == "deepagents":
            service = _agent_provider_service(request)
            configs = await service.load_provider_configs()
            has_valid = any(
                cfg.enabled and not service.validate_provider_config(cfg) for cfg in configs
            )
            if not has_valid:
                logger.warning(
                    "Rejected deepagents override in dev mode: no valid provider configs are available"
                )
                return SettingsFlash(error="agent_backend_no_valid_providers")
        elif backend_override == "claude":
            if not (
                os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
            ):
                logger.warning(
                    "Rejected claude override in dev mode: no API credentials are available"
                )
                return SettingsFlash(error="agent_backend_claude_unavailable")

    await db.set_setting("agent_dev_mode_enabled", "1" if dev_mode_enabled else "0")
    await db.set_setting("agent_backend_override", backend_override)
    await db.set_setting(AGENT_PROMPT_TEMPLATE_SETTING, prompt_template)
    agent_manager = deps.get_agent_manager(request)
    if agent_manager is not None:
        await agent_manager.refresh_settings_cache(preflight=True)
    return SettingsFlash(msg="agent_saved")


async def handle_add_agent_provider(request: Request, form: ProviderAddForm) -> SettingsFlash:
    service = _agent_provider_service(request)
    if not service.writes_enabled:
        return SettingsFlash(error="agent_provider_secret_required")
    dev_mode_required = await _require_agent_dev_mode(request)
    if dev_mode_required is not None:
        return dev_mode_required
    provider_name = form.provider
    if deepagents_provider_spec(provider_name) is None:
        return SettingsFlash(error="agent_provider_invalid")
    configs = await service.load_provider_configs()
    if any(cfg.provider == provider_name for cfg in configs):
        return SettingsFlash(msg="agent_saved")
    priority = max((cfg.priority for cfg in configs), default=-1) + 1
    configs.append(service.create_empty_config(provider_name, priority))
    await service.save_provider_configs(configs)
    agent_manager = deps.get_agent_manager(request)
    if agent_manager is not None:
        await agent_manager.refresh_settings_cache(preflight=True)
    await _reload_llm_providers(request)
    return SettingsFlash(msg="agent_saved")


async def handle_save_agent_providers(request: Request, form: ProviderConfigForm) -> SettingsFlash:
    service = _agent_provider_service(request)
    if not service.writes_enabled:
        return SettingsFlash(error="agent_provider_secret_required")
    dev_mode_required = await _require_agent_dev_mode(request)
    if dev_mode_required is not None:
        return dev_mode_required
    existing = await service.load_provider_configs()
    configs = service.parse_provider_form(form.raw, existing)
    manager, is_persistent_manager = _settings_agent_manager(request)
    validated: list[ProviderRuntimeConfig] = []
    for cfg in configs:
        validation_error = ""
        if cfg.enabled:
            validation_error = service.validate_provider_config(cfg)
        if cfg.enabled and not validation_error:
            await _probe_provider_config(
                service,
                manager,
                cfg,
                probe_kind="save-time",
            )
        validated.append(
            ProviderRuntimeConfig(
                provider=cfg.provider,
                enabled=cfg.enabled,
                priority=cfg.priority,
                selected_model=cfg.selected_model,
                plain_fields=cfg.plain_fields,
                secret_fields=cfg.secret_fields,
                last_validation_error=validation_error,
                secret_status=cfg.secret_status,
                secret_fields_enc_preserved=cfg.secret_fields_enc_preserved,
            )
        )
    await service.save_provider_configs(validated)
    if is_persistent_manager:
        await manager.refresh_settings_cache(preflight=True)
    await _reload_llm_providers(request)
    return SettingsFlash(msg="agent_saved")


async def handle_delete_agent_provider(request: Request, provider_name: str) -> SettingsFlash:
    service = _agent_provider_service(request)
    if not service.writes_enabled:
        return SettingsFlash(error="agent_provider_secret_required")
    dev_mode_required = await _require_agent_dev_mode(request)
    if dev_mode_required is not None:
        return dev_mode_required
    configs = await service.load_provider_configs()
    configs = [cfg for cfg in configs if cfg.provider != provider_name]
    for index, cfg in enumerate(configs):
        cfg.priority = index
    await service.save_provider_configs(configs)
    agent_manager = deps.get_agent_manager(request)
    if agent_manager is not None:
        await agent_manager.refresh_settings_cache(preflight=True)
    await _reload_llm_providers(request)
    return SettingsFlash(msg="agent_saved")


async def handle_refresh_agent_provider_models(
    request: Request,
    provider_name: str,
    form: ProviderConfigForm,
) -> SettingsJson:
    service = _agent_provider_service(request)
    if not service.writes_enabled:
        return SettingsJson(
            {"ok": False, "error": "SESSION_ENCRYPTION_KEY is required."},
            status_code=409,
        )
    dev_mode_required = await _require_agent_dev_mode(request, json_mode=True)
    if dev_mode_required is not None:
        return dev_mode_required
    if deepagents_provider_spec(provider_name) is None:
        return SettingsJson({"ok": False, "error": "Unknown provider."}, status_code=404)
    configs = await service.load_provider_configs()
    cfg = service.parse_single_provider_form(form.raw, configs, provider_name)
    entry = await service.refresh_models_for_provider(provider_name, cfg)
    return SettingsJson(
        {
            "ok": True,
            "provider": provider_name,
            "models": entry.models,
            "source": entry.source,
            "error": entry.error,
            "fetched_at": entry.fetched_at,
            "compatibility": (
                service.build_compatibility_payload(cfg, entry) if cfg is not None else {}
            ),
        }
    )


async def handle_refresh_all_agent_provider_models(
    request: Request,
    form: ProviderConfigForm,
) -> SettingsJson:
    service = _agent_provider_service(request)
    if not service.writes_enabled:
        return SettingsJson(
            {"ok": False, "error": "SESSION_ENCRYPTION_KEY is required."},
            status_code=409,
        )
    dev_mode_required = await _require_agent_dev_mode(request, json_mode=True)
    if dev_mode_required is not None:
        return dev_mode_required
    configs = await _provider_configs_from_bulk_form(form, service)
    config_map = {cfg.provider: cfg for cfg in configs}
    results = await service.refresh_all_models(configs)
    return SettingsJson(
        {
            "ok": True,
            "providers": {
                provider: {
                    "models": entry.models,
                    "source": entry.source,
                    "error": entry.error,
                    "fetched_at": entry.fetched_at,
                    "compatibility": (
                        service.build_compatibility_payload(config_map[provider], entry)
                        if provider in config_map
                        else {}
                    ),
                }
                for provider, entry in results.items()
            },
        }
    )


async def handle_probe_agent_provider_model(
    request: Request,
    provider_name: str,
    form: ProviderConfigForm,
) -> SettingsJson:
    service = _agent_provider_service(request)
    if not service.writes_enabled:
        return SettingsJson(
            {"ok": False, "error": "SESSION_ENCRYPTION_KEY is required."},
            status_code=409,
        )
    dev_mode_required = await _require_agent_dev_mode(request, json_mode=True)
    if dev_mode_required is not None:
        return dev_mode_required
    if deepagents_provider_spec(provider_name) is None:
        return SettingsJson({"ok": False, "error": "Unknown provider."}, status_code=404)

    existing = await service.load_provider_configs()
    cfg = service.parse_single_provider_form(form.raw, existing, provider_name)
    validation_error = service.validate_provider_config(cfg)
    if validation_error:
        logger.info(
            "Compatibility probe skipped: provider=%s model=%s status=unsupported reason=%s",
            provider_name,
            cfg.selected_model or "<empty>",
            validation_error,
        )
        return SettingsJson(
            {
                "ok": True,
                "provider": provider_name,
                "model": cfg.selected_model,
                "status": "unsupported",
                "reason": validation_error,
                "tested_at": "",
                "config_fingerprint": service.config_fingerprint(cfg),
            }
        )

    manager, _ = _settings_agent_manager(request)
    logger.info(
        "Compatibility probe requested: provider=%s model=%s kind=auto-select",
        provider_name,
        cfg.selected_model or "<empty>",
    )
    record = await _probe_provider_config(
        service,
        manager,
        cfg,
        probe_kind="auto-select",
    )
    logger.info(
        "Compatibility probe finished: provider=%s model=%s status=%s kind=%s reason=%s",
        provider_name,
        record.model or "<empty>",
        record.status,
        record.probe_kind,
        record.reason or "",
    )
    return SettingsJson(
        {
            "ok": True,
            "provider": provider_name,
            "model": record.model,
            "status": record.status,
            "reason": record.reason,
            "tested_at": record.tested_at,
            "config_fingerprint": record.config_fingerprint,
            "probe_kind": record.probe_kind,
        }
    )


async def handle_test_all_agent_provider_models(
    request: Request,
    form: ProviderConfigForm,
) -> SettingsJson:
    service = _agent_provider_service(request)
    if not service.writes_enabled:
        return SettingsJson(
            {"ok": False, "error": "SESSION_ENCRYPTION_KEY is required."},
            status_code=409,
        )
    if not await _dev_mode_enabled(request):
        return SettingsJson({"ok": False, "error": "Developer mode is required."}, status_code=403)
    configs = await _provider_configs_from_bulk_form(form, service)
    async with _bulk_test_lock(request):
        status = _bulk_test_status_payload(request)
        if status.get("running"):
            return SettingsJson(
                {
                    "ok": False,
                    "error": "Bulk compatibility test is already running.",
                    **status,
                },
                status_code=409,
            )
        initial_status = {
            "running": True,
            "started_at": datetime.now(UTC).isoformat(),
            "finished_at": "",
            "current_provider": "",
            "current_model": "",
            "completed_probes": 0,
            "total_probes": 0,
            "summary": {"supported": 0, "unsupported": 0, "unknown": 0},
            "providers": {},
            "catalog_path": "",
            "error": "",
            "recent_events": ["Запуск массового тестирования..."],
        }
        _replace_bulk_test_status(request, initial_status)
        request.app.state.agent_provider_bulk_test_task = asyncio.create_task(
            _run_bulk_test_job(request, configs=configs)
        )
    return SettingsJson({"ok": True, "started": True, **_bulk_test_status_payload(request)})


async def handle_test_all_agent_provider_models_status(request: Request) -> SettingsJson:
    service = _agent_provider_service(request)
    if not service.writes_enabled:
        return SettingsJson(
            {"ok": False, "error": "SESSION_ENCRYPTION_KEY is required."},
            status_code=409,
        )
    if not await _dev_mode_enabled(request):
        return SettingsJson({"ok": False, "error": "Developer mode is required."}, status_code=403)
    return SettingsJson({"ok": True, **_bulk_test_status_payload(request)})


async def handle_save_filters(request: Request, form: FiltersForm) -> SettingsFlash:
    db = deps.get_db(request)
    await db.set_setting("min_subscribers_filter", form.min_subscribers)
    await db.set_setting(
        "auto_delete_filtered",
        "1" if form.auto_delete_filtered else "0",
    )
    await db.set_setting(
        "auto_delete_on_collect",
        "1" if form.auto_delete_on_collect else "0",
    )
    if int(form.min_subscribers) > 0:
        all_stats = await db.get_latest_stats_for_all()
        to_filter = [
            (channel_id, "low_subscriber_manual")
            for channel_id, stats in all_stats.items()
            if stats.subscriber_count is not None and stats.subscriber_count < int(form.min_subscribers)
        ]
        if to_filter:
            await db.set_channels_filtered_bulk(to_filter)
    return SettingsFlash(msg="filters_saved")


async def handle_save_notification_account(
    request: Request,
    form: NotificationAccountForm,
) -> SettingsFlash:
    db = deps.get_db(request)
    valid_phones = {acc.phone for acc in await db.get_account_summaries()}
    if form.selected_phone and form.selected_phone not in valid_phones:
        return SettingsFlash(error="notification_account_invalid")

    await deps.get_notification_target_service(request).set_configured_phone(form.selected_phone or None)
    notifier = deps.get_notifier(request)
    if notifier:
        notifier.invalidate_me_cache()
    return SettingsFlash(msg="notification_account_saved")


async def handle_save_credentials(request: Request, form: CredentialsForm) -> SettingsFlash:
    db = deps.get_db(request)
    auth = deps.get_auth(request)

    id_changed = form.api_id and form.api_id != CREDENTIALS_MASK
    hash_changed = form.api_hash and form.api_hash != CREDENTIALS_MASK

    if id_changed and not form.api_id.isdigit():
        return SettingsFlash(error="invalid_api_id")

    if id_changed:
        await db.set_setting("tg_api_id", form.api_id)
    if hash_changed:
        await db.set_setting("tg_api_hash", form.api_hash)

    if id_changed or hash_changed:
        actual_id = form.api_id if id_changed else (await db.get_setting("tg_api_id") or "")
        actual_hash = form.api_hash if hash_changed else (await db.get_setting("tg_api_hash") or "")
        if actual_id and actual_hash:
            if not actual_id.isdigit():
                return SettingsFlash(error="invalid_api_id")
            auth.update_credentials(int(actual_id), actual_hash)

    return SettingsFlash(msg="credentials_saved")


async def handle_setup_notification_bot(request: Request) -> SettingsFlash | SettingsJson:
    return await _enqueue_notification_command(
        request,
        "notifications.setup_bot",
        requested_by="web:settings.notifications.setup",
        redirect_code="notification_setup_queued",
    )


async def handle_notification_bot_status(request: Request) -> SettingsJson:
    payload = await _notification_snapshot_payload(request)
    raw_target = payload.get("target")
    if isinstance(raw_target, dict) and raw_target.get("state") not in {None, "available"}:
        return SettingsJson(
            {"configured": False, "error": raw_target.get("message", "")},
            status_code=409,
        )
    bot = await _notification_snapshot_bot(request)
    if bot is None:
        return SettingsJson({"configured": False})
    return SettingsJson(
        {
            "configured": True,
            "bot_username": bot.bot_username,
            "bot_id": bot.bot_id,
            "created_at": bot.created_at.isoformat() if bot.created_at else None,
        }
    )


async def handle_delete_notification_bot(request: Request) -> SettingsFlash | SettingsJson:
    return await _enqueue_notification_command(
        request,
        "notifications.delete_bot",
        requested_by="web:settings.notifications.delete",
        redirect_code="notification_delete_queued",
    )


async def handle_test_notification(request: Request) -> SettingsFlash | SettingsJson:
    return await _enqueue_notification_command(
        request,
        "notifications.test",
        requested_by="web:settings.notifications.test",
        redirect_code="notification_test_queued",
    )


async def handle_add_image_provider(request: Request, form: ProviderAddForm) -> SettingsFlash:
    service = _image_provider_service(request)
    if not service.writes_enabled:
        return SettingsFlash(error="image_provider_secret_required")
    provider_name = form.provider
    if image_provider_spec(provider_name) is None:
        return SettingsFlash(error="image_provider_invalid")
    configs = await service.load_provider_configs()
    if any(cfg.provider == provider_name for cfg in configs):
        return SettingsFlash(msg="image_saved")
    configs.append(service.create_empty_config(provider_name))
    await service.save_provider_configs(configs)
    return SettingsFlash(msg="image_saved")


async def handle_save_image_providers(
    request: Request,
    form: ImageProviderSaveForm,
) -> SettingsFlash:
    service = _image_provider_service(request)
    if not service.writes_enabled:
        return SettingsFlash(error="image_provider_secret_required")
    existing = await service.load_provider_configs()
    configs = service.parse_provider_form(form.raw, existing)
    for cfg in configs:
        if not cfg.enabled:
            continue
        spec = image_provider_spec(cfg.provider)
        if spec is None:
            continue
        has_key = bool(cfg.api_key.strip())
        has_env = any(os.environ.get(var) for var in spec.env_vars)
        if not has_key and not has_env:
            return SettingsFlash(error="image_provider_missing_key")
    await service.save_provider_configs(configs)
    await deps.get_db(request).set_setting("default_image_model", form.default_model)
    return SettingsFlash(msg="image_saved")


async def handle_delete_image_provider(request: Request, provider_name: str) -> SettingsFlash:
    service = _image_provider_service(request)
    if not service.writes_enabled:
        return SettingsFlash(error="image_provider_secret_required")
    configs = await service.load_provider_configs()
    configs = [cfg for cfg in configs if cfg.provider != provider_name]
    await service.save_provider_configs(configs)
    return SettingsFlash(msg="image_saved")


async def handle_save_translation_settings(
    request: Request,
    form: TranslationSettingsForm,
) -> SettingsFlash:
    db = deps.get_db(request)
    await db.set_setting("translation_provider", form.provider)
    await db.set_setting("translation_model", form.model)
    await db.set_setting("translation_target_lang", form.target_lang)
    await db.set_setting("translation_source_filter", form.source_filter)
    await db.set_setting("translation_auto_on_collect", "1" if form.auto_on_collect else "0")
    return SettingsFlash(msg="translation_saved", fragment="pane-translation")


async def handle_translation_backfill_lang(request: Request) -> SettingsFlash:
    db = deps.get_db(request)
    updated = await db.repos.messages.backfill_language_detection(batch_size=5000)
    return SettingsFlash(
        msg="translation_backfill_done",
        extra={"count": updated},
        fragment="pane-translation",
    )


async def handle_translation_run_batch(
    request: Request,
    form: TranslationRunForm,
) -> SettingsFlash:
    db = deps.get_db(request)
    source_filter_raw = await db.get_setting("translation_source_filter") or ""
    source_filter = [source.strip() for source in source_filter_raw.split(",") if source.strip()]

    from src.models import CollectionTaskType, TranslateBatchTaskPayload

    payload = TranslateBatchTaskPayload(
        target_lang=form.target_lang,
        source_filter=source_filter,
    )
    await db.repos.tasks.create_generic_task(
        CollectionTaskType.TRANSLATE_BATCH,
        title=f"Translation batch ({form.target_lang})",
        payload=payload,
    )
    return SettingsFlash(msg="translation_run_started", fragment="pane-translation")
