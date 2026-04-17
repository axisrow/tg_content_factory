import asyncio
import logging
import os
from datetime import UTC, datetime
from types import SimpleNamespace

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

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
from src.services.agent_provider_service import (
    AgentProviderService,
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
from src.services.notification_service import NotificationService  # noqa: F401
from src.settings_utils import parse_int_setting
from src.web import deps

router = APIRouter()
logger = logging.getLogger(__name__)

CREDENTIALS_MASK = "••••••••"

def _image_provider_service(request: Request) -> ImageProviderService:
    return ImageProviderService(deps.get_db(request), request.app.state.config)


def _wants_json(request: Request) -> bool:
    return "application/json" in request.headers.get("accept", "")


def _agent_provider_service(request: Request) -> AgentProviderService:
    return AgentProviderService(deps.get_db(request), request.app.state.config)


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
            datetime.fromisoformat(raw_bot["created_at"])
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
):
    command_id = await deps.telegram_command_service(request).enqueue(
        command_type,
        payload=payload or {},
        requested_by=requested_by,
    )
    if _wants_json(request):
        return JSONResponse({"status": "queued", "command_id": command_id}, status_code=202)
    return RedirectResponse(
        url=f"/settings?msg={redirect_code}&command_id={command_id}",
        status_code=303,
    )


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
):
    if await _dev_mode_enabled(request):
        return None
    if json_mode:
        return JSONResponse({"ok": False, "error": "Developer mode is required."}, status_code=403)
    return RedirectResponse(url="/settings?error=agent_dev_mode_required", status_code=303)


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
    request: Request,
    service: AgentProviderService,
) -> list[ProviderRuntimeConfig]:
    form = await request.form()
    existing = await service.load_provider_configs()
    if any(str(key).startswith("provider_present__") for key in form.keys()):
        return service.parse_provider_form(form, existing)
    return existing


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


async def _probe_provider_config(
    service: AgentProviderService,
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


@router.get("/", response_class=HTMLResponse)
async def settings_page(request: Request):
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
    accounts = await db.get_accounts()
    _now = datetime.now(UTC)
    for _acc in accounts:
        if _acc.flood_wait_until is not None:
            _flood_until = _acc.flood_wait_until
            if _flood_until.tzinfo is None:
                _flood_until = _flood_until.replace(tzinfo=UTC)
            if _flood_until <= _now:
                await db.update_account_flood(_acc.phone, None)
                _acc.flood_wait_until = None
    connected_phones = set(pool.clients.keys())
    account_status: dict[str, dict[str, object]] = {}
    flooded_connected = []
    for _acc in accounts:
        connected = _acc.phone in connected_phones
        if not _acc.is_active:
            state = "inactive"
            remaining_seconds = 0
        elif not connected:
            state = "disconnected"
            remaining_seconds = 0
        elif _acc.flood_wait_until is not None:
            _flood_until = _acc.flood_wait_until
            if _flood_until.tzinfo is None:
                _flood_until = _flood_until.replace(tzinfo=UTC)
            remaining_seconds = max(0, int((_flood_until - _now).total_seconds()))
            if remaining_seconds > 0:
                state = "flood"
                flooded_connected.append(_flood_until)
            else:
                state = "available"
        else:
            state = "available"
            remaining_seconds = 0
        account_status[_acc.phone] = {
            "state": state,
            "remaining_seconds": remaining_seconds,
            "remaining_minutes": max(1, remaining_seconds // 60) if remaining_seconds else 0,
        }
    all_accounts_flooded = bool(flooded_connected) and len(flooded_connected) == len(
        [acc for acc in accounts if acc.is_active and acc.phone in connected_phones]
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

    # Translation settings
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
    # Build per-phone template contexts
    phone_perm_contexts = {
        phone: build_template_context(perms)
        for phone, perms in phone_permissions.items()
    }

    return deps.get_templates(request).TemplateResponse(
        request,
        "settings.html",
        {
            "is_configured": auth.is_configured,
            "telegram_credentials_from_env": telegram_credentials_from_env,
            "api_id": CREDENTIALS_MASK if api_id_raw else "",
            "api_hash": CREDENTIALS_MASK if api_hash_raw else "",
            "min_subscribers_filter": min_subscribers_filter,
            "auto_delete_filtered": auto_delete_filtered,
            "auto_delete_on_collect": auto_delete_on_collect,
            "accounts": accounts,
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
            "agent_fallback_model": config.agent.fallback_model
            or os.environ.get("AGENT_FALLBACK_MODEL", "").strip(),
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
        },
    )


@router.post("/save-scheduler")
async def save_scheduler_settings(request: Request):
    form = await request.form()
    try:
        interval = int(form.get("collect_interval_minutes", 60))
    except (TypeError, ValueError):
        return RedirectResponse(url="/settings?error=invalid_value", status_code=303)
    interval = max(1, min(1440, interval))
    db = deps.get_db(request)
    await db.set_setting("collect_interval_minutes", str(interval))
    scheduler = getattr(request.app.state, "scheduler", None)
    if scheduler:
        scheduler.update_interval(interval)
    return RedirectResponse(url="/settings?msg=scheduler_saved", status_code=303)


@router.post("/save-semantic-search")
async def save_semantic_search_settings(request: Request):
    form = await request.form()
    db = deps.get_db(request)

    provider = str(form.get("semantic_embeddings_provider", DEFAULT_EMBEDDINGS_PROVIDER)).strip()
    model = str(form.get("semantic_embeddings_model", DEFAULT_EMBEDDINGS_MODEL)).strip()
    base_url = str(form.get("semantic_embeddings_base_url", "")).strip()
    api_key_raw = form.get("semantic_embeddings_api_key")
    batch_size_raw = str(form.get("semantic_embeddings_batch_size", DEFAULT_EMBEDDINGS_BATCH_SIZE))
    reset_index = str(form.get("semantic_reset_index", "")).strip() == "1"

    if not provider or not model:
        return RedirectResponse(url="/settings?error=semantic_invalid_value", status_code=303)
    try:
        batch_size = int(batch_size_raw)
    except (TypeError, ValueError):
        return RedirectResponse(url="/settings?error=semantic_invalid_value", status_code=303)
    batch_size = max(1, min(1000, batch_size))

    current_values = {
        EMBEDDINGS_PROVIDER_SETTING: await db.get_setting(EMBEDDINGS_PROVIDER_SETTING)
        or DEFAULT_EMBEDDINGS_PROVIDER,
        EMBEDDINGS_MODEL_SETTING: await db.get_setting(EMBEDDINGS_MODEL_SETTING)
        or DEFAULT_EMBEDDINGS_MODEL,
        EMBEDDINGS_BASE_URL_SETTING: await db.get_setting(EMBEDDINGS_BASE_URL_SETTING) or "",
        EMBEDDINGS_API_KEY_SETTING: await db.get_setting(EMBEDDINGS_API_KEY_SETTING) or "",
    }
    changed = (
        current_values[EMBEDDINGS_PROVIDER_SETTING] != provider
        or current_values[EMBEDDINGS_MODEL_SETTING] != model
        or current_values[EMBEDDINGS_BASE_URL_SETTING] != base_url
    )
    await db.set_setting(EMBEDDINGS_PROVIDER_SETTING, provider)
    await db.set_setting(EMBEDDINGS_MODEL_SETTING, model)
    await db.set_setting(EMBEDDINGS_BASE_URL_SETTING, base_url)
    await db.set_setting(EMBEDDINGS_BATCH_SIZE_SETTING, str(batch_size))
    if api_key_raw is not None:
        api_key = str(api_key_raw).strip()
        if api_key != CREDENTIALS_MASK:
            await db.set_setting(EMBEDDINGS_API_KEY_SETTING, api_key)
    if changed or reset_index:
        await db.repos.messages.reset_embeddings_index()
        deps.get_search_engine(request).invalidate_numpy_index()
    return RedirectResponse(url="/settings?msg=semantic_saved", status_code=303)


@router.post("/semantic-index")
async def run_semantic_index(request: Request):
    db = deps.get_db(request)
    if not deps.get_search_engine(request).semantic_available:
        return RedirectResponse(url="/settings?error=semantic_unavailable", status_code=303)
    form = await request.form()
    reset_index = str(form.get("semantic_reset_index", "")).strip() == "1"
    if reset_index:
        await db.repos.messages.reset_embeddings_index()
    indexed = await EmbeddingService(db, request.app.state.config).index_pending_messages()
    if indexed > 0 or reset_index:
        deps.get_search_engine(request).invalidate_numpy_index()
    return RedirectResponse(
        url=f"/settings?msg=semantic_indexed&indexed={indexed}",
        status_code=303,
    )


@router.post("/save-agent")
async def save_agent_settings(request: Request):
    form = await request.form()
    db = deps.get_db(request)

    form_scope = str(form.get("agent_form_scope", "dev_mode")).strip()

    if form_scope == "tool_permissions":
        from src.agent.tools.permissions import TOOL_CATEGORIES, save_tool_permissions

        phone = str(form.get("phone", "")).strip() or None
        permissions = {}
        for tool_name in TOOL_CATEGORIES:
            permissions[tool_name] = form.get(f"tool_perm__{tool_name}") == "1"
        await save_tool_permissions(db, permissions, phone=phone)
        return RedirectResponse(url="/settings?msg=tool_permissions_saved#pane-tool-permissions", status_code=303)

    current_dev_mode = (await db.get_setting("agent_dev_mode_enabled") or "0") == "1"
    current_backend_override = await db.get_setting("agent_backend_override") or "auto"
    current_prompt_template = (
        await db.get_setting(AGENT_PROMPT_TEMPLATE_SETTING) or DEFAULT_AGENT_PROMPT_TEMPLATE
    )

    wants_dev_mode = str(form.get("agent_dev_mode_enabled", "")).strip() == "1"
    disclaimer_accepted = str(form.get("agent_dev_mode_disclaimer", "")).strip() == "1"
    backend_override_raw = form.get("agent_backend_override")
    if backend_override_raw is None:
        backend_override = current_backend_override
    else:
        backend_override = str(backend_override_raw).strip()
    if backend_override not in {"auto", "claude", "deepagents"}:
        backend_override = "auto"

    if form_scope == "backend_override":
        dev_mode_enabled = current_dev_mode
    else:
        if not wants_dev_mode:
            dev_mode_enabled = False
        elif disclaimer_accepted:
            dev_mode_enabled = True
        else:
            dev_mode_enabled = current_dev_mode

    if form_scope == "prompt_template":
        prompt_template = str(form.get("agent_prompt_template") or "")
        if not prompt_template.strip():
            prompt_template = DEFAULT_AGENT_PROMPT_TEMPLATE
        try:
            validate_prompt_template(prompt_template)
        except PromptTemplateError as exc:
            logger.warning("Rejected invalid agent prompt template: %s", exc)
            return RedirectResponse(
                url="/settings?error=agent_prompt_template_invalid",
                status_code=303,
            )
    else:
        prompt_template = current_prompt_template

    # Reset override when dev mode is off to prevent stale override from activating later
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
                return RedirectResponse(
                    url="/settings?error=agent_backend_no_valid_providers", status_code=303
                )
        elif backend_override == "claude":
            if not (
                os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
            ):
                logger.warning(
                    "Rejected claude override in dev mode: no API credentials are available"
                )
                return RedirectResponse(
                    url="/settings?error=agent_backend_claude_unavailable", status_code=303
                )

    await db.set_setting("agent_dev_mode_enabled", "1" if dev_mode_enabled else "0")
    await db.set_setting("agent_backend_override", backend_override)
    await db.set_setting(AGENT_PROMPT_TEMPLATE_SETTING, prompt_template)
    agent_manager = deps.get_agent_manager(request)
    if agent_manager is not None:
        await agent_manager.refresh_settings_cache(preflight=True)
    return RedirectResponse(url="/settings?msg=agent_saved", status_code=303)



@router.post("/agent-providers/add")
async def add_agent_provider(request: Request):
    service = _agent_provider_service(request)
    if not service.writes_enabled:
        return RedirectResponse(
            url="/settings?error=agent_provider_secret_required", status_code=303
        )
    dev_mode_required = await _require_agent_dev_mode(request)
    if dev_mode_required is not None:
        return dev_mode_required
    form = await request.form()
    provider_name = str(form.get("provider", "")).strip()
    if deepagents_provider_spec(provider_name) is None:
        return RedirectResponse(url="/settings?error=agent_provider_invalid", status_code=303)
    configs = await service.load_provider_configs()
    if any(cfg.provider == provider_name for cfg in configs):
        return RedirectResponse(url="/settings?msg=agent_saved", status_code=303)
    priority = max((cfg.priority for cfg in configs), default=-1) + 1
    configs.append(service.create_empty_config(provider_name, priority))
    await service.save_provider_configs(configs)
    agent_manager = deps.get_agent_manager(request)
    if agent_manager is not None:
        await agent_manager.refresh_settings_cache(preflight=True)
    await _reload_llm_providers(request)
    return RedirectResponse(url="/settings?msg=agent_saved", status_code=303)


@router.post("/agent-providers/save")
async def save_agent_providers(request: Request):
    service = _agent_provider_service(request)
    if not service.writes_enabled:
        return RedirectResponse(
            url="/settings?error=agent_provider_secret_required", status_code=303
        )
    dev_mode_required = await _require_agent_dev_mode(request)
    if dev_mode_required is not None:
        return dev_mode_required
    form = await request.form()
    existing = await service.load_provider_configs()
    configs = service.parse_provider_form(form, existing)
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
            )
        )
    await service.save_provider_configs(validated)
    if is_persistent_manager:
        await manager.refresh_settings_cache(preflight=True)
    await _reload_llm_providers(request)
    return RedirectResponse(url="/settings?msg=agent_saved", status_code=303)


@router.post("/agent-providers/{provider_name}/delete")
async def delete_agent_provider(request: Request, provider_name: str):
    service = _agent_provider_service(request)
    if not service.writes_enabled:
        return RedirectResponse(
            url="/settings?error=agent_provider_secret_required", status_code=303
        )
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
    return RedirectResponse(url="/settings?msg=agent_saved", status_code=303)


@router.post("/agent-providers/{provider_name}/refresh")
async def refresh_agent_provider_models(request: Request, provider_name: str):
    service = _agent_provider_service(request)
    if not service.writes_enabled:
        return JSONResponse(
            {"ok": False, "error": "SESSION_ENCRYPTION_KEY is required."}, status_code=409
        )
    dev_mode_required = await _require_agent_dev_mode(request, json_mode=True)
    if dev_mode_required is not None:
        return dev_mode_required
    if deepagents_provider_spec(provider_name) is None:
        return JSONResponse({"ok": False, "error": "Unknown provider."}, status_code=404)
    form = await request.form()
    configs = await service.load_provider_configs()
    cfg = service.parse_single_provider_form(form, configs, provider_name)
    entry = await service.refresh_models_for_provider(provider_name, cfg)
    return JSONResponse(
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


@router.post("/agent-providers/refresh-all")
async def refresh_all_agent_provider_models(request: Request):
    service = _agent_provider_service(request)
    if not service.writes_enabled:
        return JSONResponse(
            {"ok": False, "error": "SESSION_ENCRYPTION_KEY is required."}, status_code=409
        )
    dev_mode_required = await _require_agent_dev_mode(request, json_mode=True)
    if dev_mode_required is not None:
        return dev_mode_required
    configs = await _provider_configs_from_bulk_form(request, service)
    config_map = {cfg.provider: cfg for cfg in configs}
    results = await service.refresh_all_models(configs)
    return JSONResponse(
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


@router.post("/agent-providers/{provider_name}/probe")
async def probe_agent_provider_model(request: Request, provider_name: str):
    service = _agent_provider_service(request)
    if not service.writes_enabled:
        return JSONResponse(
            {"ok": False, "error": "SESSION_ENCRYPTION_KEY is required."}, status_code=409
        )
    dev_mode_required = await _require_agent_dev_mode(request, json_mode=True)
    if dev_mode_required is not None:
        return dev_mode_required
    if deepagents_provider_spec(provider_name) is None:
        return JSONResponse({"ok": False, "error": "Unknown provider."}, status_code=404)

    form = await request.form()
    existing = await service.load_provider_configs()
    cfg = service.parse_single_provider_form(form, existing, provider_name)
    validation_error = service.validate_provider_config(cfg)
    if validation_error:
        logger.info(
            "Compatibility probe skipped: provider=%s model=%s status=unsupported reason=%s",
            provider_name,
            cfg.selected_model or "<empty>",
            validation_error,
        )
        return JSONResponse(
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
    return JSONResponse(
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


@router.post("/agent-providers/test-all")
async def test_all_agent_provider_models(request: Request):
    service = _agent_provider_service(request)
    if not service.writes_enabled:
        return JSONResponse(
            {"ok": False, "error": "SESSION_ENCRYPTION_KEY is required."}, status_code=409
        )
    if not await _dev_mode_enabled(request):
        return JSONResponse({"ok": False, "error": "Developer mode is required."}, status_code=403)
    configs = await _provider_configs_from_bulk_form(request, service)
    async with _bulk_test_lock(request):
        status = _bulk_test_status_payload(request)
        if status.get("running"):
            return JSONResponse(
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
    return JSONResponse({"ok": True, "started": True, **_bulk_test_status_payload(request)})


@router.get("/agent-providers/test-all/status")
async def test_all_agent_provider_models_status(request: Request):
    service = _agent_provider_service(request)
    if not service.writes_enabled:
        return JSONResponse(
            {"ok": False, "error": "SESSION_ENCRYPTION_KEY is required."}, status_code=409
        )
    if not await _dev_mode_enabled(request):
        return JSONResponse({"ok": False, "error": "Developer mode is required."}, status_code=403)
    return JSONResponse({"ok": True, **_bulk_test_status_payload(request)})


@router.post("/save-filters")
async def save_filters(request: Request):
    form = await request.form()
    db = deps.get_db(request)
    min_subs = str(form.get("min_subscribers_filter", "0")).strip()
    if not min_subs.isdigit():
        return RedirectResponse(url="/settings?error=invalid_value", status_code=303)
    await db.set_setting("min_subscribers_filter", min_subs)
    await db.set_setting(
        "auto_delete_filtered",
        "1" if form.get("auto_delete_filtered") else "0",
    )
    await db.set_setting(
        "auto_delete_on_collect",
        "1" if form.get("auto_delete_on_collect") else "0",
    )
    if int(min_subs) > 0:
        all_stats = await db.get_latest_stats_for_all()
        to_filter = [
            (channel_id, "low_subscriber_manual")
            for channel_id, stats in all_stats.items()
            if stats.subscriber_count is not None and stats.subscriber_count < int(min_subs)
        ]
        if to_filter:
            await db.set_channels_filtered_bulk(to_filter)
    return RedirectResponse(url="/settings?msg=filters_saved", status_code=303)


@router.post("/save-notification-account")
async def save_notification_account(request: Request):
    form = await request.form()
    selected_phone = str(form.get("notification_account_phone", "")).strip()
    db = deps.get_db(request)
    valid_phones = {acc.phone for acc in await db.get_accounts()}
    if selected_phone and selected_phone not in valid_phones:
        return RedirectResponse(url="/settings?error=notification_account_invalid", status_code=303)

    await deps.get_notification_target_service(request).set_configured_phone(selected_phone or None)
    # Invalidate cached me.id so the next notify() re-resolves it for the new account.
    notifier = deps.get_notifier(request)
    if notifier:
        notifier.invalidate_me_cache()
    return RedirectResponse(url="/settings?msg=notification_account_saved", status_code=303)


@router.post("/save-credentials")
async def save_credentials(request: Request):
    form = await request.form()
    db = deps.get_db(request)
    auth = deps.get_auth(request)

    api_id = str(form.get("api_id", "")).strip()
    api_hash = str(form.get("api_hash", "")).strip()

    id_changed = api_id and api_id != CREDENTIALS_MASK
    hash_changed = api_hash and api_hash != CREDENTIALS_MASK

    if id_changed and not api_id.isdigit():
        return RedirectResponse(url="/settings?error=invalid_api_id", status_code=303)

    if id_changed:
        await db.set_setting("tg_api_id", api_id)
    if hash_changed:
        await db.set_setting("tg_api_hash", api_hash)

    if id_changed or hash_changed:
        actual_id = api_id if id_changed else (await db.get_setting("tg_api_id") or "")
        actual_hash = api_hash if hash_changed else (await db.get_setting("tg_api_hash") or "")
        if actual_id and actual_hash:
            if not actual_id.isdigit():
                return RedirectResponse(url="/settings?error=invalid_api_id", status_code=303)
            auth.update_credentials(int(actual_id), actual_hash)

    return RedirectResponse(url="/settings?msg=credentials_saved", status_code=303)


@router.post("/notifications/setup")
async def setup_notification_bot(request: Request):
    return await _enqueue_notification_command(
        request,
        "notifications.setup_bot",
        requested_by="web:settings.notifications.setup",
        redirect_code="notification_setup_queued",
    )


@router.get("/notifications/status")
async def notification_bot_status(request: Request):
    payload = await _notification_snapshot_payload(request)
    raw_target = payload.get("target")
    if isinstance(raw_target, dict) and raw_target.get("state") not in {None, "available"}:
        return JSONResponse(
            {"configured": False, "error": raw_target.get("message", "")},
            status_code=409,
        )
    bot = await _notification_snapshot_bot(request)
    if bot is None:
        return JSONResponse({"configured": False})
    return JSONResponse(
        {
            "configured": True,
            "bot_username": bot.bot_username,
            "bot_id": bot.bot_id,
            "created_at": bot.created_at.isoformat() if bot.created_at else None,
        }
    )


@router.post("/notifications/delete")
async def delete_notification_bot(request: Request):
    return await _enqueue_notification_command(
        request,
        "notifications.delete_bot",
        requested_by="web:settings.notifications.delete",
        redirect_code="notification_delete_queued",
    )


@router.post("/notifications/test")
async def test_notification(request: Request):
    return await _enqueue_notification_command(
        request,
        "notifications.test",
        requested_by="web:settings.notifications.test",
        redirect_code="notification_test_queued",
    )


# ── Image Providers ──


@router.post("/image-providers/add")
async def add_image_provider(request: Request):
    service = _image_provider_service(request)
    if not service.writes_enabled:
        return RedirectResponse(url="/settings?error=image_provider_secret_required", status_code=303)
    form = await request.form()
    provider_name = str(form.get("provider", "")).strip()
    if image_provider_spec(provider_name) is None:
        return RedirectResponse(url="/settings?error=image_provider_invalid", status_code=303)
    configs = await service.load_provider_configs()
    if any(cfg.provider == provider_name for cfg in configs):
        return RedirectResponse(url="/settings?msg=image_saved", status_code=303)
    configs.append(service.create_empty_config(provider_name))
    await service.save_provider_configs(configs)
    return RedirectResponse(url="/settings?msg=image_saved", status_code=303)


@router.post("/image-providers/save")
async def save_image_providers(request: Request):
    service = _image_provider_service(request)
    if not service.writes_enabled:
        return RedirectResponse(url="/settings?error=image_provider_secret_required", status_code=303)
    form = await request.form()
    existing = await service.load_provider_configs()
    configs = service.parse_provider_form(form, existing)
    # Validate enabled configs have an API key or env var fallback
    for cfg in configs:
        if not cfg.enabled:
            continue
        spec = image_provider_spec(cfg.provider)
        if spec is None:
            continue
        has_key = bool(cfg.api_key.strip())
        has_env = any(os.environ.get(v) for v in spec.env_vars)
        if not has_key and not has_env:
            return RedirectResponse(
                url="/settings?error=image_provider_missing_key", status_code=303
            )
    await service.save_provider_configs(configs)
    default_model = str(form.get("default_image_model", "")).strip()
    await deps.get_db(request).set_setting("default_image_model", default_model)
    return RedirectResponse(url="/settings?msg=image_saved", status_code=303)


@router.post("/image-providers/{provider_name}/delete")
async def delete_image_provider(request: Request, provider_name: str):
    service = _image_provider_service(request)
    if not service.writes_enabled:
        return RedirectResponse(url="/settings?error=image_provider_secret_required", status_code=303)
    configs = await service.load_provider_configs()
    configs = [cfg for cfg in configs if cfg.provider != provider_name]
    await service.save_provider_configs(configs)
    return RedirectResponse(url="/settings?msg=image_saved", status_code=303)


@router.post("/save-translation")
async def save_translation_settings(request: Request):
    form = await request.form()
    db = deps.get_db(request)
    await db.set_setting("translation_provider", str(form.get("translation_provider", "")).strip())
    await db.set_setting("translation_model", str(form.get("translation_model", "")).strip())
    await db.set_setting("translation_target_lang", str(form.get("translation_target_lang", "")).strip().lower())
    await db.set_setting("translation_source_filter", str(form.get("translation_source_filter", "")).strip().lower())
    await db.set_setting("translation_auto_on_collect", "1" if form.get("translation_auto_on_collect") else "0")
    return RedirectResponse(url="/settings?msg=translation_saved#pane-translation", status_code=303)


@router.post("/translation-backfill")
async def translation_backfill_lang(request: Request):
    db = deps.get_db(request)
    updated = await db.repos.messages.backfill_language_detection(batch_size=5000)
    return RedirectResponse(
        url=f"/settings?msg=translation_backfill_done&count={updated}#pane-translation", status_code=303
    )


@router.post("/translation-run")
async def translation_run_batch(request: Request):
    form = await request.form()
    db = deps.get_db(request)
    target_lang = str(form.get("target_lang", "en")).strip().lower() or "en"
    source_filter_raw = await db.get_setting("translation_source_filter") or ""
    source_filter = [s.strip() for s in source_filter_raw.split(",") if s.strip()]

    from src.models import CollectionTaskType, TranslateBatchTaskPayload

    payload = TranslateBatchTaskPayload(
        target_lang=target_lang,
        source_filter=source_filter,
    )
    await db.repos.tasks.create_generic_task(
        CollectionTaskType.TRANSLATE_BATCH,
        title=f"Translation batch ({target_lang})",
        payload=payload,
    )
    return RedirectResponse(url="/settings?msg=translation_run_started#pane-translation", status_code=303)


    # Account toggle/delete endpoints moved to src/web/routes/accounts.py
