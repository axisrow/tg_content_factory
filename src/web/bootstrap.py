from __future__ import annotations

import asyncio
import logging
import os
import secrets
import time

from fastapi.templating import Jinja2Templates

from src.agent.manager import AgentManager
from src.collection_queue import CollectionQueue
from src.config import AppConfig, resolve_session_encryption_secret
from src.database import Database
from src.database.bundles import (
    AccountBundle,
    ChannelBundle,
    CollectionBundle,
    NotificationBundle,
    PhotoLoaderBundle,
    PipelineBundle,
    SchedulerBundle,
    SearchBundle,
    SearchQueryBundle,
)
from src.scheduler.service import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
from src.services.notification_target_service import NotificationTargetService
from src.services.photo_auto_upload_service import PhotoAutoUploadService
from src.services.photo_publish_service import PhotoPublishService
from src.services.photo_task_service import PhotoTaskService
from src.services.task_enqueuer import TaskEnqueuer
from src.services.unified_dispatcher import UnifiedDispatcher
from src.settings_utils import parse_int_setting
from src.telegram.auth import TelegramAuth
from src.telegram.client_pool import ClientPool
from src.telegram.collector import Collector
from src.telegram.notifier import Notifier
from src.web.container import AppContainer
from src.web.log_handler import LogBuffer
from src.web.paths import TEMPLATES_DIR
from src.web.template_globals import configure_template_globals
from src.web.timing import TimingBuffer

logger = logging.getLogger(__name__)
_is_dev = os.environ.get("ENV", "PROD").upper() == "DEV"
_POOL_INIT_TIMEOUT = 20


async def load_telegram_credentials(db: Database, config: AppConfig) -> tuple[int, str]:
    api_id = config.telegram.api_id
    api_hash = config.telegram.api_hash
    if api_id == 0 or not api_hash:
        stored_id = await db.get_setting("tg_api_id")
        stored_hash = await db.get_setting("tg_api_hash")
        if stored_id and stored_hash:
            api_id = parse_int_setting(
                stored_id,
                setting_name="tg_api_id",
                default=0,
                logger=logger,
            )
            api_hash = stored_hash
    return api_id, api_hash


async def build_container(config: AppConfig, *, log_buffer: LogBuffer) -> AppContainer:
    return await build_container_with_templates(config, log_buffer=log_buffer, templates=None)


async def build_container_with_templates(
    config: AppConfig,
    *,
    log_buffer: LogBuffer,
    timing_buffer: TimingBuffer | None = None,
    templates: Jinja2Templates | None,
) -> AppContainer:
    if _is_dev:
        t_build = time.monotonic()

    db = Database(
        config.database.path,
        session_encryption_secret=resolve_session_encryption_secret(config),
    )
    if _is_dev:
        t1 = time.monotonic()
    await db.initialize()
    if _is_dev:
        logger.info("startup/build: db_init %.2fs", time.monotonic() - t1)

    repos = db.repos
    account_bundle = AccountBundle(repos.accounts)
    channel_bundle = ChannelBundle(repos.channels, repos.channel_stats, repos.tasks)
    collection_bundle = CollectionBundle(
        repos.channels,
        repos.messages,
        repos.filters,
        repos.settings,
        repos.search_queries,
        repos.tasks,
        repos.channel_stats,
    )
    notification_bundle = NotificationBundle(
        repos.accounts,
        repos.settings,
        repos.notification_bots,
    )
    pipeline_bundle = PipelineBundle.from_database(db)
    photo_loader_bundle = PhotoLoaderBundle(repos.photo_loader)
    search_bundle = SearchBundle(
        repos.messages,
        repos.search_log,
        repos.channels,
        repos.settings,
    )
    scheduler_bundle = SchedulerBundle(
        repos.settings,
        repos.search_queries,
        repos.tasks,
        repos.search_log,
    )

    session_secret = await db.get_setting("session_secret_key")
    if not session_secret:
        session_secret = secrets.token_hex(32)
        await db.set_setting("session_secret_key", session_secret)

    api_id, api_hash = await load_telegram_credentials(db, config)
    auth = TelegramAuth(api_id, api_hash)
    pool = ClientPool(
        auth,
        db,
        config.scheduler.max_flood_wait_sec,
        runtime_config=config.telegram_runtime,
    )
    notification_target_service = NotificationTargetService(notification_bundle, pool)
    photo_publish_service = PhotoPublishService(pool)
    photo_task_service = PhotoTaskService(photo_loader_bundle, photo_publish_service)
    photo_auto_upload_service = PhotoAutoUploadService(photo_loader_bundle, photo_publish_service)
    notifier = Notifier(
        notification_target_service, config.notifications.admin_chat_id, notification_bundle
    )
    collector = Collector(pool, db, config.scheduler, notifier)
    collection_queue = CollectionQueue(collector, channel_bundle)
    search_engine = SearchEngine(search_bundle, pool, config=config)
    ai_search = AISearchEngine(config.llm, search_bundle)
    search_query_bundle = SearchQueryBundle(repos.search_queries, repos.messages)

    from src.services.collection_service import CollectionService

    collection_service = CollectionService(channel_bundle, collector, collection_queue)
    task_enqueuer = TaskEnqueuer(db, collection_service)
    unified_dispatcher = UnifiedDispatcher(
        collector,
        channel_bundle,
        repos.tasks,
        sq_bundle=search_query_bundle,
        photo_task_service=photo_task_service,
        photo_auto_upload_service=photo_auto_upload_service,
        search_engine=search_engine,
        pipeline_bundle=pipeline_bundle,
        db=db,
        client_pool=pool,
        notifier=notifier,
        config=config,
    )
    scheduler = SchedulerManager(
        config.scheduler,
        scheduler_bundle=scheduler_bundle,
        search_query_bundle=search_query_bundle,
        task_enqueuer=task_enqueuer,
        pipeline_bundle=pipeline_bundle,
    )
    agent_manager = AgentManager(db, config, client_pool=pool, scheduler_manager=scheduler)

    from src.services.provider_service import AgentProviderService
    from src.services.translation_service import TranslationService

    llm_provider_service = AgentProviderService(db, config)
    await llm_provider_service.load_db_providers()
    translation_provider_service = llm_provider_service
    translation_service = TranslationService(db, provider_service=translation_provider_service)

    _templates = configure_template_globals(
        templates or Jinja2Templates(directory=str(TEMPLATES_DIR)),
        config,
    )

    if _is_dev:
        logger.info("startup/build: container_build %.2fs", time.monotonic() - t_build)

    return AppContainer(
        config=config,
        db=db,
        repos=repos,
        account_bundle=account_bundle,
        channel_bundle=channel_bundle,
        collection_bundle=collection_bundle,
        notification_bundle=notification_bundle,
        pipeline_bundle=pipeline_bundle,
        photo_loader_bundle=photo_loader_bundle,
        search_bundle=search_bundle,
        scheduler_bundle=scheduler_bundle,
        search_query_bundle=search_query_bundle,
        auth=auth,
        pool=pool,
        notification_target_service=notification_target_service,
        notifier=notifier,
        photo_publish_service=photo_publish_service,
        photo_task_service=photo_task_service,
        photo_auto_upload_service=photo_auto_upload_service,
        collector=collector,
        collection_queue=collection_queue,
        task_enqueuer=task_enqueuer,
        unified_dispatcher=unified_dispatcher,
        search_engine=search_engine,
        ai_search=ai_search,
        scheduler=scheduler,
        templates=_templates,
        log_buffer=log_buffer,
        timing_buffer=timing_buffer,
        session_secret=session_secret,
        bg_tasks=set(),
        agent_manager=agent_manager,
        translation_service=translation_service,
        llm_provider_service=llm_provider_service,
        shutting_down=False,
    )


async def start_container(container: AppContainer) -> None:
    t_start = time.monotonic()
    if _is_dev:
        t1 = time.monotonic()

    recovered = await container.channel_bundle.fail_running_collection_tasks_on_startup()
    if recovered:
        logger.warning("Marked %d interrupted collection tasks as failed on startup", recovered)
    photo_recovered = await container.photo_task_service.recover_running()
    if photo_recovered:
        logger.warning("Requeued %d interrupted photo tasks on startup", photo_recovered)
    gr_recovered = await container.db.repos.generation_runs.reset_running_on_startup()
    if gr_recovered:
        logger.warning("Reset %d stuck generation_runs to 'failed' on startup", gr_recovered)
    logger.info("startup: recovery done (%.1fs)", time.monotonic() - t_start)
    if _is_dev:
        t1 = time.monotonic()

    if container.auth.is_configured:
        try:
            await asyncio.wait_for(container.pool.initialize(), timeout=_POOL_INIT_TIMEOUT)
        except asyncio.TimeoutError:
            logger.warning(
                "startup: telegram pool timed out after %ds — continuing without full init",
                _POOL_INIT_TIMEOUT,
            )
    logger.info("startup: telegram pool done (%.1fs)", time.monotonic() - t_start)
    if _is_dev:
        logger.info("startup/start: telegram_pool %.2fs", time.monotonic() - t1)

    if container.collection_queue is not None:
        requeued = await container.collection_queue.requeue_startup_tasks()
        if requeued:
            logger.info("Re-enqueued %d pending collection tasks on startup", requeued)
    logger.info("startup: collection queue done (%.1fs)", time.monotonic() - t_start)

    if _is_dev:
        t1 = time.monotonic()
    if container.unified_dispatcher is not None:
        await container.unified_dispatcher.start()
    logger.info("startup: dispatcher done (%.1fs)", time.monotonic() - t_start)
    container.ai_search.initialize()
    logger.info("startup: ai_search done (%.1fs)", time.monotonic() - t_start)
    if container.agent_manager is not None:
        await container.agent_manager.refresh_settings_cache(preflight=True)
        container.agent_manager.initialize()
    logger.info("startup: agent_manager done (%.1fs)", time.monotonic() - t_start)
    if _is_dev:
        logger.info("startup/start: dispatcher+ai+agent %.2fs", time.monotonic() - t1)
        t1 = time.monotonic()

    await container.scheduler.load_settings()
    autostart = await container.db.get_setting("scheduler_autostart")
    if autostart == "1":
        logger.info("Auto-starting scheduler (scheduler_autostart=1)")
        await container.scheduler.start()
    logger.info("startup: scheduler done (%.1fs)", time.monotonic() - t_start)
    logger.info("startup: READY (total %.1fs)", time.monotonic() - t_start)
    if _is_dev:
        logger.info("startup/start: scheduler %.2fs", time.monotonic() - t1)
        logger.info("startup/start: TOTAL %.2fs", time.monotonic() - t_start)


async def _cancel_bg_tasks(tasks: set[asyncio.Task]) -> None:
    for task in list(tasks):
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    tasks.clear()


async def stop_container(container: AppContainer) -> None:
    container.shutting_down = True
    shutdown_coroutines = []
    if container.unified_dispatcher is not None:
        shutdown_coroutines.append(("unified_dispatcher", container.unified_dispatcher.stop()))
    if container.collection_queue is not None:
        shutdown_coroutines.append(("collection_queue", container.collection_queue.shutdown()))
    if container.agent_manager is not None:
        shutdown_coroutines.append(("agent_manager", container.agent_manager.close_all()))
    shutdown_coroutines.extend(
        [
            ("scheduler", container.scheduler.stop()),
            ("collector", container.collector.cancel()),
            ("bg_tasks", _cancel_bg_tasks(container.bg_tasks)),
            ("pool", container.pool.disconnect_all()),
            ("auth", container.auth.cleanup()),
            ("db", container.db.close()),
        ]
    )
    for name, coro in shutdown_coroutines:
        try:
            await asyncio.wait_for(coro, timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Shutdown of %s timed out", name)
        except Exception:
            logger.warning("Error shutting down %s", name, exc_info=True)
