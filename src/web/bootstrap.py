from __future__ import annotations

import asyncio
import inspect
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
from src.database.repositories.accounts import AccountSessionDecryptError
from src.scheduler.service import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
from src.services.notification_target_service import NotificationTargetService
from src.services.photo_auto_upload_service import PhotoAutoUploadService
from src.services.photo_publish_service import PhotoPublishService
from src.services.photo_task_service import PhotoTaskService
from src.services.task_enqueuer import TaskEnqueuer
from src.services.telegram_command_dispatcher import TelegramCommandDispatcher
from src.services.unified_dispatcher import UnifiedDispatcher
from src.settings_utils import parse_int_setting
from src.telegram.auth import TelegramAuth
from src.telegram.client_pool import ClientPool
from src.telegram.collector import Collector
from src.telegram.notifier import Notifier
from src.web.container import AppContainer
from src.web.log_handler import LogBuffer
from src.web.paths import TEMPLATES_DIR
from src.web.runtime_shims import SnapshotClientPool, SnapshotCollector, SnapshotSchedulerManager
from src.web.template_globals import configure_template_globals
from src.web.timing import TimingBuffer

logger = logging.getLogger(__name__)
_is_dev = os.environ.get("ENV", "PROD").upper() == "DEV"
_POOL_INIT_TIMEOUT = 20
_POOL_RETRY_INTERVAL_SEC = 60.0
_SHUTDOWN_DEFAULT_TIMEOUT = 15.0
_SHUTDOWN_COLLECTION_QUEUE_TIMEOUT = 140.0


def _connected_pool_count(pool: object) -> int:
    clients = getattr(pool, "clients", {})
    try:
        return len(clients)
    except TypeError:
        return 0


async def _retry_telegram_pool_until_connected(container: AppContainer) -> None:
    while _connected_pool_count(container.pool) == 0 and getattr(
        container, "shutting_down", False
    ) is not True:
        try:
            await asyncio.wait_for(
                asyncio.sleep(_POOL_RETRY_INTERVAL_SEC),
                timeout=_POOL_RETRY_INTERVAL_SEC + 1,
            )
        except asyncio.CancelledError:
            raise
        if (
            getattr(container, "shutting_down", False) is True
            or _connected_pool_count(container.pool) > 0
        ):
            return
        try:
            await asyncio.wait_for(container.pool.initialize(), timeout=_POOL_INIT_TIMEOUT)
        except asyncio.TimeoutError:
            logger.warning(
                "startup: telegram pool retry timed out after %ds; connected_count=0",
                _POOL_INIT_TIMEOUT,
            )
        except AccountSessionDecryptError as exc:
            logger.warning(
                "startup: telegram pool retry degraded by session decrypt failure "
                "resource=%s identifier=%s status=%s action=%s",
                exc.resource,
                exc.identifier,
                exc.status,
                exc.action,
            )
        except Exception:
            logger.exception("startup: telegram pool retry failed")
        else:
            connected_count = _connected_pool_count(container.pool)
            if connected_count > 0:
                logger.info(
                    "startup: telegram pool recovered after retry; connected_count=%d",
                    connected_count,
                )
                return


def _schedule_telegram_pool_retry(container: AppContainer) -> None:
    bg_tasks = getattr(container, "bg_tasks", None)
    if not isinstance(bg_tasks, set):
        bg_tasks = set()
        setattr(container, "bg_tasks", bg_tasks)
    for task in bg_tasks:
        if not task.done() and task.get_name() == "telegram_pool_reconnect_retry":
            return
    task = asyncio.create_task(
        _retry_telegram_pool_until_connected(container),
        name="telegram_pool_reconnect_retry",
    )
    bg_tasks.add(task)
    task.add_done_callback(bg_tasks.discard)


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
    return await build_web_container(config, log_buffer=log_buffer)


async def build_web_container(config: AppConfig, *, log_buffer: LogBuffer) -> AppContainer:
    return await build_container_with_templates(
        config,
        log_buffer=log_buffer,
        templates=None,
        runtime_mode="web",
    )


async def build_worker_container(config: AppConfig, *, log_buffer: LogBuffer) -> AppContainer:
    return await build_container_with_templates(
        config,
        log_buffer=log_buffer,
        templates=None,
        runtime_mode="worker",
    )


async def build_container_with_templates(
    config: AppConfig,
    *,
    log_buffer: LogBuffer,
    timing_buffer: TimingBuffer | None = None,
    templates: Jinja2Templates | None,
    runtime_mode: str = "web",
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
    if runtime_mode == "worker":
        pool = ClientPool(
            auth,
            db,
            config.scheduler.max_flood_wait_sec,
            runtime_config=config.telegram_runtime,
        )
    else:
        pool = SnapshotClientPool(db)
        await pool.refresh()
    notification_target_service = NotificationTargetService(notification_bundle, pool)
    photo_publish_service = PhotoPublishService(pool)
    photo_task_service = PhotoTaskService(photo_loader_bundle, photo_publish_service)
    photo_auto_upload_service = PhotoAutoUploadService(photo_loader_bundle, photo_publish_service)
    notifier = None
    collection_queue = None
    unified_dispatcher = None
    telegram_command_dispatcher = None
    agent_manager = None
    if runtime_mode == "worker":
        notifier = Notifier(
            notification_target_service, config.notifications.admin_chat_id, notification_bundle
        )
        collector = Collector(pool, db, config.scheduler, notifier)
        collection_queue = CollectionQueue(collector, channel_bundle)
        search_pool = pool
    else:
        collector = SnapshotCollector(db)
        await collector.refresh()
        search_pool = None
    search_engine = SearchEngine(search_bundle, search_pool, config=config)
    ai_search = AISearchEngine(config.llm, search_bundle)
    search_query_bundle = SearchQueryBundle(repos.search_queries, repos.messages)

    from src.services.collection_service import CollectionService

    collection_service = CollectionService(channel_bundle, collector, collection_queue)
    task_enqueuer = TaskEnqueuer(db, collection_service)

    from src.services.provider_service import AgentProviderService

    llm_provider_service = AgentProviderService(db, config)
    await llm_provider_service.load_db_providers()

    if runtime_mode == "worker":
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
            llm_provider_service=llm_provider_service,
        )
        scheduler = SchedulerManager(
            config.scheduler,
            scheduler_bundle=scheduler_bundle,
            search_query_bundle=search_query_bundle,
            task_enqueuer=task_enqueuer,
            pipeline_bundle=pipeline_bundle,
            warm_dialogs_callback=pool.warm_all_dialogs,
        )
        telegram_command_dispatcher = TelegramCommandDispatcher(
            db,
            pool,
            config,
            collector,
            scheduler=scheduler,
            auth=auth,
        )
        agent_manager = AgentManager(db, config, client_pool=pool, scheduler_manager=scheduler)
    else:
        scheduler = SnapshotSchedulerManager(db, config.scheduler.collect_interval_minutes)
        await scheduler.load_settings()

    from src.services.translation_service import TranslationService

    translation_provider_service = llm_provider_service
    translation_service = TranslationService(db, provider_service=translation_provider_service)

    _templates = configure_template_globals(
        templates or Jinja2Templates(directory=str(TEMPLATES_DIR)),
        config,
    )

    if _is_dev:
        logger.info("startup/build: container_build %.2fs", time.monotonic() - t_build)

    return AppContainer(
        runtime_mode=runtime_mode,
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
        telegram_command_dispatcher=telegram_command_dispatcher,
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
    runtime_mode = getattr(container, "runtime_mode", "worker")
    if _is_dev:
        t1 = time.monotonic()

    photo_recovered = await container.photo_task_service.recover_running()
    if photo_recovered:
        logger.warning("Requeued %d interrupted photo tasks on startup", photo_recovered)
    gr_recovered = await container.db.repos.generation_runs.reset_running_on_startup()
    if gr_recovered:
        logger.warning("Reset %d stuck generation_runs to 'failed' on startup", gr_recovered)
    if runtime_mode == "worker":
        tc_recovered = await container.db.repos.telegram_commands.reset_running_on_startup()
        if tc_recovered:
            logger.warning(
                "Reset %d stuck telegram_commands from RUNNING to PENDING on startup", tc_recovered
            )
    logger.info("startup: recovery done (%.1fs)", time.monotonic() - t_start)
    if _is_dev:
        t1 = time.monotonic()

    if runtime_mode == "worker" and container.auth.is_configured:
        telegram_pool_degraded = False
        try:
            await asyncio.wait_for(container.pool.initialize(), timeout=_POOL_INIT_TIMEOUT)
        except asyncio.TimeoutError:
            logger.warning(
                "startup: telegram pool timed out after %ds — continuing without full init",
                _POOL_INIT_TIMEOUT,
            )
        except AccountSessionDecryptError as exc:
            telegram_pool_degraded = True
            logger.warning(
                "startup: telegram pool degraded by session decrypt failure "
                "resource=%s identifier=%s status=%s action=%s — continuing",
                exc.resource,
                exc.identifier,
                exc.status,
                exc.action,
            )
        connected_count = _connected_pool_count(container.pool)
        if connected_count == 0:
            telegram_pool_degraded = True
            logger.warning(
                "startup: telegram pool degraded; connected_count=0 — pending collection tasks "
                "will stay in DB until a client connects"
            )
            if container.collection_queue is not None:
                _schedule_telegram_pool_retry(container)
        # Warm entity cache + preferred_phone map for all accounts in background.
        # Store the task so the collector can wait on it during a race condition.
        if not telegram_pool_degraded and hasattr(container.pool, "warm_all_dialogs"):
            _warm_task = asyncio.create_task(
                container.pool.warm_all_dialogs(), name="warm_all_dialogs_startup"
            )
            container.pool._warming_task = _warm_task
    logger.info("startup: telegram pool done (%.1fs)", time.monotonic() - t_start)
    if _is_dev:
        logger.info("startup/start: telegram_pool %.2fs", time.monotonic() - t1)

    if runtime_mode == "worker" and container.collection_queue is not None:
        if container.auth.is_configured and _connected_pool_count(container.pool) == 0:
            logger.warning(
                "Skipping startup collection requeue because telegram pool has no connected clients"
            )
        else:
            requeued = await container.collection_queue.requeue_startup_tasks()
            if requeued:
                logger.info("Re-enqueued %d pending collection tasks on startup", requeued)
        # Periodically re-check the DB for new PENDING tasks created by the
        # web container in split / embedded-worker setups (CollectionService
        # falls back to a DB-only insert when collection_queue is None).
        container.collection_queue.start_db_pull()
    logger.info("startup: collection queue done (%.1fs)", time.monotonic() - t_start)

    if _is_dev:
        t1 = time.monotonic()
    if runtime_mode == "worker" and container.unified_dispatcher is not None:
        result = container.unified_dispatcher.start()
        if inspect.isawaitable(result):
            await result
    telegram_command_dispatcher = getattr(container, "telegram_command_dispatcher", None)
    if runtime_mode == "worker" and telegram_command_dispatcher is not None:
        start = getattr(telegram_command_dispatcher, "start", None)
        if callable(start):
            result = start()
            if inspect.isawaitable(result):
                await result
    logger.info("startup: dispatcher done (%.1fs)", time.monotonic() - t_start)
    container.ai_search.initialize()
    logger.info("startup: ai_search done (%.1fs)", time.monotonic() - t_start)
    if runtime_mode == "worker" and container.agent_manager is not None:
        await container.agent_manager.refresh_settings_cache(preflight=True)
        container.agent_manager.initialize()
    logger.info("startup: agent_manager done (%.1fs)", time.monotonic() - t_start)
    if _is_dev:
        logger.info("startup/start: dispatcher+ai+agent %.2fs", time.monotonic() - t1)
        t1 = time.monotonic()

    if runtime_mode == "worker":
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

    async def _stop_step(name: str, coro, *, timeout: float = _SHUTDOWN_DEFAULT_TIMEOUT) -> None:
        try:
            await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("Shutdown of %s timed out after %.0fs", name, timeout)
        except Exception:
            logger.warning("Error shutting down %s", name, exc_info=True)

    # Stop producers first so no new collection work is created while the
    # queue is draining the active channel collection.
    await _stop_step("scheduler", container.scheduler.stop())
    if container.unified_dispatcher is not None:
        await _stop_step("unified_dispatcher", container.unified_dispatcher.stop())
    if container.telegram_command_dispatcher is not None:
        await _stop_step("telegram_command_dispatcher", container.telegram_command_dispatcher.stop())
    if container.collection_queue is not None:
        await _stop_step(
            "collection_queue",
            container.collection_queue.shutdown(),
            timeout=_SHUTDOWN_COLLECTION_QUEUE_TIMEOUT,
        )
    if container.agent_manager is not None:
        await _stop_step("agent_manager", container.agent_manager.close_all())

    await _stop_step("collector", container.collector.cancel())
    await _stop_step("bg_tasks", _cancel_bg_tasks(container.bg_tasks))
    await _stop_step("pool", container.pool.disconnect_all())
    await _stop_step("auth", container.auth.cleanup())
    await _stop_step("db", container.db.close())
