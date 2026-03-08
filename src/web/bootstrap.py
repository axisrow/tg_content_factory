from __future__ import annotations

import asyncio
import logging
import secrets
from pathlib import Path

from fastapi.templating import Jinja2Templates

from src.collection_queue import CollectionQueue
from src.config import AppConfig, resolve_session_encryption_secret
from src.database import Database
from src.database.bundles import (
    AccountBundle,
    ChannelBundle,
    CollectionBundle,
    NotificationBundle,
    SchedulerBundle,
    SearchBundle,
)
from src.scheduler.manager import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
from src.services.notification_target_service import NotificationTargetService
from src.services.stats_task_dispatcher import StatsTaskDispatcher
from src.settings_utils import parse_int_setting
from src.telegram.auth import TelegramAuth
from src.telegram.client_pool import ClientPool
from src.telegram.collector import Collector
from src.telegram.notifier import Notifier
from src.web.container import AppContainer
from src.web.log_handler import LogBuffer

logger = logging.getLogger(__name__)

WEB_DIR = Path(__file__).parent
TEMPLATES_DIR = WEB_DIR / "templates"


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
    templates: Jinja2Templates | None,
) -> AppContainer:
    db = Database(
        config.database.path,
        session_encryption_secret=resolve_session_encryption_secret(config),
    )
    await db.initialize()

    repos = db.repos
    account_bundle = AccountBundle(repos.accounts)
    channel_bundle = ChannelBundle(repos.channels, repos.channel_stats, repos.tasks)
    collection_bundle = CollectionBundle(
        repos.channels,
        repos.messages,
        repos.filters,
        repos.settings,
        repos.keywords,
        repos.tasks,
        repos.channel_stats,
    )
    notification_bundle = NotificationBundle(
        repos.accounts,
        repos.settings,
        repos.notification_bots,
    )
    search_bundle = SearchBundle(repos.messages, repos.search_log, repos.channels)
    scheduler_bundle = SchedulerBundle(
        repos.settings,
        repos.keywords,
        repos.tasks,
        repos.search_log,
    )

    session_secret = await db.get_setting("session_secret_key")
    if not session_secret:
        session_secret = secrets.token_hex(32)
        await db.set_setting("session_secret_key", session_secret)

    api_id, api_hash = await load_telegram_credentials(db, config)
    auth = TelegramAuth(api_id, api_hash)
    pool = ClientPool(auth, db, config.scheduler.max_flood_wait_sec)
    notification_target_service = NotificationTargetService(notification_bundle, pool)
    notifier = Notifier(notification_target_service, config.notifications.admin_chat_id)
    collector = Collector(pool, db, config.scheduler, notifier)
    collection_queue = CollectionQueue(collector, channel_bundle)
    stats_dispatcher = StatsTaskDispatcher(collector, channel_bundle, default_batch_size=20)
    search_engine = SearchEngine(search_bundle, pool)
    ai_search = AISearchEngine(config.llm, search_bundle)
    scheduler = SchedulerManager(
        collector,
        config.scheduler,
        scheduler_bundle=scheduler_bundle,
        search_engine=search_engine,
    )

    return AppContainer(
        config=config,
        db=db,
        repos=repos,
        account_bundle=account_bundle,
        channel_bundle=channel_bundle,
        collection_bundle=collection_bundle,
        notification_bundle=notification_bundle,
        search_bundle=search_bundle,
        scheduler_bundle=scheduler_bundle,
        auth=auth,
        pool=pool,
        notification_target_service=notification_target_service,
        notifier=notifier,
        collector=collector,
        collection_queue=collection_queue,
        stats_dispatcher=stats_dispatcher,
        search_engine=search_engine,
        ai_search=ai_search,
        scheduler=scheduler,
        templates=templates or Jinja2Templates(directory=str(TEMPLATES_DIR)),
        log_buffer=log_buffer,
        session_secret=session_secret,
        bg_tasks=set(),
        shutting_down=False,
    )


async def start_container(container: AppContainer) -> None:
    recovered = await container.channel_bundle.fail_running_collection_tasks_on_startup()
    if recovered:
        logger.warning("Marked %d interrupted collection tasks as failed on startup", recovered)

    if container.auth.is_configured:
        await container.pool.initialize()

    if container.collection_queue is not None:
        requeued = await container.collection_queue.requeue_startup_tasks()
        if requeued:
            logger.info("Re-enqueued %d pending collection tasks on startup", requeued)

    if container.stats_dispatcher is not None:
        await container.stats_dispatcher.start()
    container.ai_search.initialize()


async def _cancel_bg_tasks(tasks: set[asyncio.Task]) -> None:
    for task in list(tasks):
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    tasks.clear()


async def stop_container(container: AppContainer) -> None:
    container.shutting_down = True
    shutdown_coroutines = []
    if container.stats_dispatcher is not None:
        shutdown_coroutines.append(("stats_dispatcher", container.stats_dispatcher.stop()))
    if container.collection_queue is not None:
        shutdown_coroutines.append(("collection_queue", container.collection_queue.shutdown()))
    shutdown_coroutines.extend([
        ("scheduler", container.scheduler.stop()),
        ("collector", container.collector.cancel()),
        ("bg_tasks", _cancel_bg_tasks(container.bg_tasks)),
        ("pool", container.pool.disconnect_all()),
        ("auth", container.auth.cleanup()),
        ("db", container.db.close()),
    ])
    for name, coro in shutdown_coroutines:
        try:
            await asyncio.wait_for(coro, timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("Shutdown of %s timed out", name)
        except Exception:
            logger.warning("Error shutting down %s", name, exc_info=True)
