from __future__ import annotations

import asyncio
import base64
import logging
import secrets
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from src.collection_queue import CollectionQueue
from src.config import AppConfig, load_config, resolve_session_encryption_secret
from src.database import Database
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
from src.web.csrf import OriginCSRFMiddleware
from src.web.session import (
    COOKIE_MAX_AGE,
    COOKIE_NAME,
    create_session_token,
    verify_session_token,
)

logger = logging.getLogger(__name__)


class BasicAuthMiddleware(BaseHTTPMiddleware):
    _USERNAME = "admin"

    def __init__(self, app, password: str):
        super().__init__(app)
        self.password = password

    async def dispatch(self, request, call_next):
        if request.url.path in ("/health", "/logout"):
            return await call_next(request)

        # 1. Check session cookie
        cookie = request.cookies.get(COOKIE_NAME)
        if cookie:
            secret = getattr(request.app.state, "session_secret", None)
            if secret:
                cookie_user = verify_session_token(cookie, secret)
                if cookie_user and cookie_user == self._USERNAME:
                    return await call_next(request)

        # 2. Fallback to Basic Auth — only password is checked
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Basic "):
            try:
                decoded = base64.b64decode(auth[6:]).decode()
            except Exception:
                decoded = ""
            # Accept any username (or empty), only match password
            _, _, pwd = decoded.partition(":")
            if secrets.compare_digest(pwd, self.password):
                response = await call_next(request)
                secret = getattr(request.app.state, "session_secret", None)
                if secret:
                    token = create_session_token(self._USERNAME, secret)
                    response.set_cookie(
                        COOKIE_NAME,
                        token,
                        max_age=COOKIE_MAX_AGE,
                        httponly=True,
                        samesite="lax",
                        secure=request.url.scheme == "https",
                    )
                return response

        return Response(
            "Unauthorized",
            status_code=401,
            headers={"WWW-Authenticate": "Basic realm='TG Post Search'"},
        )

WEB_DIR = Path(__file__).parent
TEMPLATES_DIR = WEB_DIR / "templates"
STATIC_DIR = WEB_DIR / "static"


async def _cancel_bg_tasks(tasks: set[asyncio.Task]) -> None:
    for task in list(tasks):
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    tasks.clear()


@asynccontextmanager
async def lifespan(app: FastAPI):
    config: AppConfig = app.state.config

    # Database
    db = Database(
        config.database.path,
        session_encryption_secret=resolve_session_encryption_secret(config),
    )
    await db.initialize()
    recovered = await db.fail_running_collection_tasks_on_startup()
    if recovered:
        logger.warning("Marked %d interrupted collection tasks as failed on startup", recovered)
    app.state.db = db

    # Session secret key
    session_secret = await db.get_setting("session_secret_key")
    if not session_secret:
        session_secret = secrets.token_hex(32)
        await db.set_setting("session_secret_key", session_secret)
    app.state.session_secret = session_secret

    # Telegram auth — try config, then DB settings
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

    auth = TelegramAuth(api_id, api_hash)
    app.state.auth = auth

    # Client pool
    pool = ClientPool(auth, db, config.scheduler.max_flood_wait_sec)
    if auth.is_configured:
        await pool.initialize()
    app.state.pool = pool

    # Notification account selection + notifier
    notification_target_service = NotificationTargetService(db, pool)
    app.state.notification_target_service = notification_target_service

    notifier = Notifier(notification_target_service, config.notifications.admin_chat_id)
    app.state.notifier = notifier

    # Collector
    collector = Collector(pool, db, config.scheduler, notifier)
    app.state.collector = collector

    # Collection queue
    collection_queue = CollectionQueue(collector, db)
    requeued = await collection_queue.requeue_startup_tasks()
    if requeued:
        logger.info("Re-enqueued %d pending collection tasks on startup", requeued)
    app.state.collection_queue = collection_queue

    # Deferred stats dispatcher
    stats_dispatcher = StatsTaskDispatcher(collector, db, default_batch_size=20)
    await stats_dispatcher.start()
    app.state.stats_dispatcher = stats_dispatcher

    # Search engines
    search_engine = SearchEngine(db, pool)
    app.state.search_engine = search_engine

    ai_search = AISearchEngine(config.llm, db)
    ai_search.initialize()
    app.state.ai_search = ai_search

    # Scheduler
    scheduler = SchedulerManager(
        collector, config.scheduler, search_engine=search_engine, db=db,
    )
    app.state.scheduler = scheduler

    # Background task tracking
    bg_tasks: set[asyncio.Task] = set()
    app.state.bg_tasks = bg_tasks

    app.state.shutting_down = False

    logger.info("Application started")
    try:
        yield
    finally:
        app.state.shutting_down = True
        logger.info("Shutting down...")
        for name, coro in [
            ("stats_dispatcher", stats_dispatcher.stop()),
            ("scheduler", scheduler.stop()),
            ("collector", collector.cancel()),
            ("collection_queue", collection_queue.shutdown()),
            ("bg_tasks", _cancel_bg_tasks(bg_tasks)),
            ("pool", pool.disconnect_all()),
            ("auth", auth.cleanup()),
            ("db", db.close()),
        ]:
            try:
                await asyncio.wait_for(coro, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("Shutdown of %s timed out", name)
            except Exception:
                logger.warning("Error shutting down %s", name, exc_info=True)
        logger.info("Application shut down")


def create_app(config: AppConfig | None = None) -> FastAPI:
    if config is None:
        config = load_config()

    app = FastAPI(title="TG Post Search", lifespan=lifespan)
    app.state.config = config

    if config.web.password:
        app.add_middleware(
            BasicAuthMiddleware,
            password=config.web.password,
        )
    app.add_middleware(OriginCSRFMiddleware)

    # Static files
    if STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

    # Templates
    app.state.templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

    # Health check
    @app.get("/health")
    async def health_check(request: Request):
        db_ok = False
        try:
            await request.app.state.db.execute("SELECT 1")
            db_ok = True
        except Exception:
            pass
        accounts_connected = len(request.app.state.pool.clients)
        status = "healthy" if db_ok else "degraded"
        return JSONResponse(
            {"status": status, "db": db_ok, "accounts_connected": accounts_connected}
        )

    # Logout
    @app.get("/logout")
    async def logout():
        html = (
            "<!DOCTYPE html><html lang='ru'><head><meta charset='UTF-8'>"
            "<title>Выход</title>"
            "<link rel='stylesheet' "
            "href='https://cdn.jsdelivr.net/npm/@picocss/pico@2/css/pico.min.css'>"
            "</head><body><main class='container' style='text-align:center;margin-top:20vh'>"
            "<h2>Вы вышли из системы</h2>"
            "<p><a href='/'>Войти снова</a></p>"
            "</main></body></html>"
        )
        response = Response(content=html, status_code=401, media_type="text/html")
        response.delete_cookie(COOKIE_NAME)
        return response

    # Register routes
    from src.web.routes.auth import router as auth_router
    from src.web.routes.channel_collection import router as channel_collection_router
    from src.web.routes.channels import router as channels_router
    from src.web.routes.dashboard import router as dashboard_router
    from src.web.routes.filter import router as filter_router
    from src.web.routes.import_channels import router as import_router
    from src.web.routes.keywords import router as keywords_router
    from src.web.routes.scheduler import router as scheduler_router
    from src.web.routes.search import router as search_router
    from src.web.routes.settings import router as settings_router

    app.include_router(search_router)
    app.include_router(dashboard_router, prefix="/dashboard")
    app.include_router(auth_router, prefix="/auth")
    app.include_router(channels_router, prefix="/channels")
    app.include_router(filter_router, prefix="/channels")
    app.include_router(keywords_router, prefix="/channels")
    app.include_router(channel_collection_router, prefix="/channels")
    app.include_router(import_router, prefix="/channels")
    app.include_router(scheduler_router, prefix="/scheduler")
    app.include_router(settings_router, prefix="/settings")

    return app
