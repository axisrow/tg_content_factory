from __future__ import annotations

import base64
import logging
import os
import re
import secrets
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import HTMLResponse, RedirectResponse, Response

from src.config import AppConfig, load_config
from src.web.assembly import (
    build_log_buffer,
    build_timing_buffer,
    configure_app,
    register_builtin_endpoints,
    register_routes,
)
from src.web.bootstrap import build_container_with_templates, start_container, stop_container
from src.web.csrf import OriginCSRFMiddleware
from src.web.panel_auth import (
    get_cookie_user,
    is_public_path,
    login_redirect_url,
    redirect_target_from_request,
    set_session_cookie,
)
from src.web.paths import TEMPLATES_DIR
from src.web.template_globals import configure_template_globals

logger = logging.getLogger(__name__)


_btn_logger = logging.getLogger("button")
_LOG_METHODS = {"POST", "PUT", "PATCH", "DELETE"}

# URL pattern → human-readable button label for /debug/ logs
_ACTION_LABELS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"^/scheduler/start$"), "Запустить планировщик"),
    (re.compile(r"^/scheduler/stop$"), "Остановить планировщик"),
    (re.compile(r"^/scheduler/trigger$"), "Собрать все каналы"),
    (re.compile(r"^/scheduler/test-notification$"), "Тест уведомлений"),
    (re.compile(r"^/scheduler/dry-run-notifications$"), "Dry-run уведомлений"),
    (re.compile(r"^/scheduler/tasks/\d+/cancel$"), "Отменить задачу"),
    (re.compile(r"^/scheduler/tasks/clear-pending-collect$"), "Очистить очередь"),
    (re.compile(r"^/channels/add$"), "Добавить канал"),
    (re.compile(r"^/channels/add-bulk$"), "Добавить выбранные каналы"),
    (re.compile(r"^/channels/collect-all$"), "Собрать все каналы"),
    (re.compile(r"^/channels/stats/all$"), "Обновить статистику всех"),
    (re.compile(r"^/channels/\d+/collect$"), "Загрузить канал"),
    (re.compile(r"^/channels/\d+/stats$"), "Обновить статистику"),
    (re.compile(r"^/channels/\d+/toggle$"), "Вкл/Откл канал"),
    (re.compile(r"^/channels/\d+/delete$"), "Удалить канал"),
    (re.compile(r"^/channels/\d+/filter-toggle$"), "Переключить фильтр"),
    (re.compile(r"^/channels/-?\d+/purge-messages$"), "Очистить сообщения"),
    (re.compile(r"^/moderation/\d+/approve$"), "Одобрить"),
    (re.compile(r"^/moderation/\d+/reject$"), "Отклонить"),
    (re.compile(r"^/moderation/\d+/publish$"), "Опубликовать"),
    (re.compile(r"^/moderation/bulk-approve$"), "Одобрить выбранные"),
    (re.compile(r"^/moderation/bulk-reject$"), "Отклонить выбранные"),
    (re.compile(r"^/settings/\d+/toggle$"), "Вкл/Откл аккаунт"),
    (re.compile(r"^/settings/\d+/delete$"), "Удалить аккаунт"),
    (re.compile(r"^/agent/threads$"), "Новый тред"),
    (re.compile(r"^/agent/threads/\d+/chat$"), "Сообщение агенту"),
    (re.compile(r"^/agent/threads/\d+/context$"), "Загрузить контекст"),
    (re.compile(r"^/agent/threads/\d+/stop$"), "Остановить генерацию"),
    (re.compile(r"^/agent/threads/\d+$"), "Удалить тред"),
]


def _resolve_action_label(path: str) -> str:
    for pattern, label in _ACTION_LABELS:
        if pattern.match(path):
            return label
    return ""


_PROFILING_ENABLED = os.environ.get("ENV", "PROD").upper() == "DEV"


class TimingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        profiler = None
        if _PROFILING_ENABLED:
            from src.web.timing import RequestProfiler

            profiler = RequestProfiler()
            profiler.activate()

        t0 = time.monotonic()
        response = None
        try:
            response = await call_next(request)
        finally:
            ms = int((time.monotonic() - t0) * 1000)
            path = request.url.path
            if not is_public_path(path):
                status = response.status_code if response is not None else 500
                buf = getattr(request.app.state, "timing_buffer", None)
                if buf is not None:
                    record = {
                        "time": time.strftime("%H:%M:%S"),
                        "method": request.method,
                        "path": path,
                        "status": status,
                        "ms": ms,
                    }
                    if profiler is not None:
                        record.update(profiler.to_breakdown())
                    buf.add(record)
                if ms > 500:
                    if profiler is not None:
                        bd = profiler.to_breakdown()
                        logger.warning(
                            "SLOW %s %s %dms [%d] db=%dms/%dq",
                            request.method, path, ms, status, bd["db_ms"], bd["db_queries"],
                        )
                    else:
                        logger.warning("SLOW %s %s %dms [%d]", request.method, path, ms, status)
                elif profiler is not None:
                    bd = profiler.to_breakdown()
                    logger.info(
                        "%s %s %dms [%d] db=%dms/%dq",
                        request.method, path, ms, status, bd["db_ms"], bd["db_queries"],
                    )
            if profiler is not None:
                profiler.deactivate()
        return response


class ActionLogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.method in _LOG_METHODS:
            label = request.headers.get("X-Button-Label") or _resolve_action_label(request.url.path)
            if label:
                _btn_logger.info("[%s] %s %s", label, request.method, request.url.path)
            else:
                _btn_logger.info("%s %s", request.method, request.url.path)
        return await call_next(request)


class BasicAuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, password: str):
        super().__init__(app)
        self.password = password

    async def dispatch(self, request, call_next):
        if is_public_path(request.url.path):
            return await call_next(request)

        if get_cookie_user(request):
            return await call_next(request)

        auth = request.headers.get("Authorization", "")
        if auth.startswith("Basic "):
            try:
                decoded = base64.b64decode(auth[6:]).decode()
            except Exception:
                # Invalid Basic auth should degrade to anonymous flow, not fail the request.
                decoded = ""
            _, _, pwd = decoded.partition(":")
            if secrets.compare_digest(pwd, self.password):
                response = await call_next(request)
                set_session_cookie(response, request)
                return response

        target = login_redirect_url(redirect_target_from_request(request))
        if request.headers.get("HX-Request") == "true":
            return Response(
                "Unauthorized",
                status_code=401,
                headers={"HX-Redirect": target},
            )

        accept = request.headers.get("Accept", "")
        if "text/html" in accept:
            return RedirectResponse(url=target, status_code=303)

        return Response(
            "Unauthorized",
            status_code=401,
            headers={"WWW-Authenticate": "Basic realm='TG Post Search'"},
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    container = await build_container_with_templates(
        app.state.config,
        log_buffer=app.state.log_buffer,
        timing_buffer=app.state.timing_buffer,
        templates=app.state.templates,
    )
    configure_app(app, container)
    logger.info("Application started")
    try:
        await start_container(container)
        yield
    finally:
        logger.info("Shutting down...")
        await stop_container(container)
        logger.info("Application shut down")


def create_app(config: AppConfig | None = None) -> FastAPI:
    if config is None:
        config = load_config()

    app = FastAPI(title="TG Post Search", lifespan=lifespan)
    app.state.config = config
    app.state.log_buffer = build_log_buffer()
    app.state.timing_buffer = build_timing_buffer()
    app.state.templates = configure_template_globals(
        Jinja2Templates(directory=str(TEMPLATES_DIR)),
        config,
    )
    configure_app(app, None)

    if config.web.password:
        app.add_middleware(BasicAuthMiddleware, password=config.web.password)
    app.add_middleware(OriginCSRFMiddleware)
    app.add_middleware(ActionLogMiddleware)
    app.add_middleware(TimingMiddleware)

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        logger.exception("Unhandled exception on %s %s", request.method, request.url.path)

        if request.headers.get("HX-Request") == "true":
            return HTMLResponse(
                '<div class="alert alert-danger">Внутренняя ошибка — см. /debug/</div>',
                status_code=500,
            )

        try:
            return app.state.templates.TemplateResponse(
                request,
                "error.html",
                {
                    "status_code": 500,
                    "detail": "An unexpected error occurred. See /debug/ for details.",
                },
                status_code=500,
            )
        except Exception:
            return HTMLResponse("Internal Server Error", status_code=500)

    register_builtin_endpoints(app)
    register_routes(app)
    return app
