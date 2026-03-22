from __future__ import annotations

import base64
import logging
import secrets
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import HTMLResponse, RedirectResponse, Response

from src.config import AppConfig, load_config
from src.web.assembly import (
    build_log_buffer,
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


_action_logger = logging.getLogger("src.web.actions")
_LOG_METHODS = {"POST", "PUT", "PATCH", "DELETE"}


class ActionLogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.method in _LOG_METHODS:
            _action_logger.info("%s %s", request.method, request.url.path)
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
    app.state.templates = configure_template_globals(
        Jinja2Templates(directory=str(TEMPLATES_DIR)),
        config,
    )
    configure_app(app, None)

    if config.web.password:
        app.add_middleware(BasicAuthMiddleware, password=config.web.password)
    app.add_middleware(OriginCSRFMiddleware)
    app.add_middleware(ActionLogMiddleware)

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
