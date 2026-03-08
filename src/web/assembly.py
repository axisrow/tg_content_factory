from __future__ import annotations

import logging
import secrets

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.web.container import AppContainer
from src.web.panel_auth import get_cookie_user, sanitize_next, set_session_cookie
from src.web.paths import STATIC_DIR, TEMPLATES_DIR
from src.web.session import COOKIE_NAME


def configure_app(app: FastAPI, container: AppContainer | None) -> None:
    if container is not None:
        app.state.container = container
        app.state.templates = container.templates
        # Expose frequently accessed attributes for backward compat with
        # code that reads app.state.<attr> directly (routes, tests).
        app.state.db = container.db
        app.state.auth = container.auth
        app.state.pool = container.pool
        app.state.collector = container.collector
        app.state.search_engine = container.search_engine
        app.state.ai_search = container.ai_search
        app.state.scheduler = container.scheduler
        app.state.session_secret = container.session_secret
    elif not hasattr(app.state, "templates"):
        app.state.templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    if not hasattr(app.state, "session_secret"):
        app.state.session_secret = secrets.token_hex(32)
    if STATIC_DIR.exists():
        mount_names = {route.name for route in app.routes}
        if "static" not in mount_names:
            app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def register_builtin_endpoints(app: FastAPI) -> None:
    @app.get("/health")
    async def health_check(request: Request):
        container = getattr(request.app.state, "container", None)
        if container is None:
            from src.web import deps

            container = deps.get_container(request)
        db_ok = False
        try:
            await container.db.execute("SELECT 1")
            db_ok = True
        except Exception:
            pass
        accounts_connected = len(container.pool.clients)
        status = "healthy" if db_ok else "degraded"
        return JSONResponse(
            {"status": status, "db": db_ok, "accounts_connected": accounts_connected}
        )

    @app.get("/login", response_class=HTMLResponse)
    async def login_page(request: Request, next: str = "/"):
        target = sanitize_next(next)
        if not request.app.state.config.web.password or get_cookie_user(request):
            return RedirectResponse(url=target, status_code=303)
        return request.app.state.templates.TemplateResponse(
            request,
            "web_login.html",
            {"error": None, "next": target},
        )

    @app.post("/login", response_class=HTMLResponse)
    async def login_submit(request: Request, password: str = Form(...), next: str = Form("/")):
        target = sanitize_next(next)
        expected_password = request.app.state.config.web.password
        if expected_password and secrets.compare_digest(password, expected_password):
            response = RedirectResponse(url=target, status_code=303)
            set_session_cookie(response, request)
            return response
        return request.app.state.templates.TemplateResponse(
            request,
            "web_login.html",
            {"error": "Неверный пароль", "next": target},
            status_code=401,  # RFC 7235 / Starlette convention for failed auth
        )

    @app.get("/logout")
    async def logout():
        response = RedirectResponse(url="/login", status_code=303)
        response.delete_cookie(COOKIE_NAME)
        return response


def register_routes(app: FastAPI) -> None:
    from src.web.routes.auth import router as auth_router
    from src.web.routes.channel_collection import router as channel_collection_router
    from src.web.routes.channels import router as channels_router
    from src.web.routes.dashboard import router as dashboard_router
    from src.web.routes.debug import router as debug_router
    from src.web.routes.filter import router as filter_router
    from src.web.routes.import_channels import router as import_router
    from src.web.routes.my_telegram import router as my_telegram_router
    from src.web.routes.scheduler import router as scheduler_router
    from src.web.routes.search import router as search_router
    from src.web.routes.search_queries import router as search_queries_router
    from src.web.routes.settings import router as settings_router

    app.include_router(search_router)
    app.include_router(dashboard_router, prefix="/dashboard")
    app.include_router(auth_router, prefix="/auth")
    app.include_router(channels_router, prefix="/channels")
    app.include_router(filter_router, prefix="/channels")
    app.include_router(search_queries_router, prefix="/search-queries")
    app.include_router(channel_collection_router, prefix="/channels")
    app.include_router(import_router, prefix="/channels")
    app.include_router(scheduler_router, prefix="/scheduler")
    app.include_router(settings_router, prefix="/settings")
    app.include_router(my_telegram_router, prefix="/my-telegram")
    app.include_router(debug_router, prefix="/debug")


def build_log_buffer() -> logging.Handler:
    from src.web.log_handler import LogBuffer

    log_buffer = LogBuffer(maxlen=500)
    log_buffer.setFormatter(logging.Formatter("%(message)s"))
    logging.getLogger().addHandler(log_buffer)
    return log_buffer
