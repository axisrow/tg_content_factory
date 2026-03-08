from __future__ import annotations

import base64
import logging
import secrets
from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from src.config import AppConfig, load_config
from src.web.assembly import (
    build_log_buffer,
    configure_app,
    register_builtin_endpoints,
    register_routes,
)
from src.web.bootstrap import build_container, start_container, stop_container
from src.web.csrf import OriginCSRFMiddleware, is_secure_request
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

        container = getattr(request.app.state, "container", None)
        secret = getattr(container, "session_secret", None) or getattr(
            request.app.state, "session_secret", None
        )

        cookie = request.cookies.get(COOKIE_NAME)
        if cookie and secret:
            cookie_user = verify_session_token(cookie, secret)
            if cookie_user and cookie_user == self._USERNAME:
                return await call_next(request)

        auth = request.headers.get("Authorization", "")
        if auth.startswith("Basic "):
            try:
                decoded = base64.b64decode(auth[6:]).decode()
            except Exception:
                decoded = ""
            _, _, pwd = decoded.partition(":")
            if secrets.compare_digest(pwd, self.password):
                response = await call_next(request)
                if secret:
                    token = create_session_token(self._USERNAME, secret)
                    response.set_cookie(
                        COOKIE_NAME,
                        token,
                        max_age=COOKIE_MAX_AGE,
                        httponly=True,
                        samesite="lax",
                        secure=is_secure_request(request),
                    )
                return response

        return Response(
            "Unauthorized",
            status_code=401,
            headers={"WWW-Authenticate": "Basic realm='TG Post Search'"},
        )


@asynccontextmanager
async def lifespan(app: FastAPI):
    container = await build_container(app.state.config, log_buffer=app.state.log_buffer)
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
    configure_app(app, None)

    if config.web.password:
        app.add_middleware(BasicAuthMiddleware, password=config.web.password)
    app.add_middleware(OriginCSRFMiddleware)

    register_builtin_endpoints(app)
    register_routes(app)
    return app
