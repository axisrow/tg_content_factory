from __future__ import annotations

import importlib.metadata
import logging
import os
import tomllib

from fastapi import Request
from fastapi.templating import Jinja2Templates

from src.config import AppConfig, is_provider_model_ref
from src.web.paths import TEMPLATES_DIR

logger = logging.getLogger(__name__)

PACKAGE_NAME = "tg-user-search"
PROJECT_ROOT = TEMPLATES_DIR.parent.parent.parent
PYPROJECT_PATH = PROJECT_ROOT / "pyproject.toml"


def get_app_version() -> str:
    try:
        with PYPROJECT_PATH.open("rb") as fh:
            data = tomllib.load(fh)
        version = data["project"]["version"]
        if isinstance(version, str) and version:
            return version
    except Exception:
        logger.warning("Failed to read app version from %s", PYPROJECT_PATH, exc_info=True)

    try:
        return importlib.metadata.version(PACKAGE_NAME)
    except importlib.metadata.PackageNotFoundError:
        pass
    except Exception:
        logger.warning(
            "Failed to read package metadata version for %s",
            PACKAGE_NAME,
            exc_info=True,
        )

    return "unknown"


def _agent_available(config: AppConfig | None = None) -> bool:
    claude_available = bool(
        os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
    )
    fallback_model = ""
    if config is not None:
        fallback_model = config.agent.fallback_model
    if not fallback_model:
        fallback_model = os.environ.get("AGENT_FALLBACK_MODEL", "").strip()
    return claude_available or is_provider_model_ref(fallback_model)


def _request_agent_manager(request: Request):
    manager = getattr(request.app.state, "agent_manager", None)
    if manager is not None:
        return manager
    container = getattr(request.app.state, "container", None)
    if container is None:
        return None
    return getattr(container, "agent_manager", None)


def _agent_available_for_request(request: Request) -> bool:
    manager = _request_agent_manager(request)
    if manager is not None:
        return bool(manager.available)
    return _agent_available(getattr(request.app.state, "config", None))


def configure_template_globals(
    templates: Jinja2Templates,
    config: AppConfig | None = None,
) -> Jinja2Templates:
    templates.env.globals["agent_available"] = _agent_available_for_request
    templates.env.globals["app_version"] = get_app_version()
    return templates
