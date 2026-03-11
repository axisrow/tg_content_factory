from __future__ import annotations

import importlib.metadata
import logging
import os
import tomllib
from pathlib import Path

from fastapi.templating import Jinja2Templates

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
        logger.warning("Failed to read package metadata version for %s", PACKAGE_NAME, exc_info=True)

    return "unknown"


def configure_template_globals(templates: Jinja2Templates) -> Jinja2Templates:
    templates.env.globals["agent_available"] = bool(
        os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
    )
    templates.env.globals["app_version"] = get_app_version()
    return templates
