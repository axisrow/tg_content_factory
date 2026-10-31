from __future__ import annotations

import os
from pathlib import Path

WEB_DIR = Path(__file__).parent
STATIC_DIR = WEB_DIR / "static"
TEMPLATES_DIR = WEB_DIR / "templates"
PROJECT_ROOT = WEB_DIR.parent.parent

#: Env override for the runtime data root (see :func:`data_root`).
DATA_DIR_ENV = "TG_AGENT_DATA_DIR"


def data_root() -> Path:
    """Runtime data root for write paths: logs, downloads, generated images (#1595).

    Must never resolve inside the package directory — for a wheel install that
    is site-packages, which may be read-only. Defaults to ``./data`` (cwd),
    the same working-directory convention as config.yaml and the default DB
    path ``data/tg_search.db``; ``TG_AGENT_DATA_DIR`` overrides. Read at call
    time so the override works both from the process env and from ``.env``
    (loaded after module import by CLI startup).
    """
    override = os.environ.get(DATA_DIR_ENV)
    return Path(override).expanduser() if override else Path("data")


def image_dir() -> Path:
    """Generated images: written by the image tool, served at ``/data/image``."""
    return data_root() / "image"
