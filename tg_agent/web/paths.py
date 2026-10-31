from __future__ import annotations

from pathlib import Path

# Runtime data paths moved to the core (#1720); re-exported for compatibility.
from tg_agent.paths import DATA_DIR_ENV as DATA_DIR_ENV
from tg_agent.paths import data_root as data_root
from tg_agent.paths import image_dir as image_dir

WEB_DIR = Path(__file__).parent
STATIC_DIR = WEB_DIR / "static"
TEMPLATES_DIR = WEB_DIR / "templates"
PROJECT_ROOT = WEB_DIR.parent.parent
