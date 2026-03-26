from __future__ import annotations

from pathlib import Path

WEB_DIR = Path(__file__).parent
STATIC_DIR = WEB_DIR / "static"
TEMPLATES_DIR = WEB_DIR / "templates"
PROJECT_ROOT = WEB_DIR.parent.parent
DATA_IMAGE_DIR = PROJECT_ROOT / "data" / "image"
