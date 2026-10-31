# Web-panel tests split out of tests/test_web_paths.py (#1720 wave 3).
from __future__ import annotations

from tg_webui.paths import STATIC_DIR, TEMPLATES_DIR, WEB_DIR


def test_web_dir_is_web_package():
    assert WEB_DIR.name == "tg_webui"
    assert WEB_DIR.is_dir()


def test_static_dir_exists():
    assert STATIC_DIR == WEB_DIR / "static"
    assert STATIC_DIR.is_dir()


def test_templates_dir_exists():
    assert TEMPLATES_DIR == WEB_DIR / "templates"
    assert TEMPLATES_DIR.is_dir()
