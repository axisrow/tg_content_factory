from __future__ import annotations

from src.web.paths import DATA_IMAGE_DIR, PROJECT_ROOT, STATIC_DIR, TEMPLATES_DIR, WEB_DIR


def test_web_dir_is_web_package():
    assert WEB_DIR.name == "web"
    assert WEB_DIR.is_dir()


def test_static_dir_exists():
    assert STATIC_DIR == WEB_DIR / "static"
    assert STATIC_DIR.is_dir()


def test_templates_dir_exists():
    assert TEMPLATES_DIR == WEB_DIR / "templates"
    assert TEMPLATES_DIR.is_dir()


def test_project_root():
    assert PROJECT_ROOT == WEB_DIR.parent.parent
    assert (PROJECT_ROOT / "src").is_dir()


def test_data_image_dir_path():
    assert DATA_IMAGE_DIR == PROJECT_ROOT / "data" / "image"
