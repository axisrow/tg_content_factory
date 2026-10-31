from __future__ import annotations

from pathlib import Path

from tg_agent.paths import DATA_DIR_ENV, data_root, image_dir
from tg_agent.web.paths import PROJECT_ROOT, STATIC_DIR, TEMPLATES_DIR, WEB_DIR


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
    assert (PROJECT_ROOT / "tg_agent").is_dir()


def test_data_root_defaults_to_cwd_data(tmp_path, monkeypatch):
    """Default data root is ./data relative to the working directory (#1595) —
    the same cwd convention as config.yaml and the default DB path — never a
    path inside the package (site-packages for wheel installs)."""
    monkeypatch.delenv(DATA_DIR_ENV, raising=False)
    monkeypatch.chdir(tmp_path)
    resolved = data_root()
    assert resolved == Path("data")
    assert WEB_DIR.parent.parent not in resolved.resolve().parents


def test_data_root_env_override(tmp_path, monkeypatch):
    monkeypatch.setenv(DATA_DIR_ENV, str(tmp_path / "custom-data"))
    assert data_root() == tmp_path / "custom-data"


def test_data_image_dir_follows_data_root(tmp_path, monkeypatch):
    monkeypatch.setenv(DATA_DIR_ENV, str(tmp_path / "custom-data"))
    assert image_dir() == tmp_path / "custom-data" / "image"


def test_cli_runtime_log_paths_follow_data_root(tmp_path, monkeypatch):
    """Wiring guard for #1595: app.log / TUI log hang off the runtime data root."""
    from tg_agent.cli import runtime as cli_runtime

    monkeypatch.setenv(DATA_DIR_ENV, str(tmp_path / "custom-data"))
    assert cli_runtime.app_log_path() == tmp_path / "custom-data" / "app.log"


def test_downloads_dirs_follow_data_root_env(tmp_path, monkeypatch):
    """Dispatcher and agent-tool download defaults resolve at call time (#1595)."""
    from tg_agent.agent.tools import messaging_media
    from tg_agent.services import telegram_command_dispatcher

    monkeypatch.setenv(DATA_DIR_ENV, str(tmp_path / "dl"))
    expected = tmp_path / "dl" / "downloads"
    assert telegram_command_dispatcher._downloads_dir() == expected
    assert messaging_media._downloads_dir() == expected


def test_compatibility_catalog_export_follows_data_root(tmp_path, monkeypatch):
    """Default catalog export target is under the data root, not the package (#1595)."""
    from tg_agent.services.provider_model_compatibility import (
        community_compatibility_catalog_path,
    )

    monkeypatch.setenv(DATA_DIR_ENV, str(tmp_path / "dl"))
    assert community_compatibility_catalog_path() == (
        tmp_path / "dl" / "deepagents_model_compatibility_catalog.json"
    )
